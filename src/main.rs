use std::error::Error;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use axum::{Router, extract::State, http::StatusCode, response::Json, routing::post};
use clap::Parser as ClapParser;
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::Mutex;
use tower_http::services::ServeDir;

mod executor;
mod mysql;
mod parser;
mod storage;

use executor::{ExecutionResult, Executor};
use parser::{Lexer, Parser};
use storage::StorageManager;

#[derive(ClapParser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    test_parser: bool,

    #[arg(long)]
    web_ui: bool,

    #[arg(long)]
    mysql_compat: bool,

    #[arg(long, default_value = "3306")]
    mysql_port: u16,
}

type AppState = Arc<Mutex<StorageManager>>;

const DB_FILE_PATH: &str = "sqlr_data.db";

fn run_parser_repl() {
    println!("SQLR Parser/Executor Test REPL. Enter SQL statements or type 'exit'.");

    let mut storage = match StorageManager::load(DB_FILE_PATH) {
        Ok(s) => {
            println!("Loaded existing data for REPL from {}", DB_FILE_PATH);
            s
        }
        Err(e) => {
            eprintln!(
                "WARN: Failed to load REPL data ('{}'), starting fresh: {}",
                DB_FILE_PATH, e
            );
            StorageManager::new()
        }
    };

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(0) => break,
            Ok(_) => {
                let trimmed_input = input.trim();
                if trimmed_input.eq_ignore_ascii_case("exit") {
                    break;
                }
                if trimmed_input.is_empty() {
                    continue;
                }

                if trimmed_input.eq_ignore_ascii_case("health") {
                    let health = storage.get_health_metrics();
                    println!("Health Status: healthy");
                    println!("  Uptime: {} seconds", health.uptime_seconds);
                    println!("  Total Tables: {}", health.total_tables);
                    println!("  Total Rows: {}", health.total_rows);
                    println!("  Queries Executed: {}", health.queries_executed);
                    if let Some(time) = health.last_query_time_ms {
                        println!("  Last Query Time: {} ms", time);
                    }
                    println!("  Version: SQLR 0.1.0");
                    continue;
                }

                let start = std::time::Instant::now();

                let lexer = Lexer::new(trimmed_input);
                let mut parser = Parser::new(lexer);
                let program = parser.parse_program();

                let errors = parser.errors();
                if !errors.is_empty() {
                    println!("Parser Errors:");
                    for error in errors {
                        println!("  {}", error);
                    }
                } else {
                    match program {
                        Some(stmt) => {
                            let mut executor = Executor::new(&mut storage);
                            match executor.execute_statement(stmt) {
                                Ok(result) => {
                                    let duration_ms = start.elapsed().as_millis() as u64;
                                    storage.record_query_execution(duration_ms);

                                    println!("Execution Result:");

                                    match result {
                                        ExecutionResult::RowSet { columns, rows } => {
                                            println!("Columns: {:?}", columns);
                                            println!("Rows:");
                                            for row in rows {
                                                println!("  {:?}", row);
                                            }
                                        }
                                        ExecutionResult::Msg(msg) => {
                                            println!("{}", msg);
                                        }
                                    }
                                    println!("Execution time: {} ms", duration_ms);
                                }
                                Err(exec_err) => {
                                    println!("Execution Error: {}", exec_err);
                                }
                            }
                        }
                        None => {
                            println!("Parsed successfully, but no statement was generated.");
                        }
                    }
                }
            }
            Err(error) => {
                println!("Error reading input: {}", error);
                break;
            }
        }
    }

    match storage.save(DB_FILE_PATH) {
        Ok(_) => println!("REPL data saved to {}", DB_FILE_PATH),
        Err(e) => eprintln!(
            "ERROR: Failed to save REPL data to '{}': {}",
            DB_FILE_PATH, e
        ),
    }
    println!("Exiting parser test mode.");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    if args.test_parser {
        run_parser_repl();
        Ok(())
    } else if args.web_ui {
        run_web_ui().await
    } else if args.mysql_compat {
        run_mysql_server(args.mysql_port).await
    } else {
        run_tcp_server().await
    }
}

async fn run_web_ui() -> Result<(), Box<dyn Error>> {
    let initial_storage = match StorageManager::load(DB_FILE_PATH) {
        Ok(s) => {
            println!("Loaded database state from {}", DB_FILE_PATH);
            s
        }
        Err(e) => {
            eprintln!(
                "WARN: Failed to load database state ('{}'), starting fresh: {}",
                DB_FILE_PATH, e
            );

            StorageManager::new()
        }
    };
    let storage_manager = Arc::new(Mutex::new(initial_storage));

    let app = Router::new()
        .route("/api/query", post(handle_api_query))
        .route("/api/health", axum::routing::get(handle_health_check))
        .nest_service("/", ServeDir::new("web"))
        .with_state(storage_manager.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    println!("Web UI listening on http://127.0.0.1:8080");

    let shutdown_storage_manager = storage_manager.clone();
    let shutdown_signal = async move {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        println!("\nReceived Ctrl+C, attempting to save database...");
        let mut storage = shutdown_storage_manager.lock().await;
        if let Err(e) = storage.save(DB_FILE_PATH) {
            eprintln!(
                "ERROR: Failed to save database to '{}': {}",
                DB_FILE_PATH, e
            );
        } else {
            println!("Database saved successfully to {}", DB_FILE_PATH);
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await?;

    Ok(())
}

#[derive(Deserialize)]
struct QueryPayload {
    query: String,
}

async fn handle_api_query(
    State(state): State<AppState>,
    Json(payload): Json<QueryPayload>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let sql = payload.query.trim();
    if sql.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Query cannot be empty".to_string()));
    }

    let start = std::time::Instant::now();

    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let program = parser.parse_program();

    let errors = parser.errors();
    if !errors.is_empty() {
        return Ok(Json(
            json!({ "error": true, "message": "Parser Error(s)", "details": errors }),
        ));
    }

    let statement = match program {
        Some(stmt) => stmt,
        None => {
            return Ok(Json(
                json!({ "error": false, "message": "Parsed successfully, but no statement was generated.", "result": null }),
            ));
        }
    };

    let mut storage_guard = match state.lock().await {
        storage => storage,
    };

    let mut executor = Executor::new(&mut *storage_guard);

    let result = match executor.execute_statement(statement) {
        Ok(result) => {
            let duration_ms = start.elapsed().as_millis() as u64;
            storage_guard.record_query_execution(duration_ms);
            Ok(Json(json!({
                "error": false,
                "message": "Execution successful",
                "result": result,
                "execution_time_ms": duration_ms
            })))
        }
        Err(exec_err) => Ok(Json(
            json!({ "error": true, "message": "Execution Error", "details": exec_err }),
        )),
    };

    result
}

async fn handle_health_check(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let storage_guard = state.lock().await;
    let health = storage_guard.get_health_metrics();

    Ok(Json(json!({
        "status": "healthy",
        "uptime_seconds": health.uptime_seconds,
        "total_tables": health.total_tables,
        "total_rows": health.total_rows,
        "queries_executed": health.queries_executed,
        "last_query_time_ms": health.last_query_time_ms,
        "version": "SQLR 0.1.0"
    })))
}

async fn run_tcp_server() -> Result<(), Box<dyn Error>> {
    println!("Starting SQLR TCP Server...");

    let initial_storage = match StorageManager::load(DB_FILE_PATH) {
        Ok(s) => {
            println!("Loaded database state from {}", DB_FILE_PATH);
            s
        }
        Err(e) => {
            eprintln!(
                "WARN: Failed to load database state ('{}'), starting fresh: {}",
                DB_FILE_PATH, e
            );
            StorageManager::new()
        }
    };
    let storage_manager = Arc::new(Mutex::new(initial_storage));
    let app_state: AppState = storage_manager.clone();

    let listener = TcpListener::bind("127.0.0.1:7878").await?;
    println!("TCP Server listening on 127.0.0.1:7878");

    let shutdown_storage_manager = storage_manager.clone();
    let shutdown_signal_task = tokio::spawn(async move {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        println!("\nReceived Ctrl+C, attempting to save database...");
        let mut storage = shutdown_storage_manager.lock().await;
        if let Err(e) = storage.save(DB_FILE_PATH) {
            eprintln!(
                "ERROR: Failed to save database to '{}': {}",
                DB_FILE_PATH, e
            );
        } else {
            println!("Database saved successfully to {}", DB_FILE_PATH);
        }
    });

    loop {
        tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok((stream, addr)) => {
                        println!("Accepted connection from: {}", addr);
                        let state_clone = app_state.clone();
                        tokio::spawn(async move {
                            handle_connection(stream, state_clone).await;
                        });
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                    }
                }
            }
            _ = signal::ctrl_c() => {
                println!("Ctrl+C received again, starting shutdown sequence immediately.");
                break;
            }
        }
    }

    let _ = shutdown_signal_task.await;
    println!("TCP Server shut down gracefully.");

    Ok(())
}

async fn handle_connection(mut stream: TcpStream, state: AppState) {
    let peer_addr = stream
        .peer_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "unknown peer".to_string());
    println!("Handling connection from {}", peer_addr);

    if let Err(e) = stream.write_all(b"SQLR 0.1.0\n").await {
        eprintln!("[{}] Failed to write welcome message: {}", peer_addr, e);
        return;
    }

    let mut reader = BufReader::new(&mut stream);
    let mut line_buf = String::new();

    loop {
        line_buf.clear();
        match reader.read_line(&mut line_buf).await {
            Ok(0) => {
                println!("[{}] Connection closed by client.", peer_addr);
                break;
            }
            Ok(_) => {
                let sql = line_buf.trim();
                if sql.is_empty() {
                    continue;
                }

                let response_json = process_query(sql, &state).await;

                match serde_json::to_string(&response_json) {
                    Ok(json_string) => {
                        let response_line = format!("{}\n", json_string);
                        if let Err(e) = reader.get_mut().write_all(response_line.as_bytes()).await {
                            eprintln!("[{}] Failed to write response: {}", peer_addr, e);
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("[{}] Failed to serialize response: {}", peer_addr, e);
                        let error_msg = "{\"error\":true, \"message\":\"Internal server error: Failed to serialize response\"}\n";
                        if let Err(e_write) = reader.get_mut().write_all(error_msg.as_bytes()).await
                        {
                            eprintln!(
                                "[{}] Failed to write serialization error message: {}",
                                peer_addr, e_write
                            );
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[{}] Error reading from socket: {}", peer_addr, e);
                break;
            }
        }
    }

    println!("[{}] Connection handler finished.", peer_addr);
}

async fn process_query(sql: &str, state: &AppState) -> serde_json::Value {
    if sql.eq_ignore_ascii_case("HEALTH") {
        let storage_guard = state.lock().await;
        let health = storage_guard.get_health_metrics();
        return json!({
            "error": false,
            "message": "Health check successful",
            "result": {
                "status": "healthy",
                "uptime_seconds": health.uptime_seconds,
                "total_tables": health.total_tables,
                "total_rows": health.total_rows,
                "queries_executed": health.queries_executed,
                "last_query_time_ms": health.last_query_time_ms,
                "version": "SQLR 0.1.0"
            }
        });
    }

    let start = std::time::Instant::now();

    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let program = parser.parse_program();

    let errors = parser.errors();
    if !errors.is_empty() {
        return json!({
            "error": true,
            "message": "Parser Error(s)",
            "details": errors
        });
    }

    let statement = match program {
        Some(stmt) => stmt,
        None => {
            return json!({
                "error": false,
                "message": "Parsed successfully, but no statement was generated.",
                "result": null
            });
        }
    };

    let mut storage_guard = match state.lock().await {
        storage => storage,
    };

    let mut executor = Executor::new(&mut *storage_guard);

    match executor.execute_statement(statement) {
        Ok(result) => {
            let duration_ms = start.elapsed().as_millis() as u64;
            storage_guard.record_query_execution(duration_ms);
            json!({
                "error": false,
                "message": "Execution successful",
                "result": result,
                "execution_time_ms": duration_ms
            })
        }
        Err(exec_err) => json!({
            "error": true,
            "message": "Execution Error",
            "details": exec_err.to_string()
        }),
    }
}

async fn run_mysql_server(port: u16) -> Result<(), Box<dyn Error>> {
    let initial_storage = match StorageManager::load(DB_FILE_PATH) {
        Ok(s) => {
            println!("Loaded database state from {}", DB_FILE_PATH);
            s
        }
        Err(e) => {
            eprintln!(
                "WARN: Failed to load database state ('{}'), starting fresh: {}",
                DB_FILE_PATH, e
            );
            StorageManager::new()
        }
    };
    let storage_manager = Arc::new(Mutex::new(initial_storage));

    println!("Starting MySQL protocol compatible server on port {}", port);
    mysql::server::run_server(port, storage_manager).await
}
