use std::error::Error;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use axum::{Router, extract::State, http::StatusCode, response::Json, routing::post};
use clap::Parser as ClapParser;
use serde::Deserialize;
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tower_http::services::ServeDir;

mod executor;
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

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    if args.test_parser {
        run_parser_repl();
        Ok(())
    } else if args.web_ui {
        run_web_ui()
    } else {
        run_tcp_server()
    }
}

#[tokio::main]
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
        match shutdown_storage_manager.lock() {
            Ok(storage) => {
                if let Err(e) = storage.save(DB_FILE_PATH) {
                    eprintln!(
                        "ERROR: Failed to save database to '{}': {}",
                        DB_FILE_PATH, e
                    );
                } else {
                    println!("Database saved successfully to {}", DB_FILE_PATH);
                }
            }
            Err(e) => {
                eprintln!("ERROR: Could not acquire storage lock for saving: {}", e);
            }
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

    let mut storage_guard = match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            eprintln!("Storage lock poisoned: {}", poisoned);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error: Cannot acquire storage lock".to_string(),
            ));
        }
    };

    let mut executor = Executor::new(&mut *storage_guard);

    match executor.execute_statement(statement) {
        Ok(result) => Ok(Json(json!({
            "error": false,
            "message": "Execution successful",
            "result": result
        }))),
        Err(exec_err) => Ok(Json(
            json!({ "error": true, "message": "Execution Error", "details": exec_err }),
        )),
    }
}

#[tokio::main]
async fn run_tcp_server() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:3306").await?;
    println!("SQLR TCP server listening on 127.0.0.1:3306");

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("Accepted connection from: {}", addr);

        tokio::spawn(async move {
            handle_connection(socket).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream) {
    println!("Handling connection for {}", stream.peer_addr().unwrap());

    if let Err(e) = stream
        .write_all(b"Hello from SQLR! (Raw TCP) Closing connection.\n")
        .await
    {
        eprintln!("Failed to write to socket: {}", e);
    }

    if let Err(e) = stream.shutdown().await {
        eprintln!("Failed to shutdown socket: {}", e);
    }
    println!("Connection closed for {}", stream.peer_addr().unwrap());
}
