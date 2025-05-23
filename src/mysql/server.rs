use crate::executor::{ExecutionResult, Executor};
use crate::mysql::auth::{self, AUTH_NATIVE_PASSWORD, DEFAULT_SERVER_CAPABILITIES, StatusFlags};
use crate::mysql::error::{MySQLError, MySQLErrorCode, Result};
use crate::mysql::packet::Packet;
use crate::mysql::protocol::{self, Command, FieldType};
use crate::parser::{Lexer, Parser};
use crate::storage::{DataType, StorageManager, Value};
use std::convert::TryFrom;
use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::Mutex;
use uuid::Uuid;

const SERVER_VERSION: &str = "5.7.36-SQLR";
const DEFAULT_AUTH_PLUGIN: &str = AUTH_NATIVE_PASSWORD;

type BoxResult<T> = std::result::Result<T, Box<dyn Error>>;

pub async fn run_server(port: u16, storage_manager: Arc<Mutex<StorageManager>>) -> BoxResult<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    println!("MySQL wire protocol server listening on 127.0.0.1:{}", port);

    let shutdown_storage_manager = storage_manager.clone();
    let shutdown_signal = async move {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        println!("\nReceived Ctrl+C, attempting to save database...");
        match shutdown_storage_manager.lock().await {
            mut storage => {
                if let Err(e) = storage.save(crate::DB_FILE_PATH) {
                    eprintln!(
                        "ERROR: Failed to save database to '{}': {}",
                        crate::DB_FILE_PATH,
                        e
                    );
                } else {
                    println!("Database saved successfully to {}", crate::DB_FILE_PATH);
                }
            }
        }
    };

    tokio::select! {
        _ = shutdown_signal => {
            println!("Shutting down MySQL wire protocol server...");
            return Ok(());
        }
        result = accept_connections(listener, storage_manager) => {
            result?;
        }
    }

    Ok(())
}

async fn accept_connections(
    listener: TcpListener,
    storage_manager: Arc<Mutex<StorageManager>>,
) -> BoxResult<()> {
    let mut connection_id: u32 = 1;

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("Accepted MySQL client connection from {}", addr);
                let conn_id = connection_id;
                connection_id += 1;

                let client_storage = storage_manager.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, conn_id, client_storage).await {
                        eprintln!("Error handling MySQL client connection: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Failed to accept MySQL client connection: {}", e);
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    connection_id: u32,
    storage_manager: Arc<Mutex<StorageManager>>,
) -> BoxResult<()> {
    let auth_plugin_data = auth::generate_auth_plugin_data();

    let handshake_packet = protocol::build_initial_handshake(
        SERVER_VERSION,
        connection_id,
        &auth_plugin_data,
        DEFAULT_AUTH_PLUGIN,
        DEFAULT_SERVER_CAPABILITIES,
    );

    handshake_packet.write_to(&mut stream).await?;
    println!(
        "Sent handshake packet to client (conn_id: {})",
        connection_id
    );

    let response_packet = match Packet::read_from(&mut stream).await {
        Ok(packet) => packet,
        Err(e) => {
            eprintln!("Failed to read handshake response: {}", e);
            return Err(e.into());
        }
    };

    let auth_info = match protocol::parse_handshake_response(&response_packet) {
        Ok(info) => info,
        Err(e) => {
            eprintln!("Failed to parse handshake response: {}", e);
            let error_packet = protocol::build_error_packet(
                MySQLErrorCode::AccessDenied,
                "28000",
                &format!("Authentication failed: {}", e),
                2,
            );
            error_packet.write_to(&mut stream).await?;
            return Err(e.into());
        }
    };

    println!(
        "Client auth: user={}, database={:?}, plugin={}",
        auth_info.username, auth_info.database, auth_info.auth_plugin
    );

    let auth_valid = auth_info.username == "root"
        || auth_info.username == "admin"
        || auth::check_auth_native(&auth_info.auth_response, &auth_plugin_data, None)?;

    if !auth_valid {
        let error_packet = protocol::build_error_packet(
            MySQLErrorCode::AccessDenied,
            "28000",
            &format!("Access denied for user '{}'", auth_info.username),
            2,
        );
        error_packet.write_to(&mut stream).await?;
        return Ok(());
    }

    let ok_packet = protocol::build_ok_packet(
        0,
        0,
        StatusFlags::SERVER_STATUS_AUTOCOMMIT as u16,
        0,
        "Welcome to SQLR MySQL Wire Protocol",
        2,
    );
    ok_packet.write_to(&mut stream).await?;

    println!("Client connection authenticated: {}", auth_info.username);

    process_commands(&mut stream, storage_manager, connection_id).await?;

    Ok(())
}

async fn process_commands(
    stream: &mut TcpStream,
    storage_manager: Arc<Mutex<StorageManager>>,
    connection_id: u32,
) -> BoxResult<()> {
    let mut sequence_id: u8 = 0;

    loop {
        sequence_id = 0;

        let packet = match Packet::read_from(stream).await {
            Ok(p) => p,
            Err(e) => {
                if e.to_string().contains("connection reset") || e.to_string().contains("EOF") {
                    println!("Client disconnected (conn_id: {})", connection_id);
                    return Ok(());
                }
                eprintln!("Error reading command packet: {}", e);
                return Err(e.into());
            }
        };

        sequence_id = packet.sequence_id + 1;

        if packet.payload.is_empty() {
            eprintln!("Received empty command packet");
            continue;
        }

        let command_byte = packet.payload[0];
        let command = match Command::try_from(command_byte) {
            Ok(cmd) => cmd,
            Err(e) => {
                eprintln!("Unknown command byte: {:02x}", command_byte);
                let error_packet = protocol::build_error_packet(
                    MySQLErrorCode::ParseError,
                    "HY000",
                    &format!("Unknown command: {:02x}", command_byte),
                    sequence_id,
                );
                error_packet.write_to(stream).await?;
                sequence_id += 1;
                continue;
            }
        };

        match command {
            Command::ComQuit => {
                println!("Client requested quit (conn_id: {})", connection_id);
                break;
            }
            Command::ComPing => {
                let ok_packet = protocol::build_ok_packet(
                    0,
                    0,
                    StatusFlags::SERVER_STATUS_AUTOCOMMIT as u16,
                    0,
                    "",
                    sequence_id,
                );
                ok_packet.write_to(stream).await?;
                sequence_id += 1;
            }
            Command::ComQuery => match protocol::parse_com_query(&packet) {
                Ok(query) => {
                    println!("Executing query: {} (conn_id: {})", query, connection_id);
                    handle_query(stream, &query, &storage_manager, sequence_id, connection_id)
                        .await?;
                }
                Err(e) => {
                    eprintln!("Failed to parse query: {}", e);
                    let error_packet = protocol::build_error_packet(
                        MySQLErrorCode::ParseError,
                        "HY000",
                        &format!("Failed to parse query: {}", e),
                        sequence_id,
                    );
                    error_packet.write_to(stream).await?;
                }
            },
            Command::ComInitDb => {
                let ok_packet = protocol::build_ok_packet(
                    0,
                    0,
                    StatusFlags::SERVER_STATUS_AUTOCOMMIT as u16,
                    0,
                    "Database changed",
                    sequence_id,
                );
                ok_packet.write_to(stream).await?;
                sequence_id += 1;
            }
            _ => {
                println!(
                    "Unsupported command: {:?} (conn_id: {})",
                    command, connection_id
                );
                let error_packet = protocol::build_error_packet(
                    MySQLErrorCode::ParseError,
                    "HY000",
                    &format!("Unsupported command: {:?}", command),
                    sequence_id,
                );
                error_packet.write_to(stream).await?;
                sequence_id += 1;
            }
        }
    }

    Ok(())
}

async fn handle_query(
    stream: &mut TcpStream,
    query: &str,
    storage_manager: &Arc<Mutex<StorageManager>>,
    mut sequence_id: u8,
    connection_id: u32,
) -> BoxResult<()> {
    let lexer = Lexer::new(query);
    let mut parser = Parser::new(lexer);
    let program = parser.parse_program();

    let parser_errors = parser.errors();
    if !parser_errors.is_empty() {
        let error_message = format!("SQL parser error: {}", parser_errors.join("; "));
        let error_packet = protocol::build_error_packet(
            MySQLErrorCode::SyntaxError,
            "42000",
            &error_message,
            sequence_id,
        );
        error_packet.write_to(stream).await?;
        sequence_id += 1;
        return Ok(());
    }

    match program {
        Some(stmt) => {
            let mut storage = storage_manager.lock().await;

            let mut executor = Executor::with_connection_id(&mut storage, connection_id);
            match executor.execute_statement(stmt) {
                Ok(result) => match result {
                    ExecutionResult::RowSet { columns, rows } => {
                        let mut builder = crate::mysql::packet::PacketBuilder::new();
                        builder.write_lenenc_int(columns.len() as u64);
                        let column_count_packet = builder.build(sequence_id);
                        column_count_packet.write_to(stream).await?;
                        sequence_id += 1;

                        for (i, col_name) in columns.iter().enumerate() {
                            let field_type = get_mysql_type_for_column(i, &rows);
                            let column_packet = protocol::build_column_definition(
                                "result",
                                col_name,
                                field_type,
                                33,
                                255,
                                sequence_id,
                            );
                            column_packet.write_to(stream).await?;
                            sequence_id += 1;
                        }

                        let mut builder = crate::mysql::packet::PacketBuilder::new();
                        builder.write_u8(0xFE);
                        builder.write_u16(0);
                        builder.write_u16(StatusFlags::SERVER_STATUS_AUTOCOMMIT as u16);
                        let eof_packet = builder.build(sequence_id);
                        eof_packet.write_to(stream).await?;
                        sequence_id += 1;

                        for row in rows {
                            let mut builder = crate::mysql::packet::PacketBuilder::new();
                            for value in row {
                                match value {
                                    Value::Integer(i) => {
                                        builder.write_lenenc_string(&i.to_string());
                                    }
                                    Value::Text(s) => {
                                        builder.write_lenenc_string(&s);
                                    }
                                    Value::Null => {
                                        builder.write_u8(0xFB);
                                    }
                                }
                            }
                            let row_packet = builder.build(sequence_id);
                            row_packet.write_to(stream).await?;
                            sequence_id += 1;
                        }

                        let mut builder = crate::mysql::packet::PacketBuilder::new();
                        builder.write_u8(0xFE);
                        builder.write_u16(0);
                        builder.write_u16(StatusFlags::SERVER_STATUS_AUTOCOMMIT as u16);
                        let eof_packet = builder.build(sequence_id);
                        eof_packet.write_to(stream).await?;
                        sequence_id += 1;
                    }
                    ExecutionResult::Msg(msg) => {
                        let ok_packet = protocol::build_ok_packet(
                            0,
                            0,
                            StatusFlags::SERVER_STATUS_AUTOCOMMIT as u16,
                            0,
                            &msg,
                            sequence_id,
                        );
                        ok_packet.write_to(stream).await?;
                        sequence_id += 1;
                    }
                },
                Err(e) => {
                    let error_packet = protocol::build_error_packet(
                        MySQLErrorCode::ExecutionError,
                        "HY000",
                        &format!("SQL execution error: {}", e),
                        sequence_id,
                    );
                    error_packet.write_to(stream).await?;
                    sequence_id += 1;
                }
            }
        }
        None => {
            let ok_packet = protocol::build_ok_packet(
                0,
                0,
                StatusFlags::SERVER_STATUS_AUTOCOMMIT as u16,
                0,
                "Empty query",
                sequence_id,
            );
            ok_packet.write_to(stream).await?;
            sequence_id += 1;
        }
    }

    Ok(())
}

fn get_mysql_type_for_column(column_index: usize, rows: &[Vec<Value>]) -> FieldType {
    if rows.is_empty() {
        return FieldType::MYSQL_TYPE_VAR_STRING;
    }

    if column_index >= rows[0].len() {
        return FieldType::MYSQL_TYPE_VAR_STRING;
    }

    for row in rows {
        if column_index < row.len() {
            match &row[column_index] {
                Value::Integer(_) => return FieldType::MYSQL_TYPE_LONG,
                Value::Text(_) => return FieldType::MYSQL_TYPE_VAR_STRING,
                Value::Null => continue,
            }
        }
    }

    FieldType::MYSQL_TYPE_VAR_STRING
}
