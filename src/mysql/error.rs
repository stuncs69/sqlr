use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MySQLError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("SQL execution error: {0}")]
    Execution(String),

    #[error("Unsupported feature: {0}")]
    Unsupported(String),
}

pub enum MySQLErrorCode {
    AccessDenied = 1045,
    SyntaxError = 1064,
    UnknownTable = 1051,
    DuplicateTable = 1050,
    ServerShutdown = 1053,
    ParseError = 1065,
    ExecutionError = 3100,
}

impl MySQLErrorCode {
    pub fn name(&self) -> &'static str {
        match self {
            MySQLErrorCode::AccessDenied => "ACCESS_DENIED",
            MySQLErrorCode::SyntaxError => "SYNTAX_ERROR",
            MySQLErrorCode::UnknownTable => "UNKNOWN_TABLE",
            MySQLErrorCode::DuplicateTable => "DUPLICATE_TABLE",
            MySQLErrorCode::ServerShutdown => "SERVER_SHUTDOWN",
            MySQLErrorCode::ParseError => "PARSE_ERROR",
            MySQLErrorCode::ExecutionError => "EXECUTION_ERROR",
        }
    }
}

pub type Result<T> = std::result::Result<T, MySQLError>;
