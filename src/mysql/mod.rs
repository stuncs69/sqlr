// MySQL protocol implementation for SQLR
//
// This module implements the MySQL wire protocol to allow MySQL clients
// to connect to SQLR as if it were a MySQL server.

pub mod auth;
pub mod error;
pub mod packet;
pub mod protocol;
pub mod server;
