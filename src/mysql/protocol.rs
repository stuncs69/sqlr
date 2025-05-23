use crate::mysql::auth::{AuthInfo, StatusFlags};
use crate::mysql::error::{MySQLError, MySQLErrorCode, Result};
use crate::mysql::packet::{Packet, PacketBuilder, PacketReader};
use bytes::Bytes;
use std::convert::TryFrom;

#[derive(Debug)]
pub enum Command {
    ComQuit = 0x01,
    ComInitDb = 0x02,
    ComQuery = 0x03,
    ComFieldList = 0x04,
    ComCreateDb = 0x05,
    ComDropDb = 0x06,
    ComRefresh = 0x07,
    ComShutdown = 0x08,
    ComStatistics = 0x09,
    ComProcessInfo = 0x0a,
    ComConnect = 0x0b,
    ComProcessKill = 0x0c,
    ComDebug = 0x0d,
    ComPing = 0x0e,
    ComTime = 0x0f,
    ComDelayedInsert = 0x10,
    ComChangeUser = 0x11,
    ComBinlogDump = 0x12,
    ComTableDump = 0x13,
    ComConnectOut = 0x14,
    ComRegisterSlave = 0x15,
    ComStmtPrepare = 0x16,
    ComStmtExecute = 0x17,
    ComStmtSendLongData = 0x18,
    ComStmtClose = 0x19,
    ComStmtReset = 0x1a,
    ComSetOption = 0x1b,
    ComStmtFetch = 0x1c,
    ComDaemon = 0x1d,
    ComBinlogDumpGtid = 0x1e,
    ComResetConnection = 0x1f,
}

impl TryFrom<u8> for Command {
    type Error = MySQLError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x01 => Ok(Command::ComQuit),
            0x02 => Ok(Command::ComInitDb),
            0x03 => Ok(Command::ComQuery),
            0x04 => Ok(Command::ComFieldList),
            0x05 => Ok(Command::ComCreateDb),
            0x06 => Ok(Command::ComDropDb),
            0x07 => Ok(Command::ComRefresh),
            0x08 => Ok(Command::ComShutdown),
            0x09 => Ok(Command::ComStatistics),
            0x0a => Ok(Command::ComProcessInfo),
            0x0b => Ok(Command::ComConnect),
            0x0c => Ok(Command::ComProcessKill),
            0x0d => Ok(Command::ComDebug),
            0x0e => Ok(Command::ComPing),
            0x0f => Ok(Command::ComTime),
            0x10 => Ok(Command::ComDelayedInsert),
            0x11 => Ok(Command::ComChangeUser),
            0x12 => Ok(Command::ComBinlogDump),
            0x13 => Ok(Command::ComTableDump),
            0x14 => Ok(Command::ComConnectOut),
            0x15 => Ok(Command::ComRegisterSlave),
            0x16 => Ok(Command::ComStmtPrepare),
            0x17 => Ok(Command::ComStmtExecute),
            0x18 => Ok(Command::ComStmtSendLongData),
            0x19 => Ok(Command::ComStmtClose),
            0x1a => Ok(Command::ComStmtReset),
            0x1b => Ok(Command::ComSetOption),
            0x1c => Ok(Command::ComStmtFetch),
            0x1d => Ok(Command::ComDaemon),
            0x1e => Ok(Command::ComBinlogDumpGtid),
            0x1f => Ok(Command::ComResetConnection),
            _ => Err(MySQLError::Protocol(format!(
                "Unknown command byte: 0x{:02x}",
                value
            ))),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum FieldType {
    MYSQL_TYPE_DECIMAL = 0x00,
    MYSQL_TYPE_TINY = 0x01,
    MYSQL_TYPE_SHORT = 0x02,
    MYSQL_TYPE_LONG = 0x03,
    MYSQL_TYPE_FLOAT = 0x04,
    MYSQL_TYPE_DOUBLE = 0x05,
    MYSQL_TYPE_NULL = 0x06,
    MYSQL_TYPE_TIMESTAMP = 0x07,
    MYSQL_TYPE_LONGLONG = 0x08,
    MYSQL_TYPE_INT24 = 0x09,
    MYSQL_TYPE_DATE = 0x0a,
    MYSQL_TYPE_TIME = 0x0b,
    MYSQL_TYPE_DATETIME = 0x0c,
    MYSQL_TYPE_YEAR = 0x0d,
    MYSQL_TYPE_VARCHAR = 0x0f,
    MYSQL_TYPE_BIT = 0x10,
    MYSQL_TYPE_JSON = 0xf5,
    MYSQL_TYPE_NEWDECIMAL = 0xf6,
    MYSQL_TYPE_ENUM = 0xf7,
    MYSQL_TYPE_SET = 0xf8,
    MYSQL_TYPE_TINY_BLOB = 0xf9,
    MYSQL_TYPE_MEDIUM_BLOB = 0xfa,
    MYSQL_TYPE_LONG_BLOB = 0xfb,
    MYSQL_TYPE_BLOB = 0xfc,
    MYSQL_TYPE_VAR_STRING = 0xfd,
    MYSQL_TYPE_STRING = 0xfe,
    MYSQL_TYPE_GEOMETRY = 0xff,
}

pub fn build_initial_handshake(
    server_version: &str,
    connection_id: u32,
    auth_plugin_data: &[u8],
    auth_plugin_name: &str,
    server_capabilities: u32,
) -> Packet {
    let mut builder = PacketBuilder::with_capacity(128);

    builder.write_u8(10);

    builder.write_null_terminated_string(server_version);

    builder.write_u32(connection_id);

    builder.write_bytes(&auth_plugin_data[0..8]);

    builder.write_u8(0);

    builder.write_u16((server_capabilities & 0xFFFF) as u16);

    builder.write_u8(45);

    builder.write_u16(StatusFlags::SERVER_STATUS_AUTOCOMMIT as u16);

    builder.write_u16((server_capabilities >> 16) as u16);

    builder.write_u8(auth_plugin_data.len() as u8 + 1);

    builder.write_bytes(&[0; 10]);

    builder.write_bytes(&auth_plugin_data[8..]);

    builder.write_u8(0);

    builder.write_null_terminated_string(auth_plugin_name);

    builder.build(0)
}

pub fn parse_handshake_response(packet: &Packet) -> Result<AuthInfo> {
    let mut reader = PacketReader::new(packet);

    let client_flags = reader.read_u32()?;

    let max_packet_size = reader.read_u32()?;

    let charset = reader.read_u8()?;

    reader.read_bytes(23)?;

    let username = reader.read_null_terminated_string()?;

    let auth_response_len = reader.read_u8()? as usize;
    let auth_response = reader.read_bytes(auth_response_len)?;

    let database = if client_flags & 0x08 != 0 && reader.remaining() > 0 {
        Some(reader.read_null_terminated_string()?)
    } else {
        None
    };

    let auth_plugin = if client_flags & 0x00080000 != 0 && reader.remaining() > 0 {
        reader.read_null_terminated_string()?
    } else {
        "mysql_native_password".to_string()
    };

    Ok(AuthInfo {
        username,
        database,
        client_flags,
        max_packet_size,
        charset,
        auth_plugin,
        auth_response,
    })
}

pub fn build_ok_packet(
    affected_rows: u64,
    last_insert_id: u64,
    status_flags: u16,
    warnings: u16,
    info: &str,
    sequence_id: u8,
) -> Packet {
    let mut builder = PacketBuilder::new();

    builder.write_u8(0x00);

    builder.write_lenenc_int(affected_rows);

    builder.write_lenenc_int(last_insert_id);

    builder.write_u16(status_flags);

    builder.write_u16(warnings);

    if !info.is_empty() {
        builder.write_lenenc_string(info);
    }

    builder.build(sequence_id)
}

pub fn build_error_packet(
    error_code: MySQLErrorCode,
    sql_state: &str,
    error_message: &str,
    sequence_id: u8,
) -> Packet {
    let mut builder = PacketBuilder::new();

    builder.write_u8(0xFF);

    builder.write_u16(error_code as u16);

    builder.write_u8(b'#');

    let sql_state = if sql_state.len() == 5 {
        sql_state.to_string()
    } else {
        "HY000".to_string()
    };

    builder.write_bytes(sql_state.as_bytes());

    builder.write_bytes(error_message.as_bytes());

    builder.build(sequence_id)
}

pub fn parse_com_query(packet: &Packet) -> Result<String> {
    let mut reader = PacketReader::new(packet);

    reader.read_u8()?;

    let remaining = reader.remaining();
    let query_bytes = reader.read_bytes(remaining)?;

    String::from_utf8(query_bytes)
        .map_err(|e| MySQLError::Protocol(format!("Invalid UTF-8 in query: {}", e)))
}

pub fn build_column_definition(
    table: &str,
    column_name: &str,
    column_type: FieldType,
    character_set: u16,
    column_length: u32,
    sequence_id: u8,
) -> Packet {
    let mut builder = PacketBuilder::new();

    builder.write_lenenc_string("def");

    builder.write_lenenc_string("sqlr");

    builder.write_lenenc_string(table);

    builder.write_lenenc_string(table);

    builder.write_lenenc_string(column_name);

    builder.write_lenenc_string(column_name);

    builder.write_u8(0x0c);

    builder.write_u16(character_set);

    builder.write_u32(column_length);

    builder.write_u8(column_type as u8);

    builder.write_u16(0);

    builder.write_u8(0);

    builder.write_u16(0);

    builder.build(sequence_id)
}
