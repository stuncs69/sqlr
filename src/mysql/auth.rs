use crate::mysql::error::{MySQLError, Result};
use rand::Rng;
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::convert::TryInto;

pub const AUTH_NATIVE_PASSWORD: &str = "mysql_native_password";

pub const AUTH_PLUGIN_DATA_LEN: usize = 20;

pub const DEFAULT_SERVER_CAPABILITIES: u32 = CapabilityFlags::CLIENT_LONG_PASSWORD as u32
    | CapabilityFlags::CLIENT_FOUND_ROWS as u32
    | CapabilityFlags::CLIENT_LONG_FLAG as u32
    | CapabilityFlags::CLIENT_CONNECT_WITH_DB as u32
    | CapabilityFlags::CLIENT_PROTOCOL_41 as u32
    | CapabilityFlags::CLIENT_TRANSACTIONS as u32
    | CapabilityFlags::CLIENT_SECURE_CONNECTION as u32
    | CapabilityFlags::CLIENT_MULTI_STATEMENTS as u32
    | CapabilityFlags::CLIENT_MULTI_RESULTS as u32
    | CapabilityFlags::CLIENT_PS_MULTI_RESULTS as u32
    | CapabilityFlags::CLIENT_PLUGIN_AUTH as u32
    | CapabilityFlags::CLIENT_CONNECT_ATTRS as u32
    | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA as u32
    | CapabilityFlags::CLIENT_DEPRECATE_EOF as u32;

#[repr(u32)]
#[allow(unused)]
pub enum CapabilityFlags {
    CLIENT_LONG_PASSWORD = 1,
    CLIENT_FOUND_ROWS = 2,
    CLIENT_LONG_FLAG = 4,
    CLIENT_CONNECT_WITH_DB = 8,
    CLIENT_NO_SCHEMA = 16,
    CLIENT_COMPRESS = 32,
    CLIENT_ODBC = 64,
    CLIENT_LOCAL_FILES = 128,
    CLIENT_IGNORE_SPACE = 256,
    CLIENT_PROTOCOL_41 = 512,
    CLIENT_INTERACTIVE = 1024,
    CLIENT_SSL = 2048,
    CLIENT_IGNORE_SIGPIPE = 4096,
    CLIENT_TRANSACTIONS = 8192,
    CLIENT_RESERVED = 16384,
    CLIENT_SECURE_CONNECTION = 32768,
    CLIENT_MULTI_STATEMENTS = 65536,
    CLIENT_MULTI_RESULTS = 131072,
    CLIENT_PS_MULTI_RESULTS = 262144,
    CLIENT_PLUGIN_AUTH = 524288,
    CLIENT_CONNECT_ATTRS = 1048576,
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 2097152,
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 4194304,
    CLIENT_SESSION_TRACK = 8388608,
    CLIENT_DEPRECATE_EOF = 16777216,
}

#[repr(u16)]
#[allow(unused)]
pub enum StatusFlags {
    SERVER_STATUS_IN_TRANS = 1,
    SERVER_STATUS_AUTOCOMMIT = 2,
    SERVER_MORE_RESULTS_EXISTS = 8,
    SERVER_STATUS_NO_GOOD_INDEX_USED = 16,
    SERVER_STATUS_NO_INDEX_USED = 32,
    SERVER_STATUS_CURSOR_EXISTS = 64,
    SERVER_STATUS_LAST_ROW_SENT = 128,
    SERVER_STATUS_DB_DROPPED = 256,
    SERVER_STATUS_NO_BACKSLASH_ESCAPES = 512,
    SERVER_STATUS_METADATA_CHANGED = 1024,
    SERVER_QUERY_WAS_SLOW = 2048,
    SERVER_PS_OUT_PARAMS = 4096,
    SERVER_STATUS_IN_TRANS_READONLY = 8192,
    SERVER_SESSION_STATE_CHANGED = 16384,
}

pub fn generate_auth_plugin_data() -> [u8; AUTH_PLUGIN_DATA_LEN] {
    let mut rng = rand::thread_rng();
    let mut data = [0u8; AUTH_PLUGIN_DATA_LEN];
    rng.fill(&mut data);
    data
}

pub fn check_auth_native(
    auth_data: &[u8],
    scramble: &[u8],
    expected_password: Option<&str>,
) -> Result<bool> {
    if expected_password.is_none() || expected_password.unwrap().is_empty() {
        return Ok(auth_data.is_empty());
    }

    let expected_password = expected_password.unwrap();
    let password_hash = hash_password(expected_password)?;

    if auth_data.is_empty() {
        return Ok(false);
    }

    let expected_auth_response = scramble_password(&password_hash, scramble)?;

    Ok(auth_data == expected_auth_response)
}

pub fn hash_password(password: &str) -> Result<Vec<u8>> {
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let stage1 = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(&stage1);
    let stage2 = hasher.finalize();

    Ok(stage2.to_vec())
}

pub fn scramble_password(password_hash: &[u8], scramble: &[u8]) -> Result<Vec<u8>> {
    let mut hasher = Sha1::new();
    hasher.update(scramble);
    hasher.update(password_hash);
    let hash1 = hasher.finalize();

    let result: Vec<u8> = password_hash
        .iter()
        .zip(hash1.iter())
        .map(|(p, h)| p ^ h)
        .collect();

    Ok(result)
}

pub struct AuthInfo {
    pub username: String,
    pub database: Option<String>,
    pub client_flags: u32,
    pub max_packet_size: u32,
    pub charset: u8,
    pub auth_plugin: String,
    pub auth_response: Vec<u8>,
}
