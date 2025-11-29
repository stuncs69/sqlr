#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Text,
    Integer,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Text(String),
    Integer(i64),
    Null,
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),

            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),

            (Value::Null, _) => None,
            (_, Value::Null) => None,

            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

pub type Row = Vec<Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub column_index: usize,
    pub value_map: HashMap<Value, Vec<usize>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    #[serde(default)]
    pub indexes: HashMap<String, Index>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthMetrics {
    pub uptime_seconds: u64,
    pub total_tables: usize,
    pub total_rows: usize,
    pub queries_executed: u64,
    pub last_query_time_ms: Option<u64>,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedQuery {
    pub query: String,
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

const MAX_CACHE_SIZE: usize = 100;
const CURRENT_DB_VERSION: u32 = 1;
const DB_MAGIC: &[u8; 4] = b"SQLR";

const FLAG_COMPRESSED: u8 = 0x01;
const FLAG_CHECKSUM: u8 = 0x02;

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseFile {
    pub version: u32,
    pub tables: HashMap<String, Table>,
    pub metadata: DatabaseMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseMetadata {
    pub created_at: String,
    pub last_modified: String,
    #[serde(default)]
    pub total_queries: u64,
}

impl DatabaseMetadata {
    fn new() -> Self {
        let now = chrono::Utc::now().to_rfc3339();
        Self {
            created_at: now.clone(),
            last_modified: now,
            total_queries: 0,
        }
    }

    fn touch(&mut self) {
        self.last_modified = chrono::Utc::now().to_rfc3339();
    }
}

// File format:
// [4 bytes] Magic: "SQLR"
// [1 byte]  Format version
// [1 byte]  Flags (compressed, checksum, etc.)
// [4 bytes] Data length (uncompressed)
// [4 bytes] CRC32 checksum (optional)
// [N bytes] Data (bincode-serialized DatabaseFile, optionally compressed)

#[derive(Debug, Serialize, Deserialize)]
pub struct StorageManager {
    tables: HashMap<String, Table>,
    #[serde(skip, default = "std::time::Instant::now")]
    pub start_time: std::time::Instant,
    #[serde(default)]
    pub queries_executed: u64,
    #[serde(default)]
    pub last_query_time_ms: Option<u64>,
    #[serde(skip, default)]
    query_cache: HashMap<String, CachedQuery>,
    #[serde(skip, default)]
    cache_order: VecDeque<String>,
    #[serde(default)]
    pub cache_hits: u64,
    #[serde(default)]
    pub cache_misses: u64,
}

impl Default for StorageManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageManager {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            start_time: std::time::Instant::now(),
            queries_executed: 0,
            last_query_time_ms: None,
            query_cache: HashMap::new(),
            cache_order: VecDeque::new(),
            cache_hits: 0,
            cache_misses: 0,
        }
    }

    pub fn load(path: &str) -> Result<Self, String> {
        use std::fs::File;
        use std::io::Read;

        if !Path::new(path).exists() {
            println!("Database file not found, starting fresh");
            return Ok(StorageManager::new());
        }

        let mut file = File::open(path)
            .map_err(|e| format!("Failed to open database file '{}': {}", path, e))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .map_err(|e| format!("Failed to read database file '{}': {}", path, e))?;

        if buffer.len() >= 4 && &buffer[0..4] == DB_MAGIC {
            return Self::load_binary_format(&buffer);
        }

        if let Ok(db_file) = serde_json::from_slice::<DatabaseFile>(&buffer) {
            println!("Loaded database version {} (JSON format, will convert to binary)", db_file.version);
            return Self::load_from_db_file(db_file);
        }

        println!("Attempting to load legacy bincode format...");
        match bincode::deserialize::<StorageManager>(&buffer) {
            Ok(mut storage) => {
                println!("Successfully loaded legacy database, will convert to optimized binary format on next save");
                storage.start_time = std::time::Instant::now();

                let table_names: Vec<String> = storage.tables.keys().cloned().collect();
                for table_name in table_names {
                    if let Err(e) = storage.rebuild_indexes(&table_name) {
                        eprintln!("Warning: Failed to rebuild indexes for table '{}': {}", table_name, e);
                    }
                }

                Ok(storage)
            }
            Err(e) => {
                Err(format!(
                    "Failed to load database. File may be corrupted or from an incompatible version: {}",
                    e
                ))
            }
        }
    }

    fn load_binary_format(buffer: &[u8]) -> Result<Self, String> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        if buffer.len() < 14 {
            return Err("Invalid database file: too short".to_string());
        }

        let format_version = buffer[4];
        let flags = buffer[5];
        let data_length = u32::from_le_bytes([buffer[6], buffer[7], buffer[8], buffer[9]]) as usize;

        let mut offset = 10;

        if flags & FLAG_CHECKSUM != 0 {
            let _stored_checksum = u32::from_le_bytes([buffer[10], buffer[11], buffer[12], buffer[13]]);
            offset = 14;
            // TODO: Verify checksum
        }

        let data = &buffer[offset..];

        let decompressed: Vec<u8> = if flags & FLAG_COMPRESSED != 0 {
            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder
                .read_to_end(&mut decompressed)
                .map_err(|e| format!("Failed to decompress database: {}", e))?;
            decompressed
        } else {
            data.to_vec()
        };

        let db_file: DatabaseFile = bincode::deserialize(&decompressed)
            .map_err(|e| format!("Failed to deserialize database: {}", e))?;

        println!(
            "Loaded database version {} (optimized binary format, {}compressed)",
            db_file.version,
            if flags & FLAG_COMPRESSED != 0 { "" } else { "un" }
        );

        Self::load_from_db_file(db_file)
    }

    fn load_from_db_file(db_file: DatabaseFile) -> Result<Self, String> {
        let migrated_tables = Self::migrate_from_version(db_file.version, db_file.tables)?;

        let mut storage = StorageManager {
            tables: migrated_tables,
            start_time: std::time::Instant::now(),
            queries_executed: db_file.metadata.total_queries,
            last_query_time_ms: None,
            query_cache: HashMap::new(),
            cache_order: VecDeque::new(),
            cache_hits: 0,
            cache_misses: 0,
        };

        let table_names: Vec<String> = storage.tables.keys().cloned().collect();
        for table_name in table_names {
            if let Err(e) = storage.rebuild_indexes(&table_name) {
                eprintln!("Warning: Failed to rebuild indexes for table '{}': {}", table_name, e);
            }
        }

        Ok(storage)
    }

    fn migrate_from_version(from_version: u32, tables: HashMap<String, Table>) -> Result<HashMap<String, Table>, String> {
        if from_version == CURRENT_DB_VERSION {
            return Ok(tables);
        }

        println!("Migrating database from version {} to {}", from_version, CURRENT_DB_VERSION);

        match from_version {
            1 => Ok(tables),
            _ => Err(format!("Unknown database version: {}", from_version)),
        }
    }

    pub fn save(&self, path: &str) -> Result<(), String> {
        self.save_with_compression(path, true)
    }

    pub fn save_with_compression(&self, path: &str, compress: bool) -> Result<(), String> {
        use std::fs::File;
        use std::io::Write;
        use flate2::Compression;
        use flate2::write::GzEncoder;

        if Path::new(path).exists() {
            let backup_path = format!("{}.backup", path);
            if let Err(e) = std::fs::copy(path, &backup_path) {
                eprintln!("Warning: Failed to create backup: {}", e);
            }
        }

        let mut metadata = DatabaseMetadata::new();
        metadata.total_queries = self.queries_executed;
        metadata.touch();

        let db_file = DatabaseFile {
            version: CURRENT_DB_VERSION,
            tables: self.tables.clone(),
            metadata,
        };

        let serialized = bincode::serialize(&db_file)
            .map_err(|e| format!("Failed to serialize database: {}", e))?;

        let data = if compress {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(&serialized)
                .map_err(|e| format!("Failed to compress database: {}", e))?;
            encoder
                .finish()
                .map_err(|e| format!("Failed to finish compression: {}", e))?
        } else {
            serialized.clone()
        };

        let mut header = Vec::new();
        header.extend_from_slice(DB_MAGIC);
        header.push(1);
        header.push(if compress { FLAG_COMPRESSED } else { 0 });
        header.extend_from_slice(&(serialized.len() as u32).to_le_bytes());

        let mut file_data = header;
        file_data.extend_from_slice(&data);

        let temp_path = format!("{}.tmp", path);
        let mut file = File::create(&temp_path).map_err(|e| {
            format!("Failed to create temp database file '{}': {}", temp_path, e)
        })?;

        file.write_all(&file_data)
            .map_err(|e| format!("Failed to write to temp database file '{}': {}", temp_path, e))?;

        file.sync_all()
            .map_err(|e| format!("Failed to sync temp database file: {}", e))?;

        std::fs::rename(&temp_path, path)
            .map_err(|e| format!("Failed to rename temp file to '{}': {}", path, e))?;

        Ok(())
    }

    pub fn save_uncompressed(&self, path: &str) -> Result<(), String> {
        self.save_with_compression(path, false)
    }

    pub fn save_json(&self, path: &str) -> Result<(), String> {
        use std::fs::File;
        use std::io::Write;

        let mut metadata = DatabaseMetadata::new();
        metadata.total_queries = self.queries_executed;
        metadata.touch();

        let db_file = DatabaseFile {
            version: CURRENT_DB_VERSION,
            tables: self.tables.clone(),
            metadata,
        };

        let json = serde_json::to_string_pretty(&db_file)
            .map_err(|e| format!("Failed to serialize to JSON: {}", e))?;

        let mut file = File::create(path)
            .map_err(|e| format!("Failed to create JSON file: {}", e))?;

        file.write_all(json.as_bytes())
            .map_err(|e| format!("Failed to write JSON: {}", e))?;

        Ok(())
    }

    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), String> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists.", name));
        }
        let table = Table {
            name: name.clone(),
            columns,
            rows: Vec::new(),
            indexes: HashMap::new(),
        };
        self.tables.insert(name, table);
        Ok(())
    }

    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found.", table_name))?;

        if row.len() != table.columns.len() {
            return Err(format!(
                "Column count mismatch: Table '{}' has {} columns, but {} values were supplied.",
                table_name,
                table.columns.len(),
                row.len()
            ));
        }

        let row_index = table.rows.len();
        table.rows.push(row.clone());

        for (col_name, index) in &mut table.indexes {
            if let Some(col_idx) = table.columns.iter().position(|c| &c.name == col_name) {
                let value = &row[col_idx];
                index.value_map.entry(value.clone()).or_insert_with(Vec::new).push(row_index);
            }
        }

        Ok(())
    }

    pub fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.get(table_name)
    }

    pub fn list_table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn update_row_values(
        &mut self,
        table_name: &str,
        row_index: usize,
        updates: &[(usize, Value)],
    ) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found for update.", table_name))?;

        if row_index >= table.rows.len() {
            return Err(format!(
                "Row index {} out of bounds for table '{}'",
                row_index, table_name
            ));
        }

        let row = &mut table.rows[row_index];

        for &(col_index, ref new_value) in updates {
            if col_index >= table.columns.len() {
                return Err(format!(
                    "Column index {} out of bounds for table '{}'",
                    col_index, table_name
                ));
            }
            row[col_index] = new_value.clone();
        }

        Ok(())
    }

    pub fn delete_rows(
        &mut self,
        table_name: &str,
        mut indices_to_delete: Vec<usize>,
    ) -> Result<usize, String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found for delete.", table_name))?;

        indices_to_delete.sort_unstable_by(|a, b| b.cmp(a));
        indices_to_delete.dedup();

        let mut deleted_count = 0;

        for index in indices_to_delete {
            if index < table.rows.len() {
                table.rows.remove(index);
                deleted_count += 1;
            } else {
                eprintln!(
                    "WARN: Attempted to delete invalid row index {} from table '{}'",
                    index, table_name
                );
            }
        }

        Ok(deleted_count)
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<(), String> {
        if self.tables.remove(table_name).is_some() {
            Ok(())
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    pub fn get_health_metrics(&self) -> HealthMetrics {
        let uptime_seconds = self.start_time.elapsed().as_secs();
        let total_tables = self.tables.len();
        let total_rows = self.tables.values().map(|t| t.rows.len()).sum();

        HealthMetrics {
            uptime_seconds,
            total_tables,
            total_rows,
            queries_executed: self.queries_executed,
            last_query_time_ms: self.last_query_time_ms,
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
        }
    }

    pub fn record_query_execution(&mut self, duration_ms: u64) {
        self.queries_executed += 1;
        self.last_query_time_ms = Some(duration_ms);
    }

    pub fn create_index(&mut self, table_name: &str, column_name: &str) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;

        let col_idx = table
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| format!("Column '{}' not found in table '{}'", column_name, table_name))?;

        if table.indexes.contains_key(column_name) {
            return Err(format!("Index on column '{}' already exists", column_name));
        }

        let mut value_map: HashMap<Value, Vec<usize>> = HashMap::new();
        for (row_idx, row) in table.rows.iter().enumerate() {
            let value = &row[col_idx];
            value_map.entry(value.clone()).or_insert_with(Vec::new).push(row_idx);
        }

        let index = Index {
            column_index: col_idx,
            value_map,
        };

        table.indexes.insert(column_name.to_string(), index);
        Ok(())
    }

    pub fn drop_index(&mut self, table_name: &str, column_name: &str) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;

        if table.indexes.remove(column_name).is_none() {
            return Err(format!("Index on column '{}' not found", column_name));
        }

        Ok(())
    }

    pub fn rebuild_indexes(&mut self, table_name: &str) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;

        for (col_name, index) in &mut table.indexes {
            let col_idx = index.column_index;
            let mut value_map: HashMap<Value, Vec<usize>> = HashMap::new();

            for (row_idx, row) in table.rows.iter().enumerate() {
                let value = &row[col_idx];
                value_map.entry(value.clone()).or_insert_with(Vec::new).push(row_idx);
            }

            index.value_map = value_map;
        }

        Ok(())
    }

    pub fn get_cached_query(&mut self, query: &str) -> Option<(Vec<String>, Vec<Row>)> {
        if let Some(cached) = self.query_cache.get(query) {
            self.cache_hits += 1;

            self.cache_order.retain(|q| q != query);
            self.cache_order.push_back(query.to_string());

            Some((cached.columns.clone(), cached.rows.clone()))
        } else {
            self.cache_misses += 1;
            None
        }
    }

    pub fn cache_query(&mut self, query: String, columns: Vec<String>, rows: Vec<Row>) {
        if self.query_cache.len() >= MAX_CACHE_SIZE {
            if let Some(oldest) = self.cache_order.pop_front() {
                self.query_cache.remove(&oldest);
            }
        }

        let cached = CachedQuery {
            query: query.clone(),
            columns,
            rows,
        };

        self.query_cache.insert(query.clone(), cached);
        self.cache_order.push_back(query);
    }

    pub fn invalidate_cache(&mut self) {
        self.query_cache.clear();
        self.cache_order.clear();
    }

    pub fn invalidate_cache_for_table(&mut self, _table_name: &str) {
        self.invalidate_cache();
    }

    pub fn export_to_json(&self, path: &str) -> Result<(), String> {
        self.save_json(path)
    }

    pub fn import_from_json(path: &str) -> Result<Self, String> {
        Self::load(path)
    }

    pub fn get_statistics(&self) -> DatabaseStatistics {
        let total_rows: usize = self.tables.values().map(|t| t.rows.len()).sum();
        let total_indexes: usize = self.tables.values().map(|t| t.indexes.len()).sum();
        let cache_size = self.query_cache.len();
        let cache_hit_rate = if self.cache_hits + self.cache_misses > 0 {
            (self.cache_hits as f64) / ((self.cache_hits + self.cache_misses) as f64) * 100.0
        } else {
            0.0
        };

        DatabaseStatistics {
            total_tables: self.tables.len(),
            total_rows,
            total_indexes,
            cache_size,
            cache_hit_rate,
            queries_executed: self.queries_executed,
        }
    }
}

#[derive(Debug)]
pub struct DatabaseStatistics {
    pub total_tables: usize,
    pub total_rows: usize,
    pub total_indexes: usize,
    pub cache_size: usize,
    pub cache_hit_rate: f64,
    pub queries_executed: u64,
}
