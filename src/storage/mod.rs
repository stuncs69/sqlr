#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

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
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StorageManager {
    tables: HashMap<String, Table>,
}

impl StorageManager {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn load(path: &str) -> Result<Self, String> {
        use std::fs::File;
        use std::io::Read;

        match File::open(path) {
            Ok(mut file) => {
                let mut buffer = Vec::new();
                if let Err(e) = file.read_to_end(&mut buffer) {
                    return Err(format!("Failed to read database file '{}': {}", path, e));
                }

                bincode::deserialize(&buffer)
                    .map_err(|e| format!("Failed to deserialize database file '{}': {}", path, e))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(StorageManager::new()),
            Err(e) => Err(format!("Failed to open database file '{}': {}", path, e)),
        }
    }

    pub fn save(&self, path: &str) -> Result<(), String> {
        use std::fs::File;
        use std::io::Write;

        let encoded: Vec<u8> = bincode::serialize(&self)
            .map_err(|e| format!("Failed to serialize database state: {}", e))?;

        let mut file = File::create(path).map_err(|e| {
            format!(
                "Failed to create/open database file '{}' for saving: {}",
                path, e
            )
        })?;

        file.write_all(&encoded)
            .map_err(|e| format!("Failed to write to database file '{}': {}", path, e))
    }

    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), String> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists.", name));
        }
        let table = Table {
            name: name.clone(),
            columns,
            rows: Vec::new(),
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

        table.rows.push(row);
        Ok(())
    }

    pub fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.get(table_name)
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

        let initial_rows = table.rows.len();
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
}
