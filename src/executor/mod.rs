#![allow(dead_code)]

use crate::parser::{
    BinaryOperator, CreateTableStatement, DeleteStatement, Expression, InsertStatement,
    LiteralValue as ParserLiteralValue, ParsedDataType, Statement, UnaryOperator, UpdateStatement,
};
use crate::storage::{Column, DataType, Row, StorageManager, Table, Value};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Serialize)]
pub enum ExecutionResult {
    RowSet {
        columns: Vec<String>,
        rows: Vec<Row>,
    },

    Msg(String),
}

pub struct Executor<'a> {
    storage: &'a mut StorageManager,
}

impl<'a> Executor<'a> {
    pub fn new(storage: &'a mut StorageManager) -> Self {
        Executor { storage }
    }

    pub fn execute_statement(&mut self, statement: Statement) -> Result<ExecutionResult, String> {
        match statement {
            Statement::Select(select_stmt) => self.execute_select(select_stmt),
            Statement::CreateTable(create_stmt) => self.execute_create_table(create_stmt),
            Statement::Insert(insert_stmt) => self.execute_insert(insert_stmt),
            Statement::Delete(delete_stmt) => self.execute_delete(delete_stmt),
            Statement::Update(update_stmt) => self.execute_update(update_stmt),
        }
    }

    fn execute_select(
        &self,
        statement: crate::parser::SelectStatement,
    ) -> Result<ExecutionResult, String> {
        let table_name = statement
            .from_table
            .ok_or_else(|| "FROM clause is required for SELECT".to_string())?;

        if table_name.eq_ignore_ascii_case("SQLR_TABLES") {
            println!("[execute_select] Handling meta-table SQLR_TABLES");

            let meta_columns = vec![("name".to_string(), DataType::Text)];

            let requested_col_indices: Vec<usize> = match &statement.columns[..] {
                [crate::parser::SelectItem::Wildcard] => (0..meta_columns.len()).collect(),
                select_items => {
                    let mut indices = Vec::new();
                    for item in select_items {
                        if let crate::parser::SelectItem::UnnamedExpr(Expression::Identifier(
                            name,
                        )) = item
                        {
                            if let Some(index) = meta_columns
                                .iter()
                                .position(|(col_name, _)| col_name.eq_ignore_ascii_case(name))
                            {
                                indices.push(index);
                            } else {
                                return Err(format!("Unknown column '{}' in SQLR_TABLES", name));
                            }
                        } else {
                            return Err("Unsupported item in SELECT list for SQLR_TABLES (only column names or *)".to_string());
                        }
                    }
                    indices
                }
            };

            let result_columns: Vec<String> = requested_col_indices
                .iter()
                .map(|&i| meta_columns[i].0.clone())
                .collect();

            let table_names = self.storage.list_table_names();
            let mut result_rows: Vec<Row> = Vec::with_capacity(table_names.len());

            for name in table_names {
                let row_values = vec![Value::Text(name)];
                let selected_row_values: Row = requested_col_indices
                    .iter()
                    .map(|&i| row_values[i].clone())
                    .collect();
                result_rows.push(selected_row_values);
            }

            return Ok(ExecutionResult::RowSet {
                columns: result_columns,
                rows: result_rows,
            });
        }

        let table = self
            .storage
            .get_table(&table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;

        let filtered_rows: Vec<&Row> = if let Some(where_expr) = &statement.where_clause {
            println!("[execute_select] Evaluating WHERE clause: {:?}", where_expr);
            let mut matched_rows = Vec::new();
            for row in &table.rows {
                match self.evaluate_where_condition(where_expr, table, row) {
                    Ok(true) => matched_rows.push(row),
                    Ok(false) => {}
                    Err(e) => return Err(format!("WHERE clause evaluation error: {}", e)),
                }
            }
            println!(
                "[execute_select] WHERE clause matched {} rows",
                matched_rows.len()
            );
            matched_rows
        } else {
            table.rows.iter().collect()
        };

        let result_columns: Vec<String>;
        let column_indices: Vec<usize>;

        match &statement.columns[0] {
            crate::parser::SelectItem::Wildcard => {
                result_columns = table.columns.iter().map(|c| c.name.clone()).collect();
                column_indices = (0..table.columns.len()).collect();
            }
            _ => {
                let mut temp_result_columns = Vec::new();
                let mut temp_column_indices = Vec::new();
                let table_col_map: HashMap<_, _> = table
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (c.name.as_str(), i))
                    .collect();

                for item in &statement.columns {
                    match item {
                        crate::parser::SelectItem::UnnamedExpr(expr) => {
                            match expr {
                                crate::parser::Expression::Identifier(name) => {
                                    if let Some(&index) = table_col_map.get(name.as_str()) {
                                        temp_result_columns.push(name.clone());
                                        temp_column_indices.push(index);
                                    } else {
                                        return Err(format!(
                                            "Column '{}' not found in table '{}'",
                                            name, table_name
                                        ));
                                    }
                                }
                                _ => return Err(
                                    "Only column identifiers are supported in SELECT list for now"
                                        .to_string(),
                                ),
                            }
                        }
                        crate::parser::SelectItem::Wildcard => {
                            return Err(
                                "'*' must be the only item in SELECT list if used".to_string()
                            );
                        }
                    }
                }
                result_columns = temp_result_columns;
                column_indices = temp_column_indices;
            }
        }

        let result_rows = filtered_rows
            .iter()
            .map(|&row| {
                column_indices
                    .iter()
                    .map(|&index| row[index].clone())
                    .collect::<Row>()
            })
            .collect();

        Ok(ExecutionResult::RowSet {
            columns: result_columns,
            rows: result_rows,
        })
    }

    fn evaluate_expression(
        &self,
        expr: &Expression,
        table: &Table,
        row: &Row,
    ) -> Result<Value, String> {
        match expr {
            Expression::Literal(literal) => match literal {
                ParserLiteralValue::Number(s) => s
                    .parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|e| format!("Invalid integer literal '{}': {}", s, e)),
                ParserLiteralValue::String(s) => Ok(Value::Text(s.clone())),
                ParserLiteralValue::Null => Ok(Value::Null),
            },
            Expression::Identifier(name) => {
                if let Some(index) = table.columns.iter().position(|c| c.name == *name) {
                    Ok(row[index].clone())
                } else {
                    Err(format!("Column '{}' not found during evaluation", name))
                }
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression(left, table, row)?;
                let right_val = self.evaluate_expression(right, table, row)?;

                println!(
                    "[evaluate_expression] Comparing/Combining {:?} {:?} {:?}",
                    left_val, op, right_val
                );

                let result_bool =
                    match op {
                        BinaryOperator::Eq => left_val == right_val,
                        BinaryOperator::Neq => left_val != right_val,

                        BinaryOperator::Lt
                        | BinaryOperator::Gt
                        | BinaryOperator::LtEq
                        | BinaryOperator::GtEq => match left_val.partial_cmp(&right_val) {
                            Some(ordering) => match op {
                                BinaryOperator::Lt => ordering == std::cmp::Ordering::Less,
                                BinaryOperator::Gt => ordering == std::cmp::Ordering::Greater,
                                BinaryOperator::LtEq => ordering != std::cmp::Ordering::Greater,
                                BinaryOperator::GtEq => ordering != std::cmp::Ordering::Less,
                                _ => return Err(
                                    "Internal error: Unexpected comparison op in ordered branch"
                                        .to_string(),
                                ),
                            },
                            None => {
                                return Err(format!(
                                    "Cannot compare values of incompatible types: {:?} and {:?}",
                                    left_val, right_val
                                ));
                            }
                        },

                        BinaryOperator::And => {
                            let left_bool = match left_val {
                                Value::Integer(i) => i != 0,
                                _ => false,
                            };
                            let right_bool = match right_val {
                                Value::Integer(i) => i != 0,
                                _ => false,
                            };
                            left_bool && right_bool
                        }
                        BinaryOperator::Or => {
                            let left_bool = match left_val {
                                Value::Integer(i) => i != 0,
                                _ => false,
                            };
                            let right_bool = match right_val {
                                Value::Integer(i) => i != 0,
                                _ => false,
                            };
                            left_bool || right_bool
                        }
                    };

                Ok(Value::Integer(result_bool as i64))
            }
            Expression::UnaryOp { op, expr } => {
                let val = self.evaluate_expression(expr, table, row)?;
                match op {
                    UnaryOperator::Not => {
                        let bool_val = match val {
                            Value::Integer(i) => i != 0,
                            _ => false,
                        };
                        Ok(Value::Integer((!bool_val) as i64))
                    }
                }
            }
            Expression::IsNull(expr) => {
                let val = self.evaluate_expression(expr, table, row)?;
                Ok(Value::Integer((val == Value::Null) as i64))
            }
            Expression::IsNotNull(expr) => {
                let val = self.evaluate_expression(expr, table, row)?;
                Ok(Value::Integer((val != Value::Null) as i64))
            }
        }
    }

    fn evaluate_where_condition(
        &self,
        expr: &Expression,
        table: &Table,
        row: &Row,
    ) -> Result<bool, String> {
        match self.evaluate_expression(expr, table, row)? {
            Value::Integer(i) => Ok(i != 0),
            Value::Text(_) => Err(
                "WHERE condition evaluated to a TEXT value, expected boolean-like (Integer)"
                    .to_string(),
            ),
            Value::Null => Ok(false),
        }
    }

    fn execute_create_table(
        &mut self,
        statement: CreateTableStatement,
    ) -> Result<ExecutionResult, String> {
        let table_name = statement.table_name;
        let mut storage_columns = Vec::new();

        for col_def in statement.columns {
            let storage_type = match col_def.data_type {
                ParsedDataType::Integer => DataType::Integer,
                ParsedDataType::Text => DataType::Text,
            };
            storage_columns.push(Column {
                name: col_def.name,
                data_type: storage_type,
            });
        }

        if storage_columns.is_empty() {
            return Err("Table must have at least one column".to_string());
        }

        self.storage
            .create_table(table_name.clone(), storage_columns)?;

        Ok(ExecutionResult::Msg(format!(
            "Table '{}' created successfully",
            table_name
        )))
    }

    fn execute_insert(&mut self, statement: InsertStatement) -> Result<ExecutionResult, String> {
        let table_name = statement.table_name;
        let mut rows_inserted = 0;

        let num_cols = self
            .storage
            .get_table(&table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?
            .columns
            .len();

        for value_row_exprs in statement.values {
            if value_row_exprs.len() != num_cols {
                return Err(format!(
                    "Column count mismatch: Table '{}' has {} columns, but {} values were supplied.",
                    table_name,
                    num_cols,
                    value_row_exprs.len()
                ));
            }

            let mut storage_row: Row = Vec::with_capacity(num_cols);
            for expr in value_row_exprs {
                let value = match expr {
                    Expression::Literal(literal) => match literal {
                        ParserLiteralValue::Number(s) => s
                            .parse::<i64>()
                            .map(Value::Integer)
                            .map_err(|e| format!("Invalid integer literal '{}': {}", s, e)),
                        ParserLiteralValue::String(s) => Ok(Value::Text(s)),
                        ParserLiteralValue::Null => Ok(Value::Null),
                    },
                    Expression::Identifier(_) => {
                        Err("Column identifiers not expected in VALUES clause".to_string())
                    }
                    _ => {
                        return Err(format!(
                            "Unsupported expression type in VALUES clause: {:?}",
                            expr
                        ));
                    }
                }?;
                storage_row.push(value);
            }

            self.storage.insert_row(&table_name, storage_row)?;
            rows_inserted += 1;
        }

        Ok(ExecutionResult::Msg(format!(
            "Inserted {} row(s) into '{}'",
            rows_inserted, table_name
        )))
    }

    fn execute_delete(&mut self, statement: DeleteStatement) -> Result<ExecutionResult, String> {
        let table_name = statement.table_name;

        let indices_to_delete = {
            let table = self
                .storage
                .get_table(&table_name)
                .ok_or_else(|| format!("Table '{}' not found for DELETE", table_name))?;

            let mut indices = Vec::new();
            if let Some(where_expr) = &statement.where_clause {
                println!("[execute_delete] Evaluating WHERE clause: {:?}", where_expr);
                for (index, row) in table.rows.iter().enumerate() {
                    match self.evaluate_where_condition(where_expr, table, row) {
                        Ok(true) => indices.push(index),
                        Ok(false) => {}
                        Err(e) => {
                            return Err(format!(
                                "WHERE clause evaluation error during DELETE: {}",
                                e
                            ));
                        }
                    }
                }
                println!(
                    "[execute_delete] Found {} rows matching WHERE clause",
                    indices.len()
                );
            } else {
                println!(
                    "[execute_delete] No WHERE clause, deleting all rows from '{}'",
                    table_name
                );
                indices = (0..table.rows.len()).collect();
            }
            indices
        };

        let deleted_count = self.storage.delete_rows(&table_name, indices_to_delete)?;

        Ok(ExecutionResult::Msg(format!(
            "Deleted {} row(s) from '{}'",
            deleted_count, table_name
        )))
    }

    fn execute_update(&mut self, statement: UpdateStatement) -> Result<ExecutionResult, String> {
        let table_name = statement.table_name;

        let (indices_to_update, column_map) = {
            let table = self
                .storage
                .get_table(&table_name)
                .ok_or_else(|| format!("Table '{}' not found for UPDATE", table_name))?;

            let mut indices = Vec::new();
            if let Some(where_expr) = &statement.where_clause {
                println!("[execute_update] Evaluating WHERE clause: {:?}", where_expr);
                for (index, row) in table.rows.iter().enumerate() {
                    match self.evaluate_where_condition(where_expr, table, row) {
                        Ok(true) => indices.push(index),
                        Ok(false) => {}
                        Err(e) => {
                            return Err(format!(
                                "WHERE clause evaluation error during UPDATE: {}",
                                e
                            ));
                        }
                    }
                }
                println!(
                    "[execute_update] Found {} rows matching WHERE clause",
                    indices.len()
                );
            } else {
                println!(
                    "[execute_update] No WHERE clause, updating all rows in '{}'",
                    table_name
                );
                indices = (0..table.rows.len()).collect();
            }

            let col_map: HashMap<String, usize> = table
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name.clone(), i))
                .collect();

            (indices, col_map)
        };

        let mut updated_count = 0;
        for row_index in indices_to_update {
            let table = self
                .storage
                .get_table(&table_name)
                .ok_or_else(|| format!("Table '{}' disappeared during UPDATE?", table_name))?;
            let row = &table.rows[row_index];

            let mut updates_for_row: Vec<(usize, Value)> = Vec::new();
            for (col_name, expr) in &statement.assignments {
                let col_index = *column_map.get(col_name).ok_or_else(|| {
                    format!("Column '{}' not found in table '{}'", col_name, table_name)
                })?;

                let new_value = self.evaluate_expression(expr, table, row)?;

                let expected_type = &table.columns[col_index].data_type;
                match (expected_type, &new_value) {
                    (DataType::Integer, Value::Integer(_)) => {}
                    (DataType::Text, Value::Text(_)) => {}
                    (_, Value::Null) => {}
                    (dt, val) => {
                        return Err(format!(
                            "Type mismatch for column '{}'. Expected {:?}, got {:?}",
                            col_name, dt, val
                        ));
                    }
                }

                updates_for_row.push((col_index, new_value));
            }

            self.storage
                .update_row_values(&table_name, row_index, &updates_for_row)?;
            updated_count += 1;
        }

        Ok(ExecutionResult::Msg(format!(
            "Updated {} row(s) in '{}'",
            updated_count, table_name
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Lexer, Parser as SqlParser};
    use crate::storage::{Column, DataType, Value};

    fn execute_success(executor: &mut Executor, sql: &str) -> ExecutionResult {
        let lexer = Lexer::new(sql);
        let mut parser = SqlParser::new(lexer);
        let statement = parser.parse_program().expect("Parsing failed in test");
        assert!(
            parser.errors().is_empty(),
            "Parser errors in test: {:?}",
            parser.errors()
        );
        executor
            .execute_statement(statement)
            .expect("Execution failed in test")
    }

    fn execute_fail(executor: &mut Executor, sql: &str) -> String {
        let lexer = Lexer::new(sql);
        let mut parser = SqlParser::new(lexer);
        let statement = parser.parse_program().expect("Parsing failed in test");
        assert!(
            parser.errors().is_empty(),
            "Parser errors in test: {:?}",
            parser.errors()
        );
        executor
            .execute_statement(statement)
            .err()
            .expect("Execution succeeded unexpectedly in test")
    }

    #[test]
    fn test_execute_create_and_insert() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);

        let result_create =
            execute_success(&mut executor, "CREATE TABLE test (id INT, name TEXT);");
        assert!(matches!(result_create, ExecutionResult::Msg(_)));
        if let ExecutionResult::Msg(msg) = result_create {
            assert!(msg.contains("Table 'test' created successfully"));
        }

        let result_insert = execute_success(
            &mut executor,
            "INSERT INTO test VALUES (1, 'one'), (2, 'two');",
        );
        assert!(matches!(result_insert, ExecutionResult::Msg(_)));
        if let ExecutionResult::Msg(msg) = result_insert {
            assert!(msg.contains("Inserted 2 row(s) into 'test'"));
        }

        let result_select = execute_success(&mut executor, "SELECT id, name FROM test;");
        if let ExecutionResult::RowSet { columns, rows } = result_select {
            assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0],
                vec![Value::Integer(1), Value::Text("one".to_string())]
            );
            assert_eq!(
                rows[1],
                vec![Value::Integer(2), Value::Text("two".to_string())]
            );
        } else {
            panic!("Expected RowSet from SELECT");
        }
    }

    #[test]
    fn test_create_table_already_exists() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE existing (col1 INT);");
        let error_msg = execute_fail(&mut executor, "CREATE TABLE existing (col2 TEXT);");
        assert!(error_msg.contains("Table 'existing' already exists"));
    }

    #[test]
    fn test_insert_nonexistent_table() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        let error_msg = execute_fail(&mut executor, "INSERT INTO nope VALUES (1);");
        assert!(error_msg.contains("Table 'nope' not found"));
    }

    #[test]
    fn test_insert_column_mismatch() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE two_cols (a INT, b TEXT);");
        let error_msg = execute_fail(&mut executor, "INSERT INTO two_cols VALUES (1);");
        assert!(error_msg.contains("Column count mismatch"));
        let error_msg_2 = execute_fail(&mut executor, "INSERT INTO two_cols VALUES (1, 'one', 3);");
        assert!(error_msg_2.contains("Column count mismatch"));
    }

    #[test]
    fn test_insert_invalid_literal() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE numbers (n INT);");
        let error_msg = execute_fail(
            &mut executor,
            "INSERT INTO numbers VALUES ('not a number');",
        );
        assert!(error_msg.contains("Invalid integer literal"));
    }

    fn setup_select_test() -> StorageManager {
        let mut storage = StorageManager::new();
        let columns = vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Integer,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ];
        storage.create_table("users".to_string(), columns).unwrap();
        storage
            .insert_row(
                "users",
                vec![Value::Integer(1), Value::Text("Alice".to_string())],
            )
            .unwrap();
        storage
            .insert_row(
                "users",
                vec![Value::Integer(2), Value::Text("Bob".to_string())],
            )
            .unwrap();
        storage
    }

    #[test]
    fn test_execute_select_all() {
        let mut storage = setup_select_test();
        let mut executor = Executor::new(&mut storage);
        let result = execute_success(&mut executor, "SELECT * FROM users;");
        if let ExecutionResult::RowSet { columns, rows } = result {
            assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0],
                vec![Value::Integer(1), Value::Text("Alice".to_string())]
            );
            assert_eq!(
                rows[1],
                vec![Value::Integer(2), Value::Text("Bob".to_string())]
            );
        } else {
            panic!("Expected RowSet");
        }
    }

    #[test]
    fn test_execute_select_specific_columns() {
        let mut storage = setup_select_test();
        let mut executor = Executor::new(&mut storage);
        let result = execute_success(&mut executor, "SELECT name FROM users;");
        if let ExecutionResult::RowSet { columns, rows } = result {
            assert_eq!(columns, vec!["name".to_string()]);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], vec![Value::Text("Alice".to_string())]);
            assert_eq!(rows[1], vec![Value::Text("Bob".to_string())]);
        } else {
            panic!("Expected RowSet");
        }
    }

    #[test]
    fn test_execute_select_nonexistent_table() {
        let mut storage = setup_select_test();
        let mut executor = Executor::new(&mut storage);
        let error_msg = execute_fail(&mut executor, "SELECT name FROM non_existent;");
        assert!(error_msg.contains("Table 'non_existent' not found"));
    }

    #[test]
    fn test_execute_select_nonexistent_column() {
        let mut storage = setup_select_test();
        let mut executor = Executor::new(&mut storage);
        let error_msg = execute_fail(&mut executor, "SELECT email FROM users;");
        assert!(error_msg.contains("Column 'email' not found"));
    }

    #[test]
    fn test_execute_select_with_where_match() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE data (id INT, city TEXT);");
        execute_success(
            &mut executor,
            "INSERT INTO data VALUES (1, 'London'), (2, 'Paris'), (3, 'London');",
        );

        let result = execute_success(&mut executor, "SELECT id FROM data WHERE city = 'London';");
        if let ExecutionResult::RowSet { columns, rows } = result {
            assert_eq!(columns, vec!["id".to_string()]);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], vec![Value::Integer(1)]);
            assert_eq!(rows[1], vec![Value::Integer(3)]);
        } else {
            panic!("Expected RowSet");
        }
    }

    #[test]
    fn test_execute_select_with_where_no_match() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE data (id INT, val INT);");
        execute_success(&mut executor, "INSERT INTO data VALUES (1, 10), (2, 20);");

        let result = execute_success(&mut executor, "SELECT id FROM data WHERE val > 100;");
        if let ExecutionResult::RowSet { columns, rows } = result {
            assert_eq!(columns, vec!["id".to_string()]);
            assert_eq!(rows.len(), 0);
        } else {
            panic!("Expected RowSet");
        }
    }

    #[test]
    fn test_execute_select_where_incompatible_types() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE stuff (id INT, name TEXT);");
        execute_success(&mut executor, "INSERT INTO stuff VALUES (1, 'box');");

        let error_msg = execute_fail(&mut executor, "SELECT name FROM stuff WHERE id = 'box';");
        assert!(error_msg.contains("Cannot compare values of incompatible types"));
    }

    #[test]
    fn test_select_with_where_and_logic() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(
            &mut executor,
            "CREATE TABLE data (id INT, city TEXT, status INT);",
        );
        execute_success(
            &mut executor,
            "INSERT INTO data VALUES (1, 'A', 1), (2, 'B', 0), (3, 'A', 0), (4, 'A', 1);",
        );

        let result1 = execute_success(
            &mut executor,
            "SELECT id FROM data WHERE city = 'A' AND status = 1;",
        );
        if let ExecutionResult::RowSet { rows, .. } = result1 {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], Value::Integer(1));
            assert_eq!(rows[1][0], Value::Integer(4));
        } else {
            panic!();
        }

        let result2 = execute_success(
            &mut executor,
            "SELECT id FROM data WHERE city = 'B' OR status = 1;",
        );
        if let ExecutionResult::RowSet { rows, .. } = result2 {
            assert_eq!(rows.len(), 3);
        } else {
            panic!();
        }

        let result3 = execute_success(
            &mut executor,
            "SELECT id FROM data WHERE city = 'A' AND status = 0 OR city = 'B';",
        );
        if let ExecutionResult::RowSet { rows, .. } = result3 {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], Value::Integer(2));
            assert_eq!(rows[1][0], Value::Integer(3));
        } else {
            panic!();
        }
    }

    #[test]
    fn test_where_not_logic() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE flags (id INT, is_set INT);");
        execute_success(
            &mut executor,
            "INSERT INTO flags VALUES (1, 1), (2, 0), (3, 1);",
        );
        let result = execute_success(&mut executor, "SELECT id FROM flags WHERE NOT is_set = 1;");
        if let ExecutionResult::RowSet { rows, .. } = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(2));
        } else {
            panic!();
        }
    }

    #[test]
    fn test_where_is_null_logic() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE items (id INT, name TEXT);");
        execute_success(
            &mut executor,
            "INSERT INTO items VALUES (1, 'Apple'), (2, NULL), (3, 'Banana');",
        );

        let result_null =
            execute_success(&mut executor, "SELECT id FROM items WHERE name IS NULL;");
        if let ExecutionResult::RowSet { rows, .. } = result_null {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(2));
        } else {
            panic!();
        }

        let result_not_null = execute_success(
            &mut executor,
            "SELECT id FROM items WHERE name IS NOT NULL;",
        );
        if let ExecutionResult::RowSet { rows, .. } = result_not_null {
            assert_eq!(rows.len(), 2);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_where_precedence_basic() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE data (a INT, b INT, c INT);");
        execute_success(
            &mut executor,
            "INSERT INTO data VALUES (1, 1, 1), (1, 0, 1), (0, 1, 1), (0, 0, 1), (1, 1, 0);",
        );

        let result = execute_success(
            &mut executor,
            "SELECT COUNT(*) FROM data WHERE (a = 1 OR b = 1) AND c = 1;",
        );

        assert!(matches!(result, ExecutionResult::RowSet { .. }));

        let result2 = execute_success(
            &mut executor,
            "SELECT COUNT(*) FROM data WHERE NOT (a = 1 OR b = 0);",
        );
        assert!(matches!(result2, ExecutionResult::RowSet { .. }));
    }

    #[test]
    fn test_execute_delete_no_where() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE stuff (id INT);");
        execute_success(&mut executor, "INSERT INTO stuff VALUES (1), (2), (3);");

        drop(executor);

        assert_eq!(storage.get_table("stuff").unwrap().rows.len(), 3);

        let mut executor = Executor::new(&mut storage);
        let result = execute_success(&mut executor, "DELETE FROM stuff;");
        if let ExecutionResult::Msg(msg) = result {
            assert!(msg.contains("Deleted 3 row(s)"));
        } else {
            panic!();
        }
        drop(executor);
        assert_eq!(storage.get_table("stuff").unwrap().rows.len(), 0);
    }

    #[test]
    fn test_execute_delete_with_where() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE data (id INT, city TEXT);");
        execute_success(
            &mut executor,
            "INSERT INTO data VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C');",
        );
        drop(executor);

        assert_eq!(storage.get_table("data").unwrap().rows.len(), 4);

        let mut executor = Executor::new(&mut storage);
        let result = execute_success(
            &mut executor,
            "DELETE FROM data WHERE city = 'A' OR id = 4;",
        );
        if let ExecutionResult::Msg(msg) = result {
            assert!(msg.contains("Deleted 3 row(s)"));
        } else {
            panic!();
        }

        drop(executor);
        let mut executor = Executor::new(&mut storage);
        let remaining = execute_success(&mut executor, "SELECT * FROM data;");
        if let ExecutionResult::RowSet { rows, .. } = remaining {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(2));
            assert_eq!(rows[0][1], Value::Text("B".to_string()));
        } else {
            panic!();
        }
    }

    #[test]
    fn test_execute_delete_nonexistent_table() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        let error_msg = execute_fail(&mut executor, "DELETE FROM missing_table WHERE id = 1;");
        assert!(error_msg.contains("Table 'missing_table' not found"));
    }

    #[test]
    fn test_execute_update_simple() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(
            &mut executor,
            "CREATE TABLE users (id INT, email TEXT, status INT);",
        );
        execute_success(
            &mut executor,
            "INSERT INTO users VALUES (1, 'a@a.com', 0), (2, 'b@b.com', 0), (3, 'c@c.com', 0);",
        );

        let result = execute_success(&mut executor, "UPDATE users SET status = 1 WHERE id = 2;");
        if let ExecutionResult::Msg(msg) = result {
            assert!(msg.contains("Updated 1 row(s)"), "Got: {}", msg);
        } else {
            panic!("Expected Msg result");
        }

        let select_result =
            execute_success(&mut executor, "SELECT status FROM users WHERE id = 2;");
        if let ExecutionResult::RowSet { rows, .. } = select_result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(1));
        } else {
            panic!("Expected RowSet result");
        }

        let select_result_other =
            execute_success(&mut executor, "SELECT status FROM users WHERE id = 1;");
        if let ExecutionResult::RowSet { rows, .. } = select_result_other {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(0));
        } else {
            panic!("Expected RowSet result");
        }
    }

    #[test]
    fn test_execute_update_multiple_set() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(
            &mut executor,
            "CREATE TABLE users (id INT, email TEXT, status INT);",
        );
        execute_success(&mut executor, "INSERT INTO users VALUES (1, 'a@a.com', 0);");

        execute_success(
            &mut executor,
            "UPDATE users SET status = 5, email = 'new@new.com' WHERE id = 1;",
        );

        let select_result = execute_success(
            &mut executor,
            "SELECT email, status FROM users WHERE id = 1;",
        );
        if let ExecutionResult::RowSet { rows, .. } = select_result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Text("new@new.com".to_string()));
            assert_eq!(rows[0][1], Value::Integer(5));
        } else {
            panic!("Expected RowSet result");
        }
    }

    #[test]
    fn test_execute_update_no_where() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE data (val INT);");
        execute_success(&mut executor, "INSERT INTO data VALUES (1), (2), (3);");

        let result = execute_success(&mut executor, "UPDATE data SET val = 99;");
        if let ExecutionResult::Msg(msg) = result {
            assert!(msg.contains("Updated 3 row(s)"), "Got: {}", msg);
        } else {
            panic!("Expected Msg result");
        }

        let select_result = execute_success(&mut executor, "SELECT val FROM data;");
        if let ExecutionResult::RowSet { rows, .. } = select_result {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0][0], Value::Integer(99));
            assert_eq!(rows[1][0], Value::Integer(99));
            assert_eq!(rows[2][0], Value::Integer(99));
        } else {
            panic!("Expected RowSet result");
        }
    }

    #[test]
    fn test_execute_update_nonexistent_table() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        let error = execute_fail(&mut executor, "UPDATE nonexist SET col = 1;");
        assert!(error.contains("Table 'nonexist' not found"));
    }

    #[test]
    fn test_execute_update_nonexistent_column_set() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE data (id INT);");
        execute_success(&mut executor, "INSERT INTO data VALUES (1);");
        let error = execute_fail(&mut executor, "UPDATE data SET nonexist_col = 1;");
        assert!(error.contains("Column 'nonexist_col' not found"));
    }

    #[test]
    fn test_execute_update_nonexistent_column_where() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE data (id INT);");
        execute_success(&mut executor, "INSERT INTO data VALUES (1);");
        let error = execute_fail(
            &mut executor,
            "UPDATE data SET id = 2 WHERE nonexist_col = 1;",
        );
        assert!(error.contains("Column 'nonexist_col' not found during evaluation"));
    }

    #[test]
    fn test_execute_update_type_mismatch() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE data (id INT, name TEXT);");
        execute_success(&mut executor, "INSERT INTO data VALUES (1, 'one');");
        let error = execute_fail(
            &mut executor,
            "UPDATE data SET id = 'two' WHERE name = 'one';",
        );
        assert!(error.contains("Type mismatch for column 'id'"));
    }

    #[test]
    fn test_select_from_sqlr_tables() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE users (id INT);");
        execute_success(&mut executor, "CREATE TABLE products (name TEXT);");

        let result_star = execute_success(&mut executor, "SELECT * FROM SQLR_TABLES;");
        if let ExecutionResult::RowSet { columns, rows } = result_star {
            assert_eq!(columns, vec!["name"]);
            assert_eq!(rows.len(), 2);
            let names: std::collections::HashSet<String> = rows
                .into_iter()
                .map(|row| match row[0] {
                    Value::Text(ref s) => s.clone(),
                    _ => panic!("Expected Text value"),
                })
                .collect();
            assert!(names.contains("users"));
            assert!(names.contains("products"));
        } else {
            panic!("Expected RowSet result for SQLR_TABLES (*)");
        }

        let result_name = execute_success(&mut executor, "SELECT name FROM SQLR_TABLES;");
        if let ExecutionResult::RowSet { columns, rows } = result_name {
            assert_eq!(columns, vec!["name"]);
            assert_eq!(rows.len(), 2);
            let names: std::collections::HashSet<String> = rows
                .into_iter()
                .map(|row| match row[0] {
                    Value::Text(ref s) => s.clone(),
                    _ => panic!("Expected Text value"),
                })
                .collect();
            assert!(names.contains("users"));
            assert!(names.contains("products"));
        } else {
            panic!("Expected RowSet result for SQLR_TABLES (name)");
        }
    }

    #[test]
    fn test_select_from_sqlr_tables_empty() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        let result = execute_success(&mut executor, "SELECT name FROM SQLR_TABLES;");
        if let ExecutionResult::RowSet { columns, rows } = result {
            assert_eq!(columns, vec!["name"]);
            assert_eq!(rows.len(), 0);
        } else {
            panic!("Expected RowSet result for empty SQLR_TABLES");
        }
    }

    #[test]
    fn test_select_unknown_column_sqlr_tables() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::new(&mut storage);
        execute_success(&mut executor, "CREATE TABLE users (id INT);");
        let error = execute_fail(&mut executor, "SELECT non_existent_col FROM SQLR_TABLES;");
        assert!(error.contains("Unknown column 'non_existent_col' in SQLR_TABLES"));
    }
}
