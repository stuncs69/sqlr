#![allow(dead_code)]

use crate::parser::{
    BinaryOperator, CreateTableStatement, DeleteStatement, Expression, FromClause, InsertStatement,
    JoinClause, JoinType, LiteralValue as ParserLiteralValue, ParsedDataType, QualifiedIdentifier,
    SelectItem, Statement, UnaryOperator, UpdateStatement,
};
use crate::storage::{Column, DataType, Row, StorageManager, Table, Value};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, PartialEq, Serialize)]
pub enum ExecutionResult {
    RowSet {
        columns: Vec<String>,
        rows: Vec<Row>,
    },

    Msg(String),
}

pub struct ConnectionState {
    pub connection_id: u32,
}

impl ConnectionState {
    pub fn new() -> Self {
        Self {
            connection_id: rand::random::<u32>(),
        }
    }
}

pub struct Executor<'a> {
    storage: &'a mut StorageManager,
    connection_state: ConnectionState,
}

impl<'a> Executor<'a> {
    pub fn new(storage: &'a mut StorageManager) -> Self {
        Executor {
            storage,
            connection_state: ConnectionState::new(),
        }
    }

    pub fn with_connection_id(storage: &'a mut StorageManager, connection_id: u32) -> Self {
        let mut conn_state = ConnectionState::new();
        conn_state.connection_id = connection_id;
        Executor {
            storage,
            connection_state: conn_state,
        }
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
        let from_clause = statement
            .from_clause
            .as_ref()
            .ok_or_else(|| "FROM clause is required for SELECT".to_string())?;

        if from_clause.base_table.eq_ignore_ascii_case("SQLR_TABLES")
            && from_clause.joins.is_empty()
        {
            return self.execute_select_sqlr_tables(&statement.columns);
        }

        let mut loaded_tables: HashMap<String, &Table> = HashMap::new();
        let mut current_rows: Vec<Row> = Vec::new();
        let mut current_schema: Vec<(String, Column)> = Vec::new();

        let base_table_name = &from_clause.base_table;
        let base_table = self
            .storage
            .get_table(base_table_name)
            .ok_or_else(|| format!("Table '{}' not found", base_table_name))?;
        loaded_tables.insert(base_table_name.clone(), base_table);
        current_rows = base_table.rows.iter().cloned().collect();
        current_schema = base_table
            .columns
            .iter()
            .map(|c| (base_table_name.clone(), c.clone()))
            .collect();

        for join_clause in &from_clause.joins {
            let right_table_name = &join_clause.table;
            let right_table = self
                .storage
                .get_table(right_table_name)
                .ok_or_else(|| format!("Table '{}' not found for JOIN", right_table_name))?;

            if !loaded_tables.contains_key(right_table_name) {
                loaded_tables.insert(right_table_name.clone(), right_table);
            }

            let right_table_schema_part: Vec<(String, Column)> = right_table
                .columns
                .iter()
                .map(|c| (right_table_name.clone(), c.clone()))
                .collect();

            let joined_rows = match join_clause.join_type {
                JoinType::Inner => self.execute_inner_join(
                    &current_rows,
                    &current_schema,
                    right_table,
                    &right_table_schema_part,
                    join_clause.on_condition.as_ref(),
                )?,
                JoinType::LeftOuter => {
                    return Err("LEFT JOIN not yet implemented".to_string());
                }
                JoinType::RightOuter => {
                    return Err("RIGHT JOIN not yet implemented".to_string());
                }
                JoinType::FullOuter => {
                    return Err("FULL JOIN not yet implemented".to_string());
                }
            };

            current_rows = joined_rows;
            current_schema.extend(right_table_schema_part);
        }

        let filtered_rows: Vec<Row> = if let Some(where_expr) = &statement.where_clause {
            println!(
                "[execute_select] Evaluating WHERE clause on final joined data: {:?}",
                where_expr
            );
            let mut matched_rows = Vec::new();
            for row in &current_rows {
                match self.evaluate_condition_on_combined_row(where_expr, &current_schema, row) {
                    Ok(true) => matched_rows.push(row.clone()),
                    Ok(false) => {}
                    Err(e) => {
                        return Err(format!(
                            "WHERE clause evaluation error on joined data: {}",
                            e
                        ));
                    }
                }
            }
            println!(
                "[execute_select] WHERE clause matched {} rows after joins",
                matched_rows.len()
            );
            matched_rows
        } else {
            current_rows
        };

        let (result_columns, result_rows) =
            self.project_columns(&statement.columns, &current_schema, &filtered_rows)?;

        Ok(ExecutionResult::RowSet {
            columns: result_columns,
            rows: result_rows,
        })
    }

    fn execute_select_sqlr_tables(
        &self,
        select_items: &[SelectItem],
    ) -> Result<ExecutionResult, String> {
        println!("[execute_select] Handling meta-table SQLR_TABLES");
        let meta_columns = vec![("name".to_string(), DataType::Text)];

        let requested_col_indices: Vec<usize> = match select_items {
            [SelectItem::Wildcard] => (0..meta_columns.len()).collect(),
            items => {
                let mut indices = Vec::new();
                for item in items {
                    if let SelectItem::Expression { expr, alias: _ } = item {
                        if let Expression::QualifiedIdentifier(qi) = expr {
                            if qi.table.is_none() {
                                let name = &qi.identifier;
                                if let Some(index) = meta_columns
                                    .iter()
                                    .position(|(col_name, _)| col_name.eq_ignore_ascii_case(name))
                                {
                                    indices.push(index);
                                } else {
                                    return Err(format!(
                                        "Unknown column '{}' in SQLR_TABLES",
                                        name
                                    ));
                                }
                            } else {
                                return Err("Qualified column names not supported for SQLR_TABLES"
                                    .to_string());
                            }
                        } else {
                            return Err(
                                "Only simple column names supported for SQLR_TABLES".to_string()
                            );
                        }
                    } else if matches!(item, SelectItem::QualifiedWildcard(_)) {
                        return Err("Qualified wildcard not supported for SQLR_TABLES".to_string());
                    } else {
                        return Err("Unsupported item in SELECT list for SQLR_TABLES".to_string());
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

        Ok(ExecutionResult::RowSet {
            columns: result_columns,
            rows: result_rows,
        })
    }

    fn evaluate_expression_on_combined_row(
        &self,
        expr: &Expression,
        schema: &[(String, Column)],
        row: &Row,
    ) -> Result<Value, String> {
        match expr {
            Expression::Literal(literal) => match literal {
                ParserLiteralValue::Number(s) => s
                    .parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|e| format!("Invalid number literal '{}': {}", s, e)),
                ParserLiteralValue::String(s) => Ok(Value::Text(s.clone())),
                ParserLiteralValue::Null => Ok(Value::Null),
            },
            Expression::QualifiedIdentifier(qi) => self.find_value_in_combined_row(qi, schema, row),
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression_on_combined_row(left, schema, row)?;
                let right_val = self.evaluate_expression_on_combined_row(right, schema, row)?;

                let result_bool = match op {
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
                            _ => unreachable!(),
                        },
                        None => {
                            return Err(format!(
                                "Cannot compare {:?} and {:?}",
                                left_val, right_val
                            ));
                        }
                    },
                    BinaryOperator::And => match (left_val, right_val) {
                        (Value::Integer(l), Value::Integer(r)) => l != 0 && r != 0,
                        _ => false,
                    },
                    BinaryOperator::Or => match (left_val, right_val) {
                        (Value::Integer(l), Value::Integer(r)) => l != 0 || r != 0,
                        _ => false,
                    },
                };
                Ok(Value::Integer(result_bool as i64))
            }
            Expression::UnaryOp { op, expr } => {
                let val = self.evaluate_expression_on_combined_row(expr, schema, row)?;
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
                let val = self.evaluate_expression_on_combined_row(expr, schema, row)?;
                Ok(Value::Integer((val == Value::Null) as i64))
            }
            Expression::IsNotNull(expr) => {
                let val = self.evaluate_expression_on_combined_row(expr, schema, row)?;
                Ok(Value::Integer((val != Value::Null) as i64))
            }
            Expression::FunctionCall { name, arguments } => {
                self.evaluate_function_call(name, arguments, schema, row)
            }
        }
    }

    fn find_value_in_combined_row(
        &self,
        qi: &QualifiedIdentifier,
        schema: &[(String, Column)],
        row: &Row,
    ) -> Result<Value, String> {
        let mut found_index: Option<usize> = None;
        let mut potential_indices: Vec<usize> = Vec::new();

        for (index, (table_name, column)) in schema.iter().enumerate() {
            let table_match = qi.table.as_ref().map_or(true, |t| t == table_name);
            let column_match = qi.identifier == column.name;

            if table_match && column_match {
                potential_indices.push(index);
            }
        }

        if potential_indices.len() == 1 {
            found_index = Some(potential_indices[0]);
        } else if potential_indices.len() > 1 {
            if qi.table.is_none() {
                return Err(format!(
                    "Ambiguous column reference: '{}' exists in multiple tables. Qualify with table name.",
                    qi.identifier
                ));
            } else {
                return Err(format!(
                    "Internal error: Ambiguous column reference '{}' even with qualifier '{}'. Indices: {:?}",
                    qi.identifier,
                    qi.table.as_ref().unwrap_or(&String::from("?")),
                    potential_indices
                ));
            }
        }

        match found_index {
            Some(index) if index < row.len() => Ok(row[index].clone()),
            Some(index) => Err(format!(
                "Internal error: Schema index {} out of bounds for row with length {}",
                index,
                row.len()
            )),
            None => Err(format!(
                "Column '{}'{} not found in the available tables.",
                qi.identifier,
                qi.table
                    .as_ref()
                    .map_or(String::new(), |t| format!(" (qualified by '{}')", t))
            )),
        }
    }

    fn evaluate_condition_on_combined_row(
        &self,
        expr: &Expression,
        schema: &[(String, Column)],
        row: &Row,
    ) -> Result<bool, String> {
        match self.evaluate_expression_on_combined_row(expr, schema, row)? {
            Value::Integer(i) => Ok(i != 0),
            Value::Text(_) => Err("WHERE condition evaluated to a TEXT value".to_string()),
            Value::Null => Ok(false),
        }
    }

    fn project_columns(
        &self,
        select_items: &[SelectItem],
        combined_schema: &[(String, Column)],
        filtered_rows: &[Row],
    ) -> Result<(Vec<String>, Vec<Row>), String> {
        let mut result_columns: Vec<String> = Vec::new();
        let mut projections: Vec<Box<dyn Fn(&Row) -> Result<Value, String>>> = Vec::new();

        match select_items {
            [SelectItem::Wildcard] => {
                for (i, (table_name, column)) in combined_schema.iter().enumerate() {
                    result_columns.push(format!("{}.{}", table_name, column.name));
                    projections.push(Box::new(move |row: &Row| Ok(row[i].clone())));
                }
            }
            items => {
                for item in items {
                    match item {
                        SelectItem::Expression { expr, alias } => {
                            let projection_expr = expr.clone();
                            let schema_clone = combined_schema.to_vec();
                            projections.push(Box::new(move |row: &Row| {
                                self.evaluate_expression_on_combined_row(
                                    &projection_expr,
                                    &schema_clone,
                                    row,
                                )
                            }));

                            let column_name = alias.clone().unwrap_or_else(|| match expr {
                                Expression::QualifiedIdentifier(qi) => qi.identifier.clone(),
                                _ => format!("expr_{}", result_columns.len() + 1),
                            });
                            result_columns.push(column_name);
                        }
                        SelectItem::QualifiedWildcard(table_name_prefix) => {
                            let mut found_match = false;
                            for (i, (table_name, column)) in combined_schema.iter().enumerate() {
                                if table_name == table_name_prefix {
                                    result_columns.push(format!("{}.{}", table_name, column.name));
                                    projections.push(Box::new(move |row: &Row| Ok(row[i].clone())));
                                    found_match = true;
                                }
                            }
                            if !found_match {
                                return Err(format!(
                                    "Table prefix '{}' in wildcard not found in FROM/JOIN clause",
                                    table_name_prefix
                                ));
                            }
                        }
                        SelectItem::Wildcard => {
                            return Err("Unexpected '*' after other select items".to_string());
                        }
                    }
                }
            }
        }

        let mut result_rows: Vec<Row> = Vec::with_capacity(filtered_rows.len());
        for row in filtered_rows {
            let projected_row: Result<Row, String> =
                projections.iter().map(|proj_fn| proj_fn(row)).collect();
            result_rows.push(projected_row?);
        }

        Ok((result_columns, result_rows))
    }

    fn execute_inner_join(
        &self,
        left_rows: &[Row],
        left_schema: &[(String, Column)],
        right_table: &Table,
        right_schema_part: &[(String, Column)],
        on_condition: Option<&Expression>,
    ) -> Result<Vec<Row>, String> {
        let on_expr = on_condition.ok_or("ON condition is required for INNER JOIN")?;

        let mut joined_rows = Vec::new();
        let right_rows = &right_table.rows;

        let mut combined_schema_for_on = left_schema.to_vec();
        combined_schema_for_on.extend_from_slice(right_schema_part);

        for left_row in left_rows {
            for right_row in right_rows {
                let mut temp_combined_row = left_row.clone();
                temp_combined_row.extend(right_row.clone());

                match self.evaluate_condition_on_combined_row(
                    on_expr,
                    &combined_schema_for_on,
                    &temp_combined_row,
                ) {
                    Ok(true) => {
                        joined_rows.push(temp_combined_row);
                    }
                    Ok(false) => {}
                    Err(e) => {
                        return Err(format!("Error evaluating JOIN ON condition: {}", e));
                    }
                }
            }
        }

        Ok(joined_rows)
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
                    Expression::QualifiedIdentifier(qi) => Err(format!(
                        "Unexpected qualified identifier '{}' in VALUES clause",
                        qi.identifier
                    )),
                    _ => Err(format!(
                        "Unsupported expression {:?} in VALUES clause. Only literals are allowed.",
                        expr
                    )),
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
        let table_name = &statement.table_name;

        let indices_to_delete = {
            let table = self
                .storage
                .get_table(table_name)
                .ok_or_else(|| format!("Table '{}' not found for DELETE", table_name))?;

            let single_table_schema: Vec<(String, Column)> = table
                .columns
                .iter()
                .map(|c| (table_name.clone(), c.clone()))
                .collect();

            let mut indices = Vec::new();
            if let Some(where_expr) = &statement.where_clause {
                println!("[execute_delete] Evaluating WHERE clause: {:?}", where_expr);
                for (index, row) in table.rows.iter().enumerate() {
                    match self.evaluate_condition_on_combined_row(
                        where_expr,
                        &single_table_schema,
                        row,
                    ) {
                        Ok(true) => indices.push(index),
                        Ok(false) => {}
                        Err(e) => return Err(format!("DELETE WHERE clause error: {}", e)),
                    }
                }
                println!(
                    "[execute_delete] Found {} rows matching WHERE clause",
                    indices.len()
                );
            } else {
                println!(
                    "[execute_delete] No WHERE clause, deleting all {} rows from '{}'",
                    table.rows.len(),
                    table_name
                );
                indices = (0..table.rows.len()).collect();
            }
            indices
        };

        let deleted_count = self.storage.delete_rows(table_name, indices_to_delete)?;

        Ok(ExecutionResult::Msg(format!(
            "Deleted {} row(s)",
            deleted_count
        )))
    }

    fn execute_update(&mut self, statement: UpdateStatement) -> Result<ExecutionResult, String> {
        let table_name = &statement.table_name;

        let updates_to_perform = {
            let table = self
                .storage
                .get_table(table_name)
                .ok_or_else(|| format!("Table '{}' not found for UPDATE", table_name))?;

            let single_table_schema: Vec<(String, Column)> = table
                .columns
                .iter()
                .map(|c| (table_name.clone(), c.clone()))
                .collect();

            let col_indices_to_update: Vec<usize> = statement
                .assignments
                .iter()
                .map(|(col_name, _)| {
                    table
                        .columns
                        .iter()
                        .position(|c| c.name == *col_name)
                        .ok_or_else(|| {
                            format!("Column '{}' not found in table '{}'", col_name, table_name)
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let mut updates: Vec<(usize, Vec<(usize, Value)>)> = Vec::new();

            for (row_index, row) in table.rows.iter().enumerate() {
                let should_update = if let Some(where_expr) = &statement.where_clause {
                    self.evaluate_condition_on_combined_row(where_expr, &single_table_schema, row)?
                } else {
                    true
                };

                if should_update {
                    let mut row_updates: Vec<(usize, Value)> = Vec::new();
                    for (i, (_, value_expr)) in statement.assignments.iter().enumerate() {
                        let col_index = col_indices_to_update[i];
                        let new_value = self.evaluate_expression_on_combined_row(
                            value_expr,
                            &single_table_schema,
                            row,
                        )?;
                        let expected_type = table.columns[col_index].data_type.clone();

                        let compatible_value = match (&new_value, expected_type) {
                            (Value::Integer(v), DataType::Integer) => Value::Integer(*v),
                            (Value::Text(s), DataType::Text) => Value::Text(s.clone()),
                            (Value::Null, _) => Value::Null,
                            (v, dt) => {
                                return Err(format!(
                                    "Type mismatch for column '{}': expected {:?}, got {:?}",
                                    table.columns[col_index].name, dt, v
                                ));
                            }
                        };
                        row_updates.push((col_index, compatible_value));
                    }
                    if !row_updates.is_empty() {
                        updates.push((row_index, row_updates));
                    }
                }
            }
            updates
        };

        let mut updated_count = 0;
        for (row_index, row_updates) in updates_to_perform {
            self.storage
                .update_row_values(table_name, row_index, &row_updates)?;
            updated_count += 1;
        }

        Ok(ExecutionResult::Msg(format!(
            "Updated {} row(s)",
            updated_count
        )))
    }

    fn evaluate_function_call(
        &self,
        function_name: &str,
        arguments: &[Expression],
        schema: &[(String, Column)],
        row: &Row,
    ) -> Result<Value, String> {
        match function_name.to_uppercase().as_str() {
            "CONNECTION_ID" => {
                if !arguments.is_empty() {
                    return Err(format!(
                        "CONNECTION_ID() takes no arguments, but {} provided",
                        arguments.len()
                    ));
                }
                Ok(Value::Integer(self.connection_state.connection_id as i64))
            }
            "VERSION" => {
                if !arguments.is_empty() {
                    return Err(format!(
                        "VERSION() takes no arguments, but {} provided",
                        arguments.len()
                    ));
                }
                Ok(Value::Text("SQLR 0.1.0".to_string()))
            }
            "DATABASE" => {
                if !arguments.is_empty() {
                    return Err(format!(
                        "DATABASE() takes no arguments, but {} provided",
                        arguments.len()
                    ));
                }
                Ok(Value::Text("sqlr_db".to_string()))
            }
            "USER" | "CURRENT_USER" => {
                if !arguments.is_empty() {
                    return Err(format!(
                        "USER() takes no arguments, but {} provided",
                        arguments.len()
                    ));
                }
                Ok(Value::Text("root@localhost".to_string()))
            }
            "NOW" => {
                if !arguments.is_empty() {
                    return Err(format!(
                        "NOW() takes no arguments, but {} provided",
                        arguments.len()
                    ));
                }
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                Ok(Value::Text(now.to_string()))
            }
            _ => Err(format!("Unknown function: {}", function_name)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser;
    use crate::parser::Lexer;
    use crate::storage::StorageManager;

    fn execute_success(executor: &mut Executor, sql: &str) -> ExecutionResult {
        let lexer = Lexer::new(sql);
        let mut parser = parser::Parser::new(lexer);
        let ast = parser.parse_program().unwrap();
        executor.execute_statement(ast).unwrap()
    }

    fn execute_fail(executor: &mut Executor, sql: &str) -> String {
        let lexer = Lexer::new(sql);
        let mut parser = parser::Parser::new(lexer);
        let ast = parser.parse_program().unwrap();
        match executor.execute_statement(ast) {
            Ok(_) => panic!("Expected execution to fail for SQL: {}", sql),
            Err(e) => e,
        }
    }

    #[test]
    fn test_execute_select_system_functions() {
        let mut storage = StorageManager::new();
        let mut executor = Executor::with_connection_id(&mut storage, 42);

        // CONNECTION_ID
        match execute_success(&mut executor, "SELECT CONNECTION_ID();") {
            ExecutionResult::RowSet { columns, rows } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0], "CONNECTION_ID()");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(42));
            }
            _ => panic!("Expected RowSet result"),
        }

        // VERSION
        match execute_success(&mut executor, "SELECT VERSION();") {
            ExecutionResult::RowSet { columns, rows } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0], "VERSION()");
                assert_eq!(rows.len(), 1);
                assert!(matches!(rows[0][0], Value::Text(_)));
            }
            _ => panic!("Expected RowSet result"),
        }
    }
}
