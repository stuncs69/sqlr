#![allow(dead_code)]

use crate::parser::{
    BinaryOperator, CreateTableStatement, DeleteStatement, DropTableStatement, Expression,
    FromClause, InsertStatement, JoinClause, JoinType, LiteralValue as ParserLiteralValue,
    OrderByClause, ParsedDataType, QualifiedIdentifier, SelectItem, Statement, UnaryOperator,
    UpdateStatement,
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
            Statement::DropTable(drop_stmt) => self.execute_drop_table(drop_stmt),
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
                JoinType::LeftOuter => self.execute_left_join(
                    &current_rows,
                    &current_schema,
                    right_table,
                    &right_table_schema_part,
                    join_clause.on_condition.as_ref(),
                )?,
                JoinType::RightOuter => self.execute_right_join(
                    &current_rows,
                    &current_schema,
                    right_table,
                    &right_table_schema_part,
                    join_clause.on_condition.as_ref(),
                )?,
                JoinType::FullOuter => self.execute_full_outer_join(
                    &current_rows,
                    &current_schema,
                    right_table,
                    &right_table_schema_part,
                    join_clause.on_condition.as_ref(),
                )?,
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

        let has_aggregates = statement.columns.iter().any(|item| {
            if let SelectItem::Expression { expr, .. } = item {
                matches!(expr, Expression::FunctionCall { name, .. }
                    if matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"))
            } else {
                false
            }
        });

        let (result_columns, mut result_rows) = if !statement.group_by.is_empty() || has_aggregates {
            self.execute_group_by(
                &statement.columns,
                &current_schema,
                &filtered_rows,
                &statement.group_by,
                &statement.having,
            )?
        } else {
            self.project_columns(&statement.columns, &current_schema, &filtered_rows)?
        };

        if statement.distinct {
            result_rows = self.apply_distinct(result_rows);
        }

        if !statement.order_by.is_empty() {
            result_rows = self.apply_order_by(result_rows, &result_columns, &statement.order_by)?;
        }

        if let Some(offset) = statement.offset {
            if offset < result_rows.len() {
                result_rows = result_rows[offset..].to_vec();
            } else {
                result_rows = Vec::new();
            }
        }

        if let Some(limit) = statement.limit {
            if limit < result_rows.len() {
                result_rows.truncate(limit);
            }
        }

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

    fn execute_left_join(
        &self,
        left_rows: &[Row],
        left_schema: &[(String, Column)],
        right_table: &Table,
        right_schema_part: &[(String, Column)],
        on_condition: Option<&Expression>,
    ) -> Result<Vec<Row>, String> {
        let on_expr = on_condition.ok_or("ON condition is required for LEFT JOIN")?;
        let mut joined_rows = Vec::new();
        let right_rows = &right_table.rows;

        let mut combined_schema_for_on = left_schema.to_vec();
        combined_schema_for_on.extend_from_slice(right_schema_part);

        for left_row in left_rows {
            let mut matched = false;

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
                        matched = true;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        return Err(format!("Error evaluating LEFT JOIN ON condition: {}", e));
                    }
                }
            }

            if !matched {
                let mut row_with_nulls = left_row.clone();
                row_with_nulls.extend(vec![Value::Null; right_schema_part.len()]);
                joined_rows.push(row_with_nulls);
            }
        }

        Ok(joined_rows)
    }

    fn execute_right_join(
        &self,
        left_rows: &[Row],
        left_schema: &[(String, Column)],
        right_table: &Table,
        right_schema_part: &[(String, Column)],
        on_condition: Option<&Expression>,
    ) -> Result<Vec<Row>, String> {
        let on_expr = on_condition.ok_or("ON condition is required for RIGHT JOIN")?;
        let mut joined_rows = Vec::new();
        let right_rows = &right_table.rows;

        let mut combined_schema_for_on = left_schema.to_vec();
        combined_schema_for_on.extend_from_slice(right_schema_part);

        let mut matched_right_indices: Vec<bool> = vec![false; right_rows.len()];

        for left_row in left_rows {
            for (right_idx, right_row) in right_rows.iter().enumerate() {
                let mut temp_combined_row = left_row.clone();
                temp_combined_row.extend(right_row.clone());

                match self.evaluate_condition_on_combined_row(
                    on_expr,
                    &combined_schema_for_on,
                    &temp_combined_row,
                ) {
                    Ok(true) => {
                        joined_rows.push(temp_combined_row);
                        matched_right_indices[right_idx] = true;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        return Err(format!("Error evaluating RIGHT JOIN ON condition: {}", e));
                    }
                }
            }
        }

        for (right_idx, right_row) in right_rows.iter().enumerate() {
            if !matched_right_indices[right_idx] {
                let mut row_with_nulls = vec![Value::Null; left_schema.len()];
                row_with_nulls.extend(right_row.clone());
                joined_rows.push(row_with_nulls);
            }
        }

        Ok(joined_rows)
    }

    fn execute_full_outer_join(
        &self,
        left_rows: &[Row],
        left_schema: &[(String, Column)],
        right_table: &Table,
        right_schema_part: &[(String, Column)],
        on_condition: Option<&Expression>,
    ) -> Result<Vec<Row>, String> {
        let on_expr = on_condition.ok_or("ON condition is required for FULL OUTER JOIN")?;
        let mut joined_rows = Vec::new();
        let right_rows = &right_table.rows;

        let mut combined_schema_for_on = left_schema.to_vec();
        combined_schema_for_on.extend_from_slice(right_schema_part);

        let mut matched_right_indices: Vec<bool> = vec![false; right_rows.len()];

        for left_row in left_rows {
            let mut matched = false;

            for (right_idx, right_row) in right_rows.iter().enumerate() {
                let mut temp_combined_row = left_row.clone();
                temp_combined_row.extend(right_row.clone());

                match self.evaluate_condition_on_combined_row(
                    on_expr,
                    &combined_schema_for_on,
                    &temp_combined_row,
                ) {
                    Ok(true) => {
                        joined_rows.push(temp_combined_row);
                        matched = true;
                        matched_right_indices[right_idx] = true;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        return Err(format!("Error evaluating FULL OUTER JOIN ON condition: {}", e));
                    }
                }
            }

            if !matched {
                let mut row_with_nulls = left_row.clone();
                row_with_nulls.extend(vec![Value::Null; right_schema_part.len()]);
                joined_rows.push(row_with_nulls);
            }
        }

        for (right_idx, right_row) in right_rows.iter().enumerate() {
            if !matched_right_indices[right_idx] {
                let mut row_with_nulls = vec![Value::Null; left_schema.len()];
                row_with_nulls.extend(right_row.clone());
                joined_rows.push(row_with_nulls);
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

            "UPPER" => {
                if arguments.len() != 1 {
                    return Err(format!(
                        "UPPER() requires exactly 1 argument, but {} provided",
                        arguments.len()
                    ));
                }
                let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                match val {
                    Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                    Value::Null => Ok(Value::Null),
                    _ => Err("UPPER() requires a text argument".to_string()),
                }
            }
            "LOWER" => {
                if arguments.len() != 1 {
                    return Err(format!(
                        "LOWER() requires exactly 1 argument, but {} provided",
                        arguments.len()
                    ));
                }
                let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                match val {
                    Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                    Value::Null => Ok(Value::Null),
                    _ => Err("LOWER() requires a text argument".to_string()),
                }
            }
            "LENGTH" => {
                if arguments.len() != 1 {
                    return Err(format!(
                        "LENGTH() requires exactly 1 argument, but {} provided",
                        arguments.len()
                    ));
                }
                let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                match val {
                    Value::Text(s) => Ok(Value::Integer(s.len() as i64)),
                    Value::Null => Ok(Value::Null),
                    _ => Err("LENGTH() requires a text argument".to_string()),
                }
            }
            "SUBSTRING" | "SUBSTR" => {
                if arguments.len() < 2 || arguments.len() > 3 {
                    return Err(format!(
                        "SUBSTRING() requires 2 or 3 arguments, but {} provided",
                        arguments.len()
                    ));
                }
                let text_val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                let start_val = self.evaluate_expression_on_combined_row(&arguments[1], schema, row)?;

                match (text_val, start_val) {
                    (Value::Text(s), Value::Integer(start)) => {
                        let start_idx = (start.max(1) - 1) as usize; // SQL uses 1-based indexing
                        if arguments.len() == 3 {
                            let len_val = self.evaluate_expression_on_combined_row(&arguments[2], schema, row)?;
                            if let Value::Integer(len) = len_val {
                                let len = len.max(0) as usize;
                                let end_idx = (start_idx + len).min(s.len());
                                if start_idx < s.len() {
                                    Ok(Value::Text(s[start_idx..end_idx].to_string()))
                                } else {
                                    Ok(Value::Text(String::new()))
                                }
                            } else {
                                Err("SUBSTRING() length must be an integer".to_string())
                            }
                        } else {
                            if start_idx < s.len() {
                                Ok(Value::Text(s[start_idx..].to_string()))
                            } else {
                                Ok(Value::Text(String::new()))
                            }
                        }
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err("SUBSTRING() requires text and integer arguments".to_string()),
                }
            }
            "CONCAT" => {
                if arguments.is_empty() {
                    return Err("CONCAT() requires at least 1 argument".to_string());
                }
                let mut result = String::new();
                for arg in arguments {
                    let val = self.evaluate_expression_on_combined_row(arg, schema, row)?;
                    match val {
                        Value::Text(s) => result.push_str(&s),
                        Value::Integer(i) => result.push_str(&i.to_string()),
                        Value::Null => {}, // NULL values are ignored in CONCAT
                    }
                }
                Ok(Value::Text(result))
            }
            "TRIM" => {
                if arguments.len() != 1 {
                    return Err(format!(
                        "TRIM() requires exactly 1 argument, but {} provided",
                        arguments.len()
                    ));
                }
                let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                match val {
                    Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Err("TRIM() requires a text argument".to_string()),
                }
            }

            "ABS" => {
                if arguments.len() != 1 {
                    return Err(format!(
                        "ABS() requires exactly 1 argument, but {} provided",
                        arguments.len()
                    ));
                }
                let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                match val {
                    Value::Integer(i) => Ok(Value::Integer(i.abs())),
                    Value::Null => Ok(Value::Null),
                    _ => Err("ABS() requires a numeric argument".to_string()),
                }
            }
            "ROUND" => {
                if arguments.len() != 1 {
                    return Err(format!(
                        "ROUND() requires exactly 1 argument, but {} provided",
                        arguments.len()
                    ));
                }
                let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                match val {
                    Value::Integer(i) => Ok(Value::Integer(i)), // Already integer
                    Value::Null => Ok(Value::Null),
                    _ => Err("ROUND() requires a numeric argument".to_string()),
                }
            }
            "CEIL" | "CEILING" => {
                if arguments.len() != 1 {
                    return Err(format!(
                        "CEIL() requires exactly 1 argument, but {} provided",
                        arguments.len()
                    ));
                }
                let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                match val {
                    Value::Integer(i) => Ok(Value::Integer(i)),
                    Value::Null => Ok(Value::Null),
                    _ => Err("CEIL() requires a numeric argument".to_string()),
                }
            }
            "FLOOR" => {
                if arguments.len() != 1 {
                    return Err(format!(
                        "FLOOR() requires exactly 1 argument, but {} provided",
                        arguments.len()
                    ));
                }
                let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                match val {
                    Value::Integer(i) => Ok(Value::Integer(i)),
                    Value::Null => Ok(Value::Null),
                    _ => Err("FLOOR() requires a numeric argument".to_string()),
                }
            }
            "RANDOM" | "RAND" => {
                if !arguments.is_empty() {
                    return Err(format!(
                        "RANDOM() takes no arguments, but {} provided",
                        arguments.len()
                    ));
                }
                Ok(Value::Integer(rand::random::<i32>().abs() as i64))
            }

            _ => Err(format!("Unknown function: {}", function_name)),
        }
    }

    fn apply_distinct(&self, rows: Vec<Row>) -> Vec<Row> {
        let mut seen = std::collections::HashSet::new();
        let mut distinct_rows = Vec::new();

        for row in rows {
            let row_key = format!("{:?}", row);
            if seen.insert(row_key) {
                distinct_rows.push(row);
            }
        }

        distinct_rows
    }

    fn apply_order_by(
        &self,
        mut rows: Vec<Row>,
        column_names: &[String],
        order_by_clauses: &[OrderByClause],
    ) -> Result<Vec<Row>, String> {
        let column_map: HashMap<String, usize> = column_names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        rows.sort_by(|a, b| {
            for clause in order_by_clauses {
                let column_name = match &clause.expr {
                    Expression::QualifiedIdentifier(qi) => &qi.identifier,
                    _ => continue, // Skip non-column expressions for now
                };

                let col_idx = match column_map.get(column_name) {
                    Some(&idx) => idx,
                    None => continue,
                };

                if col_idx >= a.len() || col_idx >= b.len() {
                    continue;
                }

                let ordering = a[col_idx].partial_cmp(&b[col_idx]);
                match ordering {
                    Some(std::cmp::Ordering::Equal) => continue,
                    Some(ord) => {
                        return if clause.ascending { ord } else { ord.reverse() };
                    }
                    None => continue,
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    fn execute_group_by(
        &self,
        select_items: &[SelectItem],
        schema: &[(String, Column)],
        rows: &[Row],
        group_by_exprs: &[Expression],
        having: &Option<Expression>,
    ) -> Result<(Vec<String>, Vec<Row>), String> {
        let mut groups: HashMap<Vec<Value>, Vec<Row>> = HashMap::new();

        if group_by_exprs.is_empty() {
            groups.insert(Vec::new(), rows.to_vec());
        } else {
            for row in rows {
                let mut group_key = Vec::new();
                for expr in group_by_exprs {
                    let value = self.evaluate_expression_on_combined_row(expr, schema, row)?;
                    group_key.push(value);
                }
                groups.entry(group_key).or_insert_with(Vec::new).push(row.clone());
            }
        }

        let mut result_columns = Vec::new();
        let mut result_rows = Vec::new();

        for item in select_items {
            match item {
                SelectItem::Expression { expr, alias } => {
                    let col_name = alias.clone().unwrap_or_else(|| match expr {
                        Expression::FunctionCall { name, .. } => format!("{}()", name),
                        Expression::QualifiedIdentifier(qi) => qi.identifier.clone(),
                        _ => format!("expr_{}", result_columns.len() + 1),
                    });
                    result_columns.push(col_name);
                }
                SelectItem::Wildcard => {
                    return Err("Wildcard not supported with GROUP BY".to_string());
                }
                SelectItem::QualifiedWildcard(_) => {
                    return Err("Qualified wildcard not supported with GROUP BY".to_string());
                }
            }
        }

        for (group_key, group_rows) in groups {
            let mut result_row = Vec::new();

            for item in select_items {
                if let SelectItem::Expression { expr, .. } = item {
                    let value = self.evaluate_aggregate_expression(expr, schema, &group_rows, &group_key, group_by_exprs)?;
                    result_row.push(value);
                }
            }

            if let Some(having_expr) = having {
                let having_result = self.evaluate_having_expression(having_expr, select_items, schema, &group_rows, &group_key, group_by_exprs)?;
                if !having_result {
                    continue;
                }
            }

            result_rows.push(result_row);
        }

        Ok((result_columns, result_rows))
    }

    fn evaluate_aggregate_expression(
        &self,
        expr: &Expression,
        schema: &[(String, Column)],
        group_rows: &[Row],
        group_key: &[Value],
        group_by_exprs: &[Expression],
    ) -> Result<Value, String> {
        match expr {
            Expression::FunctionCall { name, arguments } => {
                match name.to_uppercase().as_str() {
                    "COUNT" => {
                        if arguments.is_empty() || matches!(arguments[0], Expression::QualifiedIdentifier(ref qi) if qi.identifier == "*") {
                            Ok(Value::Integer(group_rows.len() as i64))
                        } else {
                            // COUNT(column) - count non-null values
                            let mut count = 0i64;
                            for row in group_rows {
                                let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                                if val != Value::Null {
                                    count += 1;
                                }
                            }
                            Ok(Value::Integer(count))
                        }
                    }
                    "SUM" => {
                        if arguments.is_empty() {
                            return Err("SUM requires an argument".to_string());
                        }
                        let mut sum = 0i64;
                        for row in group_rows {
                            let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                            match val {
                                Value::Integer(i) => sum += i,
                                Value::Null => {} // Skip nulls
                                _ => return Err("SUM requires numeric values".to_string()),
                            }
                        }
                        Ok(Value::Integer(sum))
                    }
                    "AVG" => {
                        if arguments.is_empty() {
                            return Err("AVG requires an argument".to_string());
                        }
                        let mut sum = 0i64;
                        let mut count = 0i64;
                        for row in group_rows {
                            let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                            match val {
                                Value::Integer(i) => {
                                    sum += i;
                                    count += 1;
                                }
                                Value::Null => {} // Skip nulls
                                _ => return Err("AVG requires numeric values".to_string()),
                            }
                        }
                        if count == 0 {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Integer(sum / count))
                        }
                    }
                    "MIN" => {
                        if arguments.is_empty() {
                            return Err("MIN requires an argument".to_string());
                        }
                        let mut min_val: Option<Value> = None;
                        for row in group_rows {
                            let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                            if val == Value::Null {
                                continue;
                            }
                            min_val = Some(match min_val {
                                None => val,
                                Some(ref current_min) => {
                                    if val < *current_min {
                                        val
                                    } else {
                                        current_min.clone()
                                    }
                                }
                            });
                        }
                        Ok(min_val.unwrap_or(Value::Null))
                    }
                    "MAX" => {
                        if arguments.is_empty() {
                            return Err("MAX requires an argument".to_string());
                        }
                        let mut max_val: Option<Value> = None;
                        for row in group_rows {
                            let val = self.evaluate_expression_on_combined_row(&arguments[0], schema, row)?;
                            if val == Value::Null {
                                continue;
                            }
                            max_val = Some(match max_val {
                                None => val,
                                Some(ref current_max) => {
                                    if val > *current_max {
                                        val
                                    } else {
                                        current_max.clone()
                                    }
                                }
                            });
                        }
                        Ok(max_val.unwrap_or(Value::Null))
                    }
                    _ => self.evaluate_function_call(name, arguments, schema, &group_rows[0]),
                }
            }
            Expression::QualifiedIdentifier(qi) => {
                // This should be a GROUP BY column - return the group key value
                for (i, group_expr) in group_by_exprs.iter().enumerate() {
                    if let Expression::QualifiedIdentifier(group_qi) = group_expr {
                        if group_qi.identifier == qi.identifier {
                            return Ok(group_key[i].clone());
                        }
                    }
                }
                Err(format!("Column '{}' must appear in GROUP BY clause or be used in an aggregate function", qi.identifier))
            }
            _ => Err("Only aggregate functions and GROUP BY columns allowed in SELECT with GROUP BY".to_string()),
        }
    }

    fn evaluate_having_expression(
        &self,
        expr: &Expression,
        select_items: &[SelectItem],
        schema: &[(String, Column)],
        group_rows: &[Row],
        group_key: &[Value],
        group_by_exprs: &[Expression],
    ) -> Result<bool, String> {
        let value = self.evaluate_having_value(expr, select_items, schema, group_rows, group_key, group_by_exprs)?;
        match value {
            Value::Integer(i) => Ok(i != 0),
            _ => Err("HAVING clause must evaluate to a boolean".to_string()),
        }
    }

    fn evaluate_having_value(
        &self,
        expr: &Expression,
        select_items: &[SelectItem],
        schema: &[(String, Column)],
        group_rows: &[Row],
        group_key: &[Value],
        group_by_exprs: &[Expression],
    ) -> Result<Value, String> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_having_value(left, select_items, schema, group_rows, group_key, group_by_exprs)?;
                let right_val = self.evaluate_having_value(right, select_items, schema, group_rows, group_key, group_by_exprs)?;

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
                        None => false,
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
            Expression::Literal(lit) => match lit {
                ParserLiteralValue::Number(s) => s
                    .parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|e| format!("Invalid number: {}", e)),
                ParserLiteralValue::String(s) => Ok(Value::Text(s.clone())),
                ParserLiteralValue::Null => Ok(Value::Null),
            },
            Expression::FunctionCall { .. } | Expression::QualifiedIdentifier(_) => {
                self.evaluate_aggregate_expression(expr, schema, group_rows, group_key, group_by_exprs)
            }
            _ => Err("Unsupported expression in HAVING clause".to_string()),
        }
    }

    fn execute_drop_table(&mut self, statement: DropTableStatement) -> Result<ExecutionResult, String> {
        self.storage.drop_table(&statement.table_name)?;
        Ok(ExecutionResult::Msg(format!(
            "Table '{}' dropped successfully",
            statement.table_name
        )))
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
