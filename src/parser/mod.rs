#![allow(dead_code)]

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Create,
    Table,

    Identifier(String),
    Number(String),
    String(String),
    Comma,
    Semicolon,
    Asterisk,
    Equal,
    LeftParen,
    RightParen,

    Illegal(char),
    Eof,

    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    And,
    Or,
    Not,
    Is,
    Null,
    Delete,

    Update,
    Set,

    Join,
    Inner,
    Left,
    Right,
    Full,
    Outer,
    On,

    Dot,

    As,

    Function(String),
}

pub struct Lexer<'a> {
    input: &'a str,
    position: usize,
    read_position: usize,
    ch: Option<char>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        let mut lexer = Lexer {
            input,
            position: 0,
            read_position: 0,
            ch: None,
        };
        lexer.read_char();
        lexer
    }

    fn read_char(&mut self) {
        if self.read_position >= self.input.len() {
            self.ch = None;
        } else {
            self.ch = self.input.chars().nth(self.read_position);
        }
        self.position = self.read_position;
        self.read_position += 1;
    }

    fn peek_char(&self) -> Option<char> {
        if self.read_position >= self.input.len() {
            None
        } else {
            self.input.chars().nth(self.read_position)
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.ch {
            if ch.is_whitespace() {
                self.read_char();
            } else {
                break;
            }
        }
    }

    fn read_identifier(&mut self) -> String {
        let start_pos = self.position;
        while let Some(ch) = self.ch {
            if ch.is_alphanumeric() || ch == '_' {
                self.read_char();
            } else {
                break;
            }
        }
        self.input[start_pos..self.position].to_string()
    }

    fn read_number(&mut self) -> String {
        let start_pos = self.position;
        while let Some(ch) = self.ch {
            if ch.is_digit(10) {
                self.read_char();
            } else {
                break;
            }
        }
        self.input[start_pos..self.position].to_string()
    }

    fn read_string(&mut self, quote_char: char) -> String {
        let start_pos = self.position + 1;
        self.read_char();
        while let Some(ch) = self.ch {
            if ch == quote_char {
                break;
            }

            self.read_char();
        }
        let result = self.input[start_pos..self.position].to_string();
        self.read_char();
        result
    }

    fn lookup_ident(ident: &str) -> Token {
        let upper_ident = ident.to_uppercase();

        let common_functions = [
            "CONNECTION_ID",
            "VERSION",
            "DATABASE",
            "USER",
            "CURRENT_USER",
            "NOW",
            "CURDATE",
            "CURTIME",
        ];

        if common_functions.contains(&upper_ident.as_str()) {
            return Token::Function(upper_ident);
        }

        match upper_ident.as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "DELETE" => Token::Delete,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "JOIN" => Token::Join,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "FULL" => Token::Full,
            "OUTER" => Token::Outer,
            "ON" => Token::On,
            "AS" => Token::As,
            _ => Token::Identifier(ident.to_string()),
        }
    }

    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        let tok = match self.ch {
            Some('=') => {
                self.read_char();
                Token::Equal
            }
            Some('<') => {
                if self.peek_char() == Some('>') {
                    self.read_char();
                    self.read_char();
                    Token::NotEqual
                } else if self.peek_char() == Some('=') {
                    self.read_char();
                    self.read_char();
                    Token::LessThanOrEqual
                } else {
                    self.read_char();
                    Token::LessThan
                }
            }
            Some('>') => {
                if self.peek_char() == Some('=') {
                    self.read_char();
                    self.read_char();
                    Token::GreaterThanOrEqual
                } else {
                    self.read_char();
                    Token::GreaterThan
                }
            }
            Some('!') => {
                if self.peek_char() == Some('=') {
                    self.read_char();
                    self.read_char();
                    Token::NotEqual
                } else {
                    let ch = self.ch.unwrap();
                    self.read_char();
                    Token::Illegal(ch)
                }
            }
            Some(';') => {
                self.read_char();
                Token::Semicolon
            }
            Some('(') => {
                self.read_char();
                Token::LeftParen
            }
            Some(')') => {
                self.read_char();
                Token::RightParen
            }
            Some(',') => {
                self.read_char();
                Token::Comma
            }
            Some('*') => {
                self.read_char();
                Token::Asterisk
            }
            Some('\'') | Some('"') => {
                let quote_type = self.ch.unwrap();
                let literal = self.read_string(quote_type);
                Token::String(literal)
            }
            Some('.') => {
                self.read_char();
                Token::Dot
            }
            Some(ch) => {
                if ch.is_alphabetic() || ch == '_' {
                    let ident = self.read_identifier();
                    return Lexer::lookup_ident(&ident);
                } else if ch.is_digit(10) {
                    let num = self.read_number();
                    return Token::Number(num);
                } else {
                    self.read_char();
                    Token::Illegal(ch)
                }
            }
            None => Token::Eof,
        };

        if !matches!(tok, Token::Identifier(_) | Token::Number(_)) {}

        tok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_token_simple() {
        let input = "SELECT col1, col2 FROM my_table;";
        let mut lexer = Lexer::new(input);

        let tokens = vec![
            Token::Select,
            Token::Identifier("col1".to_string()),
            Token::Comma,
            Token::Identifier("col2".to_string()),
            Token::From,
            Token::Identifier("my_table".to_string()),
            Token::Semicolon,
            Token::Eof,
        ];

        for expected_token in tokens {
            let token = lexer.next_token();
            println!("Got token: {:?}, Expected: {:?}", token, expected_token);
            assert_eq!(token, expected_token);
        }
    }

    #[test]
    fn test_next_token_with_various_symbols() {
        let input = "=*();',123";
        let mut lexer = Lexer::new(input);

        let tokens = vec![
            Token::Equal,
            Token::Asterisk,
            Token::LeftParen,
            Token::RightParen,
            Token::Semicolon,
            Token::String("".to_string()),
            Token::Comma,
            Token::Number("123".to_string()),
            Token::Eof,
        ];

        for expected_token in tokens {
            let token = lexer.next_token();
            println!("Got token: {:?}, Expected: {:?}", token, expected_token);
            assert_eq!(token, expected_token);
        }
    }

    #[test]
    fn test_next_token_more_complex() {
        let input = r#"
            SELECT customer_id, first_name, last_name 
            FROM customers
            WHERE country = 'USA';
        "#;
        let mut lexer = Lexer::new(input);

        let tokens = vec![
            Token::Select,
            Token::Identifier("customer_id".to_string()),
            Token::Comma,
            Token::Identifier("first_name".to_string()),
            Token::Comma,
            Token::Identifier("last_name".to_string()),
            Token::From,
            Token::Identifier("customers".to_string()),
            Token::Where,
            Token::Identifier("country".to_string()),
            Token::Equal,
            Token::String("USA".to_string()),
            Token::Semicolon,
            Token::Eof,
        ];

        for expected_token in tokens {
            let token = lexer.next_token();
            println!("Got token: {:?}, Expected: {:?}", token, expected_token);
            assert_eq!(token, expected_token);
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    QualifiedIdentifier(QualifiedIdentifier),
    Literal(LiteralValue),
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    FunctionCall {
        name: String,
        arguments: Vec<Expression>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum LiteralValue {
    Number(String),
    String(String),
    Null,
}

#[derive(Debug, PartialEq, Clone)]
pub struct QualifiedIdentifier {
    pub table: Option<String>,
    pub identifier: String,
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub on_condition: Option<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FromClause {
    pub base_table: String,
    pub joins: Vec<JoinClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectStatement {
    pub columns: Vec<SelectItem>,
    pub from_clause: Option<FromClause>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectItem {
    Wildcard,
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
    QualifiedWildcard(String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Statement {
    Select(SelectStatement),
    CreateTable(CreateTableStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Update(UpdateStatement),
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: ParsedDataType,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ParsedDataType {
    Integer,
    Text,
}

#[derive(Debug, PartialEq, Clone)]
pub struct InsertStatement {
    pub table_name: String,

    pub values: Vec<Vec<Expression>>,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    Gt,
    LtEq,
    GtEq,

    And,
    Or,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum UnaryOperator {
    Not,
}

#[derive(Debug, PartialEq, PartialOrd)]
enum Precedence {
    Lowest,
    Equals,
    LessGreater,
    Sum,
    Product,
    LogicalOr,
    LogicalAnd,
    Prefix,
    Call,
    Index,
}

impl Token {
    fn get_precedence(&self) -> Precedence {
        match self {
            Token::Equal | Token::NotEqual => Precedence::Equals,
            Token::LessThan
            | Token::GreaterThan
            | Token::LessThanOrEqual
            | Token::GreaterThanOrEqual => Precedence::LessGreater,
            Token::And => Precedence::LogicalAnd,
            Token::Or => Precedence::LogicalOr,
            Token::Is => Precedence::Equals,

            _ => Precedence::Lowest,
        }
    }
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current_token: Token,
    peek_token: Token,
    errors: Vec<String>,
}

impl<'a> Parser<'a> {
    pub fn new(lexer: Lexer<'a>) -> Self {
        let mut parser = Parser {
            lexer,
            current_token: Token::Eof,
            peek_token: Token::Eof,
            errors: Vec::new(),
        };

        parser.next_token();
        parser.next_token();
        parser
    }

    fn next_token(&mut self) {
        self.current_token = self.peek_token.clone();
        self.peek_token = self.lexer.next_token();
    }

    pub fn parse_program(&mut self) -> Option<Statement> {
        match self.current_token {
            Token::Select => self.parse_select_statement(),
            Token::Create => self.parse_create_table_statement(),
            Token::Insert => self.parse_insert_statement(),
            Token::Delete => self.parse_delete_statement(),
            Token::Update => self.parse_update_statement(),
            Token::Eof => None,
            _ => {
                self.errors.push(format!(
                    "Parsing error: Unexpected token {:?} at start of statement",
                    self.current_token
                ));
                None
            }
        }
    }

    fn parse_select_statement(&mut self) -> Option<Statement> {
        self.next_token();

        let columns = match self.parse_select_list() {
            Ok(cols) => cols,
            Err(e) => {
                self.errors.push(e);
                return None;
            }
        };

        if !self.current_token_is(Token::From) {
            self.current_error_expected(Token::From);
            return None;
        }
        self.next_token();

        let from_clause = match self.parse_from_clause() {
            Ok(fc) => fc,
            Err(e) => {
                self.errors.push(e);
                return None;
            }
        };

        let mut where_clause = None;
        if self.current_token_is(Token::Where) {
            self.next_token();
            where_clause = self.parse_expression(Precedence::Lowest);
            if where_clause.is_none() {
                self.errors
                    .push("Failed to parse WHERE clause expression".to_string());
                return None;
            }
        }

        if self.current_token_is(Token::Semicolon) {
            self.next_token();
        }

        Some(Statement::Select(SelectStatement {
            columns,
            from_clause: Some(from_clause),
            where_clause,
        }))
    }

    fn parse_select_list(&mut self) -> Result<Vec<SelectItem>, String> {
        let mut list = Vec::new();

        loop {
            if self.current_token_is(Token::Asterisk) {
                self.next_token();
                if list.is_empty() {
                    list.push(SelectItem::Wildcard);
                    if !matches!(
                        self.current_token,
                        Token::From | Token::Semicolon | Token::Eof
                    ) {
                        return Err(format!(
                            "Unexpected token {:?} after '*' in SELECT list. Expected FROM or end of statement.",
                            self.current_token
                        ));
                    }
                } else {
                    return Err(
                        "Unexpected '*' in SELECT list. '*' must be the only select item."
                            .to_string(),
                    );
                }
            } else {
                match self.parse_expression(Precedence::Lowest) {
                    Some(expr) => {
                        let mut alias = None;
                        if self.current_token_is(Token::As) {
                            self.next_token();
                            if let Token::Identifier(alias_name) = &self.current_token {
                                alias = Some(alias_name.clone());
                                self.next_token();
                            } else {
                                return Err(format!(
                                    "Expected identifier after AS, found {:?}",
                                    self.current_token
                                ));
                            }
                        } else if let Token::Identifier(potential_alias) = &self.current_token {
                            if !matches!(
                                self.current_token,
                                Token::From
                                    | Token::Where
                                    | Token::Join
                                    | Token::Comma
                                    | Token::Semicolon
                                    | Token::Eof
                            ) {
                                alias = Some(potential_alias.clone());
                                self.next_token();
                            }
                        }
                        list.push(SelectItem::Expression { expr, alias });
                    }
                    None => {
                        return Err(format!(
                            "Failed to parse expression item in SELECT list near {:?}",
                            self.current_token
                        ));
                    }
                }
            }

            if !self.current_token_is(Token::Comma) {
                break;
            }
            self.next_token();

            if matches!(
                self.current_token,
                Token::From | Token::Semicolon | Token::Eof
            ) {
                return Err(
                    "Trailing comma found in SELECT list, or unexpected end of statement."
                        .to_string(),
                );
            }
        }

        if !matches!(
            self.current_token,
            Token::From | Token::Semicolon | Token::Eof
        ) {
            return Err(format!(
                "Expected FROM or end of statement after SELECT list, found {:?}",
                self.current_token
            ));
        }

        Ok(list)
    }

    fn parse_expression(&mut self, precedence: Precedence) -> Option<Expression> {
        let mut left_expr = match self.parse_prefix() {
            Some(expr) => expr,
            None => {
                self.errors.push(format!(
                    "No prefix parse function found for {:?}",
                    self.current_token
                ));
                return None;
            }
        };

        while self.peek_token != Token::Semicolon && precedence < self.peek_token.get_precedence() {
            self.next_token();
            left_expr = match self.parse_infix_expression(left_expr) {
                Some(expr) => expr,
                None => return None,
            };
        }

        Some(left_expr)
    }

    fn parse_prefix(&mut self) -> Option<Expression> {
        match &self.current_token {
            Token::Identifier(name) => {
                let base_identifier = name.clone();
                if self.peek_token_is(Token::Dot) {
                    self.next_token();
                    if let Token::Identifier(ref column_name) = self.peek_token {
                        let qualified_name = QualifiedIdentifier {
                            table: Some(base_identifier),
                            identifier: column_name.clone(),
                        };
                        self.next_token();
                        Some(Expression::QualifiedIdentifier(qualified_name))
                    } else {
                        self.errors.push(format!(
                            "Expected identifier after '.', found {:?}",
                            self.peek_token
                        ));
                        None
                    }
                } else if self.peek_token_is(Token::LeftParen) {
                    self.next_token();

                    let args = if self.peek_token_is(Token::RightParen) {
                        self.next_token();
                        Vec::new()
                    } else {
                        self.next_token();
                        let mut args = Vec::new();

                        loop {
                            match self.parse_expression(Precedence::Lowest) {
                                Some(expr) => args.push(expr),
                                None => break,
                            }

                            if !self.peek_token_is(Token::Comma) {
                                break;
                            }
                            self.next_token();
                            self.next_token();
                        }

                        if !self.peek_token_is(Token::RightParen) {
                            self.errors.push(format!(
                                "Expected ')' after function arguments, found {:?}",
                                self.peek_token
                            ));
                            return None;
                        }
                        self.next_token();
                        args
                    };

                    Some(Expression::FunctionCall {
                        name: base_identifier,
                        arguments: args,
                    })
                } else {
                    Some(Expression::QualifiedIdentifier(QualifiedIdentifier {
                        table: None,
                        identifier: base_identifier,
                    }))
                }
            }
            Token::Function(name) => {
                let func_name = name.clone();
                if !self.peek_token_is(Token::LeftParen) {
                    self.errors.push(format!(
                        "Expected '(' after function name, found {:?}",
                        self.peek_token
                    ));
                    return None;
                }

                self.next_token();

                let args = if self.peek_token_is(Token::RightParen) {
                    self.next_token();
                    Vec::new()
                } else {
                    self.next_token();
                    let mut args = Vec::new();

                    loop {
                        match self.parse_expression(Precedence::Lowest) {
                            Some(expr) => args.push(expr),
                            None => break,
                        }

                        if !self.peek_token_is(Token::Comma) {
                            break;
                        }
                        self.next_token();
                        self.next_token();
                    }

                    if !self.peek_token_is(Token::RightParen) {
                        self.errors.push(format!(
                            "Expected ')' after function arguments, found {:?}",
                            self.peek_token
                        ));
                        return None;
                    }
                    self.next_token();
                    args
                };

                Some(Expression::FunctionCall {
                    name: func_name,
                    arguments: args,
                })
            }
            Token::Number(value) => Some(Expression::Literal(LiteralValue::Number(value.clone()))),
            Token::String(value) => Some(Expression::Literal(LiteralValue::String(value.clone()))),
            Token::LeftParen => self.parse_grouped_expression(),
            Token::Not => self.parse_prefix_unary(),
            Token::Null => Some(Expression::Literal(LiteralValue::Null)),
            _ => None,
        }
    }

    fn parse_infix_expression(&mut self, left: Expression) -> Option<Expression> {
        match self.current_token {
            Token::Equal
            | Token::NotEqual
            | Token::LessThan
            | Token::GreaterThan
            | Token::LessThanOrEqual
            | Token::GreaterThanOrEqual
            | Token::And
            | Token::Or => self.parse_binary_infix(left),
            Token::Is => self.parse_is_null_infix(left),

            _ => {
                self.errors.push(format!(
                    "No infix parse function found for {:?}",
                    self.current_token
                ));
                None
            }
        }
    }

    fn parse_prefix_unary(&mut self) -> Option<Expression> {
        let operator = match self.current_token {
            Token::Not => UnaryOperator::Not,

            _ => return None,
        };
        self.next_token();

        match self.parse_expression(Precedence::Prefix) {
            Some(expr) => Some(Expression::UnaryOp {
                op: operator,
                expr: Box::new(expr),
            }),
            None => None,
        }
    }

    fn parse_binary_infix(&mut self, left: Expression) -> Option<Expression> {
        let op = match self.current_token {
            Token::Equal => BinaryOperator::Eq,
            Token::NotEqual => BinaryOperator::Neq,
            Token::LessThan => BinaryOperator::Lt,
            Token::GreaterThan => BinaryOperator::Gt,
            Token::LessThanOrEqual => BinaryOperator::LtEq,
            Token::GreaterThanOrEqual => BinaryOperator::GtEq,
            Token::And => BinaryOperator::And,
            Token::Or => BinaryOperator::Or,
            _ => return None,
        };
        let precedence = self.current_token.get_precedence();
        self.next_token();

        match self.parse_expression(precedence) {
            Some(right) => Some(Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }),
            None => None,
        }
    }

    fn parse_is_null_infix(&mut self, left: Expression) -> Option<Expression> {
        if self.peek_token_is(Token::Null) {
            self.next_token();

            Some(Expression::IsNull(Box::new(left)))
        } else if self.peek_token_is(Token::Not) && self.lexer.peek_char() == Some(' ') {
            self.next_token();
            if self.peek_token_is(Token::Null) {
                self.next_token();
                Some(Expression::IsNotNull(Box::new(left)))
            } else {
                self.errors.push(format!(
                    "Expected NULL after IS NOT, found {:?}",
                    self.peek_token
                ));
                None
            }
        } else {
            self.errors.push(format!(
                "Expected NULL or NOT NULL after IS, found {:?}",
                self.peek_token
            ));
            None
        }
    }

    fn parse_grouped_expression(&mut self) -> Option<Expression> {
        self.next_token();
        let expr = self.parse_expression(Precedence::Lowest);

        if !self.peek_token_is(Token::RightParen) {
            self.peek_error_current(Token::RightParen);
            return None;
        }
        self.next_token();
        expr
    }

    fn peek_token_is(&self, expected: Token) -> bool {
        self.peek_token == expected
    }

    fn current_token_is(&self, expected: Token) -> bool {
        self.current_token == expected
    }

    fn peek_error_current(&mut self, expected: Token) {
        let peeked_token_for_error = self.peek_token.clone();
        let msg = format!(
            "Expected next token to be {:?}, got {:?} instead",
            expected, peeked_token_for_error
        );
        self.errors.push(msg);
    }

    fn current_error_expected(&mut self, expected: Token) {
        let current_token_for_error = self.current_token.clone();
        let msg = format!(
            "Expected current token to be {:?}, got {:?} instead",
            expected, current_token_for_error
        );
        self.errors.push(msg);
    }

    pub fn errors(&self) -> &Vec<String> {
        &self.errors
    }

    fn parse_create_table_statement(&mut self) -> Option<Statement> {
        if !self.peek_token_is(Token::Table) {
            self.peek_error_current(Token::Table);
            return None;
        }
        self.next_token();
        self.next_token();

        let table_name = match &self.current_token {
            Token::Identifier(name) => name.clone(),
            _ => {
                self.current_error_expected(Token::Identifier("Identifier".to_string()));
                return None;
            }
        };
        self.next_token();

        if !self.current_token_is(Token::LeftParen) {
            self.current_error_expected(Token::LeftParen);
            return None;
        }
        self.next_token();

        let columns = match self.parse_column_definitions() {
            Ok(cols) => cols,
            Err(e) => {
                self.errors.push(e);
                return None;
            }
        };

        if !self.current_token_is(Token::RightParen) {
            self.current_error_expected(Token::RightParen);
            return None;
        }
        self.next_token();

        if self.current_token_is(Token::Semicolon) {
            self.next_token();
        }

        Some(Statement::CreateTable(CreateTableStatement {
            table_name,
            columns,
        }))
    }

    fn parse_column_definitions(&mut self) -> Result<Vec<ColumnDefinition>, String> {
        let mut definitions = Vec::new();

        if self.current_token_is(Token::RightParen) {
            return Ok(definitions);
        }

        loop {
            let col_name = match &self.current_token {
                Token::Identifier(name) => name.clone(),
                _ => {
                    return Err(format!(
                        "Expected column name (Identifier), found {:?}",
                        self.current_token
                    ));
                }
            };
            self.next_token();

            let data_type = match self.parse_data_type() {
                Ok(dt) => dt,
                Err(e) => return Err(e),
            };

            definitions.push(ColumnDefinition {
                name: col_name,
                data_type,
            });

            if self.current_token_is(Token::RightParen) {
                break;
            } else if self.current_token_is(Token::Comma) {
                self.next_token();
            } else {
                return Err(format!(
                    "Expected ',' or ')' after column definition, found {:?}",
                    self.current_token
                ));
            }
        }

        Ok(definitions)
    }

    fn parse_data_type(&mut self) -> Result<ParsedDataType, String> {
        match &self.current_token {
            Token::Identifier(ident) => {
                let ident_clone = ident.clone();
                let type_name = ident_clone.to_uppercase();
                self.next_token();
                match type_name.as_str() {
                    "INTEGER" | "INT" => Ok(ParsedDataType::Integer),
                    "TEXT" | "VARCHAR" | "STRING" => Ok(ParsedDataType::Text),
                    _ => Err(format!("Unsupported data type: {}", ident_clone)),
                }
            }
            _ => Err(format!(
                "Expected data type (e.g., INTEGER, TEXT), found {:?}",
                self.current_token
            )),
        }
    }

    fn parse_insert_statement(&mut self) -> Option<Statement> {
        if !self.peek_token_is(Token::Into) {
            self.peek_error_current(Token::Into);
            return None;
        }
        self.next_token();
        self.next_token();

        let table_name = match &self.current_token {
            Token::Identifier(name) => name.clone(),
            _ => {
                self.current_error_expected(Token::Identifier("Identifier".to_string()));
                return None;
            }
        };
        self.next_token();

        if !self.current_token_is(Token::Values) {
            self.current_error_expected(Token::Values);
            return None;
        }
        self.next_token();

        let values = match self.parse_value_list() {
            Ok(v) => v,
            Err(e) => {
                self.errors.push(e);
                return None;
            }
        };

        if self.current_token_is(Token::Semicolon) {
            self.next_token();
        }

        Some(Statement::Insert(InsertStatement { table_name, values }))
    }

    fn parse_value_list(&mut self) -> Result<Vec<Vec<Expression>>, String> {
        let mut list = Vec::new();
        loop {
            if !self.current_token_is(Token::LeftParen) {
                self.current_error_expected(Token::LeftParen);
                return Err(format!(
                    "Expected '(' to start values tuple, got {:?}",
                    self.current_token
                ));
            }
            self.next_token();

            let row_values = match self.parse_expression_list() {
                Ok(vals) => vals,
                Err(e) => return Err(e),
            };
            list.push(row_values);

            if !self.current_token_is(Token::RightParen) {
                self.current_error_expected(Token::RightParen);
                return Err(format!(
                    "Expected ')' to end values tuple, got {:?}",
                    self.current_token
                ));
            }
            self.next_token();

            if !self.current_token_is(Token::Comma) {
                break;
            }
            self.next_token();
        }
        Ok(list)
    }

    fn parse_expression_list(&mut self) -> Result<Vec<Expression>, String> {
        let mut list = Vec::new();
        if self.current_token_is(Token::RightParen) {
            return Err("Value list cannot be empty inside parentheses".to_string());
        }

        loop {
            match self.parse_expression(Precedence::Lowest) {
                Some(expr) => list.push(expr),
                None => return Err("Expected expression in value list".to_string()),
            }

            self.next_token();

            if !self.current_token_is(Token::Comma) {
                break;
            }
            self.next_token();
        }

        Ok(list)
    }

    fn parse_delete_statement(&mut self) -> Option<Statement> {
        if !self.peek_token_is(Token::From) {
            self.peek_error_current(Token::From);
            return None;
        }
        self.next_token();
        self.next_token();

        let table_name = match &self.current_token {
            Token::Identifier(name) => name.clone(),
            _ => {
                self.current_error_expected(Token::Identifier("Identifier".to_string()));
                return None;
            }
        };
        self.next_token();

        let mut where_clause = None;
        if self.current_token_is(Token::Where) {
            self.next_token();
            where_clause = self.parse_expression(Precedence::Lowest);
            if where_clause.is_none() {
                self.errors
                    .push("Failed to parse WHERE clause expression".to_string());
                return None;
            }
        }

        if self.current_token_is(Token::Semicolon) {
            self.next_token();
        }

        Some(Statement::Delete(DeleteStatement {
            table_name,
            where_clause,
        }))
    }

    fn parse_update_statement(&mut self) -> Option<Statement> {
        self.next_token();
        let table_name = match &self.current_token {
            Token::Identifier(name) => name.clone(),
            _ => {
                self.current_error_expected(Token::Identifier("table name".to_string()));
                return None;
            }
        };
        self.next_token();

        if !self.current_token_is(Token::Set) {
            self.current_error_expected(Token::Set);
            return None;
        }
        self.next_token();

        let mut assignments = Vec::new();
        loop {
            let column_name = match &self.current_token {
                Token::Identifier(name) => name.clone(),
                _ => {
                    self.errors.push(format!(
                        "Expected column name in SET clause, found {:?}",
                        self.current_token
                    ));
                    return None;
                }
            };
            self.next_token();

            if !self.current_token_is(Token::Equal) {
                self.current_error_expected(Token::Equal);
                return None;
            }
            self.next_token();

            let value_expr = match self.parse_expression(Precedence::Lowest) {
                Some(expr) => expr,
                None => {
                    self.errors
                        .push("Failed to parse expression in SET clause".to_string());
                    return None;
                }
            };
            assignments.push((column_name, value_expr));

            if self.current_token_is(Token::Comma) {
                self.next_token();
            } else {
                break;
            }
        }

        let mut where_clause = None;
        if self.current_token_is(Token::Where) {
            self.next_token();
            where_clause = self.parse_expression(Precedence::Lowest);
            if where_clause.is_none() {
                self.errors
                    .push("Failed to parse WHERE clause expression for UPDATE".to_string());
                return None;
            }
        }

        if self.current_token_is(Token::Semicolon) {
            self.next_token();
        }

        Some(Statement::Update(UpdateStatement {
            table_name,
            assignments,
            where_clause,
        }))
    }

    fn parse_from_clause(&mut self) -> Result<FromClause, String> {
        let base_table = match &self.current_token {
            Token::Identifier(name) => name.clone(),
            _ => {
                return Err(format!(
                    "Expected base table name (Identifier) after FROM, found {:?}",
                    self.current_token
                ));
            }
        };
        self.next_token();

        let mut joins = Vec::new();

        loop {
            let mut current_join_type = None;

            match self.current_token {
                Token::Inner | Token::Join => {
                    current_join_type = Some(JoinType::Inner);
                    self.next_token();
                    if self.current_token_is(Token::Join) {
                        self.next_token();
                    }
                }
                Token::Left => {
                    self.next_token();
                    if self.current_token_is(Token::Outer) {
                        self.next_token();
                    }
                    if self.current_token_is(Token::Join) {
                        self.next_token();
                        current_join_type = Some(JoinType::LeftOuter);
                    } else {
                        return Err("Expected JOIN after LEFT [OUTER]".to_string());
                    }
                }
                Token::Right => {
                    self.next_token();
                    if self.current_token_is(Token::Outer) {
                        self.next_token();
                    }
                    if self.current_token_is(Token::Join) {
                        self.next_token();
                        current_join_type = Some(JoinType::RightOuter);
                    } else {
                        return Err("Expected JOIN after RIGHT [OUTER]".to_string());
                    }
                }
                Token::Full => {
                    self.next_token();
                    if self.current_token_is(Token::Outer) {
                        self.next_token();
                    }
                    if self.current_token_is(Token::Join) {
                        self.next_token();
                        current_join_type = Some(JoinType::FullOuter);
                    } else {
                        return Err("Expected JOIN after FULL [OUTER]".to_string());
                    }
                }
                _ => {
                    break;
                }
            }

            if let Some(join_type) = current_join_type {
                let joined_table = match &self.current_token {
                    Token::Identifier(name) => name.clone(),
                    _ => {
                        return Err(format!(
                            "Expected joined table name (Identifier) after JOIN, found {:?}",
                            self.current_token
                        ));
                    }
                };
                self.next_token();

                if !self.current_token_is(Token::On) {
                    return Err(format!(
                        "Expected ON after joined table name, found {:?}",
                        self.current_token
                    ));
                }
                self.next_token();

                let on_condition = self.parse_expression(Precedence::Lowest);
                if on_condition.is_none() {
                    return Err("Failed to parse ON condition expression".to_string());
                }

                joins.push(JoinClause {
                    join_type,
                    table: joined_table,
                    on_condition,
                });
            } else {
                break;
            }
        }

        Ok(FromClause { base_table, joins })
    }
}

#[cfg(test)]
mod parser_tests {
    use super::*;

    #[test]
    fn test_select_statement() {
        let input = "SELECT column1, column2 FROM my_table;";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser encountered errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some(), "Parsing failed unexpectedly");

        if let Some(Statement::Select(stmt)) = program {
            assert_eq!(stmt.columns.len(), 2);
            assert_eq!(
                stmt.columns[0],
                SelectItem::Expression {
                    expr: Expression::QualifiedIdentifier(QualifiedIdentifier {
                        table: None,
                        identifier: "column1".to_string()
                    }),
                    alias: None
                }
            );
            assert_eq!(
                stmt.columns[1],
                SelectItem::Expression {
                    expr: Expression::QualifiedIdentifier(QualifiedIdentifier {
                        table: None,
                        identifier: "column2".to_string()
                    }),
                    alias: None
                }
            );
            assert_eq!(
                stmt.from_clause,
                Some(FromClause {
                    base_table: "my_table".to_string(),
                    joins: Vec::new()
                })
            );
        } else {
            panic!("Expected SelectStatement, got: {:?}", program);
        }
    }

    #[test]
    fn test_select_wildcard_statement() {
        let input = "SELECT * FROM users;";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser encountered errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some(), "Parsing failed unexpectedly");

        if let Some(Statement::Select(stmt)) = program {
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0], SelectItem::Wildcard);
            assert_eq!(
                stmt.from_clause,
                Some(FromClause {
                    base_table: "users".to_string(),
                    joins: Vec::new()
                })
            );
        } else {
            panic!("Expected SelectStatement, got: {:?}", program);
        }
    }

    #[test]
    fn test_parsing_errors() {
        let input = "SELECT FROM my_table;";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(program.is_none(), "Parser should have failed");
        assert!(
            !parser.errors().is_empty(),
            "Parser should have recorded errors"
        );

        assert!(parser.errors()[0].contains("Expected expression or '*' after SELECT"));

        let input = "SELECT column1 column2 FROM my_table;";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let _ = parser.parse_program();
        assert!(
            !parser.errors().is_empty(),
            "Parser should have recorded errors"
        );
        assert!(
            parser.errors()[0]
                .contains("Expected next token to be From, got Identifier(\"column2\") instead")
        );
    }

    #[test]
    fn test_create_table_statement() {
        let input = "CREATE TABLE customers (id INT, name TEXT, email VARCHAR);";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some());

        if let Some(Statement::CreateTable(stmt)) = program {
            assert_eq!(stmt.table_name, "customers");
            assert_eq!(stmt.columns.len(), 3);
            assert_eq!(
                stmt.columns[0],
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: ParsedDataType::Integer
                }
            );
            assert_eq!(
                stmt.columns[1],
                ColumnDefinition {
                    name: "name".to_string(),
                    data_type: ParsedDataType::Text
                }
            );
            assert_eq!(
                stmt.columns[2],
                ColumnDefinition {
                    name: "email".to_string(),
                    data_type: ParsedDataType::Text
                }
            );
        } else {
            panic!("Expected CreateTableStatement, got: {:?}", program);
        }
    }

    #[test]
    fn test_create_table_no_semicolon() {
        let input = "CREATE TABLE products (sku TEXT)";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some());
        if let Some(Statement::CreateTable(stmt)) = program {
            assert_eq!(stmt.table_name, "products");
            assert_eq!(stmt.columns.len(), 1);
        } else {
            panic!("Expected CreateTableStatement");
        }
    }

    #[test]
    fn test_insert_statement_single_row() {
        let input = "INSERT INTO users VALUES (1, 'Alice', 'alice@test.com');";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some());

        if let Some(Statement::Insert(stmt)) = program {
            assert_eq!(stmt.table_name, "users");
            assert_eq!(stmt.values.len(), 1);
            assert_eq!(stmt.values[0].len(), 3);
            assert_eq!(
                stmt.values[0][0],
                Expression::Literal(crate::parser::LiteralValue::Number("1".to_string()))
            );
            assert_eq!(
                stmt.values[0][1],
                Expression::Literal(crate::parser::LiteralValue::String("Alice".to_string()))
            );
            assert_eq!(
                stmt.values[0][2],
                Expression::Literal(crate::parser::LiteralValue::String(
                    "alice@test.com".to_string()
                ))
            );
        } else {
            panic!("Expected InsertStatement, got: {:?}", program);
        }
    }

    #[test]
    fn test_insert_statement_multiple_rows() {
        let input = "INSERT INTO products VALUES (101, 'Laptop'), (102, 'Mouse');";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some());

        if let Some(Statement::Insert(stmt)) = program {
            assert_eq!(stmt.table_name, "products");
            assert_eq!(stmt.values.len(), 2);
            assert_eq!(stmt.values[0].len(), 2);
            assert_eq!(stmt.values[1].len(), 2);
            assert_eq!(
                stmt.values[0][0],
                Expression::Literal(crate::parser::LiteralValue::Number("101".to_string()))
            );
            assert_eq!(
                stmt.values[1][1],
                Expression::Literal(crate::parser::LiteralValue::String("Mouse".to_string()))
            );
        } else {
            panic!("Expected InsertStatement");
        }
    }

    #[test]
    fn test_select_with_where() {
        let input = "SELECT name FROM users WHERE id = 1;";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some());

        if let Some(Statement::Select(stmt)) = program {
            assert!(stmt.where_clause.is_some());
            let expected_where = Expression::BinaryOp {
                left: Box::new(Expression::QualifiedIdentifier(QualifiedIdentifier {
                    table: None,
                    identifier: "id".to_string(),
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::Literal(LiteralValue::Number("1".to_string()))),
            };
            assert_eq!(stmt.where_clause.unwrap(), expected_where);
        } else {
            panic!("Expected SelectStatement, got: {:?}", program);
        }
    }

    #[test]
    fn test_select_with_where_string() {
        let input = "SELECT id FROM products WHERE sku <> 'ABC';";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some());

        if let Some(Statement::Select(stmt)) = program {
            assert!(stmt.where_clause.is_some());
            let expected_where = Expression::BinaryOp {
                left: Box::new(Expression::QualifiedIdentifier(QualifiedIdentifier {
                    table: None,
                    identifier: "sku".to_string(),
                })),
                op: BinaryOperator::Neq,
                right: Box::new(Expression::Literal(LiteralValue::String("ABC".to_string()))),
            };
            assert_eq!(stmt.where_clause.unwrap(), expected_where);
        } else {
            panic!("Expected SelectStatement, got: {:?}", program);
        }
    }

    #[test]
    fn test_select_with_where_and() {
        let input = "SELECT name FROM users WHERE city = 'London' AND user_id > 10;";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some());

        if let Some(Statement::Select(stmt)) = program {
            assert!(stmt.where_clause.is_some());

            if let Some(Expression::BinaryOp {
                left: l_and,
                op: op_and,
                right: r_and,
            }) = stmt.where_clause
            {
                assert_eq!(op_and, BinaryOperator::And);

                if let Expression::BinaryOp {
                    left: l_eq,
                    op: op_eq,
                    right: r_eq,
                } = *l_and
                {
                    assert_eq!(op_eq, BinaryOperator::Eq);
                    assert_eq!(
                        *l_eq,
                        Expression::QualifiedIdentifier(QualifiedIdentifier {
                            table: None,
                            identifier: "city".to_string()
                        })
                    );
                    assert_eq!(
                        *r_eq,
                        Expression::Literal(LiteralValue::String("London".to_string()))
                    );
                } else {
                    panic!("Expected EQ on left side of AND");
                }

                if let Expression::BinaryOp {
                    left: l_gt,
                    op: op_gt,
                    right: r_gt,
                } = *r_and
                {
                    assert_eq!(op_gt, BinaryOperator::Gt);
                    assert_eq!(
                        *l_gt,
                        Expression::QualifiedIdentifier(QualifiedIdentifier {
                            table: None,
                            identifier: "user_id".to_string()
                        })
                    );
                    assert_eq!(
                        *r_gt,
                        Expression::Literal(LiteralValue::Number("10".to_string()))
                    );
                } else {
                    panic!("Expected GT on right side of AND");
                }
            } else {
                panic!("Expected top-level AND operator in WHERE clause");
            }
        } else {
            panic!("Expected SelectStatement, got: {:?}", program);
        }
    }

    #[test]
    fn test_not_expression() {
        let input = "SELECT * FROM test WHERE NOT id = 1;";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();
        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );

        let where_clause = match program {
            Some(Statement::Select(s)) => s.where_clause,
            _ => panic!("Expected SelectStatement"),
        }
        .expect("Expected WHERE clause");

        assert_eq!(
            where_clause,
            Expression::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::QualifiedIdentifier(QualifiedIdentifier {
                        table: None,
                        identifier: "id".to_string()
                    })),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expression::Literal(LiteralValue::Number("1".to_string())))
                })
            }
        );
    }

    #[test]
    fn test_grouped_expression() {
        let input = "SELECT * FROM test WHERE (id = 1 OR id = 2) AND city = 'A';";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();
        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );

        let where_clause = match program {
            Some(Statement::Select(s)) => s.where_clause,
            _ => panic!("Expected SelectStatement"),
        }
        .expect("Expected WHERE clause");

        if let Expression::BinaryOp {
            left: and_left,
            op: and_op,
            right: and_right,
        } = where_clause
        {
            assert_eq!(and_op, BinaryOperator::And);

            assert_eq!(
                *and_right,
                Expression::BinaryOp {
                    left: Box::new(Expression::QualifiedIdentifier(QualifiedIdentifier {
                        table: None,
                        identifier: "city".to_string()
                    })),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expression::Literal(LiteralValue::String("A".to_string())))
                }
            );

            if let Expression::BinaryOp {
                left: or_left,
                op: or_op,
                right: or_right,
            } = *and_left
            {
                assert_eq!(or_op, BinaryOperator::Or);
                assert_eq!(
                    *or_left,
                    Expression::BinaryOp {
                        left: Box::new(Expression::QualifiedIdentifier(QualifiedIdentifier {
                            table: None,
                            identifier: "id".to_string()
                        })),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expression::Literal(LiteralValue::Number("1".to_string())))
                    }
                );
                assert_eq!(
                    *or_right,
                    Expression::BinaryOp {
                        left: Box::new(Expression::QualifiedIdentifier(QualifiedIdentifier {
                            table: None,
                            identifier: "id".to_string()
                        })),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expression::Literal(LiteralValue::Number("2".to_string())))
                    }
                );
            } else {
                panic!("Expected OR expression on left side of AND");
            }
        } else {
            panic!("Expected AND expression at top level");
        }
    }

    #[test]
    fn test_is_null_expression() {
        let input = "SELECT * FROM test WHERE name IS NULL OR email IS NOT NULL;";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();
        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );

        let where_clause = match program {
            Some(Statement::Select(s)) => s.where_clause,
            _ => panic!("Expected SelectStatement"),
        }
        .expect("Expected WHERE clause");

        if let Expression::BinaryOp {
            left: or_left,
            op: or_op,
            right: or_right,
        } = where_clause
        {
            assert_eq!(or_op, BinaryOperator::Or);
            assert_eq!(
                *or_left,
                Expression::IsNull(Box::new(Expression::QualifiedIdentifier(
                    QualifiedIdentifier {
                        table: None,
                        identifier: "name".to_string()
                    }
                )))
            );
            assert_eq!(
                *or_right,
                Expression::IsNotNull(Box::new(Expression::QualifiedIdentifier(
                    QualifiedIdentifier {
                        table: None,
                        identifier: "email".to_string()
                    }
                )))
            );
        } else {
            panic!("Expected OR expression at top level");
        }
    }

    #[test]
    fn test_delete_statement_no_where() {
        let input = "DELETE FROM customers;";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some());

        if let Some(Statement::Delete(stmt)) = program {
            assert_eq!(stmt.table_name, "customers");
            assert!(stmt.where_clause.is_none());
        } else {
            panic!("Expected DeleteStatement, got: {:?}", program);
        }
    }

    #[test]
    fn test_delete_statement_with_where() {
        let input = "DELETE FROM products WHERE id = 1 OR category = 'old';";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();
        assert!(parser.errors().is_empty(), "{:?}", parser.errors());

        if let Some(Statement::Delete(stmt)) = program {
            assert_eq!(stmt.table_name, "products");
            assert!(stmt.where_clause.is_some());
        } else {
            panic!("Expected DeleteStatement");
        }
    }

    #[test]
    fn test_update_statement() {
        let input = "UPDATE users SET email = 'new@example.com', status = 1 WHERE id = 10;";
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer);
        let program = parser.parse_program();

        assert!(
            parser.errors().is_empty(),
            "Parser errors: {:?}",
            parser.errors()
        );
        assert!(program.is_some());

        if let Some(Statement::Update(stmt)) = program {
            assert_eq!(stmt.table_name, "users");
            assert_eq!(stmt.assignments.len(), 2);
            assert_eq!(stmt.assignments[0].0, "email");
            assert_eq!(stmt.assignments[1].0, "status");
            assert!(stmt.where_clause.is_some());
        } else {
            panic!("Expected UpdateStatement, got: {:?}", program);
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Expression>,
}
