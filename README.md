```
 (`-').->     (`-')               (`-')
 ( OO)_     __( OO)     <-.    <-.(OO )
(_)--\_)   '-'---\_)  ,--. )   ,------,)
/    _ /  |  .-.  |   |  (`-') |   /`. '
\_..`--.  |  | | <-'  |  |OO ) |  |_.' |
.-._)   \ |  | |  |  (|  '__ | |  .   .'
\       / '  '-'  '-. |     |' |  |\  \
 `-----'   `-----'--' `-----'  `--' '--'
```

Unleash the power of SQL with **SQLR**, the blazing-fast 🚀, Rust-powered database engine! Built from the ground up for performance and flexibility, SQLR offers a modern approach to data management.

## 🔥 Core Features 🔥

- **Robust SQL Handling:** Master your data with `CREATE TABLE`, `INSERT INTO`, `SELECT` (including `WHERE` & basic `INNER JOIN`), `UPDATE`, and `DELETE`. Need to see your schema? `SELECT * FROM SQLR_TABLES;` has you covered! 📋
- **Persistent Storage:** Your data is safe! 💾 SQLR automatically saves everything to `sqlr_data.db` using efficient `bincode` serialization.
- **Multi-Interface Access:** Connect your way! 🌐
  - **Direct TCP API:** Integrate seamlessly with any application using our raw TCP server (port `7878`). Get clean JSON responses for every query. Perfect for custom clients! 🔌
  - **Slick Web UI:** Interact visually through our simple and intuitive web interface (port `8080`), powered by Axum. Includes a handy `/api/query` endpoint. 💻
  - **Interactive REPL:** Jump right into the action with our built-in command-line REPL for quick tests and queries. ⌨️
  - **MySQL Protocol Compatible:** Connect using any MySQL client or connector by running in MySQL wire protocol mode (port `3306` by default). Use your favorite MySQL tools with SQLR! 🔄
- **Optimized Engine:** Experience the speed and safety of a custom-built parser and executor, meticulously crafted in Rust. ⚙️
- **Pure Rust Power:** Leverage the concurrency and memory safety guarantees of modern Rust. 🦀

## 🚀 Get Started in Minutes! 🚀

1.  **Install Rust:** Grab the toolchain from [rustup.rs](https://rustup.rs/) if you haven't already.
2.  **Clone:** `git clone https://github.com/stuncs69/sqlr.git`
3.  **Navigate:** `cd sqlr`
4.  **Build:** `cargo build --release` for the ultimate performance! 💪

## 🚦 Running SQLR 🚦

Launch SQLR in your preferred mode from the project root:

- **TCP Powerhouse (Default):**

  ```bash
  cargo run --release
  # Server ready on 127.0.0.1:7878! ⚡
  ```

- **Web Experience:**

  ```bash
  cargo run --release -- --web-ui
  # Point your browser to http://127.0.0.1:8080! 🖱️
  ```

- **Instant REPL:**

  ```bash
  cargo run --release -- --test-parser
  # Start querying instantly! >
  ```

- **MySQL Protocol Compatible:**
  ```bash
  cargo run --release -- --mysql-compat
  # Connect using any MySQL client to 127.0.0.1:3306! 🔌
  # Or specify a custom port: --mysql-port 3307
  ```

## 💡 Usage Examples 💡

_(The existing Usage Examples section is quite good and clear, so we'll keep it largely the same for practical illustration.)_

### 1. TCP API Interaction (via `telnet`)

```bash
telnet 127.0.0.1 7878
```

_Welcome to the SQLR TCP Interface! Send SQL queries terminated by newline._</br>
**Type:** `CREATE TABLE users (id INTEGER, name TEXT);`⏎</br>
**Response:** `{"error":false,"message":"Execution successful","result":"Table 'users' created successfully."}`

**Type:** `INSERT INTO users (id, name) VALUES (1, 'Alice');`⏎</br>
**Response:** `{"error":false,"message":"Execution successful","result":"1 row(s) inserted."}`</br>

**Type:** `SELECT name FROM users WHERE id = 1;`⏎</br>
**Response:** `{"error":false,"message":"Execution successful","result":{"columns":["name"],"rows":[["Alice"]]}}`

_(Exit telnet using Ctrl+] then `quit`)_

### 2. Web UI Interaction

- Run `cargo run --release -- --web-ui`.
- Open `http://127.0.0.1:8080`.
- Use the query box and hit submit!

_API via `curl`:_

```bash
curl -X POST http://127.0.0.1:8080/api/query \
     -H "Content-Type: application/json" \
     -d '{"query": "SELECT * FROM users;"}'
# Get back structured JSON data!
```

### 3. REPL Interaction

```bash
cargo run --release -- --test-parser
```

`>` **Type:** `CREATE TABLE items (sku TEXT, price INTEGER);`⏎
_Execution Result: Table 'items' created successfully._
`>` **Type:** `INSERT INTO items VALUES ('abc', 500);`⏎
_Execution Result: 1 row(s) inserted._
`>` **Type:** `SELECT sku FROM items WHERE price > 100;`⏎
_Execution Result: Columns: ["sku"], Rows: [["abc"]]_
`>` **Type:** `exit`⏎
_Data saved. Exiting._

### 4. MySQL Client Interaction

```bash
cargo run --release -- --mysql-compat
```

_From another terminal, connect with the MySQL CLI client:_

```bash
mysql -h 127.0.0.1 -u root -P 3306
```

_Or use any MySQL compatible tool (MySQL Workbench, HeidiSQL, DBeaver, etc.) with these connection details:_

- Host: `127.0.0.1`
- User: `root` (or any user, password authentication is minimal in this version)
- Port: `3306` (default, configurable with --mysql-port)
- No password required

_And run standard SQL commands:_

```sql
CREATE TABLE products (id INTEGER, name TEXT, price INTEGER);
INSERT INTO products VALUES (1, 'Widget', 1999);
SELECT * FROM products;
```

## 🔌 TCP API Protocol 🔌

Integrate SQLR effortlessly:

- **Connect:** TCP to `127.0.0.1:7878`.
- **Send:** UTF-8 SQL queries, ending with `\n`.
- **Receive:** UTF-8 JSON responses, ending with `\n`.
- **Format:** Simple JSON structure (`{"error":bool,...}`) indicates success/failure and data/errors. Check `src/main.rs:process_query` for the exact structure.

## 🔄 MySQL Wire Protocol Support 🔄

The MySQL compatibility mode implements the core MySQL client/server protocol:

- **Authentication:** Supports the standard MySQL authentication handshake
- **Query Execution:** Run standard SQL queries as you would with MySQL
- **Result Sets:** Properly formatted column definitions and row data
- **Compatibility:** Works with standard MySQL client libraries and tools
- **Simple to Use:** Just connect as you would to a MySQL server

This makes SQLR a drop-in replacement for basic MySQL use cases, allowing you to use familiar tools and libraries while leveraging SQLR's performance and simplicity.

## License

[BSD-3 Clause license](https://github.com/stuncs69/sqlr/blob/master/LICENSE)
