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

*   **Robust SQL Handling:** Master your data with `CREATE TABLE`, `INSERT INTO`, `SELECT` (including `WHERE` & basic `INNER JOIN`), `UPDATE`, and `DELETE`. Need to see your schema? `SELECT * FROM SQLR_TABLES;` has you covered! 📋
*   **Persistent Storage:** Your data is safe! 💾 SQLR automatically saves everything to `sqlr_data.db` using efficient `bincode` serialization.
*   **Multi-Interface Access:** Connect your way! 🌐
    *   **Direct TCP API:** Integrate seamlessly with any application using our raw TCP server (port `7878`). Get clean JSON responses for every query. Perfect for custom clients! 🔌
    *   **Slick Web UI:** Interact visually through our simple and intuitive web interface (port `8080`), powered by Axum. Includes a handy `/api/query` endpoint. 💻
    *   **Interactive REPL:** Jump right into the action with our built-in command-line REPL for quick tests and queries. ⌨️
*   **Optimized Engine:** Experience the speed and safety of a custom-built parser and executor, meticulously crafted in Rust. ⚙️
*   **Pure Rust Power:** Leverage the concurrency and memory safety guarantees of modern Rust. 🦀

## 🚀 Get Started in Minutes! 🚀

1.  **Install Rust:** Grab the toolchain from [rustup.rs](https://rustup.rs/) if you haven't already.
2.  **Clone:** `git clone https://github.com/stuncs69/sqlr.git`
3.  **Navigate:** `cd sqlr`
4.  **Build:** `cargo build --release` for the ultimate performance! 💪

## 🚦 Running SQLR 🚦

Launch SQLR in your preferred mode from the project root:

*   **TCP Powerhouse (Default):**
    ```bash
    cargo run --release
    # Server ready on 127.0.0.1:7878! ⚡
    ```

*   **Web Experience:**
    ```bash
    cargo run --release -- --web-ui
    # Point your browser to http://127.0.0.1:8080! 🖱️
    ```

*   **Instant REPL:**
    ```bash
    cargo run --release -- --test-parser
    # Start querying instantly! >
    ```

## 💡 Usage Examples 💡

*(The existing Usage Examples section is quite good and clear, so we'll keep it largely the same for practical illustration.)*

### 1. TCP API Interaction (via `telnet`)

```bash
telnet 127.0.0.1 7878
```

*Welcome to the SQLR TCP Interface! Send SQL queries terminated by newline.*
**Type:** `CREATE TABLE users (id INTEGER, name TEXT);`⏎
**Response:** `{"error":false,"message":"Execution successful","result":"Table 'users' created successfully."}`

**Type:** `INSERT INTO users (id, name) VALUES (1, 'Alice');`⏎
**Response:** `{"error":false,"message":"Execution successful","result":"1 row(s) inserted."}`

**Type:** `SELECT name FROM users WHERE id = 1;`⏎
**Response:** `{"error":false,"message":"Execution successful","result":{"columns":["name"],"rows":[["Alice"]]}}`

*(Exit telnet using Ctrl+] then `quit`)*

### 2. Web UI Interaction

*   Run `cargo run --release -- --web-ui`.
*   Open `http://127.0.0.1:8080`.
*   Use the query box and hit submit!

*API via `curl`:*
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
*Execution Result: Table 'items' created successfully.*
`>` **Type:** `INSERT INTO items VALUES ('abc', 500);`⏎
*Execution Result: 1 row(s) inserted.*
`>` **Type:** `SELECT sku FROM items WHERE price > 100;`⏎
*Execution Result: Columns: ["sku"], Rows: [["abc"]]*
`>` **Type:** `exit`⏎
*Data saved. Exiting.*

## 🔌 TCP API Protocol 🔌

Integrate SQLR effortlessly:

*   **Connect:** TCP to `127.0.0.1:7878`.
*   **Send:** UTF-8 SQL queries, ending with `\n`.
*   **Receive:** UTF-8 JSON responses, ending with `\n`.
*   **Format:** Simple JSON structure (`{"error":bool,...}`) indicates success/failure and data/errors. Check `src/main.rs:process_query` for the exact structure.

## License

[BSD-3 Clause license](https://github.com/stuncs69/sqlr/blob/master/LICENSE)