# WundraDB

WundraDB is a distributed SQL database built in Rust with B+Tree storage, Write-Ahead Logging (WAL), and TCP-based client-server architecture.

## 🔧 Setup Instructions

### Prerequisites
- Rust 1.70+ with Cargo
- Git (for cloning)

### Installation

#### From Source
```bash
git clone https://github.com/your-org/wundradb.git
cd wundradb
cargo build --release --workspace
```

#### From GitHub Releases
Download pre-built binaries from: [GitHub Releases](https://github.com/WundraDB/wundradb/releases)

Available binaries:
- `wundradb-server` - Database server
- `wundradb-cli` - Command-line client

### Building from Source
```bash
# Build all components
cargo build --workspace

# Build optimized release
cargo build --release --workspace

# Run tests
cargo test --workspace
```

## 🚀 Running the Server

Start the WundraDB server:
```bash
# Development
cargo run --bin wundradb-server

# Production (from release build)
./target/release/wundradb-server

# Custom port
cargo run --bin wundradb-server -- --port 3307
```

Server configuration:
- Default port: `3306` (MySQL compatible)
- Data directory: `./data/`
- WAL file: `./data/wal.log`
- B+Tree storage: `./data/storage.db`

## 🟣 Using the CLI

Connect to WundraDB using the CLI client:
```bash
# Connect to local server
cargo run --bin wundradb-cli

# Connect to remote server
cargo run --bin wundradb-cli -- --host 192.168.1.100 --port 3306

# From release build
./target/release/wundradb-cli --host localhost --port 3306
```

### CLI Commands
- `help` - Show available commands
- `quit` or `exit` - Exit the CLI
- `\d` - List all tables
- `\dt <table>` - Describe table schema
- Any SQL query (see below)

## 📦 Running SQL Commands

WundraDB supports core SQL operations:

### Create Tables
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255),
    age INTEGER
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    price DECIMAL(10,2),
    category VARCHAR(50)
);
```

### Insert Data
```sql
INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 25);
INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 30);

INSERT INTO products (id, name, price, category) VALUES (1, 'Laptop', 999.99, 'Electronics');
INSERT INTO products (id, name, price, category) VALUES (2, 'Book', 29.99, 'Education');
```

### Query Data
```sql
-- Select all
SELECT * FROM users;

-- Select with conditions
SELECT name, email FROM users WHERE age > 25;

-- Select with ordering
SELECT * FROM products ORDER BY price DESC;

-- Count records
SELECT COUNT(*) FROM users;
```

### Current Limitations
- No JOINs yet
- No UPDATE/DELETE operations
- No indexes beyond primary key
- No transactions (WAL handles durability)
- No advanced SQL features (GROUP BY, HAVING, etc.)

## 💾 How Persistence Works

WundraDB uses a dual-persistence approach:

### Write-Ahead Log (WAL)
- All write operations are logged to `data/wal.log`
- Sequential append-only format for durability
- Replayed on server startup to restore state
- Uses efficient binary serialization

### B+Tree Storage
- In-memory B+Tree with periodic disk snapshots
- Stored in `data/storage.db` using bincode format
- Automatic background saves every 1000 operations
- Provides fast key-value lookups and range queries

### Data Directory Structure
```
data/
├── wal.log        # Write-ahead log
├── storage.db     # B+Tree snapshot
└── metadata.json  # Database metadata
```

### Recovery Process
1. Server starts and checks for existing WAL
2. Replays all WAL entries to rebuild in-memory state
3. Loads last B+Tree snapshot if available
4. Applies any WAL entries newer than snapshot
5. Ready to accept new connections

## 🧪 Testing and Contributing

### Running Tests
```bash
# Run all tests
cargo test --workspace

# Run specific crate tests
cargo test -p wundradb-core
cargo test -p wundradb-server
cargo test -p wundradb-cli

# Run with logging
RUST_LOG=debug cargo test --workspace
```

### Benchmarking
```bash
# Build benchmarks
cargo bench --workspace

# Run specific benchmarks
cargo bench -p wundradb-core
```

### Contributing
1. Fork the repository
2. Create feature branch: `git checkout -b feature/awesome-feature`
3. Make changes with tests
4. Run: `cargo test --workspace`
5. Run: `cargo clippy --workspace`
6. Run: `cargo fmt --workspace`
7. Submit pull request

### Project Structure
```
wundradb/
├── Cargo.toml              # Workspace manifest
├── Manual.md               # This file
├── crates/
│   ├── core/               # Core database engine
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── sql/        # SQL parsing and execution
│   │   │   ├── storage/    # B+Tree and storage layer
│   │   │   ├── txn/        # Transaction and WAL
│   │   │   └── raft/       # Distributed consensus (stub)
│   ├── server/             # TCP server
│   │   └── src/main.rs
│   └── cli/                # Command-line client
│       └── src/main.rs
```

## 🌐 Future Plans

### Distributed Features (Raft Module)
- [ ] Leader election and log replication
- [ ] Cluster membership management
- [ ] Automatic failover and recovery
- [ ] Read replicas

### Advanced SQL Features
- [ ] JOINs (INNER, LEFT, RIGHT, FULL)
- [ ] UPDATE and DELETE operations
- [ ] Transactions (BEGIN, COMMIT, ROLLBACK)
- [ ] Secondary indexes
- [ ] Aggregate functions (SUM, AVG, MAX, MIN)
- [ ] GROUP BY and HAVING clauses
- [ ] Subqueries and CTEs

### Performance & Reliability
- [ ] Query optimizer and planner
- [ ] Connection pooling
- [ ] Prepared statements
- [ ] Streaming results for large datasets
- [ ] Compression for storage and network
- [ ] Backup and restore utilities

### Protocol Support
- [ ] gRPC API for better performance
- [ ] PostgreSQL wire protocol compatibility
- [ ] REST API for web applications
- [ ] GraphQL endpoint

### Monitoring & Operations
- [ ] Metrics and monitoring endpoints
- [ ] Log levels and structured logging
- [ ] Health checks and admin commands
- [ ] Configuration management
- [ ] Docker containerization

## 📞 Support

- GitHub Issues: [Report bugs and request features](https://github.com/WundraDB/wundradb/issues)
- Documentation: [Wiki](wundradb.netlify.app)
- Discussions: [Community forum](https://github.com/WundraDB/wundradb/discussions)

## 📄 License

WundraDB is licensed under the Apache 2.0 License. See LICENSE file for details.
