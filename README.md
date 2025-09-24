# Go Database

A lightweight relational database implemented in Go, supporting basic SQL operations, transactions, indexing, caching, and crash recovery.

## Features

- ✅ **SQL Parsing & Execution**: Supports basic SQL operations like CREATE, INSERT, SELECT, UPDATE, DELETE
- ✅ **Transaction Support**: ACID transactions based on MVCC + 2PL with Repeatable Read isolation level
- ✅ **B+ Tree Indexing**: Primary and secondary indexes for fast lookups and range queries
- ✅ **Smart Caching**: Supports LRU and W-TinyLFU caching strategies for improved read performance
- ✅ **Crash Recovery**: Based on WAL (Write-Ahead Logging) and Double Write Buffer
- ✅ **Concurrency Control**: Row-level locking with deadlock detection
- ✅ **Network Protocol**: TCP-based client-server architecture

## Quick Start

### 1. Requirements

- Go 1.18+

### 2. Build Project

```bash
git clone https://github.com/DyingDown/Go-Database.git
cd Go-Database
go mod tidy
```

### 3. Start Server

Create new database:
```bash
# Clean old data and create new database
rm -rf /tmp/test && mkdir /tmp/test
go run main.go --server --create --path=/tmp/test/
```

Open existing database:
```bash
go run main.go --server --open --path=/tmp/test/
```

After server starts, it will listen on `127.0.0.1:8080`. Simply type `exit` to quit.

### 4. Start Client

Open another terminal window:
```bash
go run main.go --client
```

### 5. Execute SQL Statements

Enter SQL statements in the client:

```sql

create table student (id int, name string);

insert into student (id, name) values (1, 'Alice');
insert into student (id, name) values (2, 'Bob');

select * from student;
select * from student where id = 1;

update student set name = 'Charlie' where id = 1;

delete from student where id = 2;

begin;
insert into student (id, name) values (3, 'David');
select * from student;
commit;

exit;
```

## Architecture

### Overall Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Client/Server Layer                        │
│            TCP Communication + Gob Serialization            │
└─────────────────┬───────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────┐
│                   Parser Layer                              │
│           SQL Parsing → AST (Abstract Syntax Tree)          │
└─────────────────┬───────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────┐
│                Table Manager Layer                          │
│          Business Logic Coordination, SQL Entry Point       │
└─────────────────┬───────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────┐
│              Transaction/Version Layer                      │
│      MVCC Version Control + 2PL Concurrency + Deadlock     │
└─────────────────┬───────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────┐
│                 Storage Engine Layer                        │
├──────────────┬──────────────┬──────────────┬────────────────┤
│   Cache      │    Pager     │    Index     │   Recovery     │
│ • LRU        │ • Page Mgmt  │ • B+ Tree    │ • Redo Log     │
│ • W-TinyLFU  │ • Memory Map │ • Primary    │ • Double Write │
│              │ • Page Alloc │ • Secondary  │ • WAL          │
└──────────────┴──────────────┴──────────────┴────────────────┘
```

### Core Modules

#### 1. **Parser Module**
- **Lexical Analysis**: `Tokenizer.go` converts SQL strings to Token streams
- **Syntax Analysis**: `parser.go` parses Token streams into AST
- **AST Definition**: `ast/` directory defines abstract syntax trees for various SQL statements

#### 2. **Table Manager (TBM)**
- Serves as the coordination center for business logic, handling all SQL operations
- Manages table creation, deletion, and data operations
- Coordinates storage engine, transaction management, indexing, and other modules

#### 3. **Version Manager (VM)**
- **MVCC**: Multi-Version Concurrency Control, each row contains xmin/xmax version information
- **2PL**: Two-Phase Locking protocol, UPDATE/DELETE operations use row-level locks
- **Deadlock Detection**: Deadlock detection and resolution based on wait-for graphs
- **Transaction Management**: Transaction begin, commit, and rollback

#### 4. **Storage Engine**

**Data Management (DataManager)**:
- Coordinates storage components like page manager, indexes, and cache
- Provides unified data operation interfaces

**Page Management (Pager)**:
- Manages allocation, reading, writing, and caching of data pages
- Supports different types of data pages (metadata pages, record pages, etc.)

**Index System**:
- **B+ Tree**: Supports primary and secondary indexes
- **Range Queries**: Efficient interval searches
- **Insert Optimization**: Page splitting and merging

**Cache System**:
- **LRU**: Least Recently Used caching strategy
- **W-TinyLFU**: Smart caching strategy based on frequency and time
- **Cache Hit Rate Optimization**: Improves read performance

#### 5. **Recovery System**

**WAL (Write-Ahead Logging)**:
- Writes Redo logs before all data modifications
- Ensures transaction durability and consistency

**Double Write Buffer**:
- Writes to double-write buffer before writing data pages
- Prevents data corruption from partial page writes

**Crash Recovery**:
- Scans and redos incomplete operations from Redo logs during startup
- Checkpoint-based incremental recovery

### SQL Execution Flow

Example with `SELECT * FROM student WHERE id = 1`:

```
1. Parser: SQL → AST
2. TableManager: Route to SELECT handler
3. VersionManager: Check transaction status, start temporary transaction
4. DataManager: 
   - Check if WHERE condition exists
   - Use B+ tree index to lookup id = 1
5. Cache: Check if page is in cache
6. Pager: Read page from disk (if cache miss)
7. MVCC: Check row version visibility
8. Return qualifying result set
```

## Directory Structure

```
├── client/          # Client implementation
├── parser/          # SQL parser
│   ├── ast/        # Abstract syntax tree definitions
│   └── token/      # Lexical analysis
├── server/          # Server implementation  
├── storage/         # Storage engine
│   ├── bplustree/  # B+ tree indexes
│   ├── dm/         # Data manager
│   ├── pager/      # Page management
│   ├── recovery/   # Crash recovery
│   └── redo/       # Redo logging
├── tbm/             # Table manager
├── transporter/     # Network protocol
├── util/            # Utilities
│   └── cache/      # Cache implementation
└── vm/              # Transaction and version management
```

## License

MIT License
