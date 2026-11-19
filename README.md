# tidb-binary-multistmt

English | [简体中文](README.zh-CN.md) | [日本語](README.ja.md)

High-performance TiDB statement batching and pipelining library based on the MySQL **Binary Protocol** (`COM_STMT_EXECUTE`). **Inspired by PostgreSQL's Pipeline mode**, it executes multi-statement transactions in just **2 TCP round trips**, maximizing TiDB Plan Cache hits to dramatically reduce transaction latency and cut CPU consumption.

---

## The Problem & Motivation: Why Was This Built?

In typical transactional workloads, a single business transaction often consists of multiple SQL statements (e.g., query balance, deduct inventory, insert order, write audit log).

### 1. Traditional Dilemma: Round-Trip Latency (RTT)
Standard database drivers (`database/sql`) execute statements sequentially: "send one statement, wait for its response, send the next". A transaction with 5 statements, plus `BEGIN` and `COMMIT`, requires **7 network round trips**. In cloud environments across Availability Zones (AZs) where RTT is several milliseconds, transactions spend most of their time just waiting on network round trips.

### 2. Limitations of TiDB's Native Multi-Statements (Text Mode)
To reduce round trips, developers often turn to TiDB's native **Multi-Statements** feature to send multiple SQL statements together:
- **The Good**: When multiple SQL lines are sent in a batch, if an intermediate statement fails, TiDB **stops immediately** and does not execute subsequent statements.
- **The Bad**: TiDB Multi-Statements is restricted to the **Text Protocol** (`COM_QUERY`):
  1. **Sending Raw SQL Strings**: Cannot effectively utilize TiDB's **Plan Cache**. TiDB must repeatedly parse, compile, and optimize execution plans for every statement, causing **TiDB CPU usage to spike**.
  2. **Sending Text PREPARE / EXECUTE**: In MySQL's text protocol, passing parameters to `EXECUTE` **requires user variables** (`@var`). You must prepend `SET @a=...` before each statement. Production CPU profiling revealed that these extra `SET` statements generate massive parsing and execution overhead, defeating the purpose.

### 3. Design Inspiration: Emulating PostgreSQL's Pipeline Mode
Developers familiar with PostgreSQL (e.g., via `pgx` or `libpq`) appreciate its powerful **Pipeline mode**, which allows clients to queue and send multiple queries over the wire in a batch without waiting for each query's response.

In contrast, standard MySQL client libraries (such as Go's `database/sql`) do not support pipelining:
- They **cannot batch-send multiple SQL commands in one flight**;
- Nor can they **stream back individual results or errors for each statement** in the batch.

However, at the protocol level:
- **TCP naturally supports sending multiple MySQL commands back-to-back in the same TCP packet / stream.**
- **The MySQL binary protocol naturally embeds self-contained, ordered responses (OK, ERR, or ResultSet) for each command.**

The underlying protocols have always supported this capability—standard client drivers simply never exposed it. This library unlocks this latent power and packages it into an intuitive, developer-friendly Go API, bringing PostgreSQL-grade pipelining to TiDB and MySQL!

---

## The Solution: Binary Multi-Statements Pipeline

To achieve both **ultra-low latency** and **low CPU overhead**, we developed **`tidb-binary-multistmt`**. It completely abandons TiDB's native text multi-statements and leverages the MySQL **Binary Protocol** with network pipelining:

1. **Leverages Plan Cache to Slash CPU**: By executing binary prepared statements (`COM_STMT_EXECUTE`), TiDB reuses cached execution plans, drastically cutting query optimization overhead.
2. **Zero `SET` Overhead**: Parameters are encoded directly into the binary packet. No user variables (`@var`), zero `SET` statements.
3. **Pipelined into Just 2 TCP Round Trips**: Sends `BEGIN` and all binary `EXECUTE` commands in **a single TCP packet**, then reads all responses in a stream.

### Trade-off & How We Handle It: What if a Statement Fails?
- **The Defect**: Because all commands are flushed into the TCP stream at once, the server's socket buffer already has received the whole batch. If an intermediate statement fails, **the server will still execute subsequent commands already received** (it cannot automatically abort midway like text multi-statements).
- **Our Solution: Always Use Pessimistic Transactions**:
  - This library **strictly requires and only supports pessimistic transactions**. In pessimistic mode, row locks are acquired during statement execution.
  - The client streams all responses and pinpoints exactly which statement failed.
  - **Decide Commit or Rollback**:
    - If all statements succeed -> client sends `COMMIT`.
    - If any statement fails -> client sends `ROLLBACK`.
  - Even though subsequent statements were executed on the server, issuing a `ROLLBACK` in a pessimistic transaction safely undoes everything, **guaranteeing strict all-or-nothing atomicity (ACID)**.
  - The entire pessimistic transaction takes exactly **2 TCP communications**!

---

## Architecture & Flow (ASCII Diagrams)

### Approach Comparison
```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. Traditional 1-by-1 (N + 2 RTTs): More statements = High latency          │
│    App ── BEGIN ──> TiDB ──> App ── INSERT ──> TiDB ──> App ── COMMIT ──> TiDB │
│    [RTT 1]                  [RTT 2]                    [RTT 3]              │
├─────────────────────────────────────────────────────────────────────────────┤
│ 2. TiDB Native Multi-Statements (Text Mode): Lower latency, High CPU        │
│    App ── "SET @a=1; INSERT ...; SET @b=2; UPDATE ...;" ───────────> TiDB    │
│    - Raw SQL misses Plan Cache; heavy parsing and optimization CPU cost     │
│    - Text PREPARE requires SET @var, adding massive statement overhead       │
├─────────────────────────────────────────────────────────────────────────────┤
│ 3. This Package: Binary Pipeline (Emulates PG Pipeline, Low Latency + CPU)  │
│    App ── [ BEGIN + COM_STMT_EXECUTE(binary params) + ... ] ───────> TiDB   │
│    - Overcomes standard MySQL driver limits: batch-send & get per-stmt res  │
│    - Direct binary parameters, zero SET overhead; hits TiDB Plan Cache      │
│    - Entire pessimistic transaction takes only 2 TCP round trips!           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Pessimistic Transaction 2-RTT Flow
```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ Round Trip 1: Pack BEGIN and all binary EXECUTEs in 1 TCP send              │
│                                                                             │
│ App ───────────── [ BEGIN + EXECUTE 1 + EXECUTE 2 + EXECUTE 3 ] ──────────> TiDB
│                                                                             │
│                                                                (TiDB executes)
│                                                                             │
│ App <──────────── [ OK(BEGIN) + OK(stmt1) + ERROR(stmt2) + OK(stmt3) ] <── TiDB
│  │                                                                          │
│  │ (Client reads responses in order and pinpoints any failure)              │
└──┼──────────────────────────────────────────────────────────────────────────┘
   │
   ├─► [All Succeeded] ─── (Round Trip 2) Send COMMIT   ───> Transaction committed
   │
   └─► [Any Failed]    ─── (Round Trip 2) Send ROLLBACK ───> Rolled back cleanly
```

---

## Benchmark

Measured on a real TiDB Cloud cluster (workload mixing multiple writes and reads in transactions):

| Metric | 1-by-1 Round Trips (`database/sql`) | Binary Pipeline (This package) | Improvement |
|---|---|---|---|
| **Txn P95 Latency** | 62.4 ms | **31.5 ms** | **~50% reduction** |
| **TiDB CPU** | 205% | **195%** | Hits Plan Cache, CPU drops |

---

## Quick Start

### Installation
```bash
go get github.com/dulao5/tidb-binary-multistmt
```

### Basic Example

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	binarymultistmt "github.com/dulao5/tidb-binary-multistmt"
)

func main() {
	ctx := context.Background()
	dsn := "root:@tcp(127.0.0.1:4000)/test"

	// 1. Establish connection (use binarymultistmt.Open pool in production)
	conn, err := binarymultistmt.Dial(ctx, dsn)
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()

	// 2. Build the Batch
	b := binarymultistmt.NewBatch()

	// Add write statement: inspect result in callback
	b.Add("INSERT INTO accounts (id, balance) VALUES (?, ?)", []any{1, 100}, func(sr *binarymultistmt.StatementResult) {
		if sr.Err != nil {
			log.Printf("statement #%d failed: %v", sr.Index, sr.Err)
		}
	})

	// Add query statement: stream rows via Rows iterator
	b.Add("SELECT id, balance FROM accounts WHERE id = ?", []any{1}, func(sr *binarymultistmt.StatementResult) {
		if sr.Err != nil {
			log.Printf("statement #%d failed: %v", sr.Index, sr.Err)
			return
		}
		for row := sr.Rows.Next(); row != nil; row = sr.Rows.Next() {
			fmt.Printf("Account ID=%v, Balance=%v\n", row[0], row[1])
		}
	})

	// 3. Execute batch (Round Trip 1: sends BEGIN + all EXECUTEs together)
	res, err := conn.Execute(ctx, b)

	// 4. Handle results
	var commitErr *binarymultistmt.CommitError
	switch {
	case errors.As(err, &commitErr):
		// All statements succeeded, but COMMIT was rejected (e.g., write conflict).
		// TiDB already rolled back server-side; conn is still healthy.
		log.Printf("commit rejected, already rolled back server-side: %v", commitErr)

	case err != nil:
		// Connection or protocol failure: conn is broken, close it.
		conn.Close()
		log.Fatalf("connection/protocol error: %v", err)

	case !res.AllSucceeded:
		// One or more statements failed: Execute did NOT roll back automatically.
		// Send ROLLBACK (Round Trip 2) to cleanly undo all changes.
		log.Println("Batch contains failed statements, rolling back...")
		if rbErr := conn.Rollback(ctx); rbErr != nil {
			log.Printf("rollback failed: %v", rbErr)
		}

	default:
		// All statements succeeded: Execute automatically sent COMMIT (Round Trip 2).
		log.Println("Transaction committed successfully!")
	}
}
```

---

## Key Features

### 1. Connection Pool (`Open` / `AcquireConn`)
For long-running services, avoid dialing on every request by using the built-in pool:

```go
db, err := binarymultistmt.Open(dsn, 40) // maxConns
if err != nil { ... }
defer db.Close()

conn, err := db.AcquireConn(ctx)
if err != nil { ... }
defer conn.Close() // Returns conn to idle pool (or discards if broken)

res, err := conn.Execute(ctx, b)
```

### 2. Prepared Statement Cache (Plan Cache)
- The first time a SQL statement is used on a connection, `COM_STMT_PREPARE` is sent and cached in a connection-local LRU cache.
- Subsequent executions reuse the statement ID, directly hitting TiDB's Plan Cache.
- Default capacity is 90 statements (`conn.SetStmtCacheLimit(n)` to customize). Evicted entries automatically trigger `COM_STMT_CLOSE` to prevent server-side memory leaks.

### 3. Streaming Query Results (Zero Memory Bloat)
For queries, `sr.Rows` streams rows directly off the network socket:
- Never buffers large result sets into memory.
- Allows early termination (`break`); unread rows are drained automatically to preserve stream synchronization.

### 4. Dynamic `IN (?)` and Batch `INSERT` Helpers
```go
// Expands "WHERE id IN (?)" based on slice length
sql, args, err := binarymultistmt.ExpandIn("SELECT * FROM t WHERE id IN (?)", []any{ids})
b.Add(sql, args, nil)

// Expands batch "VALUES (?, ?), (?, ?)"
sql, args, err := binarymultistmt.ExpandValues("INSERT INTO t (id, name) VALUES (?, ?)", rows)
b.Add(sql, args, nil)
```

### 5. `ExecuteAutoCommit`: Transaction-Free Execution
For read-only batches or best-effort writes where partial execution is acceptable:
- Skips `BEGIN` and `COMMIT` entirely; runs under autocommit.
- Entire batch executes in **just 1 TCP round trip**!

```go
res, err := conn.ExecuteAutoCommit(ctx, b)
```

---

## Known Limitations

1. **Only Pessimistic Transactions**: Optimistic transactions defer conflict detection to commit time, preventing per-statement attribution and mid-batch error detection. Optimistic mode is not supported.
2. **TLS and Compression Not Yet Supported**: The connection interception hooks the socket prior to TLS wrapper initialization. Recommended for VPC/internal networks without TLS.
3. **Parameter Size Limit**: Does not implement `COM_STMT_SEND_LONG_DATA`. Single parameter values are capped at ~16MB (MySQL max packet payload).
4. **Statement Results Only Visible in Callbacks**: For zero-copy streaming performance, statement results are not stored on the connection after execution; process them in each statement's callback.

---

## License

[MIT License](LICENSE)
