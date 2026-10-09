# tidb-binary-multistmt

Pipelines a transaction's statements over MySQL's **binary** protocol
(`COM_STMT_PREPARE`/`COM_STMT_EXECUTE`) with no per-statement network round
trip, by writing every statement's `EXECUTE` packet back-to-back before
reading any response — instead of `database/sql`'s (and
[go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)'s) normal
write-command-then-synchronously-read-its-response convention.

## Why

[tidb-multistmt](https://github.com/dulao5/tidb-multistmt) already solves
"one round trip for N statements" using the **text** protocol: pack every
statement into one semicolon-joined `COM_QUERY` blob
(`CLIENT_MULTI_STATEMENTS`), with a `SET @_multistmt_statement_num=N` marker
before each statement so the client can recover per-statement
success/failure from a response stream that otherwise collapses that
information.

A real production CPU-profile diff found those marker `SET`s responsible for
the large majority of multi-statement mode's extra CPU over plain
one-statement-per-round-trip — not the text-vs-binary protocol choice itself.
This package tries pipelined **binary** `EXECUTE` instead: no markers needed
at all, because each `EXECUTE` is already a separate command with its own
response, delivered in order — the N-th response you read off the wire *is*
the N-th statement you sent, for free, as long as the connection stays
healthy. Measured on a real TiDB Cloud cluster:

| | normal (prepare + binary, one round trip per statement) | pipelined binary (this package) |
|---|---|---|
| Txn P95 | 62.4ms | 31.5ms |
| TiDB CPU | 205% | 195% |

## How the connection is obtained

MySQL command packets are self-delimited (length-prefixed) — nothing in the
wire protocol or in TiDB's connection read loop requires a round trip
between commands. The reason this isn't normally possible with
`database/sql` is that no mainstream client exposes an API to write ahead of
reading.

This package doesn't fork go-sql-driver/mysql to get around that. A custom
dial function, registered via the driver's own public
`mysql.RegisterDialContext` hook, dials the real TCP connection and also
pushes it into a channel this package reads from. go-sql-driver does its
normal handshake/authentication over that connection; once `Dial` returns,
authentication is done and this package takes over all reads/writes
directly, outside `database/sql`. From that point the underlying
`*sql.Conn`/`*sql.DB` pair is kept open only to hold the pool slot — they are
never used through the driver API again, because this package's raw writes
permanently desync the driver's internal per-connection sequence-number
bookkeeping.

## Usage

```go
conn, err := binarymultistmt.Dial(ctx, "user:pass@tcp(host:4000)/db")
if err != nil { ... }
defer conn.Close()

var failed []string
b := binarymultistmt.NewBatch()
b.Add("INSERT INTO accounts (id, balance) VALUES (?, ?)", []any{1, 100}, func(sr *binarymultistmt.StatementResult) {
    if sr.Err != nil {
        failed = append(failed, fmt.Sprintf("#%d (%s): %v", sr.Index, sr.SQL, sr.Err))
    }
})
b.Add("SELECT balance FROM accounts WHERE id = ?", []any{1}, func(sr *binarymultistmt.StatementResult) {
    if sr.Err != nil {
        failed = append(failed, fmt.Sprintf("#%d (%s): %v", sr.Index, sr.SQL, sr.Err))
        return
    }
    for row := sr.Rows.Next(); row != nil; row = sr.Rows.Next() {
        fmt.Println("balance:", row[0])
    }
})

res, err := conn.Execute(ctx, b)
if err != nil {
    // connection/protocol-level failure — conn is no longer usable, Close it
    conn.Close()
    return err
}
if !res.AllSucceeded {
    for _, f := range failed {
        log.Println("failed:", f)
    }
    // Execute did NOT send ROLLBACK — the transaction is still open on conn.
    // Decide what to do (roll back, inspect further, retry) and act
    // explicitly:
    if err := conn.Rollback(ctx); err != nil { ... }
    return
}
// every statement succeeded — Execute already sent COMMIT.
```

- Whether a statement is row-returning is detected automatically, from the
  server's own `COM_STMT_PREPARE` response (its column-count field is `0`
  for a non-row-returning statement) — unlike tidb-multistmt, the caller
  never declares this and so can't get it wrong.
- A statement's error and result are only ever visible through its
  `Callback` (see below) — `Execute` keeps no per-statement record after the
  batch finishes, so there's exactly one place to look, not two. `ExecuteResult`
  itself carries nothing but `AllSucceeded`.
- `Execute` auto-sends `COMMIT` only when every statement in the batch
  succeeded. On any failure it does **not** send `ROLLBACK` — each failing
  statement's `Callback` already saw the error as it happened, and the
  transaction is left open for the caller to explicitly resolve. This
  mirrors tidb-multistmt's own library/caller split: that library never
  sends `ROLLBACK` either; its caller does.
- A connection/protocol-level failure (as opposed to one statement's SQL
  error) makes `Execute` return a non-nil `error` and leaves `conn` unusable
  — `Close` it and `Dial` a new one.
- Only pessimistic transactions make sense here: a mid-pipeline failure does
  not stop already-written `EXECUTE`s from running (each is an independent
  command to the server — it has no idea they're "one batch"), so row locks
  must already be held as each statement runs, not deferred to commit, for
  "any failure → roll back everything" to stay correct.

### Connection pool (`DB`/`AcquireConn`)

`Dial` hands out one dedicated connection per call — fine for a short-lived
tool, wasteful for a long-running process that wants to pay the dial+auth
cost once and reuse connections across many batches. `Open`/`AcquireConn`
do that:

```go
db, err := binarymultistmt.Open("user:pass@tcp(host:4000)/db", 40) // maxConns
if err != nil { ... }
defer db.Close()

conn, err := db.AcquireConn(ctx) // reuses an idle one, or dials fresh if under maxConns
if err != nil { ... }
defer conn.Close() // returns conn to db's idle pool — or discards it, see below

b := binarymultistmt.NewBatch()
b.Add(...)
res, err := conn.Execute(ctx, b)
```

`conn.Close()` here does not close the socket: it returns `conn` to `db`'s
own idle list for the next `AcquireConn` call to reuse — unless `conn`
suffered a connection/protocol-level failure (the same failures the `Dial`
usage above tells you to discard the connection for), in which case `Close`
destroys it instead, automatically, based on the `Conn`'s own internal
bookkeeping. Either way, call `Close` exactly once and don't use `conn`
afterward — same contract as a `Dial`-sourced `Conn`.

This pool is deliberately not `database/sql`'s own connection pool: this
package needs to know, with certainty, which physical connection it's about
to speak raw binary protocol on, and `database/sql`'s pool has no API to
tell a caller that up front for a connection it's reusing from its idle
list. So the underlying `*sql.DB` inside `DB` is used only to dial and
authenticate new physical connections; `AcquireConn`/`Close` implement
their own idle-list reuse on top, where "is this the same physical
connection as last time" is never in question because this package itself
decides which `Conn` to hand back out.

If your program already has other code constructing its own `*sql.DB` the
normal way, build it alongside — not from — a `binarymultistmt.DB`: the two
are separate, independently-dialed connection pools, not two views onto one
pool, and `*binarymultistmt.DB` is not a `*sql.DB` (doesn't embed one, isn't
assignable to a `*sql.DB`-typed parameter/field). A typical setup has a
factory function returning both from one dsn:

```go
func NewPools(dsn string) (plain *sql.DB, binary *binarymultistmt.DB, err error) {
    plain, err = sql.Open("mysql", dsn)
    if err != nil { return nil, nil, err }
    binary, err = binarymultistmt.Open(dsn, 40)
    if err != nil { plain.Close(); return nil, nil, err }
    return plain, binary, nil
}
```

so most of the codebase keeps using `plain` (and its existing call sites
don't change at all), while the specific code path that wants pipelined
binary batches uses `binary.AcquireConn` instead.

### Callback: handle each statement where it's queued, streaming its rows

`Add`'s third argument, if non-nil, is invoked exactly once by `Execute`,
synchronously, in queue order — right where that statement's response
becomes available. This is the *only* way to see a statement's error or
result: `Execute` keeps no per-statement record after the batch finishes
(`ExecuteResult` carries nothing but `AllSucceeded`), so there's exactly one
place to look, not two.

```go
b := binarymultistmt.NewBatch()
b.Add("SELECT id, balance FROM accounts WHERE balance > ?", []any{1000}, func(sr *binarymultistmt.StatementResult) {
    if sr.Err != nil {
        log.Printf("query failed: %v", sr.Err)
        return
    }
    for row := sr.Rows.Next(); row != nil; row = sr.Rows.Next() {
        fmt.Println(row[0], row[1])
    }
    if err := sr.Rows.Err(); err != nil {
        log.Printf("stream broke mid-result: %v", err)
    }
})
res, err := conn.Execute(ctx, b)
```

For a row-returning statement (`sr.HasResultSet`), `sr.Rows` is a
`*RowIterator` that reads and decodes rows directly off the wire as the
callback calls `Next()` — `Execute` never buffers a result set into memory.
A statement queued with a `nil` Callback still has its rows drained (to keep
the pipelined stream in sync for later statements), they are just never
decoded or exposed anywhere — so a row-returning statement whose result you
care about needs a Callback.

The callback can also stop early (`break` after the first matching row, for
example): whatever it leaves unread is drained automatically once it
returns, so later statements in the same batch stay correctly aligned on the
wire — the callback is never required to consume to EOF itself, unlike
tidb-multistmt's `Callback`/`Rows` contract.

A statement that fails before ever producing a result-set header (an ERR
packet in place of one) still gets its callback invoked exactly once, with
`sr.Rows == nil` and `sr.Err` set — `Execute`'s own bookkeeping guarantees
"exactly once" regardless of which path a statement's response took.

`Next()` decodes one row per column in `sr.Rows.Columns()` (name/type/
`Unsigned`/`Decimals`, decoded from the server's own column metadata), `nil`
for SQL `NULL`. Go value types per MySQL column type:

| MySQL type family | Go type |
|---|---|
| `TINY`/`SHORT`/`LONG`/`LONGLONG`/`INT24`/`YEAR` | `int64`, or `uint64` if the column is `UNSIGNED` |
| `FLOAT`/`DOUBLE` | `float64` |
| `DATE`/`DATETIME`/`TIMESTAMP` | `time.Time` |
| `TIME` | `time.Duration` (can be negative; MySQL `TIME` isn't bounded to 24h) |
| `VARCHAR`/`TEXT`/`BLOB` family/`DECIMAL`/`JSON`/`ENUM`/`SET`/`BIT`/`GEOMETRY` | `[]byte` — this package doesn't know a column's charset well enough to decide when a `string` conversion is safe, so it leaves that (and its cost) to the caller |

### `WHERE id IN (?)` / bulk `INSERT`

Same two functions as tidb-multistmt, same calling convention — call before
`Batch.Add`:

```go
sql, args, err := binarymultistmt.ExpandIn("SELECT c FROM t WHERE id IN (?)", []any{ids})
b.Add(sql, args, nil)

sql, args, err := binarymultistmt.ExpandValues("INSERT INTO t (id, c) VALUES (?, ?)", rows)
b.Add(sql, args, nil)
```

As with tidb-multistmt, a variable-length `IN` list changes the rendered SQL
text (and therefore this package's internal PREPARE cache key) with the
list's length, so a call site whose length jitters a lot gets little benefit
from PREPARE reuse — pad to a fixed set of bucket sizes yourself if that
matters for your workload.

## Status

Experimental, ported from a benchmark originally embedded in
[database_workload](https://github.com/dulao5/database_workload). Verified
against a real TiDB (v8.5.8): pipelined inserts commit correctly, a
duplicate-key failure mid-batch is attributed to the right statement index
and leaves the transaction open for the caller's `Rollback`, a `SELECT` in
the middle of a batch doesn't desync the statements after it, every
supported parameter type round-trips correctly (read back through a normal
driver connection, confirming TiDB itself understood the encoded values —
not just that this package's own encode/decode is self-consistent),
`ExpandIn`/`ExpandValues` compose correctly with the pipelined-binary
execution path end to end, and a `SELECT` covering every supported column
type (including an unsigned max value and a microsecond-precision
`DATETIME`) decodes correctly through this package's own `RowIterator` —
column metadata and row bytes TiDB actually sends, not hand-crafted
fixtures.

Every entry point that parses bytes coming off the wire (`readPacket`,
`decodeColumnDef`, `decodeBinaryRow`, `drainExecuteResponse`,
`readOKorErr`) has a Go native fuzz target (`fuzz_test.go`) — CI runs each
briefly on every push, and the full corpus (including past crashers) is
replayed as ordinary deterministic tests on every `go test`. Fuzzing already
found and fixed one real bug this way: an untrusted column count was used
directly as a slice-capacity argument, panicking with "cap out of range" on
a maliciously/accidentally huge value.

Parameter binding uses genuine `COM_STMT_EXECUTE` binary protocol
parameters throughout — including through `ExpandIn`/`ExpandValues` — never
string-literal substitution, so (unlike tidb-multistmt's text-protocol
`SET`-literal mechanism) there is no escaping-based injection surface to
reason about here.

**Known limitations** (tracked as issues in this repo):

- **TLS is not supported yet.** The dial-hook hijack captures the net.Conn
  *before* go-sql-driver/mysql wraps it in `tls.Client(...)` during the
  handshake, so if the DSN requests TLS, this package ends up writing
  plaintext binary-protocol bytes onto a connection the server expects to be
  encrypted — a hard protocol break. Only use this package against
  connections that don't require TLS (e.g. a private-network link) until
  this is fixed.
- **MySQL protocol compression (`compress=true`) is not supported either,
  for the same reason as TLS** — the dial-hook hijack captures the net.Conn
  before go-sql-driver/mysql would apply compression framing during the
  handshake, so this package's raw `readPacket`/`writePacket` would desync
  against a compressed stream the same way they would against a TLS one.
  Paused alongside TLS; don't use `compress=true` until this is addressed.
- **No `COM_STMT_SEND_LONG_DATA` support.** Every parameter value must fit
  in a single packet (`maxPacketPayload`, ~16MB) — there's no fallback for
  larger values the way go-sql-driver/mysql has. In practice this is a very
  generous ceiling for normal column values; it only matters for genuinely
  large BLOBs/TEXT.
- **Assumes `CLIENT_DEPRECATE_EOF` is never negotiated.** This package's
  `PREPARE`/`EXECUTE` response parsing expects EOF packets after
  param-definition and column-definition lists, because it piggybacks on
  go-sql-driver/mysql's handshake (via the dial-hook hijack above) and that
  driver doesn't request `CLIENT_DEPRECATE_EOF` today — confirmed by reading
  its source: the capability constant is defined but never set during
  handshake. That's a coupling to a specific driver version's behavior, not
  something this package independently negotiates or verifies. If you
  vendor a different (or future) go-sql-driver/mysql version that *does*
  start negotiating deprecate-EOF, check this before relying on this
  package — the failure mode is a silent parse desync, not a loud error.

## License

MIT
