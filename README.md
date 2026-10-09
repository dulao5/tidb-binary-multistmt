# tidb-binary-multistmt

English | [简体中文](README.zh-CN.md) | [日本語](README.ja.md)

Sends every statement in a batch over MySQL's **binary** protocol
(`COM_STMT_PREPARE`/`COM_STMT_EXECUTE`) in a single network round trip,
instead of one round trip per statement.

## At a glance

- **What**: write every statement's `EXECUTE` packet back-to-back, then read
  all the responses — instead of `database/sql`'s normal
  write-one-then-read-its-response convention.
- **Benefit**: measured on a real TiDB Cloud cluster, this roughly halved
  transaction P95 latency (62.4ms → 31.5ms) versus one round trip per
  statement.
- **Trade-off**: a statement's result is only visible through its
  `Callback`, not afterward; only pessimistic transactions are supported;
  TLS and protocol compression aren't supported yet. See
  [Known limitations](#known-limitations).

## Why

For a batch of N statements, this cuts N round trips down to one, because
each `EXECUTE` already carries its own response, in order — the N-th
response read off the wire *is* the N-th statement sent, for free, as long
as the connection stays healthy.

| | one round trip per statement | pipelined (this package) |
|---|---|---|
| Txn P95 | 62.4ms | 31.5ms |
| TiDB CPU | 205% | 195% |

### Background: why binary, not text

[tidb-multistmt](https://github.com/dulao5/tidb-multistmt) already batches N
statements into one round trip, over the **text** protocol: it joins them
into one `COM_QUERY` blob and inserts a `SET @_multistmt_statement_num=N`
marker before each statement, so per-statement success/failure survives a
response stream that would otherwise collapse it. A production CPU-profile
comparison found those marker `SET`s — not the text-vs-binary choice itself
— responsible for most of multi-statement mode's extra CPU. This package
pipelines binary `EXECUTE` instead, which needs no markers at all.

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
var commitErr *binarymultistmt.CommitError
switch {
case errors.As(err, &commitErr):
    // Every statement succeeded, but COMMIT itself was rejected (e.g. a
    // write conflict) — TiDB already rolled back server-side, so conn is
    // still healthy and there's nothing to Rollback. Decide whether to
    // retry the whole batch.
    log.Println("commit rejected, already rolled back:", commitErr)
case err != nil:
    // connection/protocol-level failure — conn is no longer usable, Close it
    conn.Close()
    return err
case !res.AllSucceeded:
    for _, f := range failed {
        log.Println("failed:", f)
    }
    // Execute did NOT send ROLLBACK — the transaction is still open on conn.
    // Decide what to do (roll back, inspect further, retry) and act
    // explicitly:
    if err := conn.Rollback(ctx); err != nil { ... }
default:
    // every statement succeeded — Execute already sent COMMIT.
}
```

A few things to know:

- Whether a statement returns rows is detected automatically from the
  server's own `COM_STMT_PREPARE` response — you never declare it, so you
  can't get it wrong.
- A statement's error and result are visible **only** through its
  `Callback` (see below). `Execute` keeps no per-statement record
  afterward — `ExecuteResult` carries nothing but `AllSucceeded`.
- `Execute` sends `COMMIT` only if every statement succeeded. On any
  failure it does **not** send `ROLLBACK` — the transaction stays open on
  `conn` for you to resolve explicitly.
- A connection/protocol-level failure (not a statement error, not a
  `*CommitError`) makes `Execute` return a non-nil `error` and leaves
  `conn` unusable — `Close` it and `Dial` a new one.
- If every statement succeeds but the server then rejects `COMMIT` itself
  (e.g. a write conflict), `Execute` returns a `*CommitError` (check with
  `errors.As`). TiDB has already rolled back server-side when this
  happens — `conn` stays healthy, and there's nothing to roll back.
- Only pessimistic transactions work here. A statement that fails
  mid-pipeline doesn't stop the `EXECUTE`s already written from running (the
  server has no idea they're "one batch"), so row locks must already be
  held as each statement runs — not deferred to `COMMIT` — for "any failure
  rolls back everything" to stay correct. Optimistic transactions push
  conflict detection to `COMMIT` instead, where a conflict fails the whole
  batch at once with no way to tell which statement caused it. This
  package doesn't support optimistic transactions.

### `ExecuteAutoCommit`: skip `BEGIN`/`COMMIT` entirely

`Execute` sends `BEGIN` pipelined together with the batch's `EXECUTE`s —
not as its own separate write-then-wait round trip — so once every
statement is already prepared (the common case on a reused `Conn`; see
[Prepared-statement cache](#prepared-statement-cache) below), `Execute`
costs exactly one round trip more than the pipelined `EXECUTE`s themselves:
a synchronous `COMMIT` on success. `ExecuteAutoCommit` runs the same
pipeline without `BEGIN`/`COMMIT` at all — each statement commits on its own
as it runs, same as issuing them one at a time under MySQL's default
autocommit:

```go
res, err := conn.ExecuteAutoCommit(ctx, b)
```

There's nothing to roll back afterward — whatever ran already committed,
successful or not, so calling `Rollback` would just be a harmless no-op.
`*CommitError` can't happen here either, since there's no `COMMIT` to
reject. Use this for a read-only batch, or one where a partial failure
genuinely doesn't need undoing (e.g. best-effort logging). Use `Execute`
instead whenever you need "every statement lands, or none do" — that's the
safer default when in doubt.

### Connection pool (`DB`/`AcquireConn`)

`Dial` hands out one dedicated connection per call — fine for a short-lived
tool, wasteful for a long-running process reusing connections across many
batches. `Open`/`AcquireConn` fix that:

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

`conn.Close()` here doesn't close the socket: it returns `conn` to `db`'s
idle list for the next `AcquireConn` to reuse — unless `conn` suffered a
connection/protocol-level failure, in which case `Close` destroys it
instead, automatically. Either way, call `Close` exactly once and don't
reuse `conn` afterward.

This pool is deliberately not `database/sql`'s own: this package needs to
know *which* physical connection it's about to speak raw binary protocol
on, and `database/sql`'s pool has no API to tell a caller that up front for
a connection it's reusing from its idle list. So the `*sql.DB` inside `DB`
only dials and authenticates; `AcquireConn`/`Close` implement their own
idle-list reuse on top.

If you already construct your own `*sql.DB` elsewhere, build it alongside —
not from — a `binarymultistmt.DB`: they're two separate, independently
dialed pools (`*binarymultistmt.DB` doesn't embed `*sql.DB` and isn't
assignable to one). A typical setup returns both from one DSN:

```go
func NewPools(dsn string) (plain *sql.DB, binary *binarymultistmt.DB, err error) {
    plain, err = sql.Open("mysql", dsn)
    if err != nil { return nil, nil, err }
    binary, err = binarymultistmt.Open(dsn, 40)
    if err != nil { plain.Close(); return nil, nil, err }
    return plain, binary, nil
}
```

Most of the codebase keeps using `plain` unchanged; only the code path that
wants pipelined binary batches uses `binary.AcquireConn`.

### Prepared-statement cache

A `Conn` only sends `COM_STMT_PREPARE` for a given SQL text once; every
later `Add` with the same text reuses the cached statement ID. The cache
lives as long as the `Conn` does (through however many `Execute` calls, and
however many `AcquireConn`/`Close` round trips if it's pool-sourced) and is
keyed by exact SQL text — see [the `ExpandIn`/`ExpandValues`
section](#where-id-in---bulk-insert) below for what that implies for
dynamically generated text.

Past `SetStmtCacheLimit` entries (90 by default), adding a new statement
evicts the least-recently-used one and sends `COM_STMT_CLOSE` for it, so
neither this package's own map nor the server's prepared-statement table
grows without bound on a long-lived `Conn`. 90 sits just under TiDB's own
default `tidb_session_plan_cache_size` (100 plans per session, as of
v8.5) — since this package hijacks its connection for exclusive use, that
session's plan cache holds nothing but plans for statements this `Conn`
itself prepared, so keeping the client-side cache a bit smaller keeps this
`Conn`'s own LRU the one deciding evictions, rather than occasionally
racing the server's:

```go
conn.SetStmtCacheLimit(50) // e.g. to match a non-default tidb_session_plan_cache_size
```

Call it right after `Dial`/`AcquireConn`, before the first `Execute` — it
only governs evictions `prepare()` performs afterward. `n <= 0` disables
eviction entirely (the cache — and the server's prepared-statement table
behind it — grows without bound).

### Callback: handle each statement where it's queued, streaming its rows

`Add`'s third argument, if non-nil, is invoked exactly once by `Execute`,
synchronously, in queue order — right where that statement's response
becomes available:

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
A statement queued with a `nil` Callback still has its rows drained (to
keep later statements aligned on the wire), they're just never decoded or
exposed — so a row-returning statement whose result you care about needs a
Callback.

The callback can also stop early (e.g. `break` after the first matching
row) — whatever it leaves unread is drained automatically once it returns,
so you never have to consume all the way to EOF yourself, and later
statements in the batch stay correctly aligned regardless.

A statement that fails before ever producing a result-set header (an ERR
packet instead) still gets its callback invoked exactly once, with
`sr.Rows == nil` and `sr.Err` set.

`Next()` decodes one row per column in `sr.Rows.Columns()`, `nil` for SQL
`NULL`. Go value types per MySQL column type:

| MySQL type family | Go type |
|---|---|
| `TINY`/`SHORT`/`LONG`/`LONGLONG`/`INT24`/`YEAR` | `int64`, or `uint64` if the column is `UNSIGNED` |
| `FLOAT`/`DOUBLE` | `float64` |
| `DATE`/`DATETIME`/`TIMESTAMP` | `time.Time` |
| `TIME` | `time.Duration` (can be negative; MySQL `TIME` isn't bounded to 24h) |
| `VARCHAR`/`TEXT`/`BLOB` family/`DECIMAL`/`JSON`/`ENUM`/`SET`/`BIT`/`GEOMETRY` | `[]byte` — this package doesn't know a column's charset, so it leaves the `string` conversion (and its cost) to the caller |

### `WHERE id IN (?)` / bulk `INSERT`

Two helper functions — call them before `Batch.Add`:

```go
sql, args, err := binarymultistmt.ExpandIn("SELECT c FROM t WHERE id IN (?)", []any{ids})
b.Add(sql, args, nil)

sql, args, err := binarymultistmt.ExpandValues("INSERT INTO t (id, c) VALUES (?, ?)", rows)
b.Add(sql, args, nil)
```

A variable-length `IN` list changes the rendered SQL text (and so this
package's internal PREPARE cache key) with the list's length, so a call
site whose length jitters a lot gets little benefit from PREPARE reuse — and
past [`SetStmtCacheLimit`](#prepared-statement-cache), actively thrashes the
LRU (each distinct length both evicts an older entry and cold-`PREPARE`s a
new one). Pad to a fixed set of bucket sizes if that matters for your
workload.

## How it works

MySQL command packets are self-delimited — nothing in the protocol
requires a round trip between commands. `database/sql` just doesn't expose
an API to write ahead of reading.

This applies just as much to `BEGIN` (a `COM_QUERY`) as to a `COM_STMT_EXECUTE`
pipelined behind it: `Execute` writes `BEGIN` and every statement's `EXECUTE`
in one unbroken sequence, then reads `BEGIN`'s response followed by each
`EXECUTE`'s in order — rather than waiting for `BEGIN`'s own OK before
writing anything else, which is where the "two round trips" in older
versions of this package came from.

This package doesn't fork go-sql-driver/mysql to get around that. It
registers a custom dial function via the driver's own public
[`mysql.RegisterDialContext`](https://github.com/go-sql-driver/mysql) hook,
which captures the real `net.Conn` while go-sql-driver does its normal
handshake/auth over it. Once `Dial` returns, authentication is done and
this package takes over all reads/writes directly — the underlying
`*sql.Conn`/`*sql.DB` is kept open only to hold the pool slot, and is never
touched again through the driver API (this package's raw writes would
desync the driver's internal per-connection packet-sequence bookkeeping).

## Status

Experimental, extracted from a benchmark originally embedded in
[database_workload](https://github.com/dulao5/database_workload). Verified
against a real TiDB (v8.5.8):

- pipelined inserts commit correctly; a duplicate-key failure mid-batch is
  attributed to the right statement and leaves the transaction open for
  `Rollback`
- a `SELECT` in the middle of a batch doesn't desync the statements after it
- every supported parameter type round-trips correctly, confirmed by
  reading it back through a normal driver connection (not just this
  package's own encode/decode being self-consistent)
- `ExpandIn`/`ExpandValues` compose correctly with the pipelined execution
  path end to end
- a `SELECT` covering every supported column type (including an unsigned
  max value and a microsecond-precision `DATETIME`) decodes correctly
  through `RowIterator`, using real column metadata and row bytes from TiDB
- `ExecuteAutoCommit`: a later statement's failure doesn't roll back an
  earlier one in the same batch, because there was never a transaction to
  roll back
- `*CommitError`: reproduced against a genuine write conflict (two `Conn`s
  racing an `UPDATE` under TiDB's optimistic transaction mode) — the losing
  `Conn`'s `Execute` returns a `*CommitError`, and that same `Conn` is then
  confirmed still usable for another `Execute` call
- `BEGIN` pipelined together with the batch's `EXECUTE`s (not written and
  synchronously awaited on its own) still produces a correct, committed
  transaction
- a statement evicted from `SetStmtCacheLimit`'s LRU cache (and
  `COM_STMT_CLOSE`'d for it) prepares and runs correctly the next time its
  SQL text is reused on the same `Conn`

Every entry point that parses bytes off the wire (`readPacket`,
`decodeColumnDef`, `decodeBinaryRow`, `drainExecuteResponse`,
`readOKorErr`) has a Go native fuzz target (`fuzz_test.go`) — CI runs each
briefly on every push, and the full corpus (including past crashers)
replays as ordinary deterministic tests on every `go test`. Fuzzing already
found and fixed one real bug this way: an untrusted column count used
directly as a slice-capacity argument, panicking on a maliciously/
accidentally huge value.

Parameter binding always uses genuine `COM_STMT_EXECUTE` binary protocol
parameters — never string-literal substitution — so there's no
escaping-based injection surface to reason about here.

## Known limitations

- **TLS is not supported yet.** The dial-hook captures the `net.Conn`
  *before* go-sql-driver/mysql would wrap it in `tls.Client(...)` during
  the handshake, so a TLS-requesting DSN ends up with this package writing
  plaintext binary-protocol bytes onto a connection the server expects to
  be encrypted. Only use this package where TLS isn't required (e.g. a
  private-network link) until this is fixed.
- **Protocol compression (`compress=true`) isn't supported either, for the
  same reason as TLS** — the dial-hook captures the connection before
  go-sql-driver/mysql would apply compression framing. Paused alongside
  TLS.
- **No `COM_STMT_SEND_LONG_DATA` support.** Every parameter value must fit
  in a single packet (`maxPacketPayload`, ~16MB) — generous for normal
  column values, but a real limit for large BLOBs/TEXT.
- **Assumes `CLIENT_DEPRECATE_EOF` is never negotiated.** This package
  piggybacks on go-sql-driver/mysql's handshake, and that driver doesn't
  request this capability today (confirmed by reading its source). If you
  vendor a driver version that does negotiate it, check this first — the
  failure mode is a silent parse desync, not a loud error.

## License

MIT
