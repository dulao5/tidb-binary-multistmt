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

b := binarymultistmt.NewBatch()
b.Add("INSERT INTO accounts (id, balance) VALUES (?, ?)", []any{1, 100}, false)
b.Add("SELECT balance FROM accounts WHERE id = ?", []any{1}, true)

res, err := conn.Execute(ctx, b)
if err != nil {
    // connection/protocol-level failure — conn is no longer usable, Close it
    conn.Close()
    return err
}
if !res.AllSucceeded {
    for _, r := range res.Results {
        if r.Err != nil {
            log.Printf("statement #%d (%s) failed: %v", r.Index, r.SQL, r.Err)
        }
    }
    // Execute did NOT send ROLLBACK — the transaction is still open on conn.
    // Decide what to do (roll back, inspect further, retry) and act
    // explicitly:
    if err := conn.Rollback(ctx); err != nil { ... }
    return
}
// every statement succeeded — Execute already sent COMMIT.
```

- `HasResultSet` (the third `Add` argument) must be `true` iff the statement
  is row-returning, same convention as tidb-multistmt — get it wrong and
  every later statement in the batch desyncs.
- `Execute` auto-sends `COMMIT` only when every statement in the batch
  succeeded. On any failure it does **not** send `ROLLBACK` — it returns
  per-statement results (which index, its SQL, the error) and leaves the
  transaction open for the caller to explicitly resolve. This mirrors
  tidb-multistmt's own library/caller split: that library never sends
  `ROLLBACK` either; its caller does.
- A connection/protocol-level failure (as opposed to one statement's SQL
  error) makes `Execute` return a non-nil `error` and leaves `conn` unusable
  — `Close` it and `Dial` a new one.
- Only pessimistic transactions make sense here: a mid-pipeline failure does
  not stop already-written `EXECUTE`s from running (each is an independent
  command to the server — it has no idea they're "one batch"), so row locks
  must already be held as each statement runs, not deferred to commit, for
  "any failure → roll back everything" to stay correct.

## Status

Experimental, ported from a benchmark originally embedded in
[database_workload](https://github.com/dulao5/database_workload). Verified
against a real TiDB (v8.5.8): pipelined inserts commit correctly, a
duplicate-key failure mid-batch is attributed to the right statement index
and leaves the transaction open for the caller's `Rollback`, and a `SELECT`
in the middle of a batch doesn't desync the statements after it.

**Known limitations** (tracked as issues in this repo):

- **TLS is not supported yet.** The dial-hook hijack captures the net.Conn
  *before* go-sql-driver/mysql wraps it in `tls.Client(...)` during the
  handshake, so if the DSN requests TLS, this package ends up writing
  plaintext binary-protocol bytes onto a connection the server expects to be
  encrypted — a hard protocol break. Only use this package against
  connections that don't require TLS (e.g. a private-network link) until
  this is fixed.
- **Parameter types**: only `int64`, `int`, `string` (capped at 250 bytes —
  longer values error out instead of using a real length-encoded integer),
  and `nil` are supported. No `float64`, `bool`, `time.Time`, `[]byte`,
  unsigned integers, or `driver.Valuer` yet.
- **No `WHERE ... IN (?)` / bulk `INSERT` convenience helpers** yet (the
  `ExpandIn`/`ExpandValues` equivalents tidb-multistmt has).
- **Result sets are not decoded.** A `SELECT`'s binary row packets are
  skipped (just enough parsing to stay aligned with the next statement's
  response), not turned into usable data.
- **No bounds-checking on response parsing.** A short/malformed packet from
  the server can panic instead of returning an error.
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
- **No automated test suite beyond the integration tests above** — no unit
  tests for the wire-format encoding, no fuzzing of the response parser.

## License

MIT
