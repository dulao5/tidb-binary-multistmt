# tidb-binary-multistmt

API reference for [github.com/dulao5/tidb-binary-multistmt](https://github.com/dulao5/tidb-binary-multistmt),
generated from the package's own Go doc comments. For the repository and
issue tracker, see [the GitHub repo](https://github.com/dulao5/tidb-binary-multistmt).

## At a glance

Sends every statement in a batch over MySQL's binary protocol
(`COM_STMT_PREPARE`/`COM_STMT_EXECUTE`) in a single network round trip,
instead of one round trip per statement — measured ~2x lower transaction
P95 latency on a real TiDB Cloud cluster. A statement's error and result
are only visible through its own `Callback`; only pessimistic transactions
are supported; TLS and protocol compression aren't supported yet. See
[Known limitations](https://github.com/dulao5/tidb-binary-multistmt#known-limitations).

## Quick example

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

Each statement's error and result arrive through its own `Callback`
(`sr.Err`, and for a row-returning statement `sr.Rows`, a `*RowIterator`
streaming rows directly off the wire) — `Execute` keeps no per-statement
record afterward, so this is the only place to look. See the
[repository README](https://github.com/dulao5/tidb-binary-multistmt#readme)
for more worked examples (connection pooling, `IN (?)`/bulk `INSERT`).

## Why

For a batch of N statements, this cuts N round trips down to one, because
each `EXECUTE` already carries its own response, in order — the N-th
response read off the wire *is* the N-th statement sent, for free, as long
as the connection stays healthy.

| | one round trip per statement | pipelined (this package) |
|---|---|---|
| Txn P95 | 62.4ms | 31.5ms |
| TiDB CPU | 205% | 195% |

MySQL command packets are self-delimited — nothing in the protocol
requires a round trip between commands. `database/sql` just doesn't expose
an API to write ahead of reading. This package doesn't fork
go-sql-driver/mysql to get around that: a custom dial function, registered
via the driver's own public `mysql.RegisterDialContext` hook, captures the
real `net.Conn` while go-sql-driver does its normal handshake/auth over it.
Once `Dial` returns, this package takes over all reads/writes directly,
outside `database/sql`.

## Behavior to know before using it

Whether a statement is row-returning is detected automatically from the
server's own `COM_STMT_PREPARE` response — the caller never declares it.
`Execute` auto-sends `COMMIT` only when every statement succeeded; on any
failure it does **not** send `ROLLBACK` — each failing statement's
`Callback` already saw the error, and the transaction is left open for the
caller to resolve explicitly. A connection/protocol-level failure (as
opposed to one statement's SQL error, or a `*CommitError`) makes `Execute`
return a non-nil `error` and leaves the connection unusable — `Close` it
(or let a pooled `Conn`'s own `Close` discard it automatically).

If every statement succeeds but the server then rejects `COMMIT` itself
(verified against a real conflict: TiDB already rolls the transaction back
server-side when this happens), `Execute` returns a non-nil `*CommitError`
instead — the connection is unaffected and stays usable, and there's
nothing to roll back.

`ExecuteAutoCommit` pipelines the same way but never sends `BEGIN`/`COMMIT`
— each statement commits on its own. There's nothing to roll back
afterward (calling `Rollback` would just be a harmless no-op, since
whatever ran already committed). Use it for a read-only batch, or one
where a partial failure genuinely doesn't need undoing; `Execute` is still
the right default whenever a batch needs all-or-nothing atomicity.

Only pessimistic transactions are supported, by design: a failed statement
mid-pipeline doesn't stop already-written `EXECUTE`s from running, so row
locks must already be held as each statement runs, not deferred to commit.
Optimistic transactions defer conflict detection to `COMMIT` instead, which
would fail a whole batch at once with no way to attribute the conflict back
to one statement — defeating the per-statement `Callback` this package is
built around.

See the README's
[Usage](https://github.com/dulao5/tidb-binary-multistmt#usage),
[`ExecuteAutoCommit`](https://github.com/dulao5/tidb-binary-multistmt#executeautocommit-skip-begincommit-entirely),
and [connection pool](https://github.com/dulao5/tidb-binary-multistmt#connection-pool-dbacquireconn)
sections for the full runnable examples. The API reference below documents
each piece individually.

## Array args: `IN (?)` and bulk `INSERT`

`ExpandIn` expands a single `?` into a comma-separated run for
`WHERE col IN (?)`; `ExpandValues` expands a single-row `VALUES (?, ?)`
template into one copy per row for bulk inserts. Both are plain functions
— call them before `Batch.Add`, nothing else changes. Parameter binding
always uses genuine `COM_STMT_EXECUTE` binary protocol parameters,
including through these two functions, so there is no escaping-based
injection surface to reason about. See the README's
[`WHERE id IN (?)` / bulk `INSERT`](https://github.com/dulao5/tidb-binary-multistmt#where-id-in--bulk-insert)
section for the full examples and the PREPARE-cache-key caveat around
variable-length `IN` lists.

## Known limitations

TLS and MySQL protocol compression (`compress=true`) are not supported yet —
the dial-hook hijack captures the raw `net.Conn` before go-sql-driver/mysql
would wrap or frame it during the handshake, so this package's raw reads and
writes would desync against (or defeat the encryption of) either. There is
also no `COM_STMT_SEND_LONG_DATA` support (every parameter must fit in one
~16MB packet), and this package assumes `CLIENT_DEPRECATE_EOF` is never
negotiated, coupling it to go-sql-driver/mysql's current handshake behavior.
See the README's
[Known limitations](https://github.com/dulao5/tidb-binary-multistmt#known-limitations)
section for the full list and the reasoning behind each.

---
