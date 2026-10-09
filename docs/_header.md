# tidb-binary-multistmt

API reference for [github.com/dulao5/tidb-binary-multistmt](https://github.com/dulao5/tidb-binary-multistmt),
generated from the package's own Go doc comments. For the repository and
issue tracker, see [the GitHub repo](https://github.com/dulao5/tidb-binary-multistmt).

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

Each statement's error and result arrive through its own `Callback`
(`sr.Err`, and for a row-returning statement `sr.Rows`, a `*RowIterator`
streaming rows directly off the wire) — `Execute` keeps no per-statement
record afterward, so this is the only place to look. See
[Usage](#usage-one-shot-dial-or-a-pooled-dbacquireconn) below and the
[repository README](https://github.com/dulao5/tidb-binary-multistmt#readme)
for more worked examples (connection pooling, `IN (?)`/bulk `INSERT`).

## Concept

Pipelines a transaction's statements over MySQL's **binary** protocol
(`COM_STMT_PREPARE`/`COM_STMT_EXECUTE`) with no per-statement network round
trip, by writing every statement's `EXECUTE` packet back-to-back before
reading any response — instead of `database/sql`'s (and
[go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)'s) normal
write-command-then-synchronously-read-its-response convention.

[tidb-multistmt](https://github.com/dulao5/tidb-multistmt) already solves
"one round trip for N statements" using the **text** protocol, at the cost of
a `SET @_multistmt_statement_num=N` marker before each statement so the
client can recover per-statement success/failure from a response stream that
otherwise collapses that information. A real production CPU-profile diff
found those marker `SET`s responsible for the large majority of
multi-statement mode's extra CPU over plain one-statement-per-round-trip —
not the text-vs-binary protocol choice itself. This package pipelines binary
`EXECUTE` instead: no markers needed at all, because each `EXECUTE` is
already a separate command with its own response, delivered in order — the
N-th response you read off the wire *is* the N-th statement you sent, for
free, as long as the connection stays healthy.

MySQL command packets are self-delimited (length-prefixed) — nothing in the
wire protocol or in TiDB's connection read loop requires a round trip between
commands. This package doesn't fork go-sql-driver/mysql to exploit that: a
custom dial function, registered via the driver's own public
`mysql.RegisterDialContext` hook, dials the real TCP connection and hands it
to this package once the driver's own handshake/authentication over it is
done; from that point this package takes over all reads/writes directly,
outside `database/sql`.

## Usage: one-shot `Dial`, or a pooled `DB`/`AcquireConn`

`Dial` hands out one dedicated connection per call — fine for a short-lived
tool. A long-running process that wants to pay the dial+auth cost once and
reuse connections across many batches should use `Open`/`AcquireConn`
instead, which implement this package's own idle-connection pool on top of
an internal `*sql.DB` used only for dialing — `database/sql`'s own pool has
no API to tell a caller, with certainty, which physical connection it's
about to speak raw binary protocol on, so this package never hands that
decision to it. `*binarymultistmt.DB` is a separate, independently-dialed
pool, not a view onto an existing `*sql.DB` — it doesn't embed one and isn't
assignable to a `*sql.DB`-typed parameter/field.

Both paths share the same contract: whether a statement is row-returning is
detected automatically from the server's own `COM_STMT_PREPARE` response, so
the caller never declares it and can't get it wrong. `Execute` auto-sends
`COMMIT` only when every statement succeeded; on any failure it does **not**
send `ROLLBACK` — each failing statement's `Callback` already saw the error
as it happened, and the transaction is left open for the caller to
explicitly resolve. A connection/protocol-level failure (as opposed to one
statement's SQL error) makes `Execute` return a non-nil `error` and leaves
the connection unusable — `Close` it (or let a pooled `Conn`'s own `Close`
discard it automatically) rather than reusing it.

See the README's
[Usage](https://github.com/dulao5/tidb-binary-multistmt#usage) and
[connection pool](https://github.com/dulao5/tidb-binary-multistmt#connection-pool-dbacquireconn)
sections for the full runnable examples. The API reference below documents
each piece individually.

## Array args: `IN (?)` and bulk `INSERT`

Same two functions as tidb-multistmt, same calling convention: `ExpandIn`
expands a single `?` into a comma-separated run for `WHERE col IN (?)`;
`ExpandValues` expands a single-row `VALUES (?, ?)` template into one copy
per row for bulk inserts. Both are plain functions — call them before
`Batch.Add`, nothing else changes. Unlike tidb-multistmt's text-protocol
`SET`-literal mechanism, parameter binding here always uses genuine
`COM_STMT_EXECUTE` binary protocol parameters, including through
`ExpandIn`/`ExpandValues`, so there is no escaping-based injection surface to
reason about. See the README's
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
[Status](https://github.com/dulao5/tidb-binary-multistmt#status) section for
the full list and the reasoning behind each.

---
