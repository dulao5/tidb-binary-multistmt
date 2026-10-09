package binarymultistmt

// Statement is one queued unit of work in a Batch.
type Statement struct {
	// SQL is the statement text, with "?" placeholders — same convention as
	// database/sql and tidb-multistmt.
	SQL string

	// Args are bound, in order, to SQL's "?" placeholders.
	Args []any

	// Callback, if non-nil, is invoked exactly once by Conn.Execute,
	// synchronously, in queue order, as soon as this statement's response is
	// available. This is the only way to see a statement's error or result
	// — Conn.Execute keeps no per-statement record after the batch
	// finishes, so a row-returning statement queued with a nil Callback has
	// its rows silently discarded (still drained off the wire, just never
	// decoded or exposed).
	//
	// For a row-returning statement, the StatementResult Callback receives
	// has Rows set to a RowIterator that streams rows directly off the wire
	// as the callback calls Next() — Execute never buffers the result set
	// in this case. The callback must not retain Rows past its own return;
	// any rows it doesn't consume are drained automatically once it
	// returns, to keep the pipelined stream in sync for later statements.
	Callback func(*StatementResult)
}

// Batch is an ordered queue of Statements to pipeline in one Conn.Execute
// call. The zero value is not usable; create one with NewBatch.
type Batch struct {
	stmts []Statement
}

// NewBatch creates an empty Batch.
func NewBatch() *Batch {
	return &Batch{}
}

// Add queues sqlText (with its Args) for execution, returning the Batch for
// chaining. Whether sqlText is row-returning is determined automatically
// from the server's own COM_STMT_PREPARE response — the caller no longer
// declares it. cb is as documented on Statement.Callback; pass nil if you
// don't need this statement's error or result (e.g. it's a non-row-returning
// statement and ExecuteResult.AllSucceeded is enough).
func (b *Batch) Add(sqlText string, args []any, cb func(*StatementResult)) *Batch {
	b.stmts = append(b.stmts, Statement{SQL: sqlText, Args: args, Callback: cb})
	return b
}

// Len returns the number of queued statements.
func (b *Batch) Len() int { return len(b.stmts) }

// Statements returns a copy of the queued statements, in order.
func (b *Batch) Statements() []Statement {
	return append([]Statement(nil), b.stmts...)
}
