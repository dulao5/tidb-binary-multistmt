package binarymultistmt

// Statement is one queued unit of work in a Batch.
type Statement struct {
	// SQL is the statement text, with "?" placeholders — same convention as
	// database/sql and tidb-multistmt.
	SQL string

	// Args are bound, in order, to SQL's "?" placeholders.
	Args []any

	// HasResultSet must be true iff SQL is row-returning (SELECT, SHOW,
	// ...). Getting it wrong desyncs every later statement's response in
	// the same batch, the same way it does in tidb-multistmt.
	HasResultSet bool
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
// chaining. hasResultSet is as documented on Statement.
func (b *Batch) Add(sqlText string, args []any, hasResultSet bool) *Batch {
	b.stmts = append(b.stmts, Statement{SQL: sqlText, Args: args, HasResultSet: hasResultSet})
	return b
}

// Len returns the number of queued statements.
func (b *Batch) Len() int { return len(b.stmts) }

// Statements returns a copy of the queued statements, in order.
func (b *Batch) Statements() []Statement {
	return append([]Statement(nil), b.stmts...)
}
