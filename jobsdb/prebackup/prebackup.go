package prebackup

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

// Handler alters a jobsdb table before backup happens
type Handler interface {
	Handle(ctx context.Context, txn *sql.Tx, jobsTable, jobStatusTable string) error
}

// DropSourceIds provides a pre-backup handler who is responsible for removing events
// from the tables which belong to a transient source id.
// The list of source ids is dynamically provided by the transientSourceIds function
func DropSourceIds(transientSourceIds func() []string) Handler {
	return &dropSourceIds{
		transientSourceIds: transientSourceIds,
	}
}

type dropSourceIds struct {
	transientSourceIds func() []string
}

func (r *dropSourceIds) Handle(ctx context.Context, txn *sql.Tx, jobsTable, jobStatusTable string) error {
	sourceIds := r.transientSourceIds()
	if len(sourceIds) == 0 {
		// skip
		return nil
	}
	sourceIdsParam := pq.Array(r.transientSourceIds())

	// First cleanup events from the job status table since it relies on the jobs table
	jsSql := fmt.Sprintf(`DELETE FROM "%[1]s" WHERE job_id IN (SELECT job_id FROM "%[2]s" WHERE parameters->>'source_id' = ANY ($1))`, jobStatusTable, jobsTable)
	jsStmt, err := txn.Prepare(jsSql)
	if err != nil {
		return fmt.Errorf("could not prepare delete statement: %w", err)
	}
	_, err = jsStmt.ExecContext(ctx, sourceIdsParam)
	if err != nil {
		return fmt.Errorf("could not delete transient source events from table %s: %w", jobStatusTable, err)
	}

	// Last cleanup events from the jobs table
	jSql := fmt.Sprintf(`DELETE FROM "%[1]s" WHERE parameters->>'source_id' = ANY($1)`, jobsTable)
	jStmt, err := txn.Prepare(jSql)
	if err != nil {
		return fmt.Errorf("could not delete transient source events from table %s: %w", jobsTable, err)
	}
	_, err = jStmt.ExecContext(ctx, sourceIdsParam)
	if err != nil {
		return err
	}
	return nil
}
