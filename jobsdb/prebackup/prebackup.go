package prebackup

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"
)

// Handler alters a jobsdb table before backup happens
type Handler interface {
	Handle(ctx context.Context, txn pgx.Tx, jobsTable, jobStatusTable string) error
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

func (r *dropSourceIds) Handle(ctx context.Context, txn pgx.Tx, jobsTable, jobStatusTable string) error {
	sourceIds := r.transientSourceIds()
	if len(sourceIds) == 0 {
		// skip
		return nil
	}
	sourceIdsParam := pq.Array(r.transientSourceIds())

	// First cleanup events from the job status table since it relies on the jobs table
	jsSql := fmt.Sprintf(`DELETE FROM "%[1]s" WHERE job_id IN (SELECT job_id FROM "%[2]s" WHERE parameters->>'source_id' = ANY ($1))`, jobStatusTable, jobsTable)
	if _, err := txn.Exec(ctx, jsSql, sourceIdsParam); err != nil {
		return fmt.Errorf("could not delete transient source events from table %s: %w", jobStatusTable, err)
	}

	// Last cleanup events from the jobs table
	jSql := fmt.Sprintf(`DELETE FROM "%[1]s" WHERE parameters->>'source_id' = ANY($1)`, jobsTable)
	if _, err := txn.Exec(ctx, jSql, sourceIdsParam); err != nil {
		return err
	}
	return nil
}
