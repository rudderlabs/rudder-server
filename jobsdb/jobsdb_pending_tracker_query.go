package jobsdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// fingerprint reads the pg_stat counters of the dataset's jobs and status tables.
func (t *pendingEventsTracker) fingerprint(ctx context.Context, db queryer, ds dataSetT) (*tableFingerprint, error) {
	var fp tableFingerprint
	var jobsOID, statusOID int64
	if err := db.QueryRowContext(ctx, `SELECT
		j::oid::bigint, pg_stat_get_tuples_inserted(j), pg_stat_get_tuples_updated(j), pg_stat_get_tuples_deleted(j),
		s::oid::bigint, pg_stat_get_tuples_inserted(s), pg_stat_get_tuples_updated(s), pg_stat_get_tuples_deleted(s)
		FROM (SELECT $1::regclass AS j, $2::regclass AS s) t`, ds.JobTable, ds.JobStatusTable).Scan(
		&jobsOID, &fp.jobs.ins, &fp.jobs.upd, &fp.jobs.del,
		&statusOID, &fp.status.ins, &fp.status.upd, &fp.status.del,
	); err != nil {
		return nil, fmt.Errorf("reading fingerprint of %s: %w", ds.Index, err)
	}
	fp.jobs.oid, fp.status.oid = uint32(jobsOID), uint32(statusOID)
	return &fp, nil
}

// countInto adds the pending jobs of ds with job_id >= fromID to counts, per key, once per consumer. fromID
// leaves out the jobs of a compacted dataset that an earlier transition counted already.
func (t *pendingEventsTracker) countInto(ctx context.Context, db queryer, ds dataSetT, fromID int64, counts map[pendingKey]int64) error {
	query := `SELECT j.workspace_id, COALESCE(` + SourceID.string() + `, ''), '', COUNT(*)
	FROM %[1]q j
	LEFT JOIN (
		SELECT DISTINCT ON (job_id) job_id, job_state FROM %[2]q
		WHERE job_id >= $2
		ORDER BY job_id ASC, id DESC
	) s ON s.job_id = j.job_id
	WHERE j.job_id >= $2 AND (s.job_id IS NULL OR s.job_state = ANY($1))
	GROUP BY 1, 2, 3`
	if t.h.conf.multiConsumer {
		query = `SELECT j.workspace_id, COALESCE(` + SourceID.string() + `, ''), c.consumer, COUNT(*)
	FROM %[1]q j
	CROSS JOIN LATERAL unnest(j.consumers) AS c(consumer)
	LEFT JOIN LATERAL (
		SELECT job_state FROM %[2]q WHERE job_id = j.job_id AND consumer = c.consumer ORDER BY id DESC LIMIT 1
	) s ON true
	WHERE j.job_id >= $2 AND (s.job_state IS NULL OR s.job_state = ANY($1))
	GROUP BY 1, 2, 3`
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(query, ds.JobTable, ds.JobStatusTable), pq.Array(validNonTerminalStates), fromID)
	if err != nil {
		return fmt.Errorf("counting pending jobs of %s from job id %d: %w", ds.Index, fromID, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var key pendingKey
		var count int64
		if err := rows.Scan(&key.workspaceID, &key.sourceID, &key.consumer, &count); err != nil {
			return fmt.Errorf("scanning pending jobs of %s: %w", ds.Index, err)
		}
		counts[key] += count
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating pending jobs of %s: %w", ds.Index, err)
	}
	return nil
}
