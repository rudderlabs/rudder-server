package rsources

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

type extension interface {
	getReadDB() *sql.DB
	createStatsTable(ctx context.Context, jobRunId string) error
	dropStats(ctx context.Context, jobRunId string) error
	cleanupLoop(ctx context.Context) error
}

type defaultExtension struct {
	localDB *sql.DB
}

func (r *defaultExtension) getReadDB() *sql.DB {
	return r.localDB
}

func (r *defaultExtension) createStatsTable(ctx context.Context, jobRunId string) error {
	dbHost := config.GetEnv("JOBS_DB_HOST", "localhost")
	tx, err := r.localDB.Begin()
	if err != nil {
		return err
	}
	sqlStatement := fmt.Sprintf(`create table if not exists "%s" (
		db_name text not null default '%s',
		source_id text not null,
		destination_id text not null,
		job_run_id text not null,
		task_run_id text not null,
		in_count integer not null default 0,
		out_count integer not null default 0,
		failed_count integer not null default 0,
		ts timestamp not null default NOW(),
		primary key (db_name, job_run_id, source_id, destination_id, task_run_id)
	)`, tableName, dbHost)
	_, err = tx.ExecContext(ctx, sqlStatement)
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`create index if not exists job_run_id_index on "%s" (job_run_id)`, tableName))
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (r *defaultExtension) cleanupLoop(ctx context.Context) error {
	err := r.doCleanupTables(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(sourcesStatCleanUpSleepTime):
			err := r.doCleanupTables(ctx)
			if err != nil {
				return err
			}
		}
	}
}

func (r *defaultExtension) doCleanupTables(ctx context.Context) error {
	before := time.Now().Add(-24 * time.Hour)
	return r.removeBefore(ctx, before)
}

func (r *defaultExtension) dropStats(ctx context.Context, jobRunId string) error {
	sqlStatement := fmt.Sprintf(
		`delete from %[1]s where job_run_id = $1`, tableName,
	)
	_, err := r.localDB.ExecContext(ctx, sqlStatement, jobRunId)
	return err
}

func (r *defaultExtension) removeBefore(ctx context.Context, before time.Time) error {
	sqlStatement := fmt.Sprintf(
		`delete from %[1]s where job_run_id in (
			select lastUpdateToJobRunId.job_run_id from 
				(select job_run_id, max(ts) as mts from %[1]s group by job_run_id) lastUpdateToJobRunId
			where lastUpdateToJobRunId.mts <= $1
		)`, tableName)
	_, err := r.localDB.ExecContext(ctx, sqlStatement, before)
	return err
}
