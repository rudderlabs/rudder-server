package extension

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

func NewStandardExtension(db *sql.DB) (Extension, error) {
	defExtension := &standardExtension{localDB: db}
	err := defExtension.setupStatsTable(context.Background())
	return defExtension, err
}

var _ Extension = (*standardExtension)(nil)

type standardExtension struct {
	localDB *sql.DB
}

func (r *standardExtension) GetReadDB() *sql.DB {
	return r.localDB
}

func (r *standardExtension) setupStatsTable(ctx context.Context) error {
	dbHost := config.GetEnv("JOBS_DB_HOST", "localhost")
	tx, err := r.localDB.Begin()
	if err != nil {
		return err
	}
	sqlStatement := fmt.Sprintf(`create table if not exists "rsources_stats" (
		db_name text not null default '%s',
		job_run_id text not null,
		task_run_id text not null,
		source_id text not null,
		destination_id text not null,
		in_count integer not null default 0,
		out_count integer not null default 0,
		failed_count integer not null default 0,
		ts timestamp not null default NOW(),
		primary key (db_name, job_run_id, task_run_id, source_id, destination_id)
	)`, dbHost)
	_, err = tx.ExecContext(ctx, sqlStatement)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(ctx, `create index if not exists rsources_stats_job_run_id_idx on "rsources_stats" (job_run_id)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (r *standardExtension) CleanupLoop(ctx context.Context) error {
	err := r.doCleanupTables(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(config.GetDuration("RSOURCES_STATS_CLEANUP_INTERVAL", 24, time.Hour)):
			err := r.doCleanupTables(ctx)
			if err != nil {
				return err
			}
		}
	}
}

func (r *standardExtension) doCleanupTables(ctx context.Context) error {
	before := time.Now().Add(-config.GetDuration("RSOURCES_STATS_RETENTION", 24, time.Hour))
	return r.removeBefore(ctx, before)
}

func (r *standardExtension) DropStats(ctx context.Context, jobRunId string) error {
	sqlStatement := `delete from "rsources_stats" where job_run_id = $1`
	_, err := r.localDB.ExecContext(ctx, sqlStatement, jobRunId)
	return err
}

func (r *standardExtension) removeBefore(ctx context.Context, before time.Time) error {
	sqlStatement := `delete from "rsources_stats" where job_run_id in (
			select lastUpdateToJobRunId.job_run_id from 
				(select job_run_id, max(ts) as mts from "rsources_stats" group by job_run_id) lastUpdateToJobRunId
			where lastUpdateToJobRunId.mts <= $1
		)`
	_, err := r.localDB.ExecContext(ctx, sqlStatement, before)
	return err
}
