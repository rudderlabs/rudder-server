package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
)

// In postgres, the replication slot name can contain lower-case letters, underscore characters, and numbers.
var replSlotDisallowedChars *regexp.Regexp = regexp.MustCompile(`[^a-z0-9_]`)

type sourcesHandler struct {
	config         JobServiceConfig
	localDB        *sql.DB
	sharedDB       *sql.DB
	cleanupTrigger func() <-chan time.Time
}

func (sh *sourcesHandler) GetStatus(ctx context.Context, jobRunId string, filter JobFilter) (JobStatus, error) {
	filterParams := []interface{}{}
	filters := `WHERE job_run_id = $1`
	filterParams = append(filterParams, jobRunId)

	if len(filter.TaskRunID) > 0 {
		filters += fmt.Sprintf(` AND task_run_id  = ANY ($%d)`, len(filterParams)+1)
		filterParams = append(filterParams, pq.Array(filter.TaskRunID))
	}

	if len(filter.SourceID) > 0 {
		filters += fmt.Sprintf(` AND source_id = ANY ($%d)`, len(filterParams)+1)
		filterParams = append(filterParams, pq.Array(filter.SourceID))
	}

	sqlStatement := fmt.Sprintf(
		`SELECT 
			source_id,
			destination_id,
			task_run_id,
			sum(in_count),
			sum(out_count),
			sum(failed_count) FROM "rsources_stats" %s
			GROUP BY task_run_id, source_id, destination_id
			ORDER BY task_run_id, source_id, destination_id DESC`,
		filters)

	rows, err := sh.readDB().QueryContext(ctx, sqlStatement, filterParams...)
	if err != nil {
		return JobStatus{}, err
	}
	defer rows.Close()

	statMap := make(map[JobTargetKey]Stats)
	for rows.Next() {
		var statKey JobTargetKey
		var stat Stats
		err := rows.Scan(
			&statKey.SourceID, &statKey.DestinationID, &statKey.TaskRunID,
			&stat.In, &stat.Out, &stat.Failed,
		)
		if err != nil {
			return JobStatus{}, err
		}
		statMap[statKey] = stat
	}
	if err := rows.Err(); err != nil {
		return JobStatus{}, err
	}
	if len(statMap) == 0 {
		return JobStatus{}, StatusNotFoundError
	}

	return statusFromQueryResult(jobRunId, statMap), nil
}

// IncrementStats checks for stats table and upserts the stats
func (*sourcesHandler) IncrementStats(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, stats Stats) error {
	sqlStatement := `insert into "rsources_stats" (
		job_run_id,
		task_run_id,
		source_id,
		destination_id,
		in_count,
		out_count,
		failed_count
	) values ($1, $2, $3, $4, $5, $6, $7)
	on conflict(db_name, job_run_id, task_run_id, source_id, destination_id)
	do update set 
	in_count = "rsources_stats".in_count + excluded.in_count,
	out_count = "rsources_stats".out_count + excluded.out_count,
	failed_count = "rsources_stats".failed_count + excluded.failed_count,
	ts = NOW()`

	_, err := tx.ExecContext(
		ctx,
		sqlStatement,
		jobRunId, key.TaskRunID, key.SourceID, key.DestinationID,
		stats.In, stats.Out, stats.Failed)

	return err
}

func (*sourcesHandler) AddFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, records []json.RawMessage) (err error) {
	stmt, err := tx.Prepare(`insert into "rsources_failed_keys" (
		job_run_id,
		task_run_id,
		source_id,
		destination_id,
		record_id
	) values ($1, $2, $3, $4, $5)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := range records {
		_, err = stmt.ExecContext(
			ctx,
			jobRunId,
			key.TaskRunID,
			key.SourceID,
			key.DestinationID,
			records[i])
		if err != nil {
			return err
		}
	}

	return
}

func (sh *sourcesHandler) GetFailedRecords(ctx context.Context, jobRunId string, filter JobFilter) (FailedRecords, error) {
	filterParams := []interface{}{}
	filters := `WHERE job_run_id = $1`
	filterParams = append(filterParams, jobRunId)

	if len(filter.TaskRunID) > 0 {
		filters += fmt.Sprintf(` AND task_run_id  = ANY ($%d)`, len(filterParams)+1)
		filterParams = append(filterParams, pq.Array(filter.TaskRunID))
	}

	if len(filter.SourceID) > 0 {
		filters += fmt.Sprintf(` AND source_id = ANY ($%d)`, len(filterParams)+1)
		filterParams = append(filterParams, pq.Array(filter.SourceID))
	}

	sqlStatement := fmt.Sprintf(
		`SELECT
			job_run_id,
			task_run_id,
			source_id,
			destination_id,
			record_id  FROM "rsources_failed_keys" %s
			ORDER BY task_run_id, source_id, destination_id DESC`,
		filters)

	failedRecords := make([]FailedRecord, 0)
	rows, err := sh.readDB().QueryContext(ctx, sqlStatement, filterParams...)
	if err != nil {
		return FailedRecords{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var failedRecord FailedRecord
		err := rows.Scan(
			&failedRecord.JobRunID,
			&failedRecord.TaskRunID,
			&failedRecord.SourceID,
			&failedRecord.DestinationID,
			&failedRecord.RecordID,
		)
		if err != nil {
			return FailedRecords{}, err
		}
		failedRecords = append(failedRecords, failedRecord)
	}
	if err := rows.Err(); err != nil {
		return FailedRecords{}, err
	}

	return failedRecords, nil
}

func (sh *sourcesHandler) Delete(ctx context.Context, jobRunId string) error {
	tx, err := sh.localDB.Begin()
	if err != nil {
		return err
	}
	sqlStatement := `delete from "rsources_stats" where job_run_id = $1`
	_, err = tx.ExecContext(ctx, sqlStatement, jobRunId)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	sqlStatement = `delete from "rsources_failed_keys" where job_run_id = $1`
	_, err = tx.ExecContext(ctx, sqlStatement, jobRunId)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (sh *sourcesHandler) CleanupLoop(ctx context.Context) error {
	err := sh.doCleanupTables(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sh.cleanupTrigger():
			err := sh.doCleanupTables(ctx)
			if err != nil {
				return err
			}
		}
	}
}

func (sh *sourcesHandler) doCleanupTables(ctx context.Context) error {
	tx, err := sh.localDB.Begin()
	if err != nil {
		return err
	}

	before := time.Now().Add(-config.GetDuration("RSOURCES_RETENTION", 24, time.Hour))
	sqlStatement := `delete from "%[1]s" where job_run_id in (
		select lastUpdateToJobRunId.job_run_id from 
			(select job_run_id, max(ts) as mts from "%[1]s" group by job_run_id) lastUpdateToJobRunId
		where lastUpdateToJobRunId.mts <= $1
	)`
	_, err = tx.ExecContext(ctx, fmt.Sprintf(sqlStatement, "rsources_stats"), before)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(sqlStatement, "rsources_failed_keys"), before)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (sh *sourcesHandler) readDB() *sql.DB {
	if sh.sharedDB != nil {
		return sh.sharedDB
	}
	return sh.localDB
}

func (sh *sourcesHandler) init() error {
	ctx := context.TODO()
	if sh.cleanupTrigger == nil {
		sh.cleanupTrigger = func() <-chan time.Time {
			return time.After(config.GetDuration("RSOURCES_STATS_CLEANUP_INTERVAL", 1, time.Hour))
		}
	}
	err := setupTables(ctx, sh.localDB, sh.config.LocalHostname)
	if err != nil {
		return err
	}
	if sh.sharedDB != nil {
		err = setupTables(ctx, sh.sharedDB, "shared")
		if err != nil {
			return err
		}
		err = sh.setupLogicalReplication(ctx)
		if err != nil {
			return fmt.Errorf("failed to setup logical replication: %w", err)
		}
	}
	return nil
}

func setupTables(ctx context.Context, db *sql.DB, defaultDbName string) error {
	err := setupFailedKeysTable(ctx, db, defaultDbName)
	if err != nil {
		return fmt.Errorf("failed to setup %s failed keys table: %w", defaultDbName, err)
	}
	err = setupStatsTable(ctx, db, defaultDbName)
	if err != nil {
		return fmt.Errorf("failed to setup %s stats table: %w", defaultDbName, err)
	}
	return nil
}

func setupFailedKeysTable(ctx context.Context, db *sql.DB, defaultDbName string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	sqlStatement := fmt.Sprintf(`create table if not exists "rsources_failed_keys" (
		id BIGSERIAL,
		db_name text not null default '%s',
		job_run_id text not null,
		task_run_id text not null,
		source_id text not null,
		destination_id text not null,
		record_id jsonb not null,
		ts timestamp not null default NOW(),
		primary key (job_run_id, task_run_id, source_id, destination_id, db_name, id)
	)`, defaultDbName)
	_, err = tx.ExecContext(ctx, sqlStatement)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(ctx, `create index if not exists failed_keys_job_run_id_idx on "rsources_failed_keys" (job_run_id)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func setupStatsTable(ctx context.Context, db *sql.DB, defaultDbName string) error {
	tx, err := db.Begin()
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
	)`, defaultDbName)
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

func (sh *sourcesHandler) setupLogicalReplication(ctx context.Context) error {
	publicationQuery := `CREATE PUBLICATION "rsources_stats_pub" FOR TABLE rsources_stats`
	_, err := sh.localDB.ExecContext(ctx, publicationQuery)
	if err != nil {
		pqError, ok := err.(*pq.Error)
		if !ok || pqError.Code != pq.ErrorCode("42710") { // duplicate
			return fmt.Errorf("failed to create publication on local database: %w", err)
		}
	}

	alterPublicationQuery := `ALTER PUBLICATION "rsources_stats_pub" ADD TABLE rsources_failed_keys`
	_, err = sh.localDB.ExecContext(ctx, alterPublicationQuery)
	if err != nil {
		pqError, ok := err.(*pq.Error)
		if !ok || pqError.Code != pq.ErrorCode("42710") { // duplicate
			return fmt.Errorf("failed to alter publication on local database to add failed_keys table: %w", err)
		}
	}

	normalizedHostname := replSlotDisallowedChars.ReplaceAllString(strings.ToLower(sh.config.LocalHostname), "_")
	subscriptionName := fmt.Sprintf("%s_rsources_stats_sub", normalizedHostname)
	// Create subscription for the above publication (ignore already exists error)
	subscriptionConn := sh.config.SubscriptionTargetConn
	if subscriptionConn == "" {
		subscriptionConn = sh.config.LocalConn
	}
	subscriptionQuery := fmt.Sprintf(`CREATE SUBSCRIPTION "%s" CONNECTION '%s' PUBLICATION "rsources_stats_pub"`, subscriptionName, subscriptionConn)
	_, err = sh.sharedDB.ExecContext(ctx, subscriptionQuery)
	if err != nil {
		pqError, ok := err.(*pq.Error)
		if !ok || pqError.Code != pq.ErrorCode("42710") { // duplicate
			return fmt.Errorf("failed to create subscription on shared database: %w", err)
		}
	}

	refreshSubscriptionQuery := fmt.Sprintf(`ALTER SUBSCRIPTION %s REFRESH PUBLICATION`, subscriptionName)
	_, err = sh.sharedDB.ExecContext(ctx, refreshSubscriptionQuery)
	if err != nil {
		return fmt.Errorf("failed to refresh subscription on shared database: %w", err)
	}

	return nil
}
