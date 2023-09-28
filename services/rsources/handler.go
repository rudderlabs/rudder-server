package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

const defaultRetentionPeriodInHours = 3 * 24

// ErrOperationNotSupported sentinel error indicating an unsupported operation
var ErrOperationNotSupported = errors.New("rsources: operation not supported")

// In postgres, the replication slot name can contain lower-case letters, underscore characters, and numbers.
var replSlotDisallowedChars *regexp.Regexp = regexp.MustCompile(`[^a-z0-9_]`)

type sourcesHandler struct {
	log            logger.Logger
	config         JobServiceConfig
	localDB        *sql.DB
	sharedDB       *sql.DB
	cleanupTrigger func() <-chan time.Time
}

func (sh *sourcesHandler) GetStatus(ctx context.Context, jobRunId string, filter JobFilter) (JobStatus, error) {
	filters, filterParams := sqlFilters(jobRunId, filter)

	sqlStatement := fmt.Sprintf(
		`SELECT 
			source_id,
			destination_id,
			task_run_id,
			sum(in_count),
			sum(out_count),
			sum(failed_count) FROM "rsources_stats" %s
			GROUP BY task_run_id, source_id, destination_id
			ORDER BY task_run_id, source_id, destination_id ASC`,
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
		return JobStatus{}, ErrStatusNotFound
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

func (sh *sourcesHandler) AddFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, records []json.RawMessage) (err error) {
	if sh.config.SkipFailedRecordsCollection {
		return
	}
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
	defer func() { _ = stmt.Close() }()

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

func (sh *sourcesHandler) GetFailedRecords(ctx context.Context, jobRunId string, filter JobFilter) (JobFailedRecords, error) {
	if sh.config.SkipFailedRecordsCollection {
		return JobFailedRecords{ID: jobRunId}, ErrOperationNotSupported
	}
	filters, filterParams := sqlFilters(jobRunId, filter)

	sqlStatement := fmt.Sprintf(
		`SELECT
			task_run_id,
			source_id,
			destination_id,
			record_id  FROM "rsources_failed_keys" %s
			ORDER BY task_run_id, source_id, destination_id ASC`,
		filters)

	failedRecordsMap := map[JobTargetKey]FailedRecords{}
	rows, err := sh.readDB().QueryContext(ctx, sqlStatement, filterParams...)
	if err != nil {
		return JobFailedRecords{ID: jobRunId}, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var key JobTargetKey
		var record json.RawMessage
		err := rows.Scan(
			&key.TaskRunID,
			&key.SourceID,
			&key.DestinationID,
			&record,
		)
		if err != nil {
			return JobFailedRecords{ID: jobRunId}, err
		}
		failedRecordsMap[key] = append(failedRecordsMap[key], record)
	}
	if err := rows.Err(); err != nil {
		return JobFailedRecords{ID: jobRunId}, err
	}

	return failedRecordsFromQueryResult(jobRunId, failedRecordsMap), nil
}

func (sh *sourcesHandler) Delete(ctx context.Context, jobRunId string, filter JobFilter) error {
	jobStatus, err := sh.GetStatus(ctx, jobRunId, filter)
	if err != nil {
		return err
	}
	for _, target := range jobStatus.TasksStatus {
		for _, source := range target.SourcesStatus {
			if !source.Completed {
				return ErrSourceNotCompleted
			}
		}
	}
	tx, err := sh.localDB.Begin()
	if err != nil {
		return err
	}

	filters, filterParams := sqlFilters(jobRunId, filter)

	sqlStatement := fmt.Sprintf(`delete from "rsources_stats" %s`, filters)
	_, err = tx.ExecContext(ctx, sqlStatement, filterParams...)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	sqlStatement = fmt.Sprintf(`delete from "rsources_failed_keys" %s`, filters)
	_, err = tx.ExecContext(ctx, sqlStatement, filterParams...)
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

	before := time.Now().Add(-config.GetDuration("Rsources.retention", defaultRetentionPeriodInHours, time.Hour))
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
			return time.After(config.GetDuration("Rsources.stats.cleanup.interval", 1, time.Hour))
		}
	}
	sh.log.Debugf("setting up rsources tables in %s", sh.config.LocalHostname)
	if err := setupTables(ctx, sh.localDB, sh.config.LocalHostname, sh.log); err != nil {
		return err
	}
	sh.log.Debugf("rsources tables setup successfully in %s", sh.config.LocalHostname)
	if sh.sharedDB != nil {
		sh.log.Debugf("setting up rsources tables for shared db %s", sh.config.SharedConn)
		if err := setupTables(ctx, sh.sharedDB, "shared", sh.log); err != nil {
			return err
		}
		sh.log.Debugf("rsources tables for shared db %s setup successfully", sh.config.SharedConn)

		sh.log.Debugf("setting up rsources logical replication in %s", sh.config.LocalHostname)
		if err := sh.setupLogicalReplication(ctx); err != nil {
			return fmt.Errorf("failed to setup rsources logical replication in %s: %w", sh.config.LocalHostname, err)
		}
		sh.log.Debugf("rsources logical replication setup successfully in %s", sh.config.LocalHostname)
	}
	return nil
}

func setupTables(ctx context.Context, db *sql.DB, defaultDbName string, log logger.Logger) error {
	if err := setupFailedKeysTable(ctx, db, defaultDbName, log); err != nil {
		return fmt.Errorf("failed to setup %s rsources failed keys table: %w", defaultDbName, err)
	}
	if err := setupStatsTable(ctx, db, defaultDbName, log); err != nil {
		return fmt.Errorf("failed to setup %s rsources stats table: %w", defaultDbName, err)
	}
	return nil
}

func setupFailedKeysTable(ctx context.Context, db *sql.DB, defaultDbName string, log logger.Logger) error {
	sqlStatement := fmt.Sprintf(`create table "rsources_failed_keys" (	
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
	_, err := db.ExecContext(ctx, sqlStatement)
	if err != nil {
		if pqError, ok := err.(*pq.Error); ok && pqError.Code == "42P07" {
			log.Debugf("table rsources_failed_keys already exists in %s", defaultDbName)
		} else {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, `create index if not exists rsources_failed_keys_job_run_id_idx on "rsources_failed_keys" (job_run_id)`); err != nil {
		return err
	}
	return nil
}

func setupStatsTable(ctx context.Context, db *sql.DB, defaultDbName string, log logger.Logger) error {
	sqlStatement := fmt.Sprintf(`create table "rsources_stats" (
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
	_, err := db.ExecContext(ctx, sqlStatement)
	if err != nil {
		if pqError, ok := err.(*pq.Error); ok && pqError.Code == "42P07" {
			log.Debugf("table rsources_stats already exists in db %s", defaultDbName)
		} else {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, `create index if not exists rsources_stats_job_run_id_idx on "rsources_stats" (job_run_id)`); err != nil {
		return err
	}
	return nil
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
	subscriptionQuery := fmt.Sprintf(`CREATE SUBSCRIPTION "%s" CONNECTION '%s' PUBLICATION "rsources_stats_pub"`, subscriptionName, subscriptionConn) // skipcq: GO-R4002
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

func sqlFilters(jobRunId string, filter JobFilter) (fragment string, params []interface{}) {
	var filterParams []interface{}
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
	return filters, filterParams
}

func (sh *sourcesHandler) Monitor(ctx context.Context, lagGauge, replicationSlotGauge Gauger) {
	if sh.sharedDB == nil {
		sh.log.Warn("shared database is not configured, skipping logical replication monitoring")
		return
	}
	logicalReplicationTrigger := func() <-chan time.Time {
		return time.After(config.GetDuration("Rsources.stats.monitoringInterval", 10, time.Second))
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-logicalReplicationTrigger():
			var lag sql.NullFloat64
			err := sh.localDB.QueryRowContext(
				ctx,
				`select EXTRACT(epoch from replay_lag)
					from pg_stat_replication;`).Scan(&lag)
			switch err {
			case nil:
				lagGauge.Gauge(lag.Float64)
			case sql.ErrNoRows:
				// Indicates that shared db is unavailable
				lagGauge.Gauge(-1.0)
			default:
				sh.log.Warnf("failed to get replication lag: %v", err)
			}

			var replicationSlotCount int64
			err = sh.localDB.QueryRowContext(
				ctx,
				`select count(*) from pg_replication_slots;`).
				Scan(&replicationSlotCount)
			if err != nil {
				sh.log.Warnf("failed to get replication slot count: %v", err)
			} else {
				replicationSlotGauge.Gauge(replicationSlotCount)
			}
		}
	}
}
