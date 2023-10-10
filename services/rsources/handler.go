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
	"github.com/segmentio/ksuid"

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
	filters, filterParams := sqlFilters(jobRunId, filter, "")

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

func (sh *sourcesHandler) AddFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, records []json.RawMessage) error {
	if sh.config.SkipFailedRecordsCollection {
		return nil
	}

	row := tx.QueryRow(`WITH new_key AS (
		INSERT INTO rsources_failed_keys_v2 (id, job_run_id, task_run_id, source_id, destination_id)
		VALUES ($1, $2, $3, $4, $5) 
		ON CONFLICT (job_run_id, task_run_id, source_id, destination_id, db_name) DO NOTHING
		RETURNING id
	) SELECT COALESCE(
		(SELECT id FROM new_key),
		(SELECT id FROM rsources_failed_keys_v2 WHERE job_run_id = $2 AND task_run_id = $3 AND source_id = $4 AND destination_id = $5)
	)`, ksuid.New().String(), jobRunId, key.TaskRunID, key.SourceID, key.DestinationID)
	var id string
	if err := row.Scan(&id); err != nil {
		return fmt.Errorf("scanning rsources_failed_keys_v2 id: %w", err)
	}

	stmt, err := tx.Prepare(`INSERT INTO rsources_failed_keys_v2_records (id, record_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	for i := range records {
		if _, err = stmt.ExecContext(
			ctx,
			id,
			records[i]); err != nil {
			return fmt.Errorf("inserting into rsources_failed_keys_v2_records: %w", err)
		}
	}
	return nil
}

func (sh *sourcesHandler) GetFailedRecords(ctx context.Context, jobRunId string, filter JobFilter) (JobFailedRecords, error) {
	if sh.config.SkipFailedRecordsCollection {
		return JobFailedRecords{ID: jobRunId}, ErrOperationNotSupported
	}
	filters, filterParams := sqlFilters(jobRunId, filter, "k")

	sqlStatement := fmt.Sprintf(
		`SELECT
			k.task_run_id,
			k.source_id,
			k.destination_id,
			r.record_id 
		FROM "rsources_failed_keys_v2" k 
		JOIN "rsources_failed_keys_v2_records" r ON r.id = k.id %s 
		ORDER BY r.id, r.record_id ASC`,
		filters)

	failedRecordsMap := map[JobTargetKey]FailedRecords{}
	rows, err := sh.readDB().QueryContext(ctx, sqlStatement, filterParams...)
	if err != nil {
		return JobFailedRecords{ID: jobRunId}, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var key JobTargetKey
		var record string
		err := rows.Scan(
			&key.TaskRunID,
			&key.SourceID,
			&key.DestinationID,
			&record,
		)
		if err != nil {
			return JobFailedRecords{ID: jobRunId}, err
		}
		failedRecordsMap[key] = append(failedRecordsMap[key], json.RawMessage(record))
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

	filters, filterParams := sqlFilters(jobRunId, filter, "")

	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`delete from "rsources_stats" %s`, filters), filterParams...); err != nil {
		_ = tx.Rollback()
		return err
	}

	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`delete from "rsources_failed_keys_v2_records" where id in (select id from "rsources_failed_keys_v2" %s) `, filters), filterParams...); err != nil {
		_ = tx.Rollback()
		return err
	}

	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`delete from "rsources_failed_keys_v2" %s`, filters), filterParams...); err != nil {
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
	if _, err := tx.ExecContext(ctx, `delete from "rsources_stats" where job_run_id in (
		select lastUpdateToJobRunId.job_run_id from 
			(select job_run_id, max(ts) as mts from "rsources_stats" group by job_run_id) lastUpdateToJobRunId
		where lastUpdateToJobRunId.mts <= $1
	)`, before); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `WITH to_delete AS (
		SELECT id FROM "rsources_failed_keys_v2" WHERE job_run_id IN (
			SELECT lastUpdateToJobRunId.job_run_id FROM (
				SELECT k.job_run_id, max(r.ts) as mts from "rsources_failed_keys_v2" k
				JOIN "rsources_failed_keys_v2_records" r on r.id = k.id
				GROUP BY k.job_run_id
			) lastUpdateToJobRunId WHERE lastUpdateToJobRunId.mts <= $1
		)    
	),
	deleted AS (
		DELETE FROM "rsources_failed_keys_v2_records" WHERE id IN (SELECT id FROM to_delete) RETURNING id
	)
	DELETE FROM "rsources_failed_keys_v2" WHERE id IN (SELECT id FROM to_delete) RETURNING id`, before); err != nil {
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
	if err := migrateFailedKeysTable(ctx, sh.localDB); err != nil {
		return fmt.Errorf("failed to migrate rsources_failed_keys table: %w", err)
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

// TODO: Remove this after a few releases
func migrateFailedKeysTable(ctx context.Context, db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(100020001)`); err != nil {
		return fmt.Errorf("error while acquiring advisory lock for failed keys migration: %w", err)
	}
	var previousTableExists bool
	row := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') AND  tablename  = 'rsources_failed_keys')`)
	if err := row.Scan(&previousTableExists); err != nil {
		return err
	}
	if previousTableExists {
		if _, err := tx.ExecContext(ctx, `create or replace function ksuid() returns text as $$
		declare
			v_time timestamp with time zone := null;
			v_seconds numeric(50) := null;
			v_numeric numeric(50) := null;
			v_epoch numeric(50) = 1400000000; -- 2014-05-13T16:53:20Z
			v_base62 text := '';
			v_alphabet char array[62] := array[
				'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
				'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
				'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 
				'U', 'V', 'W', 'X', 'Y', 'Z', 
				'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 
				'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
				'u', 'v', 'w', 'x', 'y', 'z'];
			i integer := 0;
		begin
		
			-- Get the current time
			v_time := clock_timestamp();
		
			-- Extract epoch seconds
			v_seconds := EXTRACT(EPOCH FROM v_time) - v_epoch;
		
			-- Generate a KSUID in a numeric variable
			v_numeric := v_seconds * pow(2::numeric(50), 128) -- 32 bits for seconds and 128 bits for randomness
				+ ((random()::numeric(70,20) * pow(2::numeric(70,20), 48))::numeric(50) * pow(2::numeric(50), 80)::numeric(50))
				+ ((random()::numeric(70,20) * pow(2::numeric(70,20), 40))::numeric(50) * pow(2::numeric(50), 40)::numeric(50))
				+  (random()::numeric(70,20) * pow(2::numeric(70,20), 40))::numeric(50);
		
			-- Encode it to base-62
			while v_numeric <> 0 loop
				v_base62 := v_base62 || v_alphabet[mod(v_numeric, 62) + 1];
				v_numeric := div(v_numeric, 62);
			end loop;
			v_base62 := reverse(v_base62);
			v_base62 := lpad(v_base62, 27, '0');
		
			return v_base62;
			
		end $$ language plpgsql;`); err != nil {
			return fmt.Errorf("failed to create ksuid function: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `WITH new_keys AS (
			INSERT INTO "rsources_failed_keys_v2" 
			(id, job_run_id, task_run_id, source_id, destination_id, db_name)
			SELECT ksuid(), t.* FROM (
				SELECT DISTINCT job_run_id, task_run_id, source_id, destination_id, db_name from "rsources_failed_keys"
			) t
			RETURNING *
		)
		INSERT INTO "rsources_failed_keys_v2_records" (id, record_id, ts)
		 SELECT n.id, o.record_id::text, min(o.ts) FROM new_keys n
			JOIN rsources_failed_keys o 
				on o.db_name = n.db_name 
				and o.destination_id = n.destination_id 
				and o.source_id = n.source_id 
				and o.task_run_id = n.task_run_id 
				and o.job_run_id = n.job_run_id 
			group by n.id, o.record_id
		`); err != nil {
			return fmt.Errorf("failed to migrate rsources_failed_keys table: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS "rsources_failed_keys" CASCADE`); err != nil {
			return fmt.Errorf("failed to drop old rsources_failed_keys table: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `drop function if exists ksuid()`); err != nil {
			return fmt.Errorf("failed to drop ksuid function: %w", err)
		}
		return tx.Commit()
	}

	return nil
}

// TODO: Remove this after a few releases
func setupFailedKeysTableV0(ctx context.Context, db *sql.DB, defaultDbName string, log logger.Logger) error {
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

func setupFailedKeysTable(ctx context.Context, db *sql.DB, defaultDbName string, log logger.Logger) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`create table "rsources_failed_keys_v2" (	
		id VARCHAR(27) COLLATE "C",
		db_name text not null default '%s',
		job_run_id text not null,
		task_run_id text not null,
		source_id text not null,
		destination_id text not null,		
		primary key (id),
		unique (job_run_id, task_run_id, source_id, destination_id, db_name)
	)`, defaultDbName)); err != nil {
		if pqError, ok := err.(*pq.Error); ok && pqError.Code == "42P07" {
			log.Debugf("table rsources_failed_keys_v2 already exists in %s", defaultDbName)
		} else {
			return err
		}
	}

	if _, err := db.ExecContext(ctx, `create table "rsources_failed_keys_v2_records" (	
		id VARCHAR(27) COLLATE "C",
		record_id text not null,
    	ts timestamp not null default NOW(),
		primary key (id, record_id)
	)`); err != nil {
		if pqError, ok := err.(*pq.Error); ok && pqError.Code == "42P07" {
			log.Debugf("table rsources_failed_keys_v2_records already exists in %s", defaultDbName)
		} else {
			return err
		}
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
	if _, err := sh.localDB.ExecContext(ctx, `CREATE PUBLICATION "rsources_stats_pub" FOR TABLE rsources_stats`); err != nil {
		pqError, ok := err.(*pq.Error)
		if !ok || pqError.Code != pq.ErrorCode("42710") { // duplicate
			return fmt.Errorf("failed to create publication on local database: %w", err)
		}
	}

	if _, err := sh.localDB.ExecContext(ctx, `ALTER PUBLICATION "rsources_stats_pub" ADD TABLE rsources_failed_keys_v2`); err != nil {
		pqError, ok := err.(*pq.Error)
		if !ok || pqError.Code != pq.ErrorCode("42710") { // duplicate
			return fmt.Errorf("failed to alter publication on local database to add rsources_failed_keys_v2 table: %w", err)
		}
	}

	if _, err := sh.localDB.ExecContext(ctx, `ALTER PUBLICATION "rsources_stats_pub" ADD TABLE rsources_failed_keys_v2_records`); err != nil {
		pqError, ok := err.(*pq.Error)
		if !ok || pqError.Code != pq.ErrorCode("42710") { // duplicate
			return fmt.Errorf("failed to alter publication on local database to add rsources_failed_keys_v2_records table: %w", err)
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
	if _, err := sh.sharedDB.ExecContext(ctx, subscriptionQuery); err != nil {
		pqError, ok := err.(*pq.Error)
		if !ok || pqError.Code != pq.ErrorCode("42710") { // duplicate
			return fmt.Errorf("failed to create subscription on shared database: %w", err)
		}
	}
	if _, err := sh.sharedDB.ExecContext(ctx, fmt.Sprintf(`ALTER SUBSCRIPTION %s REFRESH PUBLICATION`, subscriptionName)); err != nil {
		return fmt.Errorf("failed to refresh subscription on shared database: %w", err)
	}

	// TODO: Remove this after a few releases
	if _, err := sh.sharedDB.ExecContext(ctx, `DROP TABLE IF EXISTS "rsources_failed_keys" CASCADE`); err != nil {
		pqError, ok := err.(*pq.Error)
		if !ok || pqError.Code != pq.ErrorCode("22023") { // table synchronization in progress
			return fmt.Errorf("failed to drop old rsources_failed_keys table on shared database: %w", err)
		}
	}

	return nil
}

func sqlFilters(jobRunId string, filter JobFilter, alias string) (fragment string, params []interface{}) {
	var filterParams []interface{}
	var prefix string
	if alias != "" {
		prefix = alias + "."
	}
	filters := fmt.Sprintf(`WHERE %sjob_run_id = $1`, prefix)
	filterParams = append(filterParams, jobRunId)

	if len(filter.TaskRunID) > 0 {
		filters += fmt.Sprintf(` AND %stask_run_id  = ANY ($%d)`, prefix, len(filterParams)+1)
		filterParams = append(filterParams, pq.Array(filter.TaskRunID))
	}

	if len(filter.SourceID) > 0 {
		filters += fmt.Sprintf(` AND %ssource_id = ANY ($%d)`, prefix, len(filterParams)+1)
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
