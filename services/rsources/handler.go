package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
)

const (
	tablePrefix = "job_run_stats_"
)

type sourcesHandler struct {
	extension
	jobRunIdTableMapLock sync.RWMutex
	jobRunIdTableMap     map[string]struct{}
}

func (sh *sourcesHandler) GetStatus(ctx context.Context, jobRunId string, filter JobFilter) (JobStatus, error) {

	statsTableName := fmt.Sprintf("%s%s", tablePrefix, jobRunId)
	filters := ``

	if len(filter.TaskRunId) > 0 {
		filters += fmt.Sprintf(` task_run_id in ('%s')`, strings.Join(filter.TaskRunId, `','`))
	}

	if len(filter.SourceId) > 0 {
		if len(filters) > 0 {
			filters += ` and `
		}
		filters += fmt.Sprintf(` source_id in ('%s')`, strings.Join(filter.SourceId, `','`))
	}

	if len(filters) > 0 {
		filters = ` where ` + filters
	}
	sqlStatement := fmt.Sprintf(
		`select 
			source_id,
			destination_id,
			task_run_id,
			sum(in_count),
			sum(out_count),
			sum(failed_count) from %[1]s %[2]s
			group by task_run_id, source_id, destination_id
			order by task_run_id, source_id, destination_id desc`,
		statsTableName, filters)

	rows, err := sh.extension.getReadDB().QueryContext(ctx, sqlStatement)

	if err != nil {
		var e *pq.Error
		if err != nil && errors.As(err, &e) && e.Code == "42P01" {
			return JobStatus{}, StatusNotFoundError
		}
		return JobStatus{}, err
	}
	defer rows.Close()

	statMap := make(map[JobTargetKey]Stats)
	for rows.Next() {
		var statKey JobTargetKey
		var stat Stats
		err := rows.Scan(
			&statKey.SourceId, &statKey.DestinationId, &statKey.TaskRunId,
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

	return statusFromQueryResult(jobRunId, statMap), nil
}

// checks for stats table and upserts the stats
func (sh *sourcesHandler) IncrementStats(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, stats Stats) error {
	sh.checkForTable(ctx, jobRunId)

	jobRunIdStatTableName := fmt.Sprintf("%s%s", tablePrefix, jobRunId)
	sqlStatement := fmt.Sprintf(`insert into %[1]s (
		source_id,
		destination_id,
		task_run_id,
		in_count,
		out_count,
		failed_count
	) values ($1, $2, $3, $4, $5, $6)
	on conflict(db_name, source_id, destination_id, task_run_id)
	do update set 
	in_count = %[1]s.in_count + excluded.in_count,
	out_count = %[1]s.out_count + excluded.out_count,
	failed_count = %[1]s.failed_count + excluded.failed_count`, jobRunIdStatTableName)

	_, err := tx.ExecContext(
		ctx,
		sqlStatement,
		key.SourceId, key.DestinationId, key.TaskRunId,
		stats.In, stats.Out, stats.Failed)

	return err
}

func (sh *sourcesHandler) AddFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, records []json.RawMessage) error {
	return nil
}

func (sh *sourcesHandler) GetFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, filter JobFilter) (FailedRecords, error) {
	return FailedRecords{}, nil
}

func (sh *sourcesHandler) Delete(ctx context.Context, jobRunId string) error {
	return sh.extension.dropTables(ctx, jobRunId)
}

func (sh *sourcesHandler) CleanupLoop(ctx context.Context) error {
	return sh.extension.cleanupLoop(ctx)
}

// checks if the stats table for the given jobrunid exists and creates it if it doesn't
func (sh *sourcesHandler) checkForTable(ctx context.Context, jobRunId string) error {
	sh.jobRunIdTableMapLock.RLock()
	_, jobRunIdTableCreated := sh.jobRunIdTableMap[jobRunId]
	sh.jobRunIdTableMapLock.RUnlock()
	if !jobRunIdTableCreated {
		if _, ok := sh.jobRunIdTableMap[jobRunId]; !ok {
			err := sh.extension.createStatsTable(ctx, jobRunId)
			if err != nil {
				return err
			}
		}
		sh.jobRunIdTableMapLock.Lock()
		sh.jobRunIdTableMap[jobRunId] = struct{}{}
		sh.jobRunIdTableMapLock.Unlock()
	}
	return nil
}

type extension interface {
	getReadDB() *sql.DB
	createStatsTable(ctx context.Context, jobRunId string) error
	dropTables(ctx context.Context, jobRunId string) error
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
	jobRunIdStatTableName := fmt.Sprintf("%s%s", tablePrefix, jobRunId)
	sqlStatement := fmt.Sprintf(`create table if not exists %s (
		db_name text not null default '%s',
		source_id text not null,
		destination_id text not null,
		task_run_id text not null,
		in_count integer not null default 0,
		out_count integer not null default 0,
		failed_count integer not null default 0,
		primary key (db_name, source_id, destination_id, task_run_id)
	)`, jobRunIdStatTableName, dbHost)
	_, err := r.localDB.ExecContext(ctx, sqlStatement)
	return err
}

func (r *defaultExtension) dropTables(ctx context.Context, jobRunId string) error {
	jobRunIdStatTableName := fmt.Sprintf("%s%s", tablePrefix, jobRunId)
	sqlStatement := fmt.Sprintf(`drop table if exists %s`, jobRunIdStatTableName)
	_, err := r.localDB.ExecContext(ctx, sqlStatement)
	return err
}

func (r *defaultExtension) cleanupLoop(ctx context.Context) error {
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
	statTableNameLike := tablePrefix + `%`
	sqlStatement := fmt.Sprintf(`
	select table_name from information_schema.tables
	where table_schema='public' and table_name ilike '%s'`, statTableNameLike)
	rows, err := r.localDB.QueryContext(ctx, sqlStatement)
	if err != nil {
		return err
	}
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return err
		}
		sqlStatement := fmt.Sprintf(`drop table if exists %s`, tableName)
		_, err = r.localDB.ExecContext(ctx, sqlStatement)
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

type multitenantExtension struct {
	*defaultExtension
	sharedDB *sql.DB
}

func (r *multitenantExtension) getReadDB() *sql.DB {
	return r.sharedDB
}
