package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/lib/pq"
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
	filterParams := []interface{}{}
	filters := ``

	if len(filter.TaskRunId) > 0 {
		filters += ` task_run_id  = ANY ($1)`
		filterParams = append(filterParams, pq.Array(filter.TaskRunId))
	}

	if len(filter.SourceId) > 0 {
		if len(filterParams) > 0 {
			filters += ` AND `
		}
		filters += fmt.Sprintf(` source_id = ANY ($%d)`, len(filterParams)+1)
		filterParams = append(filterParams, pq.Array(filter.SourceId))
	}

	if len(filters) > 0 {
		filters = ` WHERE ` + filters
	}
	sqlStatement := fmt.Sprintf(
		`SELECT 
			source_id,
			destination_id,
			task_run_id,
			sum(in_count),
			sum(out_count),
			sum(failed_count) FROM "%[1]s" %[2]s
			GROUP BY task_run_id, source_id, destination_id
			ORDER BY task_run_id, source_id, destination_id DESC`,
		statsTableName, filters)

	rows, err := sh.extension.getReadDB().QueryContext(ctx, sqlStatement, filterParams...)

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

// IncrementStats checks for stats table and upserts the stats
func (sh *sourcesHandler) IncrementStats(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, stats Stats) error {
	err := sh.checkForTable(ctx, jobRunId)
	if err != nil {
		return err
	}

	jobRunIdStatTableName := fmt.Sprintf("%s%s", tablePrefix, jobRunId)
	sqlStatement := fmt.Sprintf(`insert into "%[1]s" (
		source_id,
		destination_id,
		task_run_id,
		in_count,
		out_count,
		failed_count
	) values ($1, $2, $3, $4, $5, $6)
	on conflict(db_name, source_id, destination_id, task_run_id)
	do update set 
	in_count = "%[1]s".in_count + excluded.in_count,
	out_count = "%[1]s".out_count + excluded.out_count,
	failed_count = "%[1]s".failed_count + excluded.failed_count,
	ts = NOW()`, jobRunIdStatTableName)

	_, err = tx.ExecContext(
		ctx,
		sqlStatement,
		key.SourceId, key.DestinationId, key.TaskRunId,
		stats.In, stats.Out, stats.Failed)

	return err
}

func (*sourcesHandler) AddFailedRecords(_ context.Context, _ *sql.Tx, _ string, _ JobTargetKey, _ []json.RawMessage) error {
	return nil
}

func (*sourcesHandler) GetFailedRecords(_ context.Context, _ *sql.Tx, _ string, _ JobFilter) (FailedRecords, error) {
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
