package jobsdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/lib/pq"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/utils/misc"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

/*
JobStatusT is used for storing status of the job. It is
the responsibility of the user of this module to set appropriate
job status. State can be one of
ENUM waiting, executing, succeeded, waiting_retry,  failed, aborted
*/
type JobStatusT struct {
	JobID         int64           `json:"JobID"`
	JobState      string          `json:"JobState"` // ENUM waiting, executing, succeeded, waiting_retry, filtered, failed, aborted, migrating, migrated, wont_migrate
	AttemptNum    int             `json:"AttemptNum"`
	ExecTime      time.Time       `json:"ExecTime"`
	RetryTime     time.Time       `json:"RetryTime"`
	ErrorCode     string          `json:"ErrorCode"`
	ErrorResponse json.RawMessage `json:"ErrorResponse"`
	Parameters    json.RawMessage `json:"Parameters"`
	JobParameters json.RawMessage `json:"-"` // not stored in DB
	WorkspaceId   string          `json:"-"` // not stored in DB
	PartitionID   string          `json:"-"` // not stored in DB
	CustomVal     string          `json:"-"` // not stored in DB
}

func (r *JobStatusT) sanitizeJson() error {
	var err error
	r.ErrorResponse, err = misc.SanitizeJSON(r.ErrorResponse)
	if err != nil {
		return err
	}

	r.Parameters, err = misc.SanitizeJSON(r.Parameters)
	if err != nil {
		return err
	}
	return nil
}

func (jd *Handle) WithUpdateSafeTx(ctx context.Context, f func(tx UpdateSafeTx) error) error {
	return jd.inUpdateSafeCtx(ctx, func(dsList []dataSetT, dsRangeList []dataSetRangeT) error {
		return jd.WithTx(ctx, func(tx *Tx) error {
			return f(&updateSafeTx{
				tx:          tx,
				identity:    jd.tablePrefix,
				dsList:      dsList,
				dsRangeList: dsRangeList,
			})
		})
	})
}

func (jd *Handle) WithUpdateSafeTxFromTx(ctx context.Context, tx *Tx, f func(tx UpdateSafeTx) error) error {
	return jd.inUpdateSafeCtx(ctx, func(dsList []dataSetT, dsRangeList []dataSetRangeT) error {
		return f(&updateSafeTx{
			tx:          tx,
			identity:    jd.tablePrefix,
			dsList:      dsList,
			dsRangeList: dsRangeList,
		})
	})
}

func (jd *Handle) inUpdateSafeCtx(ctx context.Context, f func(dsList []dataSetT, dsRangeList []dataSetRangeT) error) error {
	// The order of lock is very important. The compactionLoop
	// takes lock in this order so reversing this will cause
	// deadlocks
	if !jd.dsCompactionLock.RTryLockWithCtx(ctx) {
		return fmt.Errorf("could not acquire a compaction read lock: %w", ctx.Err())
	}
	defer jd.dsCompactionLock.RUnlock()

	dsList, dsRangeList, release, err := jd.acquireDSListForRead(ctx)
	if err != nil {
		return err
	}
	defer release()
	return f(dsList, dsRangeList)
}

func (jd *Handle) UpdateJobStatus(ctx context.Context, statusList []*JobStatusT) error {
	return jd.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
		return jd.UpdateJobStatusInTx(ctx, tx, statusList)
	})
}

func (jd *Handle) UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT) error {
	if len(statusList) == 0 {
		return nil
	}
	if tx.updateSafeTxSealIdentifier() != jd.Identifier() {
		return fmt.Errorf("invalid UpdateSafeTx: expected identifier %s, got %s", jd.Identifier(), tx.updateSafeTxSealIdentifier())
	}
	updateCmd := func() error {
		tags := statTags{}
		command := func() error {
			return jd.internalUpdateJobStatusInTx(ctx, tx.Tx(), tx.getDSList(), tx.getDSRangeList(), statusList)
		}
		err := executeDbRequest(ctx, jd, newWriteDbRequest("update_job_status", &tags, command))
		return err
	}
	const (
		savepointName = "updateJobStatusInTx"
		savepointSql  = "SAVEPOINT " + savepointName
		rollbackSql   = "ROLLBACK TO SAVEPOINT " + savepointName
	)
	for {
		if _, err := tx.Tx().ExecContext(ctx, savepointSql); err != nil {
			return fmt.Errorf("executing updateJobStatusInTx savepoint: %w", err)
		}
		err := updateCmd()
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrStaleDsList) {
			return err
		}
		if _, rbErr := tx.Tx().ExecContext(ctx, rollbackSql); rbErr != nil {
			return fmt.Errorf("rolling back to updateJobStatusInTx savepoint: %w", rbErr)
		}

		jd.logger.Warnn("[JobsDB] :: Stale dataset list detected while updating job statuses, retrying after refreshing DS cache", obskit.Error(ErrStaleDsList))
		if refreshErr := func() error {
			// we don't need to actually refresh the ds list from the database, since compaction already does this,
			// but we do need to acquire a read lock on dsListLock to ensure that the ds list is actually refreshed.
			if lock := jd.dsListLock.RTryLockWithCtx(ctx); !lock {
				return fmt.Errorf("acquiring read lock for refreshing ds list in update job status: %w", ctx.Err())
			}
			dsList, dsRangeList := jd.dsList.snapshot()
			tx.setDSList(dsList, dsRangeList)
			jd.dsListLock.RUnlock()
			return nil
		}(); refreshErr != nil {
			return refreshErr
		}
	}
}

/*
internalUpdateJobStatusInTx updates the status of a batch of jobs
customValFilters[] is passed, so we can efficiently mark empty cache
Later we can move this to query
*/
func (jd *Handle) internalUpdateJobStatusInTx(ctx context.Context, tx *Tx, dsList []dataSetT, dsRangeList []dataSetRangeT, statusList []*JobStatusT) error {
	// capture stats
	defer jd.getTimerStat("update_job_status_time", nil).RecordDuration()()

	// do update
	updatedStatesByDS, err := jd.doUpdateJobStatusInTx(ctx, tx, dsList, dsRangeList, statusList)
	if err != nil {
		if !errors.Is(err, ErrStaleDsList) {
			jd.logger.Infon("Error occurred while updating job statuses",
				logger.NewStringField("tablePrefix", jd.tablePrefix),
				obskit.Error(err),
			)
		}
		return err
	}

	tx.AddSuccessListener(func() {
		// clear cache
		for ds, dsStats := range updatedStatesByDS {
			if len(dsStats) == 0 { // if no keys, we need to invalidate all keys
				jd.noResultsCache.Invalidate(ds.Index, nil, "", nil, nil, nil)
			}
			for partition, partStats := range dsStats {
				if len(partStats) == 0 { // if no keys, we need to invalidate all keys
					jd.noResultsCache.Invalidate(ds.Index, []string{string(partition)}, "", nil, nil, nil)
				}
				for workspace, wsStats := range partStats {
					if len(wsStats) == 0 { // if no keys, we need to invalidate all keys
						jd.noResultsCache.Invalidate(ds.Index, []string{string(partition)}, string(workspace), nil, nil, nil)
					}
					for customVal, customValStats := range wsStats {
						if len(customValStats) == 0 { // if no keys, we need to invalidate all keys
							jd.noResultsCache.Invalidate(ds.Index, []string{string(partition)}, string(workspace), []string{string(customVal)}, nil, nil)
							continue
						}
						for state, parametersStats := range customValStats {
							stateList := []string{string(state)}
							parameterFilters := lo.UniqBy( // gather unique parameter filters
								lo.FlatMap(
									lo.Values(parametersStats), // from all JobStatusMetrics
									func(ujss *UpdateJobStatusStats, _ int) []ParameterFilterT {
										return ujss.parameters
									},
								),
								func(pf ParameterFilterT) string {
									return pf.String() // uniqueness by string representation
								},
							)
							// invalidate cache for this combination
							jd.noResultsCache.Invalidate(ds.Index, []string{string(partition)}, string(workspace), []string{string(customVal)}, stateList, parameterFilters)
						}
					}
				}
			}

		}
	})

	// use the aggregated stats from updateJobStatusInTx
	tx.AddSuccessListener(func() {
		merged := updateJobStatusStats{}
		for _, dsStats := range updatedStatesByDS {
			merged.Merge(dsStats)
		}
		statsByCustomValAndState := merged.StatsByCustomValAndState()
		for customVal, statsByState := range statsByCustomValAndState {
			for state, parametersMap := range statsByState {
				for _, metrics := range parametersMap {
					statTags := (&statTags{}).getStatsTags(jd.tablePrefix)
					statTags["jobState"] = string(state)
					statTags["customVal"] = string(customVal)
					for _, pf := range metrics.parameters {
						statTags[pf.Name] = pf.Value
					}
					jd.stats.NewTaggedStat("jobsdb_updated_jobs", stats.CountType, statTags).Count(metrics.count)
					jd.stats.NewTaggedStat("jobsdb_updated_bytes", stats.CountType, statTags).Count(metrics.bytes)
				}
			}
		}
	})

	return nil
}

/*
doUpdateJobStatusInTx updates the status of a batch of jobs
customValFilters[] is passed, so we can efficiently mark empty cache
Later we can move this to query
*/
func (jd *Handle) doUpdateJobStatusInTx(ctx context.Context, tx *Tx, dsList []dataSetT, dsRangeList []dataSetRangeT, statusList []*JobStatusT) (updatedStatesByDS map[dataSetT]updateJobStatusStats, err error) {
	if len(statusList) == 0 {
		return updatedStatesByDS, err
	}

	// First we sort by JobID
	sort.Slice(statusList, func(i, j int) bool {
		return statusList[i].JobID < statusList[j].JobID
	})

	// We scan through the list of jobs and map them to DS
	var lastPos int
	updatedStatesByDS = make(map[dataSetT]updateJobStatusStats)
	for _, ds := range dsRangeList {
		minID := ds.minJobID
		maxID := ds.maxJobID
		// We have processed upto (but excluding) lastPos on statusList.
		// Hence, that element must lie in this or subsequent dataset's
		// range
		jd.assert(statusList[lastPos].JobID >= minID, fmt.Sprintf("statusList[lastPos].JobID: %d < minID:%d", statusList[lastPos].JobID, minID))
		var i int
		for i = lastPos; i < len(statusList); i++ {
			// The JobID is outside this DS's range
			if statusList[i].JobID > maxID {
				if i > lastPos && jd.logger.IsDebugLevel() {
					jd.logger.Debugn("Range",
						logger.NewStringField("ds", ds.String()),
						logger.NewIntField("lastPosJobID", statusList[lastPos].JobID),
						logger.NewIntField("prevJobID", statusList[i-1].JobID),
						logger.NewIntField("lastPos", int64(lastPos)),
						logger.NewIntField("prevPos", int64(i-1)),
					)
				}
				var updatedStates updateJobStatusStats
				updatedStates, err = jd.updateJobStatusDSInTx(ctx, tx, ds.ds, statusList[lastPos:i])
				if err != nil {
					return updatedStatesByDS, err
				}
				// do not set for ds without any new state written as it would clear emptyCache
				if len(updatedStates) > 0 {
					updatedStatesByDS[ds.ds] = updatedStates
				}
				lastPos = i
				break
			}
		}
		// Reached the end. Need to process this range
		if i == len(statusList) && lastPos < i {
			jd.logger.Debugn("Range",
				logger.NewStringField("ds", ds.String()),
				logger.NewIntField("lastPosJobID", statusList[lastPos].JobID),
				logger.NewIntField("prevJobID", statusList[i-1].JobID),
				logger.NewIntField("index", int64(i)),
			)
			var updatedStates updateJobStatusStats
			updatedStates, err = jd.updateJobStatusDSInTx(ctx, tx, ds.ds, statusList[lastPos:i])
			if err != nil {
				return updatedStatesByDS, err
			}
			// do not set for ds without any new state written as it would clear emptyCache
			if len(updatedStates) > 0 {
				updatedStatesByDS[ds.ds] = updatedStates
			}
			lastPos = i
			break
		}
	}

	// The last (most active DS) might not have range element as it is being written to
	if lastPos < len(statusList) {
		// Make sure range is missing for the last ds and migration ds (if at all present)
		jd.assert(len(dsRangeList) >= len(dsList)-2, fmt.Sprintf("len(dsRangeList):%d < len(dsList):%d-2", len(dsRangeList), len(dsList)))
		// Update status in the last element
		jd.logger.Debugn("RangeEnd",
			logger.NewIntField("jobID", statusList[lastPos].JobID),
			logger.NewIntField("lenStatusList", int64(len(statusList))))
		var updatedStates updateJobStatusStats
		updatedStates, err = jd.updateJobStatusDSInTx(ctx, tx, dsList[len(dsList)-1], statusList[lastPos:])
		if err != nil {
			return updatedStatesByDS, err
		}
		// do not set for ds without any new state written as it would clear emptyCache
		if len(updatedStates) > 0 {
			updatedStatesByDS[dsList[len(dsList)-1]] = updatedStates
		}
	}
	return updatedStatesByDS, err
}

// updateJobStatusStats is a map containing statistics of job status updates grouped by: partition -> workspace -> state -> set of params (stringified) -> stats
type updateJobStatusStats map[partitionIDKey]map[workspaceIDKey]map[customValKey]map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats

// partitionIDKey represents partition id as key
type partitionIDKey string

// workspaceIDKey represents workspace id as key
type workspaceIDKey string

// customValKey represents custom value as key
type customValKey string

// jobStateKey represents job state as key (failed, succeeded, etc)
type jobStateKey string

// parameterFiltersKey represents a list of job parameter filters (stringified) as key
type parameterFiltersKey string

// Merges metrics from two updateJobStatusStats together
func (ujss updateJobStatusStats) Merge(other updateJobStatusStats) {
	for partitionID, workspaces := range other {
		if _, ok := ujss[partitionID]; !ok {
			ujss[partitionID] = make(map[workspaceIDKey]map[customValKey]map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
		}
		for ws, customVals := range workspaces {
			if _, ok := ujss[partitionID][ws]; !ok {
				ujss[partitionID][ws] = make(map[customValKey]map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
			}
			for cv, states := range customVals {
				if _, ok := ujss[partitionID][ws][cv]; !ok {
					ujss[partitionID][ws][cv] = make(map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
				}
				for state, paramsMetrics := range states {
					if _, ok := ujss[partitionID][ws][cv][state]; !ok {
						ujss[partitionID][ws][cv][state] = make(map[parameterFiltersKey]*UpdateJobStatusStats)
					}
					for params, metrics := range paramsMetrics {
						existingMetrics, ok := ujss[partitionID][ws][cv][state][params]
						if !ok {
							existingMetrics = &UpdateJobStatusStats{parameters: metrics.parameters}
							ujss[partitionID][ws][cv][state][params] = existingMetrics
						}
						existingMetrics.count += metrics.count
						existingMetrics.bytes += metrics.bytes
					}
				}
			}
		}
	}
}

// Aggregates metrics by state across all workspaces
func (ujss updateJobStatusStats) StatsByCustomValAndState() map[customValKey]map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats {
	result := make(map[customValKey]map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
	for _, workspaces := range ujss {
		for _, customVals := range workspaces {
			for customVal, states := range customVals {
				if _, ok := result[customVal]; !ok {
					result[customVal] = make(map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
				}
				for state, paramsMetrics := range states {
					if _, ok := result[customVal][state]; !ok {
						result[customVal][state] = make(map[parameterFiltersKey]*UpdateJobStatusStats)
					}
					for params, metrics := range paramsMetrics {
						existingMetrics, ok := result[customVal][state][params]
						if !ok {
							existingMetrics = &UpdateJobStatusStats{parameters: metrics.parameters}
							result[customVal][state][params] = existingMetrics
						}
						existingMetrics.count += metrics.count
						existingMetrics.bytes += metrics.bytes
					}
				}
			}
		}
	}
	return result
}

// Stats for jobs grouped by status and parameters
type UpdateJobStatusStats struct {
	// job parameters
	parameters ParameterFilterList
	// number of jobs
	count int
	// total size of error responses in bytes
	bytes int
}

func (jd *Handle) updateJobStatusDSInTx(ctx context.Context, tx *Tx, ds dataSetT, statusList []*JobStatusT) (updatedStates updateJobStatusStats, err error) {
	if len(statusList) == 0 {
		return updatedStates, err
	}

	defer jd.getTimerStat("update_job_status_ds_time", nil).RecordDuration()()
	updatedStates = updateJobStatusStats{}
	store := func() error {
		updatedStates = updateJobStatusStats{} // reset in case of retry
		stmt, err := tx.PrepareContext(ctx, misc.DBCopyIn(ds.JobStatusTable, "job_id", "job_state", "attempt", "exec_time",
			"retry_time", "error_code", "error_response", "parameters"))
		if err != nil {
			return err
		}

		defer func() { _ = stmt.Close() }()
		for _, status := range statusList {
			partitionID := status.PartitionID
			if partitionID == "" {
				if jd.conf.numPartitions > 0 && jd.conf.warnOnStatusMissingPartitionID.Load() {
					// log a warning if partition id is not set but partitioning is enabled
					fields := []logger.Field{
						logger.NewStringField("tablePrefix", jd.tablePrefix),
						logger.NewIntField("job_id", status.JobID),
					}
					jd.logger.Warnn("Job status partition id is empty while partitioning is enabled, using none", fields...)
				}
				partitionID = "none"
			}
			if _, ok := updatedStates[partitionIDKey(partitionID)]; !ok {
				updatedStates[partitionIDKey(partitionID)] = make(map[workspaceIDKey]map[customValKey]map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
			}
			if _, ok := updatedStates[partitionIDKey(partitionID)][workspaceIDKey(status.WorkspaceId)]; !ok {
				updatedStates[partitionIDKey(partitionID)][workspaceIDKey(status.WorkspaceId)] = make(map[customValKey]map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
			}
			if _, ok := updatedStates[partitionIDKey(partitionID)][workspaceIDKey(status.WorkspaceId)][customValKey(status.CustomVal)]; !ok {
				updatedStates[partitionIDKey(partitionID)][workspaceIDKey(status.WorkspaceId)][customValKey(status.CustomVal)] = make(map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
			}
			if _, ok := updatedStates[partitionIDKey(partitionID)][workspaceIDKey(status.WorkspaceId)][customValKey(status.CustomVal)][jobStateKey(status.JobState)]; !ok {
				updatedStates[partitionIDKey(partitionID)][workspaceIDKey(status.WorkspaceId)][customValKey(status.CustomVal)][jobStateKey(status.JobState)] = make(map[parameterFiltersKey]*UpdateJobStatusStats)
			}
			var parameters ParameterFilterList
			var parametersKey parameterFiltersKey
			if status.JobParameters != nil {
				for _, param := range cacheParameterFilters {
					v := gjson.GetBytes(status.JobParameters, param).Str
					parameters = append(parameters, ParameterFilterT{Name: param, Value: v})
				}
				parametersKey = parameterFiltersKey(parameters.String())

			}
			pm, ok := updatedStates[partitionIDKey(partitionID)][workspaceIDKey(status.WorkspaceId)][customValKey(status.CustomVal)][jobStateKey(status.JobState)][parametersKey]
			if !ok {
				pm = &UpdateJobStatusStats{parameters: parameters}
				updatedStates[partitionIDKey(partitionID)][workspaceIDKey(status.WorkspaceId)][customValKey(status.CustomVal)][jobStateKey(status.JobState)][parametersKey] = pm
			}
			pm.count++
			pm.bytes += len(status.ErrorResponse)

			//  Handle the case when google analytics returns gif in response
			if !utf8.ValidString(string(status.ErrorResponse)) {
				status.ErrorResponse = []byte(`{}`)
			}
			_, err = stmt.ExecContext(ctx, status.JobID, status.JobState, status.AttemptNum, status.ExecTime,
				status.RetryTime, status.ErrorCode, string(status.ErrorResponse), string(status.Parameters))
			if err != nil {
				return err
			}
		}
		if _, err = stmt.ExecContext(ctx); err != nil {
			return err
		}

		if len(statusList) > jd.conf.analyzeThreshold.Load() {
			_, err = tx.ExecContext(ctx, fmt.Sprintf(`ANALYZE %q`, ds.JobStatusTable))
		}

		return err
	}
	const (
		savepointSql = "SAVEPOINT updateJobStatusDSInTx"
		rollbackSql  = "ROLLBACK TO " + savepointSql
	)
	if _, err = tx.ExecContext(ctx, savepointSql); err != nil {
		return updatedStates, err
	}
	err = store()
	var e *pq.Error
	if err != nil && errors.As(err, &e) {
		if e.Code == pgErrorCodeTableReadonly {
			if _, err = tx.ExecContext(ctx, rollbackSql); err != nil {
				return updatedStates, err
			}
			return updatedStates, ErrStaleDsList
		}
		if _, ok := dbInvalidJsonErrors[string(e.Code)]; ok {
			if _, err = tx.ExecContext(ctx, rollbackSql); err != nil {
				return updatedStates, err
			}
			for i := range statusList {
				err = statusList[i].sanitizeJson()
				if err != nil {
					return updatedStates, err
				}
			}
			err = store()
		}
	}
	return updatedStates, err
}

// UpdateSafeTx sealed interface
type UpdateSafeTx interface {
	Tx() *Tx
	SqlTx() *sql.Tx
	getDSList() []dataSetT
	getDSRangeList() []dataSetRangeT
	setDSList([]dataSetT, []dataSetRangeT)
	updateSafeTxSealIdentifier() string
}
type updateSafeTx struct {
	tx          *Tx
	identity    string
	dsList      []dataSetT
	dsRangeList []dataSetRangeT
}

func (r *updateSafeTx) updateSafeTxSealIdentifier() string {
	return r.identity
}

func (r *updateSafeTx) getDSList() []dataSetT {
	return r.dsList
}

func (r *updateSafeTx) getDSRangeList() []dataSetRangeT {
	return r.dsRangeList
}

func (r *updateSafeTx) setDSList(dsList []dataSetT, dsRangeList []dataSetRangeT) {
	r.dsList = dsList
	r.dsRangeList = dsRangeList
}

func (r *updateSafeTx) Tx() *Tx {
	return r.tx
}

func (r *updateSafeTx) SqlTx() *sql.Tx {
	return r.tx.Tx
}

// EmptyUpdateSafeTx returns an empty interface usable only for tests
func EmptyUpdateSafeTx() UpdateSafeTx {
	return &updateSafeTx{tx: &Tx{}}
}
