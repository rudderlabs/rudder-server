package jobsdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/cache"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
)

// GetQueryParams is a struct to hold jobsdb query params.
type GetQueryParams struct {
	// query conditions

	WorkspaceID      string
	CustomValFilters []string
	ParameterFilters []ParameterFilterT
	PartitionFilters []string
	// Consumer scopes the query to a single consumer.
	// On a multi-consumer JobsDB this is always applied — an empty string targets the default '' consumer.
	// On a single-consumer JobsDB this field is ignored.
	Consumer string

	stateFilters                   []string
	afterJobID                     *int64
	ignoreReadPartitionsExclusions bool // if true, includes results from all partitions, ignoring any preconfigured excluded read partitions

	// query limits

	// Limit the total number of jobs.
	// A value less than or equal to zero will return no results
	JobsLimit int
	// Limit the total number of events, 1 job contains 1+ event(s).
	// A value less than or equal to zero will disable this limit (no limit),
	// only values greater than zero are considered as valid limits.
	EventsLimit int
	// Limit the total job payload size.
	// A value of zero disables this limit (no limit).
	// A negative value returns no results.
	PayloadSizeLimit int64
}

// MoreToken is a token that can be used to fetch more jobs
type MoreToken any

// MoreJobsResult is a JobsResult with a MoreToken
type MoreJobsResult struct {
	JobsResult
	More MoreToken
}

type moreToken struct {
	afterJobID *int64
}

func (jd *Handle) GetToProcess(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error) { // skipcq: CRT-P0003
	return jd.getMoreJobs(ctx, []string{Failed.State, Waiting.State, Unprocessed.State}, params, more)
}

var cacheParameterFilters = []string{"source_id", "destination_id"}

const consumerParamName = "consumer" // the name of the consumer parameter, used for multi-consumer scoping of queries and cache entries

func (jd *Handle) GetPileUpCounts(ctx context.Context, cutoffTime time.Time, increaseFunc rmetrics.IncreasePendingEventsFunc) error {
	// pause compaction to avoid any read locks being blocked during pileup count
	jd.compactionPaused.Store(true)
	defer jd.compactionPaused.Store(false)
	if !jd.dsCompactionLock.RTryLockWithCtx(ctx) {
		return fmt.Errorf("could not acquire a compaction read lock: %w", ctx.Err())
	}
	defer jd.dsCompactionLock.RUnlock()
	dsList, _, release, err := jd.acquireDSListForRead(ctx)
	if err != nil {
		return err
	}
	defer release()

	queryString := `WITH pending AS (
	SELECT
		j.workspace_id AS workspace_id,
		j.custom_val AS custom_val,
		j.parameters->>'destination_id' AS destination_id
	FROM
		%[1]q j
		LEFT JOIN (SELECT DISTINCT ON (job_id) job_id, job_state FROM %[2]q WHERE exec_time < $1 ORDER BY job_id ASC, id DESC) s ON j.job_id = s.job_id
	WHERE
		s.job_id is null OR s.job_state = ANY($2)
)
SELECT
	workspace_id,
	custom_val,
	destination_id,
	COUNT(*)
FROM pending GROUP BY workspace_id, custom_val, destination_id;`

	g, ctx := errgroup.WithContext(ctx)
	const defaultConcurrency = 4
	conc := jd.config.GetIntVar(defaultConcurrency, 1, jd.configKeys("pileupCountConcurrency")...)
	if conc < 1 || conc > defaultConcurrency {
		jd.logger.Warnn("GetPileUpCounts concurrency out of safe bounds, using default value",
			logger.NewIntField("concurrency", int64(conc)),
			logger.NewIntField("default", int64(defaultConcurrency)),
		)
		conc = defaultConcurrency
	}
	g.SetLimit(conc)
	for _, ds := range dsList {
		g.Go(func() error {
			rows, err := jd.getDB(ctx).QueryContext(ctx, fmt.Sprintf(queryString, ds.JobTable, ds.JobStatusTable),
				cutoffTime,
				pq.Array([]string{Executing.State, Failed.State, Importing.State, Waiting.State}))
			if err != nil {
				return fmt.Errorf("getting pileup counts for %q: %w", ds.JobTable, err)
			}
			defer func() {
				_ = rows.Close()
			}()
			for rows.Next() {
				var (
					workspace, customVal, destinationID sql.NullString
					count                               sql.NullInt64
				)
				if err := rows.Scan(&workspace, &customVal, &destinationID, &count); err != nil {
					return fmt.Errorf("scanning pileup counts rows for %q: %w", ds.JobTable, err)
				}
				if count.Valid {
					increaseFunc(jd.tablePrefix, workspace.String, customVal.String, destinationID.String, float64(count.Int64))
				}
			}
			if err = rows.Err(); err != nil {
				return fmt.Errorf("iterating pileup counts for %q: %w", ds.JobTable, err)
			}
			return nil
		})
	}
	return g.Wait()
}

func (jd *Handle) getDistinctValuesPerDataset(
	ctx context.Context,
	dsList []string,
	param ParameterName,
	customVal string,
) (map[string][]string, error) {
	var queries []string
	for _, ds := range dsList {
		if customVal == "" {
			queries = append(queries, fmt.Sprintf(`SELECT '%[2]s', * FROM (
				WITH RECURSIVE t AS (
					(SELECT %[1]s as parameter FROM %[2]q ORDER BY %[1]s LIMIT 1)
					UNION ALL
					(
						SELECT s.* FROM t, LATERAL(
							SELECT %[1]s as parameter FROM %[2]q f
							WHERE f.%[1]s > t.parameter
							ORDER BY %[1]s LIMIT 1
							)s
						)
					)
				SELECT * FROM t) a`, param.string(), ds))
		} else {
			queries = append(queries, fmt.Sprintf(`SELECT '%[2]s', * FROM (
				WITH RECURSIVE t AS (
					(SELECT %[1]s as parameter FROM %[2]q WHERE custom_val = '%[3]s' ORDER BY %[1]s LIMIT 1)
					UNION ALL
					(
						SELECT s.* FROM t, LATERAL(
							SELECT %[1]s as parameter FROM %[2]q f
							WHERE custom_val = '%[3]s' AND f.%[1]s > t.parameter
							ORDER BY %[1]s LIMIT 1
							)s
						)
					)
				SELECT * FROM t) a`, param.string(), ds, customVal))
		}
	}
	query := strings.Join(queries, " UNION ")
	rows, err := jd.getDB(ctx).QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("couldn't query distinct parameter-%s: %w", param.string(), err)
	}
	defer func() { _ = rows.Close() }()
	result := make(map[string][]string)
	for rows.Next() {
		var (
			ds    string
			value string
		)
		err := rows.Scan(&ds, &value)
		if err != nil {
			return nil, fmt.Errorf("couldn't scan distinct parameter-%s: %w", param.string(), err)
		}
		result[ds] = append(result[ds], value)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err() on distinct parameter-%s: %w", param.string(), err)
	}
	return result, nil
}

func (jd *Handle) GetDistinctParameterValues(ctx context.Context, parameter ParameterName, customValFilter string) ([]string, error) {
	if !jd.dsCompactionLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a compaction read lock: %w", ctx.Err())
	}
	defer jd.dsCompactionLock.RUnlock()
	dsList, _, release, err := jd.acquireDSListForRead(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return jd.distinctValuesCache.GetDistinctValues(
		parameter.string()+customValFilter,
		lo.Map(dsList, func(ds dataSetT, _ int) string { return ds.JobTable }),
		func(datasets []string) (map[string][]string, error) {
			return jd.getDistinctValuesPerDataset(ctx, datasets, parameter, customValFilter)
		},
	)
}

type JobsResult struct {
	Jobs            []*JobT
	LimitsReached   bool
	DSLimitsReached bool
	EventsCount     int
	PayloadSize     int64
}

/*
stateFilters, customValFilters, and parameterFilters each OR the values passed
within that filter list. JobsLimit less than or equal to zero returns no results.
*/
func (jd *Handle) getJobsDS(ctx context.Context, ds dataSetT, lastDS bool, params GetQueryParams) (JobsResult, bool, error) { // skipcq: CRT-P0003
	stateFilters := params.stateFilters
	customValFilters := params.CustomValFilters
	var parameterFilters ParameterFilterList = params.ParameterFilters
	partitionFilters := lo.Uniq(params.PartitionFilters)
	workspaceID := params.WorkspaceID
	checkValidJobState(jd, stateFilters)

	// For multi-consumer JobsDB, consumer is a cache dimension: consumer=X scopes lookups to that consumer.
	cacheParamFilters := parameterFilters
	if jd.conf.multiConsumer {
		cacheParamFilters = append(slices.Clone(parameterFilters), ParameterFilterT{Name: consumerParamName, Value: params.Consumer})
	}

	if jd.noResultsCache.Get(ds.Index, partitionFilters, workspaceID, customValFilters, stateFilters, cacheParamFilters) {
		jd.logger.Debugn("[getJobsDS] Empty cache hit for ds: %v, stateFilters: %v, customValFilters: %v, parameterFilters: %v",
			logger.NewStringField("ds", ds.String()),
			logger.NewStringField("stateFilters", strings.Join(stateFilters, ",")),
			logger.NewStringField("customValFilters", strings.Join(customValFilters, ",")),
			logger.NewStringField("parameterFilters", cacheParamFilters.String()),
		)
		return JobsResult{}, false, nil
	}

	tags := statTags{
		StateFilters:     stateFilters,
		CustomValFilters: params.CustomValFilters,
		WorkspaceID:      workspaceID,
	}
	stateFilterOptimization := jd.conf.noResultsCacheStateOptimization.Load()

	if stateFilterOptimization {
		stateFilters = lo.Filter(stateFilters, func(state string, _ int) bool { // exclude states for which we already know that there are no jobs
			return !jd.noResultsCache.Get(ds.Index, partitionFilters, workspaceID, customValFilters, []string{state}, cacheParamFilters)
		})
	}

	defer jd.getTimerStat("jobsdb_get_jobs_ds_time", &tags).RecordDuration()()

	containsUnprocessed := lo.Contains(stateFilters, Unprocessed.State)
	skipCacheResult := params.afterJobID != nil
	cacheTx := map[string]*cache.NoResultTx[ParameterFilterT]{}
	if !skipCacheResult {
		for _, state := range stateFilters {
			// avoid setting result as noJobs if
			//  (1) state is unprocessed and
			//  (2) jobsdb owner is a reader and
			//  (3) ds is the right most one
			if state == Unprocessed.State && jd.ownerType == Read && lastDS {
				continue
			}
			cacheTx[state] = jd.noResultsCache.StartNoResultTx(ds.Index, partitionFilters, workspaceID, customValFilters, []string{state}, cacheParamFilters)
		}
	}

	var filterConditions []string
	additionalPredicates := lo.FilterMap(stateFilters, func(s string, _ int) (string, bool) {
		return "(job_latest_state.job_id IS NULL)", s == Unprocessed.State
	})
	stateQuery := constructQueryOR("job_latest_state.job_state", lo.Filter(stateFilters, func(s string, _ int) bool {
		return s != Unprocessed.State
	}), additionalPredicates...)
	filterConditions = append(filterConditions, stateQuery)

	if params.afterJobID != nil {
		filterConditions = append(filterConditions, fmt.Sprintf("jobs.job_id > %d", *params.afterJobID))
	}

	if len(customValFilters) > 0 {
		filterConditions = append(filterConditions, constructQueryOR("jobs.custom_val", customValFilters))
	}

	if len(parameterFilters) > 0 {
		filterConditions = append(filterConditions, constructParameterJSONQuery("jobs", parameterFilters))
	}

	if workspaceID != "" {
		filterConditions = append(filterConditions, fmt.Sprintf("jobs.workspace_id = '%s'", workspaceID))
	}

	if len(partitionFilters) > 0 {
		filterConditions = append(filterConditions, fmt.Sprintf("jobs.partition_id IN (%s)", strings.Join(lo.Map(partitionFilters, func(p string, _ int) string { return pq.QuoteLiteral(p) }), ",")))
	} else if !params.ignoreReadPartitionsExclusions {
		// excludedReadPartitions are mutually exclusive with partitionFilters
		// Use NOT EXISTS against the exclusions table to avoid generating large NOT IN lists and
		// improve performance by taking advantage of anti-join optimizations.
		jd.excludedReadPartitionsLock.RLock()
		if len(jd.excludedReadPartitions) > 0 {
			filterConditions = append(filterConditions,
				fmt.Sprintf(
					`NOT EXISTS (SELECT 1 FROM %s AS excluded WHERE excluded.partition_id = jobs.partition_id)`,
					jd.tablePrefix+"_read_excluded_partitions",
				),
			)
		}
		jd.excludedReadPartitionsLock.RUnlock()
	}

	// query args are shared between the inner query (consumer) and the outer wrap (limits).
	// Consumer must be added first so its $n position is stable when wrap args are appended.
	var args []any

	// innerConsumerClause is added inside LATERAL subqueries; outerConsumerClause is added to ON clauses.
	var innerConsumerClause, outerConsumerClause string
	if jd.conf.multiConsumer {
		args = append(args, params.Consumer)
		n := len(args)
		filterConditions = append(filterConditions, fmt.Sprintf("jobs.consumers @> ARRAY[$%d]", n))
		innerConsumerClause = fmt.Sprintf(" AND consumer = $%d", n)
		outerConsumerClause = fmt.Sprintf(" AND job_latest_state.consumer = $%d", n)
	}

	var filterQuery string
	if len(filterConditions) > 0 {
		filterQuery = "WHERE " + strings.Join(filterConditions, " AND ")
	}

	var limitQuery string
	if params.JobsLimit > 0 {
		limitQuery = fmt.Sprintf(" LIMIT %d ", params.JobsLimit)
	}

	joinType := "LEFT"
	viewPrefix := "v_last_"
	if jd.conf.multiConsumer {
		viewPrefix = "v_last_c_"
	}
	joinTable := viewPrefix + ds.JobStatusTable
	onlyUnprocessed := slices.Equal(stateFilters, []string{Unprocessed.State})

	if !containsUnprocessed { // If we are not querying for unprocessed jobs, we can use an inner join
		joinType = "INNER"
	} else if onlyUnprocessed {
		// If we are querying only for unprocessed jobs, we should join with the status table instead of the view (performance reasons)
		joinTable = ds.JobStatusTable
	}

	// For the pure-unprocessed case the join is a NOT-EXISTS pattern (LEFT JOIN + IS NULL):
	// we only need to know whether any status row exists, not which one is latest,
	// so the lateral's ORDER BY / LIMIT 1 would be wasteful — skip it.
	if jd.conf.getJobsUseLateralJoin.Load() && !onlyUnprocessed {
		joinTable = fmt.Sprintf(`LATERAL (SELECT * FROM %q WHERE job_id = jobs.job_id%s ORDER BY id DESC LIMIT 1) job_latest_state ON true`, ds.JobStatusTable, innerConsumerClause)
	} else {
		joinTable = fmt.Sprintf(`%q job_latest_state ON jobs.job_id=job_latest_state.job_id%s`, joinTable, outerConsumerClause)
	}

	sqlStatement := fmt.Sprintf(`SELECT
									jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count,
									jobs.created_at, jobs.expire_at, jobs.workspace_id, jobs.partition_id, jobs.consumers,
									octet_length(jobs.event_payload::text) as payload_size,
									sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts,
									sum(octet_length(jobs.event_payload::text)) over (order by jobs.job_id) as running_payload_size,
									job_latest_state.job_state, job_latest_state.attempt,
									job_latest_state.exec_time, job_latest_state.retry_time,
									job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters
								FROM
									%[1]q AS jobs
									%[2]s JOIN %[3]s
								    %[4]s
									ORDER BY jobs.job_id %[5]s`,
		ds.JobTable, joinType, joinTable, filterQuery, limitQuery)

	var wrapQuery []string
	if params.EventsLimit > 0 {
		// If there is a single job in the dataset containing more events than the EventsLimit, we should return it,
		// otherwise processing will halt.
		// Therefore, we always retrieve one more job from the database than our limit dictates.
		// This job will only be returned to the result in case of the aforementioned scenario, otherwise it gets filtered out
		// later, during row scanning
		wrapQuery = append(wrapQuery, fmt.Sprintf(`running_event_counts - t.event_count <= $%d`, len(args)+1))
		args = append(args, params.EventsLimit)
	}

	if params.PayloadSizeLimit > 0 {
		wrapQuery = append(wrapQuery, fmt.Sprintf(`running_payload_size - t.payload_size <= $%d`, len(args)+1))
		args = append(args, params.PayloadSizeLimit)
	}

	if len(wrapQuery) > 0 {
		sqlStatement = `SELECT * FROM (` + sqlStatement + `) t WHERE ` + strings.Join(wrapQuery, " AND ")
	}

	rows, err := jd.getDB(ctx).QueryContext(ctx, sqlStatement, args...)
	if err != nil {
		return JobsResult{}, false, err
	}
	defer func() { _ = rows.Close() }()

	var jobList []*JobT
	var limitsReached bool
	var eventCount int
	var payloadSize int64

	// we don't need the payload_size but still need to scan it because it is part of the resultset
	// The query uses it for limits checking. The variable is declared before the for loop to avoid extra allocations,
	// but if we were to actually use it in the future for returning in the result, we would need to move its declaration
	// inside the loop
	var discardRowPayloadSize int64

	resultsetStates := map[string]struct{}{}
	for rows.Next() {
		var job JobT
		var runningEventCount int
		var runningPayloadSize int64
		var payload []byte
		var jsState sql.NullString
		var jsAttemptNum sql.NullInt64
		var jsExecTime sql.NullTime
		var jsRetryTime sql.NullTime
		var jsErrorCode sql.NullString
		var jsErrorResponse []byte
		var jsParameters []byte
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&payload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.WorkspaceId, &job.PartitionID, pq.Array(&job.Consumers), &discardRowPayloadSize, &runningEventCount, &runningPayloadSize,
			&jsState, &jsAttemptNum,
			&jsExecTime, &jsRetryTime,
			&jsErrorCode, &jsErrorResponse, &jsParameters)
		if err != nil {
			return JobsResult{}, false, err
		}
		job.EventPayload = payload
		if jsState.Valid {
			resultsetStates[jsState.String] = struct{}{}
			job.LastJobStatus.JobState = jsState.String
			job.LastJobStatus.AttemptNum = int(jsAttemptNum.Int64)
			job.LastJobStatus.ExecTime = jsExecTime.Time
			job.LastJobStatus.RetryTime = jsRetryTime.Time
			job.LastJobStatus.ErrorCode = jsErrorCode.String
			job.LastJobStatus.ErrorResponse = jsErrorResponse
			job.LastJobStatus.Parameters = jsParameters
			job.LastJobStatus.JobParameters = job.Parameters
			job.LastJobStatus.WorkspaceId = job.WorkspaceId
			job.LastJobStatus.PartitionID = job.PartitionID
			job.LastJobStatus.CustomVal = job.CustomVal
		} else {
			resultsetStates[Unprocessed.State] = struct{}{}
		}

		if params.EventsLimit > 0 && runningEventCount > params.EventsLimit && len(jobList) > 0 {
			// events limit overflow is triggered as long as we have read at least one job
			limitsReached = true
			break
		}
		if params.PayloadSizeLimit > 0 && runningPayloadSize > params.PayloadSizeLimit && len(jobList) > 0 {
			// payload size limit overflow is triggered as long as we have read at least one job
			limitsReached = true
			break
		}
		// we are adding the job only after testing for limitsReached
		// so that we don't always overflow
		jobList = append(jobList, &job)
		payloadSize = runningPayloadSize
		eventCount = runningEventCount
	}
	if err := rows.Err(); err != nil {
		return JobsResult{}, false, err
	}
	if !limitsReached &&
		(params.JobsLimit > 0 && len(jobList) == params.JobsLimit) || // we reached the jobs limit
		(params.EventsLimit > 0 && eventCount >= params.EventsLimit) || // we reached the events limit
		(params.PayloadSizeLimit > 0 && payloadSize >= params.PayloadSizeLimit) { // we reached the payload limit
		limitsReached = true
	}

	if !skipCacheResult {
		for state, cacheTx := range cacheTx {
			// we are committing the cache Tx only if
			// (a) no jobs are returned by the query or
			// (b) the state is not present in the resultset and limits have not been reached
			//     (skipped when the noResultsCache state-filter optimization is disabled)
			_, ok := resultsetStates[state]
			if len(jobList) == 0 || (stateFilterOptimization && !ok && !limitsReached) {
				if allEntriesCommitted := cacheTx.Commit(); !allEntriesCommitted {
					tags := &statTags{
						StateFilters:     []string{state},
						CustomValFilters: params.CustomValFilters,
						WorkspaceID:      params.WorkspaceID,
						ParameterFilters: params.ParameterFilters,
					}
					statTags := tags.getStatsTags(jd.tablePrefix)
					jd.stats.NewTaggedStat("jobsdb_cache_commit_misses", stats.CountType, statTags).Increment()
				}
			}
		}
	}

	return JobsResult{
		Jobs:          jobList,
		LimitsReached: limitsReached,
		PayloadSize:   payloadSize,
		EventsCount:   eventCount,
	}, true, nil
}

// GetUnprocessed finds unprocessed jobs, i.e. new jobs whose state hasn't been marked in the database yet
func (jd *Handle) GetUnprocessed(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Unprocessed.State}, params)
}

// GetImporting finds jobs in importing state
func (jd *Handle) GetImporting(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error) { // skipcq: CRT-P0003
	// Importing jobs are not to be migrated, they are a special case of [executing] jobs, which get queried by
	// batchrouter's async handler periodically to check for the status of imports. Eventually, these jobs will
	// transition to [succeeded]/[failed]/[aborted] state once the import is complete.
	// Hence, we ignore read partition exclusions when fetching importing jobs, so that we can always
	// find all of them regardless of any partition exclusions.
	params.ignoreReadPartitionsExclusions = true
	return jd.getMoreJobs(ctx, []string{Importing.State}, params, more)
}

// GetAborted finds jobs in aborted state
func (jd *Handle) GetAborted(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Aborted.State}, params)
}

// GetWaiting finds jobs in waiting state
func (jd *Handle) GetWaiting(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Waiting.State}, params)
}

// GetSucceeded finds jobs in succeeded state
func (jd *Handle) GetSucceeded(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Succeeded.State}, params)
}

// GetFailed finds jobs in failed state
func (jd *Handle) GetFailed(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Failed.State}, params)
}

/*
getJobs returns jobs matching the requested states and filters. It does not update
state, so repeated calls can return the same jobs until the caller writes new statuses.
*/
func (jd *Handle) getJobs(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error) { // skipcq: CRT-P0003
	// Retry loop: if the published dsList drops one of the datasets we queried
	// while we were iterating (e.g. a compaction completed mid-read), our result
	// may have been computed against a snapshot that is no longer canonical. Re-run
	// from scratch against the freshly published list.
	for {
		res, err := jd.doGetJobs(ctx, params, more)
		if err != nil {
			if errors.Is(err, ErrStaleDsList) {
				jd.logger.Warnn("[JobsDB] getJobs: dsList compacted during query, retrying",
					logger.NewStringField("tablePrefix", jd.tablePrefix))
				continue
			}
			return nil, err
		}
		return res, nil
	}
}

func (jd *Handle) doGetJobs(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error) { // skipcq: CRT-P0003
	mtoken := &moreToken{}
	if more != nil {
		var ok bool
		if mtoken, ok = more.(*moreToken); !ok {
			return nil, fmt.Errorf("invalid token: %+v", more)
		}
	}

	if mtoken.afterJobID != nil {
		params.afterJobID = mtoken.afterJobID
	}

	if params.JobsLimit <= 0 {
		return &MoreJobsResult{JobsResult: JobsResult{}, More: mtoken}, nil
	}
	tags := &statTags{
		StateFilters:     params.stateFilters,
		CustomValFilters: params.CustomValFilters,
		WorkspaceID:      params.WorkspaceID,
		ParameterFilters: params.ParameterFilters,
		MoreToken:        more != nil,
	}
	defer jd.getTimerStat("jobsdb_get_jobs_time", tags).RecordDuration()()

	// Keep this order: take the compaction read lock before acquiring the
	// dataset-list snapshot. Compaction and drop paths publish snapshots under
	// the same ordering, so reversing it can deadlock.
	if !jd.dsCompactionLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a compaction read lock: %w", ctx.Err())
	}
	defer jd.dsCompactionLock.RUnlock()
	dsList, dsRangeList, release, err := jd.acquireDSListForRead(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	limitByEventCount := params.EventsLimit > 0

	limitByPayloadSize := false
	if params.PayloadSizeLimit > 0 {
		limitByPayloadSize = true
	} else if params.PayloadSizeLimit < 0 {
		return &MoreJobsResult{JobsResult: JobsResult{}, More: mtoken}, nil
	}

	res := &MoreJobsResult{More: mtoken}
	dsQueryCount := 0
	cacheHitCount := 0

	// Track every dataset we actually queried via getJobsDS so we can validate
	// the post-iteration snapshot against the set of datasets our result was
	// computed from.
	queried := make(map[string]struct{}, len(dsList))
	var dsLimit int
	if jd.conf.dsLimit != nil {
		dsLimit = jd.conf.dsLimit.Load()
	}
	for idx, ds := range dsList {
		if params.afterJobID != nil {
			if idx < len(dsRangeList) { // ranges are not stored for the last ds
				// so the following condition cannot be applied the last ds
				if *params.afterJobID > dsRangeList[idx].maxJobID {
					// skip this ds as afterJobID is beyond this ds's range
					continue
				}
				if *params.afterJobID < dsRangeList[idx].minJobID {
					// we have reached the ds where afterJobID would be present
					// so clear it for next ds
					params.afterJobID = nil
				}
			}
		}
		if dsLimit > 0 && dsQueryCount >= dsLimit {
			res.DSLimitsReached = true
			break
		}
		jobs, dsHit, err := jd.getJobsDS(ctx, ds, len(dsList)-1 == idx, params)
		if err != nil {
			return nil, err
		}
		queried[ds.Index] = struct{}{}
		if dsHit {
			dsQueryCount++
		} else {
			cacheHitCount++
		}
		res.Jobs = append(res.Jobs, jobs.Jobs...)
		res.EventsCount += jobs.EventsCount
		res.PayloadSize += jobs.PayloadSize

		if jobs.LimitsReached {
			res.LimitsReached = true
			break
		}
		// decrement our limits for the next query
		if params.JobsLimit > 0 {
			params.JobsLimit -= len(jobs.Jobs)
		}
		if limitByEventCount {
			params.EventsLimit -= jobs.EventsCount
		}
		if limitByPayloadSize {
			params.PayloadSizeLimit -= jobs.PayloadSize
		}
	}

	if len(res.Jobs) > 0 {
		retryAfterJobID := res.Jobs[len(res.Jobs)-1].JobID
		mtoken.afterJobID = &retryAfterJobID
	}

	statTags := tags.getStatsTags(jd.tablePrefix)
	statTags["query"] = "get"
	jd.stats.NewTaggedStat("jobsdb_tables_queried", stats.CountType, statTags).Count(dsQueryCount) // number of actual ds tables that we queried
	jd.stats.NewTaggedStat("jobsdb_cache_hits", stats.CountType, statTags).Count(cacheHitCount)    // number of ds tables that we skipped querying due to noResultsCache
	if len(res.Jobs) == 0 {
		jd.stats.NewTaggedStat("jobsdb_queried_no_jobs", stats.CountType, statTags).Increment() // number of times that we queried and got no jobs
	}
	jd.stats.NewTaggedStat("jobsdb_queried_jobs", stats.CountType, statTags).Count(len(res.Jobs))                                                         // number of jobs that we queried
	jd.stats.NewTaggedStat("jobsdb_queried_bytes", stats.CountType, statTags).Count(lo.SumBy(res.Jobs, func(j *JobT) int { return len(j.EventPayload) })) // number of bytes that we queried
	// Validate that none of the datasets we examined were compacted while we
	// were querying. If any of them were, it means our result may be stale and we need to retry.
	if jd.conf.compaction.nonBlockingCompaction.Load() && jd.conf.compaction.getJobsRetryOnCompaction.Load() {
		jd.dropDSListLock.RLock()
		_, found := lo.Find(jd.dropDSList, func(entry dropDSEntry) bool {
			if !entry.compacted {
				return false
			}
			_, ok := queried[entry.ds.Index]
			return ok
		})
		jd.dropDSListLock.RUnlock()
		if found {
			return nil, ErrStaleDsList
		}
	}
	return res, nil
}

/*
GetJobs returns jobs matching any of the requested states. It does not update state,
so repeated calls can return the same jobs until the caller writes new statuses.
*/
func (jd *Handle) GetJobs(ctx context.Context, states []string, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit == 0 {
		return JobsResult{}, nil
	}
	params.stateFilters = states
	slices.Sort(params.stateFilters)
	tags := statTags{
		StateFilters:     params.stateFilters,
		CustomValFilters: params.CustomValFilters,
		WorkspaceID:      params.WorkspaceID,
		ParameterFilters: params.ParameterFilters,
	}
	command := func() queryResult {
		return queryResultWrapper(jd.getJobs(ctx, params, nil))
	}
	res := executeDbRequest(ctx, jd, newReadDbRequest("get_jobs", &tags, command))
	return res.JobsResult, res.err
}

func (jd *Handle) getMoreJobs(ctx context.Context, states []string, params GetQueryParams, more MoreToken) (*MoreJobsResult, error) { // skipcq: CRT-P0003

	if params.JobsLimit == 0 {
		return &MoreJobsResult{More: more}, nil
	}
	params.stateFilters = slices.Clone(states)
	slices.Sort(params.stateFilters)
	tags := statTags{
		StateFilters:     params.stateFilters,
		CustomValFilters: params.CustomValFilters,
		WorkspaceID:      params.WorkspaceID,
		ParameterFilters: params.ParameterFilters,
		MoreToken:        more != nil,
	}
	command := func() moreQueryResult {
		return moreQueryResultWrapper(jd.getJobs(ctx, params, more))
	}
	res := executeDbRequest(ctx, jd, newReadDbRequest("get_jobs", &tags, command))
	return res.MoreJobsResult, res.err
}

type queryResult struct {
	JobsResult
	err error
}

func queryResultWrapper(res *MoreJobsResult, err error) queryResult {
	if res == nil {
		res = &MoreJobsResult{}
	}
	return queryResult{
		JobsResult: res.JobsResult,
		err:        err,
	}
}

type moreQueryResult struct {
	*MoreJobsResult
	err error
}

func moreQueryResultWrapper(res *MoreJobsResult, err error) moreQueryResult {
	return moreQueryResult{
		MoreJobsResult: res,
		err:            err,
	}
}
