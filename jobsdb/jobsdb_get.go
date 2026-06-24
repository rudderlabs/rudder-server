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
	// allPendingConsumers requests the multi-consumer "pending" variant (set internally by GetPendingConsumerJobs):
	// instead of scoping to a single consumer, a job is returned once if any of its consumers' latest
	// status matches stateFilters, with its Consumers field rewritten to exactly those matching consumers
	// and no per-job status carried. Ignored on single-consumer handles.
	allPendingConsumers bool

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

func (jd *Handle) GetPendingConsumerJobs(ctx context.Context, states []string, params GetQueryParams) (JobsResult, error) {
	params.allPendingConsumers = true // honored only by multi-consumer handles, in getJobsDS
	return jd.GetJobs(ctx, states, params)
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

	// Single-consumer: one pending row per job, destination_id read from the job's parameters.
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

	// Multi-consumer: assuming for now that a consumer IS a destination, so fan out per consumer over the narrow consumers
	// array (no payload), counting each (job, consumer) whose latest pre-cutoff status is missing or
	// non-terminal. The per-consumer latest status uses the (job_id, consumer, id DESC) index.
	if jd.conf.multiConsumer {
		queryString = `WITH pending AS (
	SELECT
		j.workspace_id AS workspace_id,
		j.custom_val AS custom_val,
		c.consumer AS destination_id
	FROM
		%[1]q j
		CROSS JOIN LATERAL unnest(j.consumers) AS c(consumer)
		LEFT JOIN LATERAL (
			SELECT job_state FROM %[2]q WHERE job_id = j.job_id AND consumer = c.consumer AND exec_time < $1 ORDER BY id DESC LIMIT 1
		) s ON true
	WHERE
		s.job_state is null OR s.job_state = ANY($2)
)
SELECT
	workspace_id,
	custom_val,
	destination_id,
	COUNT(*)
FROM pending GROUP BY workspace_id, custom_val, destination_id;`
	}

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

// distinctConsumersCacheKey is the distinctValuesCache key namespace for the consumer dimension.
// Parameter keys are always `parameters->>'...'` or `workspace_id`, so this cannot collide.
const distinctConsumersCacheKey = "__consumers__"

func (jd *Handle) GetDistinctConsumers(ctx context.Context) ([]string, error) {
	// A single-consumer handle is structurally a handle whose only consumer is the legacy ''
	// consumer, so it has exactly that one distinct consumer.
	if !jd.conf.multiConsumer {
		return []string{""}, nil
	}
	if !jd.dsCompactionLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a compaction read lock: %w", ctx.Err())
	}
	defer jd.dsCompactionLock.RUnlock()
	dsList, _, release, err := jd.acquireDSListForRead(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	// Only datasets that carry a registry table participate; during a rolling flip a peer
	// may briefly have created a dataset without one (healed on its next boot). With no
	// registry yet, every job is still legacy — the '' consumer.
	mcDS := lo.Filter(dsList, func(ds dataSetT, _ int) bool { return ds.ConsumersTable != "" })
	if len(mcDS) == 0 {
		return []string{""}, nil
	}
	// The registry table is keyed by JobTable so the load function can resolve it. JobTable is
	// also the cache's per-dataset key, matching the RemoveDataset(ds.JobTable) drop invalidation.
	registryByJobTable := make(map[string]string, len(mcDS))
	for _, ds := range mcDS {
		registryByJobTable[ds.JobTable] = ds.ConsumersTable
	}
	return jd.distinctValuesCache.GetDistinctValues(
		distinctConsumersCacheKey,
		lo.Map(mcDS, func(ds dataSetT, _ int) string { return ds.JobTable }),
		func(datasets []string) (map[string][]string, error) {
			return jd.getDistinctConsumersPerDataset(ctx, datasets, registryByJobTable)
		},
	)
}

// getDistinctConsumersPerDataset reads the consumers registry table of each requested dataset
// in a single UNION ALL query, returning the consumers keyed by the dataset's JobTable.
func (jd *Handle) getDistinctConsumersPerDataset(
	ctx context.Context,
	datasets []string,
	registryByJobTable map[string]string,
) (map[string][]string, error) {
	queries := make([]string, 0, len(datasets))
	for _, jobTable := range datasets {
		queries = append(queries, fmt.Sprintf(
			`SELECT '%[1]s' AS ds, consumer FROM %[2]q`, jobTable, registryByJobTable[jobTable],
		))
	}
	rows, err := jd.getDB(ctx).QueryContext(ctx, strings.Join(queries, " UNION ALL "))
	if err != nil {
		return nil, fmt.Errorf("couldn't query distinct consumers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := make(map[string][]string, len(datasets))
	for rows.Next() {
		var ds, consumer string
		if err := rows.Scan(&ds, &consumer); err != nil {
			return nil, fmt.Errorf("couldn't scan distinct consumers: %w", err)
		}
		result[ds] = append(result[ds], consumer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err() on distinct consumers: %w", err)
	}
	return result, nil
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

	// Multi-consumer "pending" variant (set internally by GetPendingConsumerJobs): instead of scoping to a single
	// consumer, return each job once with its Consumers rewritten to the consumers whose latest status
	// matches stateFilters, carrying no per-job status. Being an all-consumers query, it keys the
	// noResultsCache on the wildcard consumer=* (which store/update writes invalidate).
	pendingConsumersMode := jd.conf.multiConsumer && params.allPendingConsumers

	// For multi-consumer JobsDB, consumer is a cache dimension: consumer=X scopes lookups to that consumer;
	// the all-consumers pending variant uses consumer=*.
	cacheParamFilters := parameterFilters
	if jd.conf.multiConsumer {
		consumerKey := params.Consumer
		if pendingConsumersMode {
			consumerKey = "*"
		}
		cacheParamFilters = append(slices.Clone(parameterFilters), ParameterFilterT{Name: consumerParamName, Value: consumerKey})
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
	// In pendingConsumersMode the state predicate is evaluated per-consumer inside the aggregation lateral below,
	// not against a single job_latest_state row.
	if !pendingConsumersMode {
		additionalPredicates := lo.FilterMap(stateFilters, func(s string, _ int) (string, bool) {
			return "(job_latest_state.job_id IS NULL)", s == Unprocessed.State
		})
		stateQuery := constructQueryOR("job_latest_state.job_state", lo.Filter(stateFilters, func(s string, _ int) bool {
			return s != Unprocessed.State
		}), additionalPredicates...)
		filterConditions = append(filterConditions, stateQuery)
	}

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

	// query args are shared between the inner query (consumer/state) and the outer wrap (limits).
	// They must be added first so their $n positions are stable when wrap args are appended.
	var args []any

	// innerConsumerClause is added inside LATERAL subqueries; outerConsumerClause is added to ON clauses.
	var innerConsumerClause, outerConsumerClause string
	switch {
	case pendingConsumersMode:
		// $1 is the set of matched (non-unprocessed) states, used by the aggregation lateral below.
		args = append(args, pq.Array(lo.Filter(stateFilters, func(s string, _ int) bool { return s != Unprocessed.State })))
	case jd.conf.multiConsumer:
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
	consumersExpr := "jobs.consumers"
	statusExpr := `job_latest_state.job_state, job_latest_state.attempt,
									job_latest_state.exec_time, job_latest_state.retry_time,
									job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters`
	if params.allPendingConsumers {
		statusExpr = `NULL::text AS job_state, NULL::int AS attempt,
									NULL::timestamptz AS exec_time, NULL::timestamptz AS retry_time,
									NULL::text AS error_code, NULL::jsonb AS error_response, NULL::jsonb AS parameters`
	}
	viewPrefix := "v_last_"
	if jd.conf.multiConsumer {
		viewPrefix = "v_last_c_"
	}
	joinTable := viewPrefix + ds.JobStatusTable
	onlyUnprocessed := slices.Equal(stateFilters, []string{Unprocessed.State})

	if pendingConsumersMode {
		// Aggregate the matching consumers per job inside a LATERAL over the narrow consumers array, so the
		// payload is never fanned out. The lateral exposes the same job_latest_state columns the SELECT/scan
		// expect (all NULL — no status is carried) plus the trimmed consumers array; the INNER JOIN's ON
		// drops jobs whose every consumer is already done (array_agg over the empty match set → NULL).
		joinType = "INNER"
		consumersExpr = "job_latest_state.consumers"
		statePredicate := "last.job_state = ANY($1)"
		if containsUnprocessed {
			statePredicate = "(last.job_state IS NULL OR last.job_state = ANY($1))"
		}
		joinTable = fmt.Sprintf(`LATERAL (
			SELECT array_agg(c.consumer) AS consumers
			FROM unnest(jobs.consumers) AS c(consumer)
			LEFT JOIN LATERAL (
				SELECT job_state FROM %q WHERE job_id = jobs.job_id AND consumer = c.consumer ORDER BY id DESC LIMIT 1
			) last ON true
			WHERE %s
		) job_latest_state ON job_latest_state.consumers IS NOT NULL`, ds.JobStatusTable, statePredicate)
	} else if !containsUnprocessed { // If we are not querying for unprocessed jobs, we can use an inner join
		joinType = "INNER"
	} else if onlyUnprocessed {
		// If we are querying only for unprocessed jobs, we should join with the status table instead of the view (performance reasons)
		joinTable = ds.JobStatusTable
	}

	// For the pure-unprocessed case the join is a NOT-EXISTS pattern (LEFT JOIN + IS NULL):
	// we only need to know whether any status row exists, not which one is latest,
	// so the lateral's ORDER BY / LIMIT 1 would be wasteful — skip it.
	// (pendingConsumersMode already built its own join above.)
	if !pendingConsumersMode {
		if !onlyUnprocessed {
			joinTable = fmt.Sprintf(`LATERAL (SELECT * FROM %q WHERE job_id = jobs.job_id%s ORDER BY id DESC LIMIT 1) job_latest_state ON true`, ds.JobStatusTable, innerConsumerClause)
		} else {
			joinTable = fmt.Sprintf(`%q job_latest_state ON jobs.job_id=job_latest_state.job_id%s`, joinTable, outerConsumerClause)
		}
	}

	sqlStatement := fmt.Sprintf(`SELECT
									jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count,
									jobs.created_at, jobs.expire_at, jobs.workspace_id, jobs.partition_id, %[6]s,
									octet_length(jobs.event_payload::text) as payload_size,
									sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts,
									sum(octet_length(jobs.event_payload::text)) over (order by jobs.job_id) as running_payload_size,
								%[7]s
								FROM
									%[1]q AS jobs
									%[2]s JOIN %[3]s
								    %[4]s
									ORDER BY jobs.job_id %[5]s`,
		ds.JobTable,   // 1
		joinType,      // 2
		joinTable,     // 3
		filterQuery,   // 4
		limitQuery,    // 5
		consumersExpr, // 6
		statusExpr,    // 7
	)

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
			// we only cache a no-result when the whole query returned no jobs
			if len(jobList) == 0 {
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
	if jd.conf.compaction.getJobsRetryOnCompaction.Load() {
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
