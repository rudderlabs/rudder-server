package jobsdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/lib/pq/pqerror"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/utils/misc"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

func (jd *Handle) internalStoreJobsInTx(ctx context.Context, tx *Tx, ds dataSetT, jobList []*JobT) error {
	tags := &statTags{CustomValFilters: []string{jd.tablePrefix}}
	defer jd.getTimerStat(
		"store_jobs",
		tags,
	).RecordDuration()()

	tx.AddSuccessListener(func() {
		jd.invalidateCacheForJobs(ds, jobList)
	})
	tx.AddSuccessListener(func() {
		statTags := tags.getStatsTags(jd.tablePrefix)
		jd.stats.NewTaggedStat("jobsdb_stored_jobs", stats.CountType, statTags).Count(len(jobList))
		jd.stats.NewTaggedStat("jobsdb_stored_bytes", stats.CountType, statTags).Count(lo.SumBy(jobList, func(j *JobT) int { return len(j.EventPayload) }))
	})

	return jd.doStoreJobsInTx(ctx, tx, ds, jobList)
}

func (jd *Handle) WithStoreSafeTx(ctx context.Context, f func(tx StoreSafeTx) error) error {
	return jd.inStoreSafeCtx(ctx, func(lastDS dataSetT) error {
		return jd.WithTx(ctx, func(tx *Tx) error {
			return f(&storeSafeTx{tx: tx, identity: jd.tablePrefix, lastDS: lastDS})
		})
	})
}

func (jd *Handle) WithStoreSafeTxFromTx(ctx context.Context, tx *Tx, f func(tx StoreSafeTx) error) error {
	return jd.inStoreSafeCtx(ctx, func(lastDS dataSetT) error {
		return f(&storeSafeTx{tx: tx, identity: jd.tablePrefix, lastDS: lastDS})
	})
}

func (jd *Handle) inStoreSafeCtx(ctx context.Context, f func(lastDS dataSetT) error) error {
	var lastDS dataSetT
	op := func() error {
		if !jd.dsListLock.RTryLockWithCtx(ctx) {
			return fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
		}
		lastDS = jd.getLastDS()
		if jd.conf.holdDSListLockDuringStore.Load() {
			defer jd.dsListLock.RUnlock()
			return f(lastDS)
		}
		jd.dsListLock.RUnlock()
		return f(lastDS)
	}
	maxRetries := jd.conf.staleDSListMaxRetries.Load()
	for attempt := 0; ; attempt++ {
		err := op()
		if err != nil && errors.Is(err, ErrStaleDsList) {
			if attempt >= maxRetries {
				return fmt.Errorf("stale dataset list after %d retries: %w", maxRetries, err)
			}
			jd.logger.Warnn("[JobsDB] :: Stale dataset list detected, retrying after refreshing DS cache", obskit.Error(ErrStaleDsList))
			if err := jd.dsListLock.WithLockInCtx(ctx, func(l lock.LockToken) error {
				if jd.getLastDS().Index != lastDS.Index {
					// The last dataset has already been refreshed, so we can just retry
					return nil
				}
				err = jd.doRefreshDSRangeList(l)
				if err != nil {
					return fmt.Errorf("refreshing ds list: %w", err)
				}
				return nil
			}); err != nil {
				return err
			}
		} else {
			return err
		}
	}
}

func (jd *Handle) invalidateCacheForJobs(ds dataSetT, jobList []*JobT) {
	cacheKeys := make(map[string]map[string]map[string]struct{})
	for _, job := range jobList {
		partitionID := job.PartitionID
		if partitionID == "" {
			// If there is no partition id, use "none" so that cache invalidation doesn't invalidate the whole tree
			// No need to log a warning, because partitioning is optional, it is not enabled for all jobsdbs
			partitionID = "none"
		}
		partitions := []string{partitionID}
		workspace := job.WorkspaceId
		customVal := job.CustomVal

		if _, ok := cacheKeys[workspace]; !ok {
			cacheKeys[workspace] = make(map[string]map[string]struct{})
		}
		if _, ok := cacheKeys[workspace][customVal]; !ok {
			cacheKeys[workspace][customVal] = make(map[string]struct{})
		}

		var params []string
		var parameterFilters []ParameterFilterT

		for _, key := range cacheParameterFilters {
			val := gjson.GetBytes(job.Parameters, key).String()
			params = append(params, key+":"+val)
			parameterFilters = append(parameterFilters, ParameterFilterT{Name: key, Value: val})
		}

		if jd.conf.multiConsumer {
			consumers := job.Consumers
			if len(consumers) == 0 {
				consumers = []string{""}
			}
			for _, c := range consumers {
				params = append(params, consumerParamName+":"+c)
				parameterFilters = append(parameterFilters, ParameterFilterT{Name: consumerParamName, Value: c})
			}
			// also invalidate all-consumer query entries
			parameterFilters = append(parameterFilters, ParameterFilterT{Name: consumerParamName, Value: "*"})
		}

		paramsKey := strings.Join(params, "#")
		if _, ok := cacheKeys[workspace][customVal][paramsKey]; !ok {
			cacheKeys[workspace][customVal][paramsKey] = struct{}{}
			jd.noResultsCache.Invalidate(ds.Index, partitions, workspace, []string{customVal}, []string{Unprocessed.State}, parameterFilters)
		}
	}
}

func (jd *Handle) doStoreJobsInTx(ctx context.Context, tx *Tx, ds dataSetT, jobList []*JobT) error {
	store := func() error {
		var stmt *sql.Stmt
		var err error

		stmt, err = tx.PrepareContext(ctx, misc.DBCopyIn(ds.JobTable, "uuid", "user_id", "custom_val", "parameters", "event_payload", "event_count", "workspace_id", "partition_id", "consumers"))
		if err != nil {
			return err
		}

		defer func() { _ = stmt.Close() }()
		for _, job := range jobList {
			eventCount := max(job.EventCount, 1)
			// Assign partition ID if not already assigned
			if job.PartitionID == "" && jd.conf.numPartitions > 0 {
				job.PartitionID = jd.conf.partitionFunction(job)
			}
			consumers := job.Consumers
			if len(consumers) == 0 {
				consumers = []string{""}
			}
			if _, err = stmt.ExecContext(ctx, job.UUID, job.UserID, job.CustomVal, string(job.Parameters), string(job.EventPayload), eventCount, job.WorkspaceId, job.PartitionID, pq.Array(consumers)); err != nil {
				return err
			}
		}
		if _, err = stmt.ExecContext(ctx); err != nil {
			return err
		}
		if len(jobList) > jd.conf.analyzeThreshold.Load() {
			if _, err = tx.ExecContext(ctx, fmt.Sprintf(`ANALYZE %q`, ds.JobTable)); err != nil {
				return err
			}
		}
		if jd.conf.multiConsumer {
			err = jd.registerConsumers(ctx, tx, ds, jobList)
		}
		return err
	}
	const (
		savepointSql = "SAVEPOINT doStoreJobsInTx"
		rollbackSql  = "ROLLBACK TO " + savepointSql
	)
	if _, err := tx.ExecContext(ctx, savepointSql); err != nil {
		return err
	}
	sanitized := false
	for {
		err := store()
		if err == nil {
			return nil
		}
		var e *pq.Error
		if !errors.As(err, &e) {
			return err
		}
		// The table might be in read-only mode or might no longer exist (dropped by compaction goroutine).
		// In both cases, we should return a stale dataset list error to trigger a refresh
		if e.Code == pgErrorCodeTableReadonly || e.Code == pqerror.UndefinedTable {
			if _, rbErr := tx.ExecContext(ctx, rollbackSql); rbErr != nil {
				return rbErr
			}
			return ErrStaleDsList
		}
		if _, ok := dbInvalidJsonErrors[string(e.Code)]; ok && !sanitized {
			if _, rbErr := tx.ExecContext(ctx, rollbackSql); rbErr != nil {
				return rbErr
			}
			for i := range jobList {
				if sErr := jobList[i].sanitizeJSON(); sErr != nil {
					return fmt.Errorf("sanitizeJSON: %w", sErr)
				}
			}
			sanitized = true
			continue
		}
		return err
	}
}

// Store stores new jobs to the jobsdb.
// If enableWriterQueue is true, this goes through writer worker pool.
func (jd *Handle) Store(ctx context.Context, jobList []*JobT) error {
	return jd.WithStoreSafeTx(ctx, func(tx StoreSafeTx) error {
		return jd.StoreInTx(ctx, tx, jobList)
	})
}

// StoreInTx stores new jobs to the jobsdb.
// If enableWriterQueue is true, this goes through writer worker pool.
func (jd *Handle) StoreInTx(ctx context.Context, tx StoreSafeTx, jobList []*JobT) error {
	if tx.storeSafeTxIdentifier() != jd.Identifier() {
		return fmt.Errorf("invalid store safe tx identifier, expected: %s, actual: %s", jd.Identifier(), tx.storeSafeTxIdentifier())
	}
	storeCmd := func() error {
		command := func() error {
			err := jd.internalStoreJobsInTx(ctx, tx.Tx(), tx.getLastDS(), jobList)
			return err
		}
		err := executeDbRequest(ctx, jd, newWriteDbRequest("store", nil, command))
		return err
	}
	return storeCmd()
}
