package jobsdb

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
)

/*
Ping returns health check for pg database
*/
func (jd *HandleT) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := jd.dbHandle.ExecContext(ctx, `SELECT 'Rudder DB Health Check'::text as message`)
	if err != nil {
		return err
	}
	return nil
}

/*
DeleteExecuting deletes events whose latest job state is executing.
This is only done during recovery, which happens during the server start.
*/
func (jd *HandleT) DeleteExecuting() {
	tags := statTags{CustomValFilters: []string{jd.tablePrefix}}
	command := func() interface{} {
		jd.deleteJobStatus()
		return nil
	}
	_ = jd.executeDbRequest(newWriteDbRequest("delete_job_status", &tags, command))
}

// deleteJobStatus deletes the latest status of a batch of jobs
func (jd *HandleT) deleteJobStatus() {
	err := jd.WithUpdateSafeTx(context.TODO(), func(tx UpdateSafeTx) error {
		defer jd.getTimerStat(
			"jobsdb_delete_job_status_time",
			&statTags{
				CustomValFilters: []string{jd.tablePrefix},
			}).RecordDuration()()

		dsList := jd.getDSList()

		for _, ds := range dsList {
			ds := ds
			if err := jd.deleteJobStatusDSInTx(tx.SqlTx(), ds); err != nil {
				return err
			}
			tx.Tx().AddSuccessListener(func() {
				jd.noResultsCache.InvalidateDataset(ds.Index)
			})
		}

		return nil
	})
	jd.assertError(err)
}

func (jd *HandleT) deleteJobStatusDSInTx(txHandler transactionHandler, ds dataSetT) error {
	defer jd.getTimerStat(
		"jobsdb_delete_job_status_ds_time",
		&statTags{
			CustomValFilters: []string{jd.tablePrefix},
		}).RecordDuration()()

	_, err := txHandler.Exec(
		fmt.Sprintf(
			`DELETE FROM %[1]q
				WHERE id = ANY(
					SELECT id from "v_last_%[1]s" where job_state='executing'
				)`,
			ds.JobStatusTable,
		),
	)
	return err
}

/*
FailExecuting fails events whose latest job state is executing.

This is only done during recovery, which happens during the server start.
*/
func (jd *HandleT) FailExecuting() {
	tags := statTags{
		CustomValFilters: []string{jd.tablePrefix},
	}
	command := func() interface{} {
		jd.failExecuting()
		return nil
	}
	_ = jd.executeDbRequest(newWriteDbRequest("fail_executing", &tags, command))
}

// failExecuting sets the state of the executing jobs to failed
func (jd *HandleT) failExecuting() {
	err := jd.WithUpdateSafeTx(context.TODO(), func(tx UpdateSafeTx) error {
		defer jd.getTimerStat(
			"jobsdb_fail_executing_time",
			&statTags{CustomValFilters: []string{jd.tablePrefix}},
		).RecordDuration()()

		dsList := jd.getDSList()

		for _, ds := range dsList {
			ds := ds
			err := jd.failExecutingDSInTx(tx.SqlTx(), ds)
			if err != nil {
				return err
			}
			tx.Tx().AddSuccessListener(func() {
				jd.noResultsCache.InvalidateDataset(ds.Index)
			})
		}
		return nil
	})
	jd.assertError(err)
}

func (jd *HandleT) failExecutingDSInTx(txHandler transactionHandler, ds dataSetT) error {
	defer jd.getTimerStat(
		"jobsdb_fail_executing_ds_time",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()

	_, err := txHandler.Exec(
		fmt.Sprintf(
			`UPDATE %[1]q SET job_state='failed'
				WHERE id = ANY(
					SELECT id from "v_last_%[1]s" where job_state='executing'
				)`,
			ds.JobStatusTable,
		),
	)
	return err
}

func (jd *HandleT) startCleanupLoop(ctx context.Context) {
	jd.backgroundGroup.Go(misc.WithBugsnag(func() error {
		jd.oldJobsCleanupLoop(ctx)
		return nil
	}))
}

func (jd *HandleT) oldJobsCleanupLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-jd.TriggerJobCleanUp():
			jd.doCleanupOldJobs(ctx)
		}
	}
}

func (jd *HandleT) doCleanupOldJobs(ctx context.Context) {
	var (
		batchSize            = config.GetInt("jobsdb.cleanupBatchSize", 100)
		cleanupRetryInterval = config.GetDuration(
			"jobsdb.cleanupRetryInterval",
			10, time.Second,
		)

		numJobsCleaned = 0

		jq = jobQuery{
			queryUnprocessed:      true,
			queryProcessed:        true,
			unprocessedAfterJobID: nil,
			processedAfterJobID:   nil,
		}
	)

	for {
		res, err := jd.doCleanup(ctx, jq, batchSize)
		if err != nil {
			jd.logger.Errorf("error while cleaning up old jobs: %w", err)
			if err := misc.SleepCtx(ctx, cleanupRetryInterval); err != nil {
				return
			}
			continue
		}
		if res.numUnprocessed < batchSize {
			jq.queryUnprocessed = false
		}
		if res.numProcessed < batchSize {
			jq.queryProcessed = false
		}
		jq.unprocessedAfterJobID = res.unprocessedAfterJobID
		jq.processedAfterJobID = res.processedAfterJobID
		numJobsCleaned += res.numProcessed + res.numUnprocessed
		if !jq.queryUnprocessed && !jq.queryProcessed {
			break
		}
	}
	jd.logger.Infof("cleaned up %d old jobs", numJobsCleaned)
}

type cleanupResult struct {
	numProcessed   int
	numUnprocessed int

	unprocessedAfterJobID *int64
	processedAfterJobID   *int64
}

type jobQuery struct {
	queryUnprocessed bool
	queryProcessed   bool

	unprocessedAfterJobID *int64
	processedAfterJobID   *int64
}

func (jd *HandleT) doCleanup(ctx context.Context, jq jobQuery, batchSize int) (cleanupResult, error) {
	var (
		jobsToCleanup = make([]*JobT, 0)
		res           cleanupResult
	)

	if jq.queryUnprocessed {
		unprocessed, err := jd.GetUnprocessed(ctx, GetQueryParamsT{
			IgnoreCustomValFiltersInQuery: true,
			JobsLimit:                     batchSize,
			AfterJobID:                    jq.unprocessedAfterJobID,
		})
		if err != nil {
			return cleanupResult{}, err
		}

		if len(unprocessed.Jobs) > 0 {
			res.unprocessedAfterJobID = &(unprocessed.Jobs[len(unprocessed.Jobs)-1].JobID)
			unprocessedJobsToCleanup := lo.Filter(
				unprocessed.Jobs,
				func(job *JobT, _ int) bool {
					return job.CreatedAt.Before(time.Now().Add(-jd.JobMaxAge))
				})
			jobsToCleanup = append(jobsToCleanup, unprocessedJobsToCleanup...)
			res.numUnprocessed = len(unprocessedJobsToCleanup)
		}
	}

	if jq.queryProcessed {
		processed, err := jd.GetProcessed(ctx, GetQueryParamsT{
			IgnoreCustomValFiltersInQuery: true,
			JobsLimit:                     batchSize,
			AfterJobID:                    jq.processedAfterJobID,
		})
		if err != nil {
			return cleanupResult{}, err
		}

		if len(processed.Jobs) > 0 {
			res.processedAfterJobID = &(processed.Jobs[len(processed.Jobs)-1].JobID)
			processedJobsToCleanup := lo.Filter(processed.Jobs, func(job *JobT, _ int) bool {
				return job.CreatedAt.Before(time.Now().Add(-jd.JobMaxAge))
			})
			jobsToCleanup = append(jobsToCleanup, processedJobsToCleanup...)
			res.numProcessed = len(processedJobsToCleanup)
		}
	}

	if len(jobsToCleanup) > 0 {
		statusList := make([]*JobStatusT, 0)
		for _, job := range jobsToCleanup {
			statusList = append(statusList, &JobStatusT{
				JobID:         job.JobID,
				JobState:      Aborted.State,
				ErrorCode:     "0",
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ErrorResponse: []byte(`{"reason": "job max age exceeded"}`),
			})
		}
		if err := jd.UpdateJobStatus(ctx, statusList, nil, nil); err != nil {
			return cleanupResult{}, err
		}
	}

	return res, nil
}
