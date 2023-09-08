package jobsdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

/*
Ping returns health check for pg database
*/
func (jd *Handle) Ping() error {
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
func (jd *Handle) DeleteExecuting() {
	tags := statTags{CustomValFilters: []string{jd.tablePrefix}}
	command := func() any {
		jd.deleteJobStatus()
		return nil
	}
	_ = executeDbRequest(jd, newWriteDbRequest("delete_job_status", &tags, command))
}

// deleteJobStatus deletes the latest status of a batch of jobs
func (jd *Handle) deleteJobStatus() {
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

func (jd *Handle) deleteJobStatusDSInTx(txHandler transactionHandler, ds dataSetT) error {
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
func (jd *Handle) FailExecuting() {
	tags := statTags{
		CustomValFilters: []string{jd.tablePrefix},
	}
	command := func() any {
		jd.failExecuting()
		return nil
	}
	_ = executeDbRequest(jd, newWriteDbRequest("fail_executing", &tags, command))
}

// failExecuting sets the state of the executing jobs to failed
func (jd *Handle) failExecuting() {
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

func (jd *Handle) failExecutingDSInTx(txHandler transactionHandler, ds dataSetT) error {
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

func (jd *Handle) startCleanupLoop(ctx context.Context) {
	jd.backgroundGroup.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-jd.TriggerJobCleanUp():
				func() {
					for {
						if err := jd.doCleanup(ctx, jd.config.GetInt("jobsdb.cleanupBatchSize", 100)); err != nil && ctx.Err() == nil {
							jd.logger.Errorf("error while cleaning up old jobs: %w", err)
							if err := misc.SleepCtx(ctx, jd.config.GetDuration("jobsdb.cleanupRetryInterval", 10, time.Second)); err != nil {
								return
							}
							continue
						}
						return
					}
				}()
			}
		}
	}))
}

func (jd *Handle) doCleanup(ctx context.Context, batchSize int) error {
	// 1. cleanup old jobs
	gather := func(f func(ctx context.Context, states []string, params GetQueryParams) (JobsResult, error), states []string, params GetQueryParams) ([]*JobStatusT, error) {
		res := make([]*JobStatusT, 0)
		var done bool
		var afterJobID *int64
		for !done {
			params.IgnoreCustomValFiltersInQuery = true
			params.JobsLimit = batchSize
			params.afterJobID = afterJobID
			jobsResult, err := f(ctx, states, params)
			if err != nil {
				return nil, err
			}
			jobs := lo.Filter(
				jobsResult.Jobs,
				func(job *JobT, _ int) bool {
					return job.CreatedAt.Before(time.Now().Add(-jd.conf.jobMaxAge()))
				},
			)
			if len(jobs) > 0 {
				afterJobID = &(jobs[len(jobs)-1].JobID)
				res = append(res, lo.Map(jobs, func(job *JobT, _ int) *JobStatusT {
					return &JobStatusT{
						JobID:         job.JobID,
						JobState:      Aborted.State,
						ErrorCode:     "0",
						AttemptNum:    job.LastJobStatus.AttemptNum,
						ErrorResponse: []byte(`{"reason": "job max age exceeded"}`),
					}
				})...)
			}
			if len(jobs) < batchSize {
				done = true
			}
		}
		return res, nil
	}

	statusList, err := gather(jd.GetJobs, []string{Failed.State, Executing.State, Waiting.State, Unprocessed.State}, GetQueryParams{})
	if err != nil {
		return fmt.Errorf("gathering job statuses: %w", err)
	}

	if len(statusList) > 0 {
		if err := jd.UpdateJobStatus(ctx, statusList, nil, nil); err != nil {
			return err
		}
		jd.logger.Infof("cleaned up %d old jobs", len(statusList))
	}

	// 2. cleanup journal
	{
		deleteStmt := "DELETE FROM %s_journal WHERE start_time < NOW() - INTERVAL '%d DAY' returning id"
		var journalEntryCount int64
		if err := jd.dbHandle.QueryRowContext(
			ctx,
			fmt.Sprintf(
				deleteStmt,
				jd.tablePrefix,
				jd.config.GetInt("JobsDB.archivalTimeInDays", 10),
			),
		).Scan(&journalEntryCount); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("cleaning up journal: %w", err)
		}
		jd.logger.Infof("cleaned up %d journal entries", journalEntryCount)
	}

	return nil
}
