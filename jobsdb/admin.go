package jobsdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
		jd.oldJobsCleanupRoutine(ctx)
		return nil
	}))
}

func (jd *HandleT) oldJobsCleanupRoutine(ctx context.Context) {
	jd.doCleanupOldJobs(ctx)
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
	tags := statTags{CustomValFilters: []string{jd.tablePrefix}}
	command := func() interface{} {
		return jd.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
			dsList := jd.getDSList()
			for _, ds := range dsList {
				if err := jd.doCleanupDSInTx(ctx, tx.Tx(), ds); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err, _ := jd.executeDbRequest(
		newWriteDbRequest("clean_up_old_jobs", &tags, command),
	).(error); err != nil {
		jd.logger.Errorf("error cleaning up DS: %w", err)
	}
}

func (jd *HandleT) doCleanupDSInTx(ctx context.Context, tx *Tx, ds dataSetT) error {
	abortStatusQuery := fmt.Sprintf(
		`with abort_jobs as (
			select jobs.job_id, jobs.custom_val, jobs.workspace_id, jobs.created_at from 
			%[1]q as jobs left join "v_last_%[2]s" as status on jobs.job_id = status.job_id
			where jobs.created_at < $1 and (
				status.job_state = ANY('{%[3]s}')
				or
				status.job_state is null
			)
		),
		inserted_status as (
		insert into %[2]q (job_id, job_state, attempt, error_code, error_response) 
		(select job_id, 'aborted', 0, '0', '{"reason": "job max age exceeded"}' from abort_jobs)
		)
		select count(*), workspace_id, custom_val from abort_jobs group by workspace_id, custom_val`,
		ds.JobTable,
		ds.JobStatusTable,
		strings.Join(validNonTerminalStates, ","),
	)
	rows, err := tx.QueryContext(
		ctx,
		abortStatusQuery,
		time.Now().Add(-jd.JobMaxAge),
	)
	if err != nil {
		jd.logger.Info("error aborting jobs after maxAge: %w", err)
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var count int
		var workspaceID, customVal string
		if err := rows.Scan(&count, &workspaceID, &customVal); err != nil {
			jd.logger.Info("error scanning maxAge aborted jobs: %w", err)
			return err
		}
		// may lead to negative count on gateway counters? do we even use them anywhere?
		tx.AddSuccessListener(func() {
			rmetrics.DecreasePendingEvents(
				jd.tablePrefix,
				workspaceID,
				customVal,
				float64(count),
			)
		})
	}
	if err := rows.Err(); err != nil {
		jd.logger.Info("error iterating maxAge aborted jobs: %w", err)
		return err
	}
	return nil
}
