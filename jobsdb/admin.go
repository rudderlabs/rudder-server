package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
)

/*
Ping returns health check for pg database
*/
func (jd *HandleT) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := jd.dbHandle.QueryContext(ctx, `SELECT 'Rudder DB Health Check'::text as message`)
	if err != nil {
		return err
	}
	_ = rows.Close()
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

/*
deleteJobStatus deletes the latest status of a batch of jobs
This is only done during recovery, which happens during the server start.
So, we don't have to worry about dsEmptyResultCache
*/
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
				jd.dropDSFromCache(ds)
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

/*
failExecuting sets the state of the executing jobs to failed
This is only done during recovery, which happens during the server start.
So, we don't have to worry about dsEmptyResultCache
*/
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
				jd.dropDSFromCache(ds)
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

func (jd *HandleT) CleanUpRetiredJobs(ctx context.Context, retiredWorkspaces []string) error {
	totalAbortedCountMap := map[string]map[string]int64{}
	return jd.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
		for _, ds := range jd.getDSList() {
			rows, err := tx.Tx().QueryContext(
				ctx,
				fmt.Sprintf(
					`with retired_jobs as (
						select job_id, workspace_id, custom_val from %[1]q where workspace_id = ANY($1)
					),
					abortedJobs as (
						insert into %[2]q (job_id, job_state, error_response)
						(select job_id, 'aborted', '{"reason" : "Job aborted due to workspace retirement"}' from retired_jobs)
						returning job_id
					)
					select count(*), workspace_id, custom_val from retired_jobs group by workspace_id, custom_val;`, ds.JobTable, ds.JobStatusTable),
				pq.Array(retiredWorkspaces),
			)
			if err != nil {
				return err
			}
			for rows.Next() {
				var (
					dsCount   sql.NullInt64
					workspace string
					customVal string
				)
				err := rows.Scan(&dsCount, &workspace, &customVal)
				if err != nil {
					return err
				}
				if _, ok := totalAbortedCountMap[workspace]; !ok {
					totalAbortedCountMap[workspace] = make(map[string]int64)
				}
				if !dsCount.Valid {
					continue
				}
				totalAbortedCountMap[workspace][customVal] += dsCount.Int64
			}
			_ = rows.Close()

			for workspace := range totalAbortedCountMap {
				for customVal, count := range totalAbortedCountMap[workspace] {
					tx.Tx().AddSuccessListener(func() {
						jd.logger.Infof(
							"Aborted %d %s-%s jobs due to workspace retirement",
							count, workspace, customVal,
						)
						rmetrics.DecreasePendingEvents(
							jd.tablePrefix,
							workspace,
							customVal,
							float64(count),
						)
					})
				}
			}
		}
		return nil
	})
}
