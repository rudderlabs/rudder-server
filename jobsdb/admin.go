package jobsdb

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
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

func (jd *Handle) doCleanup(ctx context.Context) error {
	if err := jd.abortOldJobs(ctx, jd.getDSList()); err != nil {
		return fmt.Errorf("aborting old jobs: %w", err)
	}

	// 2. cleanup journal
	{
		deleteStmt := "DELETE FROM %s_journal WHERE start_time < NOW() - INTERVAL '%d DAY'"
		var journalEntryCount int64
		res, err := jd.dbHandle.ExecContext(
			ctx,
			fmt.Sprintf(
				deleteStmt,
				jd.tablePrefix,
				jd.config.GetInt("JobsDB.archivalTimeInDays", 10),
			),
		)
		if err != nil {
			return fmt.Errorf("cleaning up journal: %w", err)
		}
		journalEntryCount, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("finding journal rows affected during cleanup: %w", err)
		}
		jd.logger.Infon("journal cleanup",
			logger.NewIntField("journalEntriesCleaned", journalEntryCount),
		)
	}

	return nil
}

func (jd *Handle) abortOldJobs(ctx context.Context, dsList []dataSetT) error {
	jobState := "aborted"
	maxAgeStatusResponse := `{"reason": "job max age exceeded"}`
	maxAge := jd.conf.jobMaxAge()
	for _, ds := range dsList {
		res, err := jd.dbHandle.ExecContext(
			ctx,
			fmt.Sprintf(
				`INSERT INTO %[1]q (job_id, job_state, error_response)
				SELECT job_id, $1, $2 FROM %[2]q WHERE created_at <= $3`,
				ds.JobStatusTable,
				ds.JobTable,
			),
			jobState,
			maxAgeStatusResponse,
			time.Now().Add(-maxAge),
		)
		if err != nil {
			return fmt.Errorf("aborting old jobs on ds %v: %w", ds, err)
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("finding rows affected during aborting old jobs on ds %v: %w", ds, err)
		}
		jd.logger.Infon("aborting old jobs",
			logger.NewIntField("rowsAffected", rowsAffected),
			logger.NewDurationField("maxAge", maxAge),
			logger.NewField("dataSet", ds),
		)
		if rowsAffected == 0 {
			break
		}
	}
	return nil
}
