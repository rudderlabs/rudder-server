package jobsdb

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
)

func (jd *HandleT) Status() interface{} {
	statusObj := map[string]interface{}{
		"dataset-list":    jd.getDSList(),
		"dataset-ranges":  jd.getDSRangeList(),
		"backups-enabled": jd.BackupSettings.isBackupEnabled(),
	}
	emptyResults := make(map[string]interface{})
	for ds, entry := range jd.dsEmptyResultCache {
		emptyResults[ds.JobTable] = entry
	}
	statusObj["empty-results-cache"] = emptyResults

	pendingEventMetrics := metric.Instance.
		GetRegistry(metric.PublishedMetrics).
		GetMetricsByName(fmt.Sprintf(rmetrics.JobsdbPendingEventsCount, jd.tablePrefix))

	if len(pendingEventMetrics) == 0 {
		return statusObj
	}

	var pendingEvents []map[string]interface{}
	for _, pendingEvent := range pendingEventMetrics {
		count := pendingEvent.Value.(metric.Gauge).IntValue()
		if count != 0 {
			pendingEvents = append(pendingEvents, map[string]interface{}{
				"tags":  pendingEvent.Tags,
				"count": count,
			})
		}
	}
	statusObj["pending-events"] = pendingEvents

	return statusObj
}

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
