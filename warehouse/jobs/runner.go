package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"golang.org/x/sync/errgroup"
)

//Initializes AsyncJobWh structure with appropriate
func InitWarehouseJobsAPI(dbHandle *sql.DB, notifier *pgnotifier.PgNotifierT, ctx context.Context) *AsyncJobWhT {

	AsyncJobWh := AsyncJobWhT{
		dbHandle:   dbHandle,
		enabled:    false,
		pgnotifier: notifier,
		context:    ctx,
	}
	pkgLogger = logger.NewLogger().Child("warehouse-asyncjob")
	return &AsyncJobWh
}

/*
Gets Table names from the jobrunid.
Should not belong here but need to create separate package for deletebyjobs or
refactor to a more generic getTableName with jobrundid and taskrunid as params
*/
func (asyncWhJob *AsyncJobWhT) getTableNamesBy(sourceid string, destinationid string, jobrunid string, taskrunid string) ([]string, error) {
	pkgLogger.Infof("Extracting tablenames for the job run id %s", jobrunid)
	var tableNames []string
	var err error
	query := fmt.Sprintf(`SELECT id from %s where metadata->>'%s'=$1 and metadata->>'%s'=$2 and metadata in (SELECT metadata FROM wh_uploads where source_id=$3 and destination_id=$4)`, warehouseutils.WarehouseUploadsTable, "source_job_run_id", "source_task_run_id")
	pkgLogger.Debugf("Query is %s\n", query)
	rows, err := asyncWhJob.dbHandle.Query(query, jobrunid, taskrunid, sourceid, destinationid)
	if err != nil {
		pkgLogger.Errorf("Error carrying out the query %s ", query)
		return nil, err
	}
	for rows.Next() {
		var uploadId string
		err := rows.Scan(&uploadId)
		if err != nil {
			pkgLogger.Errorf("Error carrying the scan operation to uploadId\n")
			return nil, err
		}
		query = fmt.Sprintf(`select table_name from %s where wh_upload_id=$1`, warehouseutils.WarehouseTableUploadsTable)
		tables, err := asyncWhJob.dbHandle.Query(query, uploadId)
		if err != nil {
			pkgLogger.Errorf("Error carrying out the query %s ", query)
			return nil, err
		}
		for tables.Next() {
			var tableName string
			err = tables.Scan(&tableName)
			if err != nil {
				pkgLogger.Errorf("Error carrying the scan operation to tablename\n")
				return nil, err
			}
			tableNames = append(tableNames, tableName)
		}
	}
	pkgLogger.Infof("Got the TableNames as %s\n", tableNames)
	return tableNames, nil
}

//Takes AsyncJobPayloadT and adds rows to table wh_async_jobs
func (asyncWhJob *AsyncJobWhT) addJobstoDB(ctx context.Context, payload *AsyncJobPayloadT) (jobId int64, err error) {
	pkgLogger.Infof("Adding job to the wh_asnc_jobs jobrunid:%s, taskrunid: %s and tablename: %s", payload.JobRunID, payload.TaskRunID, payload.TableName)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (sourceid, namespace, destinationid, jobrunid, taskrunid, tablename,  status, created_at, updated_at,jobtype,async_job_type,starttime)
	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10,$11,$12) RETURNING id`, warehouseutils.WarehouseAsyncJobTable)

	stmt, err := asyncWhJob.dbHandle.Prepare(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("Error preparing out the query %s ", sqlStatement)
		err = errors.New("error preparing out the query, while addJobstoDb")
		return
	}
	startTime, err := time.Parse("01-02-2006 15:04:05", payload.StartTime)
	if err != nil {
		err = errors.New("error parsing time, while addJobstoDb")
		return
	}

	defer stmt.Close()
	now := timeutil.Now()
	row := stmt.QueryRow(payload.SourceID, payload.Namespace, payload.DestinationID, payload.JobRunID, payload.TaskRunID, payload.TableName, WhJobWaiting, now, now, payload.JobType, payload.AsyncJobType, startTime)
	err = row.Scan(&jobId)
	if err != nil {
		pkgLogger.Errorf("Error processing the %s, %s ", sqlStatement, err.Error())
		err = errors.New("error processing the query, while addJobstoDb")
		return
	}
	return
}

/*
Async Job runner's main job is to
1) Scan the database for entries into wh_async_jobs
2) Publish data to pg_notifier queue
3) Move any executing jobs to waiting
*/
func (asyncWhJob *AsyncJobWhT) InitAsyncJobRunner() error {
	//Start the asyncJobRunner
	pkgLogger.Info("WH-Jobs: Initializing async job runner")
	ctx, cancel := context.WithCancel(asyncWhJob.context)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	asyncWhJob.context = ctx
	var err error
	for retry := 0; retry < MaxCleanUpRetries; retry++ {
		err = asyncWhJob.cleanUpAsyncTable(ctx)
		if err == nil {
			asyncWhJob.enabled = true
			break
		}
		pkgLogger.Infof("WH-Jobs: Cleanup asynctable failed with error %s", err.Error())
	}

	if err != nil {
		return err
	}
	if asyncWhJob.enabled {
		//How does threading policy work here? Should we have one thread per customer/namespace?
		g.Go(func() error {
			return asyncWhJob.startAsyncJobRunner(ctx)
		})
		g.Wait()
	}
	return errors.New("unable to enable warehouse Async Job")

}

func (asyncWhJob *AsyncJobWhT) cleanUpAsyncTable(ctx context.Context) error {
	pkgLogger.Info("WH-Jobs: Cleaning up the zombie asyncjobs")
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1 WHERE status=$2`, warehouseutils.WarehouseAsyncJobTable)
	pkgLogger.Infof("WH-Jobs: resetting up async jobs table query %s", sqlStatement)
	_, err := asyncWhJob.dbHandle.Query(sqlStatement, WhJobWaiting, WhJobExecuting)
	return err
}

/*
startAsyncJobRunner is the main runner that
1) Periodically queries the db for any pending async jobs
2) Groups them together
3) Publishes them to the pgnotifier
4) Spawns a subroutine that periodically checks for responses from pgNotifier/slave worker post trackBatch
*/
func (asyncWhJob *AsyncJobWhT) startAsyncJobRunner(ctx context.Context) error {
	pkgLogger.Info("WH-Jobs: Starting async job runner")
	for {
		pkgLogger.Info("WH-Jobs: Scanning for waiting async job")
		select {
		case <-ctx.Done():
			pkgLogger.Info("WH-Jobs: Stopping AsyncJobRunner")
			return nil
		case <-time.After(RetryTimeInterval):

		}

		asyncjobpayloads, err := asyncWhJob.getPendingAsyncJobs(ctx)
		if err != nil {
			return err
		}

		if len(asyncjobpayloads) > 0 {
			pkgLogger.Info("WH-Jobs: Got pending wh async jobs")
			pkgLogger.Infof("WH-Jobs: Number of async wh jobs left = %d\n", len(asyncjobpayloads))
			notifierClaims, err := getMessagePayloadsFromAsyncJobPayloads(asyncjobpayloads)

			if err != nil {
				pkgLogger.Errorf("Error converting the asyncJobType to notifier payload %s ", err)
				asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(asyncjobpayloads, WhJobFailed, err)
				asyncWhJob.updateAsyncJobs(ctx, asyncJobStatusMap)
				return err
			}
			messagePayload := pgnotifier.MessagePayload{
				Jobs:    notifierClaims,
				JobType: "async_job",
			}
			schema := warehouseutils.SchemaT{}
			ch, err := asyncWhJob.pgnotifier.Publish(messagePayload, &schema, 100)
			if err != nil {
				asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(asyncjobpayloads, WhJobFailed, err)
				asyncWhJob.updateAsyncJobs(ctx, asyncJobStatusMap)
				return err
			}
			asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(asyncjobpayloads, WhJobExecuting, err)
			asyncWhJob.updateAsyncJobs(ctx, asyncJobStatusMap)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				responses := <-ch
				pkgLogger.Info("Response received from the pgnotifier track batch")
				var asyncJobsStatusMap []AsyncJobsStatusMap
				for _, resp := range responses {
					var jobID pgnotifier.AsyncOutput
					var jobstatusmap AsyncJobsStatusMap
					err = json.Unmarshal(resp.Output, &jobID)
					if err != nil {
						pkgLogger.Errorf("Unable unmarshal output %s", err)
						// Todo: Need to program retry strategy, timeout strategy for each row in the asyncJobtable
						continue
					}
					tableNameIndex := getSinglePayloadFromBatchPayloadByTableName(asyncjobpayloads, jobID.TableName)
					if tableNameIndex == -1 {
						pkgLogger.Errorf("tableName is not found while updating the response after completing the trackasyncJob %s", jobID.TableName)
						continue
					}
					if resp.Status == pgnotifier.AbortedState {
						pkgLogger.Errorf("[WH]: Error in running async task %v", resp.Error)
						jobError := fmt.Errorf(resp.Error)
						jobstatusmap = AsyncJobsStatusMap{
							Id:     asyncjobpayloads[tableNameIndex].Id,
							Status: WhJobAborted,
							Error:  jobError,
						}
						asyncJobsStatusMap = append(asyncJobsStatusMap, jobstatusmap)
						continue
					}
					if resp.Status == pgnotifier.FailedState {
						pkgLogger.Errorf("[WH]: Error in running async task %v", resp.Error)
						jobError := fmt.Errorf(resp.Error)
						jobstatusmap = AsyncJobsStatusMap{
							Id:     asyncjobpayloads[tableNameIndex].Id,
							Status: WhJobFailed,
							Error:  jobError,
						}
						asyncJobsStatusMap = append(asyncJobsStatusMap, jobstatusmap)
						continue
					}
					if resp.Status == pgnotifier.SucceededState {
						jobstatusmap = AsyncJobsStatusMap{
							Id:     asyncjobpayloads[tableNameIndex].Id,
							Status: WhJobSucceeded,
							Error:  nil,
						}
						asyncJobsStatusMap = append(asyncJobsStatusMap, jobstatusmap)
						continue
					}
				}
				asyncWhJob.updateAsyncJobs(ctx, asyncJobsStatusMap)
				wg.Done()
			}()
			wg.Wait()
		}
	}
}

//Queries the jobsDB and gets active async job and returns it in a
func (asyncWhJob *AsyncJobWhT) getPendingAsyncJobs(ctx context.Context) ([]AsyncJobPayloadT, error) {
	var asyncjobpayloads = make([]AsyncJobPayloadT, 0)
	pkgLogger.Info("WH-Jobs: Get pending wh async jobs")
	//Filter to get most recent row for the sourceId/destinationID combo and remaining ones should relegated to aborted.
	query := fmt.Sprintf(
		`select 
	id,
	sourceid,
	destinationid,
	jobrunid,
	starttime,
	taskrunid,
	tablename,
	jobtype,
	async_job_type,
	namespace from %s where status=$1 LIMIT $2`, warehouseutils.WarehouseAsyncJobTable)
	rows, err := asyncWhJob.dbHandle.Query(query, WhJobWaiting, MaxBatchSizeToProcess)
	if err != nil {
		pkgLogger.Errorf("WH-Jobs: Error in getting pending wh async jobs with error %s", err.Error())
		return asyncjobpayloads, err
	}
	defer rows.Close()
	for rows.Next() {
		var asyncjobpayload AsyncJobPayloadT
		err = rows.Scan(
			&asyncjobpayload.Id,
			&asyncjobpayload.SourceID,
			&asyncjobpayload.DestinationID,
			&asyncjobpayload.JobRunID,
			&asyncjobpayload.StartTime,
			&asyncjobpayload.TaskRunID,
			&asyncjobpayload.TableName,
			&asyncjobpayload.JobType,
			&asyncjobpayload.AsyncJobType,
			&asyncjobpayload.Namespace,
		)
		if err != nil {
			pkgLogger.Errorf("WH-Jobs: Error scanning rows %s\n", err)
			return asyncjobpayloads, err
		}
		asyncjobpayloads = append(asyncjobpayloads, asyncjobpayload)
	}
	return asyncjobpayloads, nil
}

//Updates the warehouse async jobs with the status sent as a parameter
func (asyncWhJob *AsyncJobWhT) updateAsyncJobs(ctx context.Context, payloads []AsyncJobsStatusMap) error {
	pkgLogger.Info("WH-Jobs: Updating pending wh async jobs to Executing")
	var err error
	for _, payload := range payloads {
		if payload.Error != nil {
			err = asyncWhJob.updateAsyncJob(ctx, payload.Id, payload.Status, payload.Error.Error())
			continue
		}
		err = asyncWhJob.updateAsyncJob(ctx, payload.Id, payload.Status, "")

	}
	return err
}

//The above function and the following function can be merged into one. This can be part of future work
func (asyncWhJob *AsyncJobWhT) updateAsyncJob(ctx context.Context, Id string, status string, errMessage string) error {
	pkgLogger.Infof("WH-Jobs: Updating pending wh async jobs to %s", status)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1 , error=$2 WHERE id=$3 AND status!=$4 `, warehouseutils.WarehouseAsyncJobTable)
	var err error
	for queryretry := 0; queryretry < MaxQueryRetries; queryretry++ {
		pkgLogger.Infof("WH-Jobs: updating async jobs table query %s, retry no : %d", sqlStatement, queryretry)
		_, err = asyncWhJob.dbHandle.Query(sqlStatement, status, errMessage, Id, WhJobAborted)
		if err == nil {
			pkgLogger.Info("Updation successful")
			pkgLogger.Debugf("query: %s successfully executed", sqlStatement)
			return err
		}
	}
	pkgLogger.Errorf("query: %s failed with Error : %s", sqlStatement, err.Error())
	return err
}

//returns status and errMessage
//Only succeeded, executing & waiting states should have empty errMessage
//Rest of the states failed, aborted should send an error message conveying a message
func (asyncWhJob *AsyncJobWhT) getStatusAsyncJob(ctx context.Context, payload *StartJobReqPayload) (statusResponse WhStatusResponse) {
	pkgLogger.Info("WH-Jobs: Getting status for wh async jobs %v", payload)
	//Need to check for count first and see if there are any rows matching the jobrunid and taskrunid. If none, then raise an error instead of showing complete
	sqlStatement := fmt.Sprintf(`SELECT status,error FROM %s WHERE jobrunid=$1 AND taskrunid=$2`, warehouseutils.WarehouseAsyncJobTable)
	pkgLogger.Debugf("Query inside getStatusAsync function is %s", sqlStatement)
	rows, err := asyncWhJob.dbHandle.Query(sqlStatement, payload.JobRunID, payload.TaskRunID)
	if err != nil {
		pkgLogger.Errorf("WH-Jobs: Error executing the query %s", err.Error())
		statusResponse.Status = WhJobFailed
		statusResponse.Err = err.Error()
		return
	}

	for rows.Next() {
		var status string
		var errMessage sql.NullString
		err = rows.Scan(&status, &errMessage)
		if err != nil {
			pkgLogger.Errorf("WH-Jobs: Error scanning rows %s\n", err)
			statusResponse = WhStatusResponse{
				Status: WhJobFailed,
				Err:    err.Error(),
			}
			return
		}
		if status == WhJobFailed {
			pkgLogger.Infof("[WH-Jobs] Async Job with jobrunid: %s, taskrunid: %s is failed", payload.JobRunID, payload.TaskRunID)
			statusResponse.Status = WhJobFailed
			if !errMessage.Valid {
				statusResponse.Err = "Failed while scanning"
				return
			}
			statusResponse.Err = errMessage.String
			return
		}
		if status == WhJobAborted {
			pkgLogger.Infof("[WH-Jobs] Async Job with jobrunid: %s, taskrunid: %s is aborted", payload.JobRunID, payload.TaskRunID)
			statusResponse.Status = WhJobAborted
			if !errMessage.Valid {
				statusResponse.Err = "Failed while scanning"
				return

			}
			statusResponse.Err = errMessage.String
			return
		}
		if status != WhJobSucceeded {
			pkgLogger.Infof("[WH-Jobs] Async Job with jobrunid: %s, taskrunid: %s is under processing", payload.JobRunID, payload.TaskRunID)
			statusResponse.Status = WhJobExecuting
			return
		}

	}
	pkgLogger.Infof("[WH-Jobs] Async Job with jobrunid: %s, taskrunid: %s is complete", payload.JobRunID, payload.TaskRunID)
	statusResponse.Status = WhJobSucceeded
	return
}
