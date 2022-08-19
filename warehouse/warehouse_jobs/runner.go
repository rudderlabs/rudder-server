package warehouse_jobs

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

//Global variable that initializes the AsyncJobWhT with necessary values
var AsyncJobWH AsyncJobWhT

func InitWarehouseJobsAPI(dbHandle *sql.DB, notifier *pgnotifier.PgNotifierT, log logger.LoggerI, connectionsMap *map[string]map[string]warehouseutils.WarehouseT) {
	AsyncJobWH = AsyncJobWhT{
		dbHandle:       dbHandle,
		log:            log,
		enabled:        true,
		pgnotifier:     notifier,
		connectionsMap: connectionsMap,
	}
	// warehousejob_deletebyjobruntaskrunid.AsyncWh = &AsyncJobWH
}

const AsyncTableName string = "wh_async_jobs"

//Gets Table names from the jobrunid. Should not belong here but need to create separate package for deletebyjobs
func (asyncWhJob *AsyncJobWhT) getTableNamesByJobRunIDTaskRunID(jobrunid string, taskrunid string) ([]string, error) {
	asyncWhJob.log.Infof("Extracting tablenames for the job run id %s", jobrunid)
	var tableNames []string
	query := fmt.Sprintf(`select id from %s where metadata->>'%s'='%s' and metadata->>'%s'='%s'`, warehouseutils.WarehouseUploadsTable, "source_job_run_id", jobrunid, "source_task_run_id", taskrunid)
	asyncWhJob.log.Infof("Query is %s\n", query)
	rows, err := asyncWhJob.dbHandle.Query(query)
	if err != nil {
		asyncWhJob.log.Errorf("Error carrying out the query %s ", query)
		return nil, err
	}
	for rows.Next() {
		var upload_id string
		rows.Scan(&upload_id)
		query = fmt.Sprintf(`select table_name from %s where wh_upload_id=%s`, warehouseutils.WarehouseTableUploadsTable, upload_id)
		tables, err := asyncWhJob.dbHandle.Query(query)
		if err != nil {
			asyncWhJob.log.Errorf("Error carrying out the query %s ", query)
			return nil, err
		}
		for tables.Next() {
			var tablename string
			tables.Scan(&tablename)
			tableNames = append(tableNames, tablename)
		}
	}
	asyncWhJob.log.Infof("%s\n", tableNames)
	return tableNames, nil
}

//Add data to the tables
func (asyncWhJob *AsyncJobWhT) addJobstoDB(payload *AsyncJobPayloadT) {
	asyncWhJob.log.Infof("Adding job to the wh_asnc_jobs jobrunid:%s, taskrunid: %s and tablename: %s", payload.JobRunID, payload.TaskRunID, payload.TableName)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (sourceid, namespace, destinationid, destination_type, jobrunid, taskrunid, tablename,  status, created_at, updated_at,jobtype,async_job_type)
	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10,$11,$12) RETURNING id`, warehouseutils.WarehouseAsyncJobTable)

	stmt, err := asyncWhJob.dbHandle.Prepare(sqlStatement)
	if err != nil {
		asyncWhJob.log.Errorf("Error preparing out the query %s ", sqlStatement)
		panic(err)
	}

	defer stmt.Close()
	now := timeutil.Now()
	row := stmt.QueryRow(payload.SourceID, payload.Namespace, payload.DestinationID, payload.DestType, payload.JobRunID, payload.TaskRunID, payload.TableName, asyncJobWaiting, now, now, payload.JobType, payload.AsyncJobType)
	var asynJobID int64
	err = row.Scan(&asynJobID)
	fmt.Println(asynJobID)
	if err != nil {
		asyncWhJob.log.Errorf("Error processing the %s ", sqlStatement)
		panic(err)
	}
}

/*
Async Job runner's main job is to
1) Scan the database for entries into wh_async_jobs
2) Publish data to pg_notifier queue
*/
func (asyncWhJob *AsyncJobWhT) InitAsyncJobRunner() {
	//Start the asyncJobRunner
	asyncWhJob.log.Info("WH-Jobs: Initializing async job runner")
	ctx, cancel := context.WithCancel(context.Background())
	asyncWhJob.Cancel = cancel
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		asyncWhJob.startAsyncJobRunner(ctx)
		return nil
	})
	g.Wait()
}

/*
startAsyncJobRunner is the main runner that
1) Periodically queries the db for any pending async jobs
2) Groups them together
3) Publishes them to the pgnotifier
4) Spawns a subroutine that periodically checks for responses from pgNotifier/slave worker post trackBatch
*/
func (asyncWhJob *AsyncJobWhT) startAsyncJobRunner(ctx context.Context) {
	asyncWhJob.log.Info("WH-Jobs: Starting async job runner")
	for {
		asyncWhJob.log.Info("WH-Jobs: Scanning for waiting async job")
		select {
		case <-ctx.Done():
			asyncWhJob.log.Info("WH-Jobs: Stopping AsyncJobRunner")
			return
		case <-time.After(10 * time.Second):

		}

		asyncjobpayloads, err := asyncWhJob.getPendingAsyncJobs(ctx)
		if err != nil {
			return
		}

		if len(asyncjobpayloads) > 0 {
			asyncWhJob.log.Info("WH-Jobs: Got pending wh async jobs")
			asyncWhJob.log.Infof("WH-Jobs: Number of async wh jobs left = %d\n", len(asyncjobpayloads))
			notifierClaims, err := getMessagePayloadsFromAsyncJobPayloads(asyncjobpayloads)

			if err != nil {
				asyncWhJob.log.Errorf("Error converting the asyncJobType to notifier payload %s ", err)
				panic(err)
			}
			messagePayload := pgnotifier.MessagePayload{
				Jobs:    notifierClaims,
				JobType: "async_job",
			}
			schema := warehouseutils.SchemaT{}
			ch, err := asyncWhJob.pgnotifier.Publish(messagePayload, &schema, 100)
			if err != nil {
				panic(err)
			}
			asyncWhJob.updateMultipleAsyncJobs(&asyncjobpayloads, warehouseutils.StagingFileExecutingState, "")
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				responses := <-ch
				asyncWhJob.log.Info("Response recieved from the pgnotifier track batch")
				var successfulAsyncJobs []AsyncJobPayloadT
				for _, resp := range responses {
					var jobID pgnotifier.AsyncOutput
					err = json.Unmarshal(resp.Output, &jobID)
					if err != nil {
						asyncWhJob.log.Errorf("Unable unmarshal output %s", err)
						panic(err)
					}
					ind := getSinglePayloadFromBatchPayloadByTableName(&asyncjobpayloads, jobID.TableName)
					if ind == -1 {
						panic(fmt.Errorf("tableName is not found while updating the response after completing the trackasyncJob %s", jobID.TableName))
					}
					if resp.Status == "aborted" {
						pkgLogger.Errorf("[WH]: Error in running clean up task %v", resp.Error)
						sampleError := fmt.Errorf(resp.Error)
						asyncWhJob.updateSingleAsyncJob(&asyncjobpayloads[ind], warehouseutils.StagingFileAbortedState, sampleError.Error())
						continue
					}
					successfulAsyncJobs = append(successfulAsyncJobs, asyncjobpayloads[ind])

				}
				asyncWhJob.updateMultipleAsyncJobs(&successfulAsyncJobs, warehouseutils.StagingFileSucceededState, "")
				wg.Done()
			}()
			wg.Wait()
		}
	}
}

//Queries the jobsDB and gets active async job and returns it in a
func (asyncWhJob *AsyncJobWhT) getPendingAsyncJobs(ctx context.Context) ([]AsyncJobPayloadT, error) {
	var asyncjobpayloads = make([]AsyncJobPayloadT, 0)
	query := fmt.Sprintf(
		`select 
	id,
	sourceid,
	destinationid,
	status,
	jobrunid,
	COALESCE(starttime, ''),
	created_at,
	updated_at,
	destination_type,
	taskrunid,
	tablename,
	jobtype,
	async_job_type,
	namespace from %s where status='%s'`, warehouseutils.WarehouseAsyncJobTable, "waiting")
	rows, err := asyncWhJob.dbHandle.Query(query)
	if err != nil {
		return asyncjobpayloads, err
	}
	defer rows.Close()
	for rows.Next() {
		var asyncjobpayload AsyncJobPayloadT
		var status string
		var updated_at, created_at string
		err = rows.Scan(
			&asyncjobpayload.Id,
			&asyncjobpayload.SourceID,
			&asyncjobpayload.DestinationID,
			&status,
			&asyncjobpayload.JobRunID,
			&asyncjobpayload.StartTime,
			&created_at,
			&updated_at,
			&asyncjobpayload.DestType,
			&asyncjobpayload.TaskRunID,
			&asyncjobpayload.TableName,
			&asyncjobpayload.JobType,
			&asyncjobpayload.AsyncJobType,
			&asyncjobpayload.Namespace,
		)
		if err != nil {
			asyncWhJob.log.Errorf("WH-Jobs: Error scanning rows %s\n", err)
			return asyncjobpayloads, err
		}
		asyncjobpayloads = append(asyncjobpayloads, asyncjobpayload)
	}
	return asyncjobpayloads, nil
}

//Updates the warehouse async jobs with the status sent as a parameter
func (asyncWhJob *AsyncJobWhT) updateMultipleAsyncJobs(payloads *[]AsyncJobPayloadT, status string, errMessage string) {
	asyncWhJob.log.Info("WH-Jobs: Updating pending wh async jobs to Executing")
	for _, payload := range *payloads {
		sqlStatement := fmt.Sprintf(`UPDATE %s SET status='%s' , error='%s' WHERE id=%s`, warehouseutils.WarehouseAsyncJobTable, status, errMessage, payload.Id)
		asyncWhJob.log.Infof("WH-Jobs: updating async jobs table query %s", sqlStatement)
		_, err := asyncWhJob.dbHandle.Query(sqlStatement)
		if err != nil {
			panic(fmt.Errorf("query: %s failed with Error : %w", sqlStatement, err))
		}
	}
}

func (asyncWhJob *AsyncJobWhT) updateSingleAsyncJob(payload *AsyncJobPayloadT, status string, errMessage string) {
	asyncWhJob.log.Info("WH-Jobs: Updating pending wh async jobs to Aborted")
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status='%s' , error='%s' WHERE id=%s`, warehouseutils.WarehouseAsyncJobTable, status, errMessage, payload.Id)
	asyncWhJob.log.Infof("WH-Jobs: updating async jobs table query %s", sqlStatement)
	_, err := asyncWhJob.dbHandle.Query(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("query: %s failed with Error : %w", sqlStatement, err))
	}
}

func (asyncWhJob *AsyncJobWhT) getStatusAsyncJob(payload *StartJobReqPayload) (status string, errMessage error) {
	asyncWhJob.log.Info("WH-Jobs: Getting status for wh async jobs %v", payload)
	sqlStatement := fmt.Sprintf(`SELECT status,error FROM %s WHERE jobrunid='%s' AND taskrunid='%s'`, warehouseutils.WarehouseAsyncJobTable, payload.JobRunID, payload.TaskRunID)
	asyncWhJob.log.Infof("Query is %s", sqlStatement)
	rows, err := asyncWhJob.dbHandle.Query(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("query: %s failed with Error : %w", sqlStatement, err))
	}

	for rows.Next() {
		var status string
		var errMessage sql.NullString
		err = rows.Scan(&status, &errMessage)
		if err != nil {
			asyncWhJob.log.Errorf("WH-Jobs: Error scanning rows %s\n", err)
			return "error", err
		}
		if status == warehouseutils.StagingFileAbortedState {
			asyncWhJob.log.Infof("[WH-Jobs] Async Job with jobrunid: %s, taskrunid: %s is aborted", payload.JobRunID, payload.TaskRunID)
			return "aborted", errors.New(errMessage.String)
		}
		if status != warehouseutils.StagingFileSucceededState {
			asyncWhJob.log.Infof("[WH-Jobs] Async Job with jobrunid: %s, taskrunid: %s is not complete", payload.JobRunID, payload.TaskRunID)
			return "processing", err
		}

	}
	asyncWhJob.log.Infof("[WH-Jobs] Async Job with jobrunid: %s, taskrunid: %s is complete", payload.JobRunID, payload.TaskRunID)
	return warehouseutils.StagingFileSucceededState, err
}

//convert to pgNotifier Payload and return the array of payloads
func getMessagePayloadsFromAsyncJobPayloads(asyncjobs []AsyncJobPayloadT) ([]pgnotifier.JobPayload, error) {
	var messages []pgnotifier.JobPayload
	for _, job := range asyncjobs {
		message, err := json.Marshal(job)
		if err != nil {
			return messages, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func getSinglePayloadFromBatchPayloadByTableName(asyncJobs *[]AsyncJobPayloadT, tableName string) int {
	for ind, asyncjob := range *asyncJobs {
		if asyncjob.TableName == tableName {
			return ind
		}
	}
	return -1
}
