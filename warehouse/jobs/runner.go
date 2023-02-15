package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"golang.org/x/sync/errgroup"
)

// InitWarehouseJobsAPI Initializes AsyncJobWh structure with appropriate variabless
func InitWarehouseJobsAPI(
	ctx context.Context,
	dbHandle *sql.DB,
	notifier *pgnotifier.PgNotifierT,
) *AsyncJobWhT {
	return &AsyncJobWhT{
		dbHandle:   dbHandle,
		enabled:    false,
		pgnotifier: notifier,
		context:    ctx,
		logger:     logger.NewLogger().Child("asyncjob"),
	}
}

func WithConfig(a *AsyncJobWhT, config *config.Config) {
	a.MaxBatchSizeToProcess = config.GetInt("Warehouse.jobs.maxBatchSizeToProcess", 10)
	a.MaxCleanUpRetries = config.GetInt("Warehouse.jobs.maxCleanUpRetries", 5)
	a.MaxQueryRetries = config.GetInt("Warehouse.jobs.maxQueryRetries", 3)
	a.MaxAttemptsPerJob = config.GetInt("Warehouse.jobs.maxAttemptsPerJob", 3)
	a.RetryTimeInterval = config.GetDuration("Warehouse.jobs.retryTimeInterval", 10, time.Second)
	a.AsyncJobTimeOut = config.GetDuration("Warehouse.jobs.asyncJobTimeOut", 300, time.Second)
}

func (a *AsyncJobWhT) getTableNamesBy(sourceID, destinationID, jobRunID, taskRunID string) ([]string, error) {
	a.logger.Infof("[WH-Jobs]: Extracting table names for the job run id %s", jobRunID)
	var tableNames []string
	var err error
	query := `SELECT id 
		FROM 
		` + warehouseutils.WarehouseUploadsTable + `
		WHERE metadata->>'source_job_run_id'=$1 
			AND metadata->>'source_task_run_id'=$2 
			AND metadata in (
				SELECT 
					metadata 
				FROM 
				` + warehouseutils.WarehouseUploadsTable + ` 
				WHERE source_id=$3 
				AND destination_id=$4
			)`
	a.logger.Debugf("[WH-Jobs]: Query is %s\n", query)
	rows, err := a.dbHandle.Query(query, jobRunID, taskRunID, sourceID, destinationID)
	if err != nil {
		a.logger.Errorf("[WH-Jobs]: Error carrying out the query %s ", query)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var uploadId string
		err := rows.Scan(&uploadId)
		if err != nil {
			a.logger.Errorf("[WH-Jobs]: Error carrying the scan operation to uploadId\n")
			return nil, err
		}
		query = fmt.Sprintf(`select table_name from %s where wh_upload_id=$1`, warehouseutils.WarehouseTableUploadsTable)
		tables, err := a.dbHandle.Query(query, uploadId)
		if err != nil {
			a.logger.Errorf("[WH-Jobs]: Error carrying out the query %s ", query)
			return nil, err
		}
		for tables.Next() {
			var tableName string
			err = tables.Scan(&tableName)
			if err != nil {
				a.logger.Errorf("[WH-Jobs]: Error carrying the scan operation to tablename\n")
				return nil, err
			}
			if !contains(tableNames, tableName) {
				tableNames = append(tableNames, tableName)
			}

		}
	}
	a.logger.Infof("Got the TableNames as %s\n", tableNames)
	return tableNames, nil
}

// Takes AsyncJobPayloadT and adds rows to table wh_async_jobs
func (a *AsyncJobWhT) addJobsToDB(ctx context.Context, payload *AsyncJobPayloadT) (jobId int64, err error) {
	if ctx.Err() != nil {
		return
	}
	a.logger.Infof("[WH-Jobs]: Adding job to the wh_async_jobs %s for tableName: %s", payload.MetaData, payload.TableName)

	sqlStatement := `INSERT INTO ` + warehouseutils.WarehouseAsyncJobTable + ` (
		source_id, destination_id, tablename, 
		status, created_at, updated_at, async_job_type, 
		workspace_id, metadata
	)
	VALUES 
		($1, $2, $3, $4, $5, $6 ,$7, $8, $9 ) RETURNING id`

	stmt, err := a.dbHandle.Prepare(sqlStatement)
	if err != nil {
		a.logger.Errorf("[WH-Jobs]: Error preparing out the query %s ", sqlStatement)
		err = errors.New("error preparing out the query, while addJobsToDB")
		return
	}

	defer stmt.Close()
	now := timeutil.Now()
	row := stmt.QueryRow(payload.SourceID, payload.DestinationID, payload.TableName, WhJobWaiting, now, now, payload.AsyncJobType, payload.WorkspaceID, payload.MetaData)
	err = row.Scan(&jobId)
	if err != nil {
		a.logger.Errorf("[WH-Jobs]: Error processing the %s, %s ", sqlStatement, err.Error())
		return
	}
	return
}

// InitAsyncJobRunner Async Job runner's main job is to
// 1. Scan the database for entries into wh_async_jobs
// 2. Publish data to pg_notifier queue
// 3. Move any executing jobs to waiting
func (a *AsyncJobWhT) InitAsyncJobRunner() error {
	// Start the asyncJobRunner
	a.logger.Info("[WH-Jobs]: Initializing async job runner")
	ctx, cancel := context.WithCancel(a.context)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	a.context = ctx
	var err error
	for retry := 0; retry < a.MaxCleanUpRetries; retry++ {
		err = a.cleanUpAsyncTable(ctx)
		if err == nil {
			a.logger.Info("[WH-Jobs]: successfully cleanup async table with error")
			a.enabled = true
			break
		}
	}

	if err != nil {
		a.logger.Errorf("[WH-Jobs]: unable to cleanup asynctable with error %s", err.Error())
		return err
	}
	if a.enabled {
		g.Go(func() error {
			return a.startAsyncJobRunner(ctx)
		})
		g.Wait()
	}
	return errors.New("unable to enable warehouse Async Job")
}

func (a *AsyncJobWhT) cleanUpAsyncTable(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	a.logger.Info("[WH-Jobs]: Cleaning up the zombie asyncjobs")
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1 WHERE status=$2 or status=$3`, warehouseutils.WarehouseAsyncJobTable)
	a.logger.Debugf("[WH-Jobs]: resetting up async jobs table query %s", sqlStatement)
	row, err := a.dbHandle.Query(sqlStatement, WhJobWaiting, WhJobExecuting, WhJobFailed)
	if err != nil {
		return err
	}
	defer row.Close()
	return nil
}

/*
startAsyncJobRunner is the main runner that
1) Periodically queries the db for any pending async jobs
2) Groups them together
3) Publishes them to the pgnotifier
4) Spawns a subroutine that periodically checks for responses from pgNotifier/slave worker post trackBatch
*/
func (a *AsyncJobWhT) startAsyncJobRunner(ctx context.Context) error {
	a.logger.Info("[WH-Jobs]: Starting async job runner")

	var wg sync.WaitGroup
	for {
		a.logger.Debug("[WH-Jobs]: Scanning for waiting async job")

		select {
		case <-ctx.Done():
			a.logger.Info("[WH-Jobs]: Stopping AsyncJobRunner")
			return nil
		case <-time.After(a.RetryTimeInterval):

		}

		pendingAsyncJobs, err := a.getPendingAsyncJobs(ctx)
		if err != nil {
			a.logger.Errorf("[WH-Jobs]: unable to get pending async jobs with error %s", err.Error())
			continue
		}

		if len(pendingAsyncJobs) > 0 {
			a.logger.Info("[WH-Jobs]: Got pending wh async jobs")
			a.logger.Infof("[WH-Jobs]: Number of async wh jobs left = %d\n", len(pendingAsyncJobs))
			notifierClaims, err := getMessagePayloadsFromAsyncJobPayloads(pendingAsyncJobs)
			if err != nil {
				a.logger.Errorf("Error converting the asyncJobType to notifier payload %s ", err)
				asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(pendingAsyncJobs, WhJobFailed, err)
				_ = a.updateAsyncJobs(ctx, asyncJobStatusMap)
				continue
			}
			messagePayload := pgnotifier.MessagePayload{
				Jobs:    notifierClaims,
				JobType: AsyncJobType,
			}
			var schema warehouseutils.SchemaT
			ch, err := a.pgnotifier.Publish(messagePayload, &schema, 100)
			if err != nil {
				a.logger.Errorf("[WH-Jobs]: unable to get publish async jobs to pgnotifier. Task failed with error %s", err.Error())
				asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(pendingAsyncJobs, WhJobFailed, err)
				_ = a.updateAsyncJobs(ctx, asyncJobStatusMap)
				continue
			}
			asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(pendingAsyncJobs, WhJobExecuting, err)
			_ = a.updateAsyncJobs(ctx, asyncJobStatusMap)
			wg.Add(1)
			go func() {
				select {
				case responses := <-ch:
					a.logger.Info("[WH-Jobs]: Response received from the pgnotifier track batch")
					asyncJobsStatusMap := getAsyncStatusMapFromAsyncPayloads(pendingAsyncJobs)
					a.updateStatusJobPayloadsFromPgNotifierResponse(responses, asyncJobsStatusMap)
					_ = a.updateAsyncJobs(ctx, asyncJobsStatusMap)
					wg.Done()
				case <-time.After(a.AsyncJobTimeOut):
					a.logger.Errorf("Go Routine timed out waiting for a response from PgNotifier", pendingAsyncJobs[0].Id)
					asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(pendingAsyncJobs, WhJobFailed, err)
					_ = a.updateAsyncJobs(ctx, asyncJobStatusMap)
					wg.Done()
				}
			}()
			wg.Wait()
		}
	}
}

func (a *AsyncJobWhT) updateStatusJobPayloadsFromPgNotifierResponse(r []pgnotifier.ResponseT, m map[string]AsyncJobStatus) {
	for _, resp := range r {
		var pgNotifierOutput PGNotifierOutput
		err := json.Unmarshal(resp.Output, &pgNotifierOutput)
		if err != nil {
			a.logger.Errorf("error unmarshalling pgnotifier payload to AsyncJobStatusMa for Id: %s", pgNotifierOutput.Id)
			continue
		}

		if output, ok := m[pgNotifierOutput.Id]; ok {
			output.Status = resp.Status
			if resp.Error != "" {
				output.Error = fmt.Errorf(resp.Error)
			}
			m[pgNotifierOutput.Id] = output
		}
	}
}

// Queries the jobsDB and gets active async job and returns it in a
func (a *AsyncJobWhT) getPendingAsyncJobs(ctx context.Context) ([]AsyncJobPayloadT, error) {
	asyncJobPayloads := make([]AsyncJobPayloadT, 0)
	if ctx.Err() != nil {
		return asyncJobPayloads, ctx.Err()
	}
	a.logger.Debug("[WH-Jobs]: Get pending wh async jobs")
	// Filter to get most recent row for the sourceId/destinationID combo and remaining ones should relegate to abort.
	var attempt int
	query := fmt.Sprintf(
		`SELECT
			id,
			source_id,
			destination_id,
			tablename,
			async_job_type,
			metadata,
			attempt
		FROM %s WHERE (status=$1 OR status=$2) LIMIT $3`, warehouseutils.WarehouseAsyncJobTable)
	rows, err := a.dbHandle.Query(query, WhJobWaiting, WhJobFailed, a.MaxBatchSizeToProcess)
	if err != nil {
		a.logger.Errorf("[WH-Jobs]: Error in getting pending wh async jobs with error %s", err.Error())
		return asyncJobPayloads, err
	}
	defer rows.Close()
	for rows.Next() {
		var asyncJobPayload AsyncJobPayloadT
		err = rows.Scan(
			&asyncJobPayload.Id,
			&asyncJobPayload.SourceID,
			&asyncJobPayload.DestinationID,
			&asyncJobPayload.TableName,
			&asyncJobPayload.AsyncJobType,
			&asyncJobPayload.MetaData,
			&attempt,
		)
		if err != nil {
			a.logger.Errorf("[WH-Jobs]: Error scanning rows %s\n", err)
			return asyncJobPayloads, err
		}
		asyncJobPayloads = append(asyncJobPayloads, asyncJobPayload)
		a.logger.Infof("Adding row with Id = %s & attempt no %d", asyncJobPayload.Id, attempt)
	}
	return asyncJobPayloads, nil
}

// Updates the warehouse async jobs with the status sent as a parameter
func (a *AsyncJobWhT) updateAsyncJobs(ctx context.Context, payloads map[string]AsyncJobStatus) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	a.logger.Info("[WH-Jobs]: Updating wh async jobs to Executing")
	var err error
	for _, payload := range payloads {
		if payload.Error != nil {
			err = a.updateAsyncJobStatus(ctx, payload.Id, payload.Status, payload.Error.Error())
			continue
		}
		err = a.updateAsyncJobStatus(ctx, payload.Id, payload.Status, "")

	}
	return err
}

func (a *AsyncJobWhT) updateAsyncJobStatus(ctx context.Context, Id, status, errMessage string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	a.logger.Infof("[WH-Jobs]: Updating status of wh async jobs to %s", status)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=(CASE
															WHEN attempt >= $1
															THEN $2
															ELSE  $3
															END) ,
															error=$4 WHERE id=$5 AND status!=$6 AND status!=$7 `, warehouseutils.WarehouseAsyncJobTable)
	var err error
	for retryCount := 0; retryCount < a.MaxQueryRetries; retryCount++ {
		a.logger.Debugf("[WH-Jobs]: updating async jobs table query %s, retry no : %d", sqlStatement, retryCount)
		_, err = a.dbHandle.Query(sqlStatement, a.MaxAttemptsPerJob, WhJobAborted, status, errMessage, Id, WhJobAborted, WhJobSucceeded)
		if err == nil {
			a.logger.Info("Update successful")
			a.logger.Debugf("query: %s successfully executed", sqlStatement)
			if status == WhJobFailed {
				err = a.updateAsyncJobAttempt(ctx, Id)
				return err
			}
			return err
		}
	}
	if err != nil {
		a.logger.Errorf("query: %s failed with Error : %s", sqlStatement, err.Error())
	}
	return err
}

func (a *AsyncJobWhT) updateAsyncJobAttempt(ctx context.Context, Id string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	a.logger.Info("[WH-Jobs]: Incrementing wh async jobs attempt")
	sqlStatement := fmt.Sprintf(`UPDATE %s SET attempt=attempt+1 WHERE id=$1 AND status!=$2 AND status!=$3 `, warehouseutils.WarehouseAsyncJobTable)
	var err error
	for queryRetry := 0; queryRetry < a.MaxQueryRetries; queryRetry++ {
		a.logger.Debugf("[WH-Jobs]: updating async jobs table query %s, retry no : %d", sqlStatement, queryRetry)
		_, err = a.dbHandle.Query(sqlStatement, Id, WhJobAborted, WhJobSucceeded)
		if err == nil {
			a.logger.Info("Update successful")
			a.logger.Debugf("query: %s successfully executed", sqlStatement)
			return nil
		}

	}
	a.logger.Errorf("query: %s failed with Error : %s", sqlStatement, err.Error())
	return err
}

// returns status and errMessage
// Only succeeded, executing & waiting states should have empty errMessage
// Rest of the states failed, aborted should send an error message conveying a message
func (a *AsyncJobWhT) getStatusAsyncJob(ctx context.Context, payload *StartJobReqPayload) (statusResponse WhStatusResponse) {
	if ctx.Err() != nil {
		return
	}
	a.logger.Info("[WH-Jobs]: Getting status for wh async jobs %v", payload)
	// Need to check for count first and see if there are any rows matching the job_run_id and task_run_id. If none, then raise an error instead of showing complete
	sqlStatement := fmt.Sprintf(`SELECT status,error FROM %s WHERE metadata->>'job_run_id'=$1 AND metadata->>'task_run_id'=$2`, warehouseutils.WarehouseAsyncJobTable)
	a.logger.Debugf("Query inside getStatusAsync function is %s", sqlStatement)
	rows, err := a.dbHandle.Query(sqlStatement, payload.JobRunID, payload.TaskRunID)
	if err != nil {
		a.logger.Errorf("[WH-Jobs]: Error executing the query %s", err.Error())
		statusResponse.Status = WhJobFailed
		statusResponse.Err = err.Error()
		return
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var errMessage sql.NullString
		err = rows.Scan(&status, &errMessage)
		if err != nil {
			a.logger.Errorf("[WH-Jobs]: Error scanning rows %s\n", err)
			statusResponse = WhStatusResponse{
				Status: WhJobFailed,
				Err:    err.Error(),
			}
			return
		}
		if status == WhJobFailed {
			a.logger.Infof("[WH-Jobs] Async Job with job_run_id: %s, task_run_id: %s is failed", payload.JobRunID, payload.TaskRunID)
			statusResponse.Status = WhJobFailed
			if !errMessage.Valid {
				statusResponse.Err = "Failed while scanning"
				return
			}
			statusResponse.Err = errMessage.String
			return
		}
		if status == WhJobAborted {
			a.logger.Infof("[WH-Jobs] Async Job with job_run_id: %s, task_run_id: %s is aborted", payload.JobRunID, payload.TaskRunID)
			statusResponse.Status = WhJobAborted
			if !errMessage.Valid {
				statusResponse.Err = "Failed while scanning"
				return

			}
			statusResponse.Err = errMessage.String
			return
		}
		if status != WhJobSucceeded {
			a.logger.Infof("[WH-Jobs] Async Job with job_run_id: %s, task_run_id: %s is under processing", payload.JobRunID, payload.TaskRunID)
			statusResponse.Status = WhJobExecuting
			return
		}

	}

	a.logger.Infof("[WH-Jobs] Async Job with job_run_id: %s, task_run_id: %s is complete", payload.JobRunID, payload.TaskRunID)
	statusResponse.Status = WhJobSucceeded
	return
}
