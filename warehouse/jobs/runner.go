package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/services/notifier/model"

	"github.com/rudderlabs/rudder-server/services/notifier"

	"github.com/lib/pq"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// InitWarehouseJobsAPI Initializes AsyncJobWh structure with appropriate variabless
func InitWarehouseJobsAPI(
	ctx context.Context,
	dbHandle *sql.DB,
	notifier *notifier.Notifier,
) *AsyncJobWh {
	return &AsyncJobWh{
		dbHandle: dbHandle,
		enabled:  false,
		notifier: notifier,
		context:  ctx,
		logger:   logger.NewLogger().Child("asyncjob"),
	}
}

func WithConfig(a *AsyncJobWh, config *config.Config) {
	a.maxBatchSizeToProcess = config.GetInt("Warehouse.jobs.maxBatchSizeToProcess", 10)
	a.maxCleanUpRetries = config.GetInt("Warehouse.jobs.maxCleanUpRetries", 5)
	a.maxQueryRetries = config.GetInt("Warehouse.jobs.maxQueryRetries", 3)
	a.maxAttemptsPerJob = config.GetInt("Warehouse.jobs.maxAttemptsPerJob", 3)
	a.retryTimeInterval = config.GetDuration("Warehouse.jobs.retryTimeInterval", 10, time.Second)
	a.asyncJobTimeOut = config.GetDuration("Warehouse.jobs.asyncJobTimeOut", 300, time.Second)
}

func (a *AsyncJobWh) getTableNamesBy(sourceID, destinationID, jobRunID, taskRunID string) ([]string, error) {
	a.logger.Infof("[WH-Jobs]: Extracting table names for the job run id %s", jobRunID)
	var tableNames []string
	var err error

	query := `SELECT table_name FROM ` + warehouseutils.WarehouseTableUploadsTable + ` WHERE wh_upload_id IN ` +
		` (SELECT id FROM ` + warehouseutils.WarehouseUploadsTable + ` WHERE metadata->>'source_job_run_id'=$1
				AND metadata->>'source_task_run_id'=$2
				AND source_id=$3
				AND destination_id=$4)`
	a.logger.Debugf("[WH-Jobs]: Query is %s\n", query)
	rows, err := a.dbHandle.QueryContext(a.context, query, jobRunID, taskRunID, sourceID, destinationID)
	if err != nil {
		a.logger.Errorf("[WH-Jobs]: Error executing the query %s with error %v", query, err)
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			a.logger.Errorf("[WH-Jobs]: Error scanning the rows %s", err.Error())
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}
	if err = rows.Err(); err != nil {
		a.logger.Errorf("[WH-Jobs]: Error iterating the rows %s", err.Error())
		return nil, err
	}
	a.logger.Infof("Got the TableNames as %s", tableNames)
	return lo.Uniq(tableNames), nil
}

// Takes AsyncJobPayload and adds rows to table wh_async_jobs
func (a *AsyncJobWh) addJobsToDB(payload *AsyncJobPayload) (int64, error) {
	a.logger.Infof("[WH-Jobs]: Adding job to the wh_async_jobs %s for tableName: %s", payload.MetaData, payload.TableName)
	var jobId int64
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
		err = fmt.Errorf("error preparing out the query, while addJobsToDB %v", err)
		return 0, err
	}

	defer func() { _ = stmt.Close() }()
	now := timeutil.Now()
	row := stmt.QueryRowContext(a.context, payload.SourceID, payload.DestinationID, payload.TableName, WhJobWaiting, now, now, payload.AsyncJobType, payload.WorkspaceID, payload.MetaData)
	err = row.Scan(&jobId)
	if err != nil {
		a.logger.Errorf("[WH-Jobs]: Error processing the %s, %s ", sqlStatement, err.Error())
		return 0, err
	}
	return jobId, nil
}

// InitAsyncJobRunner Async Job runner's main job is to
// 1. Scan the database for entries into wh_async_jobs
// 2. Publish data to pg_notifier queue
// 3. Move any executing jobs to waiting
func (a *AsyncJobWh) InitAsyncJobRunner() error {
	// Start the asyncJobRunner
	a.logger.Info("[WH-Jobs]: Initializing async job runner")
	g, ctx := errgroup.WithContext(a.context)
	a.context = ctx
	err := misc.RetryWith(a.context, a.retryTimeInterval, a.maxCleanUpRetries, func(ctx context.Context) error {
		err := a.cleanUpAsyncTable(ctx)
		if err != nil {
			a.logger.Errorf("[WH-Jobs]: unable to cleanup asynctable with error %s", err.Error())
			return err
		}
		a.enabled = true
		return nil
	})
	if err != nil {
		a.logger.Errorf("[WH-Jobs]: unable to cleanup asynctable with error %s", err.Error())
		return err
	}
	if a.enabled {
		g.Go(func() error {
			return a.startAsyncJobRunner(ctx)
		})
	}
	return g.Wait()
}

func (a *AsyncJobWh) cleanUpAsyncTable(ctx context.Context) error {
	a.logger.Info("[WH-Jobs]: Cleaning up the zombie asyncjobs")
	sqlStatement := fmt.Sprintf(
		`UPDATE %s SET status=$1 WHERE status=$2 or status=$3`,
		pq.QuoteIdentifier(warehouseutils.WarehouseAsyncJobTable),
	)
	a.logger.Debugf("[WH-Jobs]: resetting up async jobs table query %s", sqlStatement)
	_, err := a.dbHandle.ExecContext(ctx, sqlStatement, WhJobWaiting, WhJobExecuting, WhJobFailed)
	return err
}

/*
startAsyncJobRunner is the main runner that
1) Periodically queries the db for any pending async jobs
2) Groups them together
3) Publishes them to the notifier
4) Spawns a subroutine that periodically checks for responses from Notifier/slave worker post trackBatch
*/
func (a *AsyncJobWh) startAsyncJobRunner(ctx context.Context) error {
	a.logger.Info("[WH-Jobs]: Starting async job runner")
	defer a.logger.Info("[WH-Jobs]: Stopping AsyncJobRunner")

	for {
		a.logger.Debug("[WH-Jobs]: Scanning for waiting async job")

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(a.retryTimeInterval):
		}

		pendingAsyncJobs, err := a.getPendingAsyncJobs(ctx)
		if err != nil {
			a.logger.Errorf("[WH-Jobs]: unable to get pending async jobs with error %s", err.Error())
			continue
		}
		if len(pendingAsyncJobs) == 0 {
			continue
		}

		a.logger.Infof("[WH-Jobs]: Number of async wh jobs left = %d", len(pendingAsyncJobs))

		notifierClaims, err := getMessagePayloadsFromAsyncJobPayloads(pendingAsyncJobs)
		if err != nil {
			a.logger.Errorf("Error converting the asyncJobType to notifier payload %s ", err)
			asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(pendingAsyncJobs, WhJobFailed, err)
			_ = a.updateAsyncJobs(ctx, asyncJobStatusMap)
			continue
		}

		ch, err := a.notifier.Publish(ctx, &model.PublishRequest{
			Payloads: notifierClaims,
			JobType:  AsyncJobType,
			Priority: 100,
		})
		if err != nil {
			a.logger.Errorf("[WH-Jobs]: unable to get publish async jobs to notifier. Task failed with error %s", err.Error())
			asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(pendingAsyncJobs, WhJobFailed, err)
			_ = a.updateAsyncJobs(ctx, asyncJobStatusMap)
			continue
		}
		asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(pendingAsyncJobs, WhJobExecuting, err)
		_ = a.updateAsyncJobs(ctx, asyncJobStatusMap)

		select {
		case <-ctx.Done():
			a.logger.Infof("[WH-Jobs]: Context cancelled for async job runner")
			return nil
		case responses, ok := <-ch:
			if !ok {
				continue
			}
			if responses.Err != nil {
				a.logger.Errorf("[WH-Jobs]: Error received from the notifier track batch %s", responses.Err.Error())
				asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(pendingAsyncJobs, WhJobFailed, err)
				_ = a.updateAsyncJobs(ctx, asyncJobStatusMap)
				continue
			}
			a.logger.Info("[WH-Jobs]: Response received from the notifier track batch")
			asyncJobsStatusMap := getAsyncStatusMapFromAsyncPayloads(pendingAsyncJobs)
			a.updateStatusJobPayloadsFromNotifierResponse(responses, asyncJobsStatusMap)
			_ = a.updateAsyncJobs(ctx, asyncJobsStatusMap)
		case <-time.After(a.asyncJobTimeOut):
			a.logger.Errorf("Go Routine timed out waiting for a response from Notifier", pendingAsyncJobs[0].Id)
			asyncJobStatusMap := convertToPayloadStatusStructWithSingleStatus(pendingAsyncJobs, WhJobFailed, err)
			_ = a.updateAsyncJobs(ctx, asyncJobStatusMap)
		}
	}
}

func (a *AsyncJobWh) updateStatusJobPayloadsFromNotifierResponse(r *model.PublishResponse, m map[string]AsyncJobStatus) {
	for _, resp := range r.Notifiers {
		var response NotifierResponse
		err := json.Unmarshal(resp.Payload, &response)
		if err != nil {
			a.logger.Errorf("error unmarshalling notifier payload to AsyncJobStatusMa for Id: %s", response.Id)
			continue
		}

		if output, ok := m[response.Id]; ok {
			output.Status = resp.Status
			if resp.Error != nil {
				output.Error = fmt.Errorf(resp.Error.Error())
			}
			m[response.Id] = output
		}
	}
}

// Queries the jobsDB and gets active async job and returns it in a
func (a *AsyncJobWh) getPendingAsyncJobs(ctx context.Context) ([]AsyncJobPayload, error) {
	asyncJobPayloads := make([]AsyncJobPayload, 0)
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
	rows, err := a.dbHandle.QueryContext(ctx, query, WhJobWaiting, WhJobFailed, a.maxBatchSizeToProcess)
	if err != nil {
		a.logger.Errorf("[WH-Jobs]: Error in getting pending wh async jobs with error %s", err.Error())
		return asyncJobPayloads, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var asyncJobPayload AsyncJobPayload
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
	if err := rows.Err(); err != nil {
		a.logger.Errorf("[WH-Jobs]: Error in getting pending wh async jobs with error %s", rows.Err().Error())
		return asyncJobPayloads, err
	}
	return asyncJobPayloads, nil
}

// Updates the warehouse async jobs with the status sent as a parameter
func (a *AsyncJobWh) updateAsyncJobs(ctx context.Context, payloads map[string]AsyncJobStatus) error {
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

func (a *AsyncJobWh) updateAsyncJobStatus(ctx context.Context, Id, status, errMessage string) error {
	a.logger.Infof("[WH-Jobs]: Updating status of wh async jobs to %s", status)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=(CASE
								WHEN attempt >= $1
								THEN $2
								ELSE $3
								END) ,
								error=$4 WHERE id=$5 AND status!=$6 AND status!=$7 `,
		warehouseutils.WarehouseAsyncJobTable,
	)
	var err error
	for retryCount := 0; retryCount < a.maxQueryRetries; retryCount++ {
		a.logger.Debugf("[WH-Jobs]: updating async jobs table query %s, retry no : %d", sqlStatement, retryCount)
		_, err := a.dbHandle.ExecContext(ctx, sqlStatement,
			a.maxAttemptsPerJob, WhJobAborted, status, errMessage, Id, WhJobAborted, WhJobSucceeded,
		)
		if err == nil {
			a.logger.Info("Update successful")
			a.logger.Debugf("query: %s successfully executed", sqlStatement)
			if status == WhJobFailed {
				return a.updateAsyncJobAttempt(ctx, Id)
			}
			return err
		}
	}

	a.logger.Errorf("Query: %s failed with error: %s", sqlStatement, err.Error())
	return err
}

func (a *AsyncJobWh) updateAsyncJobAttempt(ctx context.Context, Id string) error {
	a.logger.Info("[WH-Jobs]: Incrementing wh async jobs attempt")
	sqlStatement := fmt.Sprintf(`UPDATE %s SET attempt=attempt+1 WHERE id=$1 AND status!=$2 AND status!=$3 `, warehouseutils.WarehouseAsyncJobTable)
	var err error
	for queryRetry := 0; queryRetry < a.maxQueryRetries; queryRetry++ {
		a.logger.Debugf("[WH-Jobs]: updating async jobs table query %s, retry no : %d", sqlStatement, queryRetry)
		row, err := a.dbHandle.QueryContext(ctx, sqlStatement, Id, WhJobAborted, WhJobSucceeded)
		if err == nil {
			a.logger.Info("Update successful")
			a.logger.Debugf("query: %s successfully executed", sqlStatement)
			return nil
		}
		_ = row.Err()
	}
	a.logger.Errorf("query: %s failed with Error : %s", sqlStatement, err.Error())
	return err
}

// returns status and errMessage
// Only succeeded, executing & waiting states should have empty errMessage
// Rest of the states failed, aborted should send an error message conveying a message
func (a *AsyncJobWh) getStatusAsyncJob(payload *StartJobReqPayload) WhStatusResponse {
	var statusResponse WhStatusResponse
	a.logger.Info("[WH-Jobs]: Getting status for wh async jobs %v", payload)
	// Need to check for count first and see if there are any rows matching the job_run_id and task_run_id. If none, then raise an error instead of showing complete
	sqlStatement := fmt.Sprintf(`SELECT status,error FROM %s WHERE metadata->>'job_run_id'=$1 AND metadata->>'task_run_id'=$2`, warehouseutils.WarehouseAsyncJobTable)
	a.logger.Debugf("Query inside getStatusAsync function is %s", sqlStatement)
	rows, err := a.dbHandle.QueryContext(a.context, sqlStatement, payload.JobRunID, payload.TaskRunID)
	if err != nil {
		a.logger.Errorf("[WH-Jobs]: Error executing the query %s", err.Error())
		return WhStatusResponse{
			Status: WhJobFailed,
			Err:    err.Error(),
		}
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var status string
		var errMessage sql.NullString
		err = rows.Scan(&status, &errMessage)
		if err != nil {
			a.logger.Errorf("[WH-Jobs]: Error scanning rows %s\n", err)
			return WhStatusResponse{
				Status: WhJobFailed,
				Err:    err.Error(),
			}
		}

		switch status {
		case WhJobFailed:
			a.logger.Infof("[WH-Jobs] Async Job with job_run_id: %s, task_run_id: %s is failed", payload.JobRunID, payload.TaskRunID)
			statusResponse.Status = WhJobFailed
			if !errMessage.Valid {
				statusResponse.Err = "Failed while scanning"
			}
			statusResponse.Err = errMessage.String
		case WhJobAborted:
			a.logger.Infof("[WH-Jobs] Async Job with job_run_id: %s, task_run_id: %s is aborted", payload.JobRunID, payload.TaskRunID)
			statusResponse.Status = WhJobAborted
			if !errMessage.Valid {
				statusResponse.Err = "Failed while scanning"
			}
			statusResponse.Err = errMessage.String
		case WhJobSucceeded:
			a.logger.Infof("[WH-Jobs] Async Job with job_run_id: %s, task_run_id: %s is complete", payload.JobRunID, payload.TaskRunID)
			statusResponse.Status = WhJobSucceeded
		default:
			a.logger.Infof("[WH-Jobs] Async Job with job_run_id: %s, task_run_id: %s is under processing", payload.JobRunID, payload.TaskRunID)
			statusResponse.Status = WhJobExecuting
		}
	}

	if err = rows.Err(); err != nil {
		a.logger.Errorf("[WH-Jobs]: Error scanning rows %s\n", err)
		return WhStatusResponse{
			Status: WhJobFailed,
			Err:    err.Error(),
		}
	}
	return statusResponse
}
