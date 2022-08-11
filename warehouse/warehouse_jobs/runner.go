package warehouse_jobs

import (
	"context"
	"database/sql"
	"encoding/json"
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
}

const AsyncTableName string = "wh_async_jobs"

//Gets Table names from the jobrunid
func (asyncWhJob *AsyncJobWhT) getTableNamesByJobRunID(jobrunid string) ([]string, error) {
	asyncWhJob.log.Infof("Extracting tablenames for the job run id %s", jobrunid)
	var tableNames []string
	query := fmt.Sprintf(`select id from %s where metadata->>'%s'='%s'`, warehouseutils.WarehouseUploadsTable, "source_job_run_id", jobrunid)
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
	return tableNames, nil
}

//Add data to the tables
func (asyncWhJob *AsyncJobWhT) addJobstoDB(payload *AsyncJobPayloadT) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (sourceid, namespace, destinationid, destination_type, jobrunid, taskrunid, tablename,  status, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10) RETURNING id`, warehouseutils.WarehouseAsyncJobTable)

	stmt, err := asyncWhJob.dbHandle.Prepare(sqlStatement)
	if err != nil {
		asyncWhJob.log.Errorf("Error preparing out the query %s ", sqlStatement)
		panic(err)
	}

	defer stmt.Close()
	now := timeutil.Now()
	row := stmt.QueryRow(payload.SourceID, payload.Namespace, payload.DestinationID, payload.DestType, payload.JobRunID, payload.TaskRunID, payload.TableName, asyncJobWaiting, now, now)
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

func (asyncWhJob *AsyncJobWhT) startAsyncJobRunner(ctx context.Context) {
	asyncWhJob.log.Info("WH-Jobs: Starting async job runner")
	for {
		select {
		case <-ctx.Done():
			asyncWhJob.log.Info("WH-Jobs: Stopping AsyncJobRunner")
			return
		case <-time.After(10 * time.Second):

		}
		asyncWhJob.log.Info("WH-Jobs: Getting pending wh async jobs")
		asyncjobpayloads, err := asyncWhJob.getPendingAsyncJobs(ctx)
		if err != nil {
			return
		}
		asyncWhJob.log.Infof("WH-Jobs: Number of async wh jobs left = %d\n", len(asyncjobpayloads))
		if len(asyncjobpayloads) > 0 {
			notifierClaims, err := getMessagePayloadsFromAsyncJobPayloads(asyncjobpayloads)

			if err != nil {
				asyncWhJob.log.Errorf("Error converting the asyncJobType to notifier payload %s ", err)
				panic(err)
			}
			messagePayload := pgnotifier.MessagePayload{
				Jobs:    notifierClaims,
				JobType: "async_job",
			}
			ch, err := asyncWhJob.pgnotifier.Publish(messagePayload, 100)
			if err != nil {
				panic(err)
			}
			asyncWhJob.updateAsyncJobs(&asyncjobpayloads, "processing")
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case responses := <-ch:
						asyncWhJob.log.Info("Waiting for responses from the pgnotifier track batch %s", responses)
					}
				}
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
	COALESCE(jobtype,''),
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
func (asyncWhJob *AsyncJobWhT) updateAsyncJobs(payloads *[]AsyncJobPayloadT, status string) {
	asyncWhJob.log.Info("WH-Jobs: Updating pending wh async jobs")
	for _, payload := range *payloads {
		sqlStatement := fmt.Sprintf(`UPDATE %s SET status='%s' WHERE id=%s`, warehouseutils.WarehouseAsyncJobTable, status, payload.Id)
		asyncWhJob.log.Infof("WH-Jobs: updating async jobs table query %s", sqlStatement)
		_, err := asyncWhJob.dbHandle.Query(sqlStatement)
		if err != nil {
			panic(fmt.Errorf("query: %s failed with Error : %w", sqlStatement, err))
		}
	}
}

// func (asyncWhJob *AsyncJobWhT) getMessagePayloadsFromAsyncJobPayloads(asyncjobpayloads []AsyncJobPayloadT) []pgnotifier.JobPayload {
// 	// var asyncjobs []asyncJobT
// 	var messages []pgnotifier.JobPayload
// 	for _, asyncjobpayload := range asyncjobpayloads {
// 		var asyncjob asyncJobT
// 		asyncjob.asyncjobpayload = asyncjobpayload
// 		destconfig := (*asyncWhJob.connectionsMap)[asyncjobpayload.DestinationID][asyncjobpayload.SourceID]
// 		destType := destconfig.Destination.DestinationDefinition.Name
// 		whManager, err := manager.New(destType)
// 		if err != nil {
// 			panic(err)
// 		}
// 		asyncjob.whManager = whManager
// 		asyncjobs = append(asyncjobs, asyncjob)
// 	}
// 	return messages
// }

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
