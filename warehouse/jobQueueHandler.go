package warehouse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	etl "github.com/rudderlabs/rudder-server/warehouse/etl"
	. "github.com/rudderlabs/rudder-server/warehouse/etl/transform"
)

const NUM_WORKERS = 4

type JobQueueHandleT struct {
	wh                          *HandleT
	workers                     [NUM_WORKERS]*ETLTransformWorkerT
	workerStatuses              [NUM_WORKERS]WorkerStatus //If not busy, JobQueue will assign work
	jobQueueNotificationChannel chan StatusMsg            //Workers notify jobQueue on this channel

	jobQueueTable         string //DB Table Name - loaded from config
	jobQueueNotifyChannel string //DB Channel Name - to get notifications

	isSetup bool
}

//Setup JobQueue Tables & Triggers - Must be called once during server initialisation
func (jq *JobQueueHandleT) Setup(whHandle *HandleT) {
	logger.Infof("WH-JQ: Setting up Job Queue ")

	if jq.isSetup {
		etl.AssertString(jq, etl.STR_JOBQUEUE_ALREADY_SETUP)
	}
	defer func() { jq.isSetup = true }()

	jq.wh = whHandle

	//create channel to receive from workers
	jq.jobQueueNotificationChannel = make(chan StatusMsg)

	jq.loadConfig()

	jq.setupTables()

	jq.setupTriggerAndChannel()

	jq.setupWorkers()

	//Main Loop  - waits for notifications from the channel
	rruntime.Go(func() {
		jq.mainLoop()
	})
}

func (jq *JobQueueHandleT) loadConfig() {
	jq.jobQueueTable = config.GetString("Warehouse.jobQueueTable", "wh_job_queue")
	jq.jobQueueNotifyChannel = config.GetString("Warehouse.jobQueueNotifyChannel", "wh_job_queue_status_channel")
}

//Release any resources held
func (jq *JobQueueHandleT) TearDown() {
	jq.wh = nil
	for i := 0; i < NUM_WORKERS; i++ {
		jq.workers[i].TearDown()
	}
}

//Worker saying it is free - Runs in worker thread - just write to channel, nothing else
func (jq *JobQueueHandleT) SetTransformWorkerFree(workerIdx int) {
	jq.jobQueueNotificationChannel <- StatusMsg{
		Status: FREE, WorkerIdx: workerIdx,
	}
}

func (jq *JobQueueHandleT) assignJobToWorker(prettyJSON bytes.Buffer) {
	for i := 0; i < NUM_WORKERS; i++ {
		if jq.workerStatuses[i] == FREE {
			logger.Debugf("WH-JQ: Job Request will be made by worker %v", i)
			jq.workers[i].WorkerNotificationChannel <- prettyJSON
			return
		}
	}
}

//Wait for notifications - if etl transform worker is busy, ingore - else notify etl transform worker
func (jq *JobQueueHandleT) mainLoop() {

	//TODO: this connInfo for SLAVE mode should come from another config variable
	connInfo := jobsdb.GetConnectionString()

	//Create a listener & start listening -- TODO: check if panic is required
	listener := pq.NewListener(connInfo,
		10*time.Second,
		time.Minute,
		func(ev pq.ListenerEventType, err error) {
			etl.AssertError(jq, err)
		})
	err := listener.Listen(jq.jobQueueNotifyChannel)
	etl.AssertError(jq, err)

	logger.Debugf("WH-JQ: Wait for Status Notifications")
	for {
		select {
		case notif := <-listener.Notify:
			logger.Debugf("WH-JQ: Received data from channel [", notif.Channel, "] :")

			//Pass the Json to a free worker if available
			var prettyJSON bytes.Buffer
			err = json.Indent(&prettyJSON, []byte(notif.Extra), "", "\t")
			etl.AssertError(jq, err)
			logger.Debugf("WH-JQ: %v", string([]byte(notif.Extra)))

			jq.assignJobToWorker(prettyJSON)

		case <-time.After(90 * time.Second):
			logger.Debugf("WH-JQ: Received no events for 90 seconds, checking connection")
			go func() {
				listener.Ping()
			}()

		// Also listen for worker queue channel
		case statusMsg := <-jq.jobQueueNotificationChannel:
			jq.workerStatuses[statusMsg.WorkerIdx] = statusMsg.Status
		}
	}
}

//Setup Workers & Comms channels
func (jq *JobQueueHandleT) setupWorkers() {
	for i := 0; i < NUM_WORKERS; i++ {
		var tw ETLTransformWorkerT
		jq.workerStatuses[i] = BUSY
		jq.workers[i] = &tw
		jq.workers[i].Init(jq, i)
	}
}

//Setup Postgres Trigger & Wait for notifications on the channel
func (jq *JobQueueHandleT) setupTriggerAndChannel() {
	//create a postgres function that notifies on the specified channel
	sqlStmt := fmt.Sprintf(`CREATE OR REPLACE FUNCTION wh_job_queue_status_notify() RETURNS TRIGGER AS $$

							DECLARE 
								data json;
								notification json;
							
							BEGIN
							
								-- Convert the old or new row to JSON, based on the kind of action.
								-- Action = DELETE?             -> OLD row
								-- Action = INSERT or UPDATE?   -> NEW row
								IF (TG_OP = 'DELETE') THEN
									data = row_to_json(OLD);
								ELSE
									data = row_to_json(NEW);
								END IF;
								
								-- Contruct the notification as a JSON string.
								notification = json_build_object(
												'table',TG_TABLE_NAME,
												'action', TG_OP,
												'data', data);
								
												
								-- Execute pg_notify(channel, notification)
								PERFORM pg_notify('%s',notification::text);
								
								-- Result is ignored since this is an AFTER trigger
								RETURN NULL; 
							END;
							
						$$ LANGUAGE plpgsql;`, jq.jobQueueNotifyChannel)

	_, err := jq.wh.dbHandle.Exec(sqlStmt)
	etl.AssertError(jq, err)

	//create the trigger
	sqlStmt = fmt.Sprintf(`DO $$ BEGIN
									CREATE TRIGGER %[1]s_status_trigger
											AFTER INSERT OR UPDATE OF status
											ON %[1]s
											FOR EACH ROW
										EXECUTE PROCEDURE wh_job_queue_status_notify();
									EXCEPTION
										WHEN others THEN null;
								END $$`, jq.jobQueueTable)

	_, err = jq.wh.dbHandle.Exec(sqlStmt)
	etl.AssertError(jq, err)
}

//Create the required tables
//TODO: In SLAVE mode , do not setup tables
func (jq *JobQueueHandleT) setupTables() {
	logger.Debugf("WH-JQ: Creating Job Queue Tables ")

	//create status type
	sqlStmt := `DO $$ BEGIN
						CREATE TYPE wh_job_queue_status_type
							AS ENUM(
								'new', 
								'running',
								'success', 
								'error'
									);
							EXCEPTION
								WHEN duplicate_object THEN null;
					END $$;`

	_, err := jq.wh.dbHandle.Exec(sqlStmt)
	etl.AssertError(jq, err)

	//create the table
	sqlStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
										  id BIGSERIAL PRIMARY KEY,
										  staging_file_id BIGINT, 
										  status wh_job_queue_status_type NOT NULL, 
										  worker_id VARCHAR(64) NOT NULL,
										  job_create_time TIMESTAMP NOT NULL,
										  status_update_time TIMESTAMP NOT NULL,
										  last_error VARCHAR(512));`, jq.jobQueueTable)

	_, err = jq.wh.dbHandle.Exec(sqlStmt)
	etl.AssertError(jq, err)

	// create index on status
	sqlStmt = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_status_idx ON %[1]s (status);`, jq.jobQueueTable)
	_, err = jq.wh.dbHandle.Exec(sqlStmt)
	etl.AssertError(jq, err)
}
