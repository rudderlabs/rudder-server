package clusterman

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	ci "github.com/rudderlabs/rudder-server/warehouse/clusterinterface"
	etl "github.com/rudderlabs/rudder-server/warehouse/etl/transform"
	"github.com/rudderlabs/rudder-server/warehouse/strings"
	utils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

//NUMWORKERS for each node - to be obtained from config
const NUMWORKERS = 4

//JobQueueHandleT will be shared among master and worker nodes to handle pub/sub comms
type JobQueueHandleT struct {
	//wh                          *HandleT
	cn                    *clusterNodeT
	workers               [NUMWORKERS]*etl.WorkerT
	workerStatuses        [NUMWORKERS]ci.WorkerStatus //If not busy, JobQueue will assign work
	jobQueueNotificationQ chan ci.StatusMsg           //Workers notify jobQueue on this channel

	isSetup bool
}

//Setup JobQueue Tables & Triggers - Must be called once during server initialisation
func (jq *JobQueueHandleT) Setup(cn *clusterNodeT) {
	logger.Infof("WH-JQ: Setting up Job Queue ")

	if jq.isSetup {
		utils.AssertString(jq, strings.STR_JOBQUEUE_ALREADY_SETUP)
	}
	defer func() { jq.isSetup = true }()

	jq.cn = cn

	//create channel to receive from workers
	jq.jobQueueNotificationQ = make(chan ci.StatusMsg)

	jq.setupTriggerAndChannel()

	jq.setupWorkers()

	//Main Loop  - waits for notifications from the channel
	rruntime.Go(func() {
		jq.mainLoop()
	})
}

//TearDown to release any resources held
func (jq *JobQueueHandleT) TearDown() {
	for i := 0; i < NUMWORKERS; i++ {
		jq.workers[i].TearDown()
	}
	jq.cn = nil
}

//SetTransformWorker to set its status - Runs in worker thread - just write to channel, nothing else
func (jq *JobQueueHandleT) SetTransformWorker(status ci.StatusMsg) {
	jq.jobQueueNotificationQ <- status
}

func (jq *JobQueueHandleT) assignJobToWorker(prettyJSON *bytes.Buffer) {
	for i := 0; i < NUMWORKERS; i++ {
		if jq.workerStatuses[i] == ci.FREE {
			jq.workerStatuses[i] = ci.BUSY
			logger.Infof("WH-JQ: Job Request will be made by worker %v", i)
			jq.workers[i].WorkerNotificationQ <- prettyJSON
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
			logger.Infof("WH-JQ: event received %v", ev)
			//utils.AssertError(jq, err)
		})
	err := listener.Listen(jq.cn.config.jobQueueNotifyChannel)
	utils.AssertError(jq, err)

	logger.Infof("WH-JQ: Wait for Status Notifications")
	for {
		select {
		case notif := <-listener.Notify:
			if notif != nil {
				logger.Infof("WH-JQ: Received data from channel [", notif.Channel, "] :")

				//Pass the Json to a free worker if available
				var prettyJSON bytes.Buffer
				err = json.Indent(&prettyJSON, []byte(notif.Extra), "", "\t")
				utils.AssertError(jq, err)

				jq.assignJobToWorker(&prettyJSON)
			}

		case <-time.After(90 * time.Second):
			logger.Infof("WH-JQ: Received no events for 90 seconds, checking connection")
			go func() {
				listener.Ping()
			}()

		// Also listen for worker queue channel
		case statusMsg := <-jq.jobQueueNotificationQ:
			jq.workerStatuses[statusMsg.WorkerIdx] = statusMsg.Status
		}
	}
}

//Setup Workers & Comms channels
func (jq *JobQueueHandleT) setupWorkers() {
	for i := 0; i < NUMWORKERS; i++ {
		var tw etl.WorkerT
		jq.workerStatuses[i] = ci.BUSY
		jq.workers[i] = &tw
		jq.workers[i].Init(jq, i)
	}
}

//Setup Postgres Trigger & Wait for notifications on the channel
func (jq *JobQueueHandleT) setupTriggerAndChannel() {
	//create a postgres function that notifies on the specified channel
	sqlStmt := fmt.Sprintf(`DO $$ 
							BEGIN  
							IF  NOT EXISTS (select  from pg_proc where proname = 'wh_job_queue_status_notify') THEN
								CREATE FUNCTION wh_job_queue_status_notify() RETURNS TRIGGER AS '
								DECLARE 
									data json;
									notification json;
								
								BEGIN
								
									-- Convert the old or new row to JSON, based on the kind of action.
									-- Action = DELETE?             -> OLD row
									-- Action = INSERT or UPDATE?   -> NEW row
									IF (TG_OP = ''DELETE'') THEN
										data = row_to_json(OLD);
									ELSE
										data = row_to_json(NEW);
									END IF;
									
									-- Contruct the notification as a JSON string.
									notification = json_build_object(
													''table'',TG_TABLE_NAME,
													''action'', TG_OP,
													''data'', data);
									
													
									-- Execute pg_notify(channel, notification)
									PERFORM pg_notify(''%s'',notification::text);
									
									-- Result is ignored since this is an AFTER trigger
									RETURN NULL; 
								END;' LANGUAGE plpgsql;
							
							END IF;
							
							END $$  `, jq.cn.config.jobQueueNotifyChannel)

	_, err := jq.cn.dbHandle.Exec(sqlStmt)
	utils.AssertError(jq, err)

	//create the trigger
	sqlStmt = fmt.Sprintf(`DO $$ BEGIN
									CREATE TRIGGER %[1]s_status_trigger
											AFTER INSERT OR UPDATE OF status
											ON %[1]s
											FOR EACH ROW
										EXECUTE PROCEDURE wh_job_queue_status_notify();
									EXCEPTION
										WHEN others THEN null;
								END $$`, jq.cn.config.jobQueueTable)

	_, err = jq.cn.dbHandle.Exec(sqlStmt)
	utils.AssertError(jq, err)
}
