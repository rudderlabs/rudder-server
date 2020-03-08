package transform

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	ci "github.com/rudderlabs/rudder-server/warehouse/clusterinterface"
	utils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
)

//WorkerT is the worker state along with its behavior
type WorkerT struct {
	jobQueue            ci.JobQueueI
	WorkerNotificationQ chan *bytes.Buffer //JobQueue notifies workers on these channels

	IP_UUID string
	ID      int

	interrupt bool
}

//Init to initialise the worker
func (tw *WorkerT) Init(jobQueue ci.JobQueueI, workerIdx int) {
	tw.jobQueue = jobQueue
	tw.IP_UUID = fmt.Sprintf("%v-%v", utils.GetMyIP(), uuid.NewV4().String())
	tw.ID = workerIdx
	tw.WorkerNotificationQ = make(chan *bytes.Buffer)

	//Waits for notification from jobQueue and processes it
	rruntime.Go(func() {
		tw.process()
	})
}

//TearDown to release any resources held
func (tw *WorkerT) TearDown() {
	tw.jobQueue = nil
	tw.interrupt = true
}

//Waits for notification from jobQueue, process it & communicate back that it is free once done.
func (tw *WorkerT) process() {

	for {
		logger.Infof("WH-JQ: Setting transfom worker-%v free", tw.ID)
		//First set itself free & then wait on the channel
		tw.jobQueue.SetTransformWorker(ci.StatusMsg{Status: ci.FREE, WorkerIdx: tw.ID})

		//Kill worker routine on interrupt
		if tw.interrupt {
			break
		}

		// Wait for notifications from Master
		select {
		// Also listen for worker queue channel
		case jsonMsg := <-tw.WorkerNotificationQ:
			if tw.jobQueue != nil {
				tw.jobQueue.ClaimJob(jsonMsg, tw.ID)
			}
		}

	}
}

//HandleJob runs on worker thread to act on the job received from job queue
func (tw *WorkerT) HandleJob(staging_file_id string) error {
	logger.Infof("WH-JQ: Handling job id %v by worker-%v", staging_file_id, tw.ID)
	time.Sleep(5 * time.Second)
	if rand.Intn(2) == 1 {
		return nil
	}
	return utils.CreateError("because FAKE FAILURE")
}

//CleanUp the worker's footprint
func (tw *WorkerT) CleanUp(jobFailed bool, staging_file_id string) {
	logger.Infof("WH-JQ: Cleaning job id %v by worker-%v failed-%v", staging_file_id, tw.ID, jobFailed)
}
