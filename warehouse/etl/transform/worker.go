package transform

import (
	"bytes"
	"fmt"
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
	interrupt           bool
	uuidIP              string
	workerIdx           int
}

//Init to initialise the worker
func (tw *WorkerT) Init(jobQueue ci.JobQueueI, workerIdx int) {
	tw.jobQueue = jobQueue
	tw.uuidIP = fmt.Sprintf("%v-%v", utils.GetMyIP(), uuid.NewV4().String())
	tw.workerIdx = workerIdx
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
		logger.Infof("WH-JQ: Setting transfom worker free")
		//First set itself free & then wait on the channel
		tw.jobQueue.SetTransformWorker(ci.StatusMsg{Status: ci.FREE, WorkerIdx: tw.workerIdx})

		//Kill worker routine on interrupt
		if tw.interrupt {
			break
		}

		// Wait for notifications from Master
		select {
		// Also listen for worker queue channel
		case jsonMsg := <-tw.WorkerNotificationQ:
			//TODO: claim job with SKIP LOCK and update status
			logger.Infof("WH-JQ: worker-%v trying to claim job %v", tw.workerIdx, string(jsonMsg.Bytes()))

		}

		time.Sleep(5 * time.Second)

	}

}
