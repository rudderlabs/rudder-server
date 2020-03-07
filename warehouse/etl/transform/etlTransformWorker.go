package transform

import (
	"bytes"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/etl"
	. "github.com/rudderlabs/rudder-server/warehouse/etl/jobqueueinterface"
	uuid "github.com/satori/go.uuid"
)

type ETLTransformWorkerT struct {
	jobQueue                  TransformWorkerJobQueueIface
	WorkerNotificationChannel chan *bytes.Buffer //JobQueue notifies workers on these channels
	interrupt                 bool
	workerId                  string
	workerIdx                 int
}

//initialise the worker
func (tw *ETLTransformWorkerT) Init(jobQueue TransformWorkerJobQueueIface, workerIdx int) {
	tw.jobQueue = jobQueue
	tw.workerId = fmt.Sprintf("%v-%v", etl.GetMyIp(), uuid.NewV4().String())
	tw.workerIdx = workerIdx
	tw.WorkerNotificationChannel = make(chan *bytes.Buffer)

	//Waits for notification from jobQueue and processes it
	rruntime.Go(func() {
		tw.process()
	})
}

//Release any resources held
func (tw *ETLTransformWorkerT) TearDown() {
	tw.jobQueue = nil
	tw.interrupt = true
}

//Waits for notification from jobQueue, process it & communicate back that it is free once done.
func (tw *ETLTransformWorkerT) process() {

	for {
		logger.Infof("WH-JQ: Setting transfom worker free")
		//First set itself free & then wait on the channel
		tw.jobQueue.SetTransformWorker(StatusMsg{Status: FREE, WorkerIdx: tw.workerIdx})

		//Kill worker routine on interrupt
		if tw.interrupt {
			break
		}

		// Wait for notifications from Master
		select {
		// Also listen for worker queue channel
		case jsonMsg := <-tw.WorkerNotificationChannel:
			//TODO: claim job with SKIP LOCK and update status
			logger.Infof("WH-JQ: worker-%v trying to claim job %v", tw.workerIdx, string(jsonMsg.Bytes()))

		}

		time.Sleep(5 * time.Second)

	}

}
