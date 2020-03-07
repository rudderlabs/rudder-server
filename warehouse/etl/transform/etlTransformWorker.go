package transform

import (
	"bytes"
	"fmt"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/warehouse/etl"
	. "github.com/rudderlabs/rudder-server/warehouse/etl/jobqueueinterface"
	uuid "github.com/satori/go.uuid"
)

type ETLTransformWorkerT struct {
	jobQueue                  TransformWorkerJobQueueIface
	WorkerNotificationChannel chan bytes.Buffer //JobQueue notifies workers on these channels
	interrupt                 bool
	workerId                  string
	workerIdx                 int
}

// type to denote worker status
type WorkerStatus string

const (
	BUSY = "busy"
	FREE = "free"
)

type StatusMsg struct {
	Status    WorkerStatus
	WorkerIdx int
}

//initialise the worker
func (tw *ETLTransformWorkerT) Init(jobQueue TransformWorkerJobQueueIface, workerIdx int) {
	tw.jobQueue = jobQueue
	tw.workerId = fmt.Sprintf("%v-%v", etl.GetMyIp(), uuid.NewV4().String())
	tw.workerIdx = workerIdx
	tw.WorkerNotificationChannel = make(chan bytes.Buffer)

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
	//First set itself free & then wait on the channel
	tw.jobQueue.SetTransformWorkerFree(tw.workerIdx)
	for {
		if tw.interrupt {
			break
		}

		// Wait for notifications from Master
		switch {
			case statusMsg := <-jq.jobQueueNotificationChanne
		}
	}
}
