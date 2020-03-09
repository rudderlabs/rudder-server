package clusterinterface

import "bytes"

//WorkerStatus type to denote worker status
type WorkerStatus string

//various Worker states
const (
	BUSY = "busy"
	FREE = "free"
)

//StatusMsg contains workerId and its state
type StatusMsg struct {
	Status    WorkerStatus
	WorkerIdx int
}

//ClusterConfig parameters
type ClusterConfig struct {
	JobQueueTable         string
	JobQueueNotifyChannel string
	WorkerInfoTable       string
}

//JobQueueI is the interface to jobqueue from its dependants
type JobQueueI interface {
	SetTransformWorker(status StatusMsg)
	ClaimJob(jsonMsg *bytes.Buffer, workerIdx int)
}
