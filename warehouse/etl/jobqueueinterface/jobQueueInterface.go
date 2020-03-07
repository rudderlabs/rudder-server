package jobqueueinterface

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
type TransformWorkerJobQueueIface interface
{
	SetTransformWorker(status StatusMsg)
}