package router

// workerSlot represents a reserved slot in the worker's input channel
type workerSlot struct {
	worker *worker
}

// Release releases the reserved slot from the worker's input channel
func (s *workerSlot) Release() {
	s.worker.releaseSlot()
}

// Use sends a job into the worker's input channel
func (s *workerSlot) Use(wj workerJob) {
	s.worker.accept(wj)
}
