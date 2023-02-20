package processor

import (
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// workerHandleAdapter is a wrapper around processor.Handle that implements the workerHandle interface
type workerHandleAdapter struct {
	*Handle
}

func (h *workerHandleAdapter) logger() logger.Logger {
	return h.Handle.logger
}

func (h *workerHandleAdapter) config() workerHandleConfig {
	return workerHandleConfig{
		enablePipelining:      h.Handle.config.enablePipelining,
		pipelineBufferedItems: h.Handle.config.pipelineBufferedItems,
		maxEventsToProcess:    h.Handle.config.maxEventsToProcess,
		subJobSize:            h.Handle.config.subJobSize,
		readLoopSleep:         h.Handle.config.readLoopSleep,
		maxLoopSleep:          h.Handle.config.maxLoopSleep,
	}
}

func (h *workerHandleAdapter) rsourcesService() rsources.JobService {
	return h.Handle.rsourcesService
}

func (h *workerHandleAdapter) stats() *processorStats {
	return &h.Handle.stats
}
