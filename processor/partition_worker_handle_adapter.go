package processor

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/rsources"
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
		pipelinesPerPartition: h.Handle.config.pipelinesPerPartition,
		partitionProcessingDelay: func(partition string) config.ValueLoader[time.Duration] {
			return h.conf.GetReloadableDurationVar(0, time.Second, "Processor.preprocessDelay."+partition)
		},
	}
}

func (h *workerHandleAdapter) rsourcesService() rsources.JobService {
	return h.Handle.rsourcesService
}

func (h *workerHandleAdapter) stats() *processorStats {
	return &h.Handle.stats
}
