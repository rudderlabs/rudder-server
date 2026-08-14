package router

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
)

func TestNewPartitionWorkerUsesPartitionNoOfWorkersOverride(t *testing.T) {
	config.Reset()
	t.Cleanup(config.Reset)
	config.Set("Router.ITERABLE.partition-a.noOfWorkers", 5)

	rt := &Handle{
		logger:                  logger.NOP,
		destType:                "ITERABLE",
		noOfWorkers:             2,
		maxNoOfJobsPerChannel:   1000,
		noOfJobsPerChannel:      1,
		reloadableConfig:        newPartitionWorkerTestReloadableConfig(),
		newBarrierFn:            func() *eventorder.Barrier { return eventorder.NewBarrier() },
		guaranteeUserEventOrder: true,
	}

	partitionAWorker := newPartitionWorker(context.Background(), rt, "partition-a")
	defer partitionAWorker.Stop()
	require.Len(t, partitionAWorker.workers, 5)
	assertDynamicBufferCapacity(t, partitionAWorker, 20)

	partitionBWorker := newPartitionWorker(context.Background(), rt, "partition-b")
	defer partitionBWorker.Stop()
	require.Len(t, partitionBWorker.workers, rt.noOfWorkers)
	assertDynamicBufferCapacity(t, partitionBWorker, 50)
}

func newPartitionWorkerTestReloadableConfig() *reloadableConfig {
	return &reloadableConfig{
		jobQueryBatchSize:                 config.SingleValueLoader(100),
		noOfJobsToBatchInAWorker:          config.SingleValueLoader(1),
		jobsBatchTimeout:                  config.SingleValueLoader(time.Hour),
		enableDynamicBufferSizeCalculator: config.SingleValueLoader(true),
		dynamicBufferSizeScalingFactor:    config.SingleValueLoader(1.0),
		dynamicBufferSizeMinimum:          config.SingleValueLoader(1),
	}
}

func assertDynamicBufferCapacity(t *testing.T, pw *partitionWorker, expected int) {
	t.Helper()
	pw.pickupBatchSizeGauge.Gauge(100)
	worker := pw.workers[0]
	worker.workLoopThroughput.Observe(1)
	require.Equal(t, expected, worker.workerBuffer.refreshCapacity())
}
