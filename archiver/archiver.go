package archiver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/payload"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

type archiver struct {
	jobsDB          jobsdb.JobsDB
	storageProvider fileuploader.Provider
	log             logger.Logger
	statHandle      stats.Stats

	archiveTrigger           func() <-chan time.Time
	adaptivePayloadLimitFunc payload.AdaptiveLimiterFunc
	partitionStrategy        partitionStrategy

	stopArchivalTrigger context.CancelFunc
	limiterWaitGroup    *sync.WaitGroup

	archiveFrom string
	config      struct {
		concurrency      func() int
		payloadLimit     func() int64
		maxRetryAttempts func() int
		instanceID       string
		eventsLimit      func() int
		minWorkerSleep   time.Duration
		maxWorkerSleep   time.Duration
	}
}

func New(
	jobsDB jobsdb.JobsDB,
	storageProvider fileuploader.Provider,
	c config,
	statHandle stats.Stats,
	opts ...Option,
) *archiver {
	a := &archiver{
		jobsDB:          jobsDB,
		storageProvider: storageProvider,
		log:             logger.NewLogger().Child("archiver"),
		statHandle:      statHandle,
	}

	a.config.concurrency = func() int {
		return c.GetInt("archival.ArchiveConcurrency", 10)
	}
	a.config.payloadLimit = func() int64 {
		return c.GetInt64("archival.ArchivePayloadSizeLimit", 1*bytesize.GB)
	}
	a.config.maxRetryAttempts = func() int {
		return c.GetInt("archival.MaxRetryAttempts", 3)
	}
	a.config.eventsLimit = func() int {
		return c.GetInt("archival.ArchiveEventsLimit", 100000)
	}
	a.config.instanceID = c.GetString("INSTANCE_ID", "1")
	a.config.minWorkerSleep = c.GetDuration("archival.MinWorkerSleep", 1, time.Minute)
	a.config.maxWorkerSleep = c.GetDuration("archival.MaxWorkerSleep", 5, time.Minute)

	for _, opt := range opts {
		opt(a)
	}

	if a.archiveTrigger == nil {
		a.archiveTrigger = func() <-chan time.Time {
			return time.After(c.GetDuration(
				"archival.ArchiveSleepDuration",
				30,
				time.Second,
			))
		}
	}
	if a.adaptivePayloadLimitFunc == nil {
		a.adaptivePayloadLimitFunc = func(i int64) int64 { return i }
	}
	if a.partitionStrategy == nil {
		a.partitionStrategy = getPartitionStrategy(
			c.GetString("archival.PartitionStrategy", SourcePartition),
		)
	}

	return a
}

func (a *archiver) Start() error {
	a.log.Info("Starting archiver")
	ctx, cancel := context.WithCancel(context.Background())
	a.stopArchivalTrigger = cancel
	g, ctx := errgroup.WithContext(ctx)
	a.limiterWaitGroup = &sync.WaitGroup{}

	jobFetchLimit := kitsync.NewLimiter(
		ctx,
		a.limiterWaitGroup,
		"arc_work_jobFetch",
		a.config.concurrency(),
		a.statHandle,
	)
	uploadLimit := kitsync.NewLimiter(
		ctx,
		a.limiterWaitGroup,
		"arc_work_upload",
		a.config.concurrency(),
		a.statHandle,
	)
	statusUpdateLimit := kitsync.NewLimiter(
		ctx,
		a.limiterWaitGroup,
		"arc_work_statusUpdate",
		a.config.concurrency(),
		a.statHandle,
	)

	g.Go(func() error {
		workerPool := workerpool.New(
			ctx,
			func(partition string) workerpool.Worker {
				w := &worker{
					partition:           partition,
					jobsDB:              a.jobsDB,
					log:                 a.log.Child(fmt.Sprintf("archiveWorker-%s", partition)),
					jobFetchLimiter:     jobFetchLimit,
					uploadLimiter:       uploadLimit,
					statusUpdateLimiter: statusUpdateLimit,
					storageProvider:     a.storageProvider,
					archiveFrom:         a.archiveFrom,
					payloadLimitFunc:    a.adaptivePayloadLimitFunc,
				}
				w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(ctx)
				w.config.payloadLimit = a.config.payloadLimit
				w.config.maxRetryAttempts = a.config.maxRetryAttempts
				w.config.instanceID = a.config.instanceID
				w.config.eventsLimit = a.config.eventsLimit
				w.config.minSleep = a.config.minWorkerSleep
				w.config.maxSleep = a.config.maxWorkerSleep

				queryParams := &jobsdb.GetQueryParamsT{}
				a.partitionStrategy.augmentQueryParams(partition, queryParams)
				w.queryParams = *queryParams

				return w
			},
			a.log,
			workerpool.WithIdleTimeout(2*a.config.maxWorkerSleep),
		)
		defer workerPool.Shutdown()
		// pinger loop
		for {
			sources, err := a.partitionStrategy.activePartitions(ctx, a.jobsDB)
			a.log.Infof("Archiving %ss: %v", a.partitionStrategy, sources)
			if err != nil {
				a.log.Error("Failed to fetch sources", err)
				continue
			}
			for _, source := range sources {
				workerPool.PingWorker(source)
			}

			select {
			case <-ctx.Done():
				return nil
			case <-a.archiveTrigger():
			}
		}
	})

	return nil
}

func (a *archiver) Stop() {
	a.log.Info("Stopping archiver")
	a.stopArchivalTrigger()
	a.limiterWaitGroup.Wait()
}
