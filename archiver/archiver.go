package archiver

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/payload"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

type archiver struct {
	jobsDB          jobsdb.JobsDB
	storageProvider fileuploader.Provider
	log             logger.Logger
	stats           stats.Stats

	archiveTrigger           func() <-chan time.Time
	adaptivePayloadLimitFunc payload.AdaptiveLimiterFunc

	stopArchivalTrigger context.CancelFunc
	waitGroup           *errgroup.Group

	archiveFrom string
	config      struct {
		concurrency      config.ValueLoader[int]
		payloadLimit     func() int64
		jobsdbMaxRetries func() int
		instanceID       string
		eventsLimit      func() int
		minWorkerSleep   time.Duration
		uploadFrequency  time.Duration
		enabled          func() bool
		customVal        string
	}
}

func New(
	jobsDB jobsdb.JobsDB,
	storageProvider fileuploader.Provider,
	c *config.Config,
	statHandle stats.Stats,
	opts ...Option,
) *archiver {
	a := &archiver{
		jobsDB:          jobsDB,
		storageProvider: storageProvider,
		log:             logger.NewLogger().Child("archiver"),
		stats:           statHandle,

		archiveFrom: "gw",
		archiveTrigger: func() <-chan time.Time {
			return time.After(c.GetDurationVar(30, time.Second, "archival.ArchiveSleepDuration"))
		},
		adaptivePayloadLimitFunc: func(i int64) int64 { return i },
	}

	a.config.enabled = func() bool {
		return c.GetBoolVar(true, "archival.Enabled")
	}
	a.config.concurrency = c.GetReloadableIntVar(10, 1, "archival.ArchiveConcurrency")
	a.config.payloadLimit = func() int64 {
		return c.GetInt64Var(1*bytesize.GB, 1, "archival.ArchivePayloadSizeLimit")
	}
	a.config.jobsdbMaxRetries = func() int {
		if c.IsSet("JobsDB.Archiver.MaxRetries") {
			return c.GetIntVar(3, 1, "JobsDB.Archiver.MaxRetries")
		}
		return c.GetIntVar(3, 1, "JobsDB.MaxRetries")
	}
	a.config.eventsLimit = func() int {
		return c.GetIntVar(100000, 1, "archival.ArchiveEventsLimit")
	}
	a.config.instanceID = c.GetStringVar("1", "INSTANCE_ID")
	a.config.minWorkerSleep = c.GetDurationVar(1, time.Minute, "archival.MinWorkerSleep")
	a.config.uploadFrequency = c.GetDurationVar(5, time.Minute, "archival.UploadFrequency")
	a.config.customVal = c.GetStringVar("GW", "Gateway.CustomVal")

	for _, opt := range opts {
		opt(a)
	}

	return a
}

func (a *archiver) Start() error {
	a.log.Infon("Starting archiver")
	ctx, cancel := context.WithCancel(context.Background())
	a.stopArchivalTrigger = cancel
	g, ctx := errgroup.WithContext(ctx)
	a.waitGroup = g

	var limiterGroup sync.WaitGroup
	jobFetchLimit := kitsync.NewReloadableLimiter(
		ctx,
		&limiterGroup,
		"arc_fetch",
		a.config.concurrency,
		a.stats,
	)
	uploadLimit := kitsync.NewReloadableLimiter(
		ctx,
		&limiterGroup,
		"arc_upload",
		a.config.concurrency,
		a.stats,
	)
	statusUpdateLimit := kitsync.NewReloadableLimiter(
		ctx,
		&limiterGroup,
		"arc_update",
		a.config.concurrency,
		a.stats,
	)

	g.Go(func() error {
		workerPool := workerpool.New(
			ctx,
			func(sourceID string) workerpool.Worker {
				w := &worker{
					sourceID:         sourceID,
					jobsDB:           a.jobsDB,
					log:              a.log.Child("worker").Withn(obskit.SourceID(sourceID)),
					fetchLimiter:     jobFetchLimit,
					uploadLimiter:    uploadLimit,
					updateLimiter:    statusUpdateLimit,
					storageProvider:  a.storageProvider,
					archiveFrom:      a.archiveFrom,
					payloadLimitFunc: a.adaptivePayloadLimitFunc,
					stats:            a.stats,
				}
				w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(ctx)
				w.config.payloadLimit = a.config.payloadLimit
				w.config.instanceID = a.config.instanceID
				w.config.eventsLimit = a.config.eventsLimit
				w.config.minSleep = a.config.minWorkerSleep
				w.config.uploadFrequency = a.config.uploadFrequency
				w.config.jobsdbMaxRetries = a.config.jobsdbMaxRetries

				queryParams := &jobsdb.GetQueryParams{
					ParameterFilters: []jobsdb.ParameterFilterT{{Name: "source_id", Value: sourceID}},
					CustomValFilters: []string{a.config.customVal},
				}
				w.queryParams = *queryParams

				return w
			},
			a.log,
			workerpool.WithIdleTimeout(2*a.config.uploadFrequency),
		)
		defer workerPool.Shutdown()
		// pinger loop
		for {
			if a.config.enabled() {
				start := time.Now()
				sources, err := a.jobsDB.GetDistinctParameterValues(ctx, jobsdb.SourceID, "")
				a.stats.NewStat("arc_active_partitions_time", stats.TimerType).Since(start)
				if err != nil {
					if ctx.Err() != nil {
						return err
					}
					a.log.Errorn("Failed to fetch sources", obskit.Error(err))
					continue
				}
				a.stats.NewStat("arc_active_partitions", stats.GaugeType).Gauge(len(sources))
				for _, source := range sources {
					workerPool.PingWorker(source)
				}
			}

			select {
			case <-ctx.Done():
				return nil
			case <-a.archiveTrigger():
			}
		}
	})
	g.Go(func() error {
		limiterGroup.Wait()
		return nil
	})

	return nil
}

func (a *archiver) Stop() {
	a.log.Infon("Stopping archiver")
	a.stopArchivalTrigger()
	_ = a.waitGroup.Wait()
}
