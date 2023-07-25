package jobs_archival

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/isolation"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/payload"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

var (
	// tmpDir       = lo.Must(misc.CreateTMPDIR()) + "/rudder-backups/"
	concurrency  = func() int { return config.GetInt("archival.ArchiveConcurrency", 10) }
	payloadLimit = func() int64 { return config.GetInt64("archival.ArchivePayloadSizeLimit", 1*bytesize.GB) }
	eventsLimit  = func() int { return config.GetInt("archival.ArchiveEventsLimit", 100000) }
	sourceParam  = func(sourceID string) []jobsdb.ParameterFilterT {
		return []jobsdb.ParameterFilterT{{
			Name:  "source_id",
			Value: sourceID,
		}}
	}
	maxRetryAttempts = func() int { return config.GetInt("archival.MaxRetryAttempts", 3) }
	instanceID       = config.GetString("INSTANCE_ID", "1")
)

type archiver struct {
	jobsDB              jobsdb.JobsDB
	storageProvider     fileuploader.Provider
	log                 logger.Logger
	stopArchivalTrigger context.CancelFunc
	archiveTrigger      func() <-chan time.Time
	limiter             payload.AdaptiveLimiterFunc
	archiveFrom         string
}

func New(jobsDB jobsdb.JobsDB, storageProvider fileuploader.Provider, opts ...Option) *archiver {
	a := &archiver{
		jobsDB:          jobsDB,
		storageProvider: storageProvider,
		log:             logger.NewLogger().Child("archiver"),
	}
	for _, opt := range opts {
		opt(a)
	}
	if a.archiveTrigger == nil {
		a.archiveTrigger = func() <-chan time.Time {
			return time.After(config.GetDuration(
				"archival.ArchiveSleepDuration",
				5,
				time.Minute,
			))
		}
	}
	if a.limiter == nil {
		a.limiter = func(i int64) int64 { return i }
	}
	return a
}

type Option func(*archiver)

func WithAdaptiveLimit(limiter payload.AdaptiveLimiterFunc) Option {
	return func(a *archiver) {
		a.limiter = limiter
	}
}

func WithArchiveTrigger(trigger func() <-chan time.Time) Option {
	return func(a *archiver) {
		a.archiveTrigger = trigger
	}
}

func WithArchiveFrom(from string) Option {
	return func(a *archiver) {
		a.archiveFrom = from
	}
}

func (a *archiver) Start() error {
	a.log.Info("Starting archiver")
	partitionsFunc := isolation.GetSourceStrategy().ActivePartitions
	ctx, cancel := context.WithCancel(context.Background())
	a.stopArchivalTrigger = cancel
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		workerPool := workerpool.New(
			ctx,
			func(partition string) workerpool.Worker {
				w := &worker{
					partition: partition,
					archiver:  a,
					log:       a.log.Child(fmt.Sprintf("archiveWorker-%s", partition)),
				}
				w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(ctx)
				return w
			},
			a.log,
			workerpool.WithIdleTimeout(0),
		)
		defer workerPool.Shutdown()
		// pinger loop
		for {
			if err := misc.AfterCtx(ctx, a.archiveTrigger); err != nil {
				a.log.Error("Failed to wait for archival trigger: ", err)
				return err
			}
			sources, err := partitionsFunc(ctx, a.jobsDB)
			a.log.Infof("Archiving %v sources", sources)
			if err != nil {
				a.log.Error("Failed to fetch sources", err)
				continue
			}
			g, _ := errgroup.WithContext(ctx)
			g.SetLimit(concurrency())
			for _, source := range sources {
				source := source
				g.Go(func() error {
					workerPool.PingWorker(source)
					return nil
				})
			}
			_ = g.Wait()
		}
	})

	return nil
}

func (a *archiver) Stop() {
	a.log.Info("Stopping archiver")
	a.stopArchivalTrigger()
}
