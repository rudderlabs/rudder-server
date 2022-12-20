package service

import (
	"context"
	"errors"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/sync/errgroup"
)

var pkgLogger = logger.NewLogger().Child("service")

type Looper struct {
	Svc JobSvc
}

func (l *Looper) Loop(ctx context.Context) error {
	pkgLogger.Infof("running regulation worker in infinite loop")

	maxLoopSleep := config.GetDuration("MAX_LOOP_SLEEP", 10, time.Minute)
	retryDelay := config.GetDuration("RETRY_DELAY", 60, time.Second)
	workers := config.GetInt("NUM_WORKERS", 5)

	g, ctx := errgroup.WithContext(ctx)
	b := make(chan *model.Job, 1)

	// job pickup loop
	g.Go(misc.WithBugsnag(func() error {
		var sleep time.Duration
		for {
			select {
			case <-ctx.Done():
				close(b)
				return nil
			case <-time.After(sleep):
				job, err := l.Svc.Pickup(ctx)
				if err != nil {
					if errors.Is(err, model.ErrNoRunnableJob) {
						sleep = maxLoopSleep
						continue
					}
					if errors.Is(err, model.ErrRequestTimeout) {
						pkgLogger.Errorf("context deadline exceeded... retrying after %d minute(s): %v", retryDelay, err)
						sleep = retryDelay
						continue
					}
					close(b)
					return err
				}
				sleep = 0
				b <- job
			}
		}
	}))

	// job processing loops
	for i := 0; i < workers; i++ {
		g.Go(misc.WithBugsnag(func() error {
			for job := range b {
				err := l.Svc.Process(ctx, job)
				if err != nil {
					pkgLogger.Errorf("Failed to process job id: %d workspaceID: %s destinationID: %s: %v", job.ID, job.WorkspaceID, job.DestinationID, err)
				}
			}
			return nil
		}))
	}

	return g.Wait()
}
