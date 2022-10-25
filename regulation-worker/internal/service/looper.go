package service

import (
	"context"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var pkgLogger = logger.NewLogger().Child("service")

type Looper struct {
	Backoff backoff.BackOffContext
	Svc     JobSvc
}

func (l *Looper) Loop(ctx context.Context) error {
	pkgLogger.Infof("running regulation worker in infinite loop")
	totalJobTime := stats.Default.NewTaggedStat("regulation_worker_loop_time", stats.TimerType, stats.Tags{})
	for {
		pkgLogger.Infof("-------starting regulation worker new loop-------------")
		loopTime := time.Now()
		err := l.Svc.JobSvc(ctx)
		if err == model.ErrNoRunnableJob {
			pkgLogger.Debugf("no runnable job found... sleeping")
			if err := misc.SleepCtx(ctx, 10*time.Minute); err != nil {
				pkgLogger.Debugf("context cancelled... exiting infinite loop %v", err)
				return nil
			}
		} else if err != nil {
			return err
		}
		pkgLogger.Info("regulation worker loop time: ", time.Since(loopTime))
		totalJobTime.Since(loopTime)

	}
}
