package service

import (
	"context"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
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
	for {

		err := l.Svc.JobSvc(ctx)

		if err == model.ErrNoRunnableJob {
			pkgLogger.Debugf("no runnable job found... sleeping")
			if err := misc.SleepCtx(ctx, 10*time.Minute); err != nil {
				pkgLogger.Debugf("context cancelled... exiting infinite loop %v", err)
				return nil
			}

			continue
		}

		if err != nil {
			return err
		}
	}
}
