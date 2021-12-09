package service

import (
	"context"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type Looper struct {
	Backoff backoff.BackOffContext
	Svc     JobSvc
}

func (l *Looper) Loop(ctx context.Context) error {
	for {
		err := l.Svc.JobSvc(ctx)
		if err == model.ErrNoRunnableJob {
			if ctxCanceled := misc.SleepCtx(ctx, 10*time.Minute); ctxCanceled {
				return nil
			}
		} else if err != nil {
			return err
		}
	}
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(delay):
		return false
	}
}
