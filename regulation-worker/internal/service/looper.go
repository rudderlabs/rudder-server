package service

import (
	"context"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Looper struct {
	Backoff backoff.BackOffContext
	Svc     JobSvc
}

func (l *Looper) Loop(ctx context.Context) error {
	for {
		err := l.Svc.JobSvc(ctx)
		if err == model.ErrNoRunnableJob {
			time.Sleep(10 * time.Minute)
		} else if err != nil {
			return err
		}
	}
}
