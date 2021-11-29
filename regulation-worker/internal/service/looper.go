package service

import (
	"context"
	"fmt"
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
		fmt.Println("loop new iterator")
		err := l.Svc.JobSvc(ctx)
		if err == model.ErrNoRunnableJob {
			sleepContext(ctx, 10*time.Minute)
			time.Sleep(10 * time.Minute)
		} else if err != nil {
			return err
		}
	}
}

func sleepContext(ctx context.Context, delay time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}
