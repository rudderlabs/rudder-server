package service

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Looper struct {
	Svc JobSvc
}

func (l *Looper) Loop(ctx context.Context) error {
	for {
		fmt.Println("loop iterator tracker")
		err := l.Svc.JobSvc(ctx)
		if err == model.ErrNoRunnableJob {
			time.Sleep(10 * time.Minute)
		} else if err != nil {
			return err
		}
	}
}
