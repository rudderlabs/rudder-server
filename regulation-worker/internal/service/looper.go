package service

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Looper struct {
	Svc JobSvc
}

func (l *Looper) Loop() error{
	for {
		err := l.Svc.JobSvc(context.Background())
		if err == model.ErrNoRunnableJob {
			time.Sleep(10 * time.Minute)
		}else if err!=nil{
			return err
		}
	}
}
