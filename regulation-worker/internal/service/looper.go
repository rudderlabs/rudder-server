package service

import (
	"context"
	"fmt"
	"os"
	"strconv"
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

	interval, err := getenvInt("INTERVAL_IN_MINUTES", 10)
	if err != nil {
		return fmt.Errorf("reading value: %s from env: %s", "INTERVAL_IN_MINUTES", err.Error())
	}

	for {

		err := l.Svc.JobSvc(ctx)

		if err == model.ErrNoRunnableJob {
			pkgLogger.Debugf("no runnable job found... sleeping")
			if err := misc.SleepCtx(ctx, time.Duration(interval)*time.Minute); err != nil {
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

func getenvInt(key string, fallback int) (int, error) {
	k := os.Getenv(key)
	if len(k) == 0 {
		return fallback, nil
	}
	return strconv.Atoi(k)
}
