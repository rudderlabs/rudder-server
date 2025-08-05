package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var pkgLogger = logger.NewLogger().Child("service")

type Looper struct {
	Svc JobSvc
}

func (l *Looper) Loop(ctx context.Context) error {
	pkgLogger.Infon("running regulation worker in infinite loop")

	interval, err := getenvInt("INTERVAL_IN_MINUTES", 10)
	if err != nil {
		return fmt.Errorf("reading value: %s from env: %s", "INTERVAL_IN_MINUTES", err.Error())
	}
	retryDelay, err := getenvInt("RETRY_DELAY_IN_SECONDS", 60)
	if err != nil {
		return fmt.Errorf("reading value: %s from env: %s", "INTERVAL_IN_MINUTES", err.Error())
	}

	for {

		err := l.Svc.JobSvc(ctx)

		if errors.Is(err, model.ErrNoRunnableJob) {
			pkgLogger.Debugn("no runnable job found... sleeping")
			if err := misc.SleepCtx(ctx, time.Duration(interval)*time.Minute); err != nil {
				pkgLogger.Debugn("context cancelled... exiting infinite loop", obskit.Error(err))
				return nil
			}
			continue
		}
		// this is to make sure that we don't panic when any of the API call fails with deadline exceeded error.
		if errors.Is(err, model.ErrRequestTimeout) {
			pkgLogger.Errorn("context deadline exceeded... retrying after minutes",
				logger.NewIntField("retryDelaySeconds", int64(retryDelay)),
				obskit.Error(err))
			if err := misc.SleepCtx(ctx, time.Duration(retryDelay)*time.Second); err != nil {
				pkgLogger.Debugn("context cancelled... exiting infinite loop", obskit.Error(err))
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
	if k == "" {
		return fallback, nil
	}
	return strconv.Atoi(k)
}
