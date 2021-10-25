package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		<-c
		cancel()
		close(c)
	}()

	Run(ctx)

}

func Run(ctx context.Context) {
	svc := service.JobSvc{
		API: &client.JobAPI{
			WorkspaceID: getEnv("workspaceID", "1001"),
			URLPrefix:   getEnv("urlPrefix", "http://localhost:35359"),
		},
		Deleter: &delete.Deleter{},
	}

	l := withLoop(ctx, svc)
	err := l.Loop(ctx)
	if err != nil {
		panic(err)
	}

}

func getEnv(name, defaultValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defaultValue
}

func withLoop(ctx context.Context, svc service.JobSvc) *service.Looper {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Hour * 72
	boCtx := backoff.WithContext(bo, ctx)

	return &service.Looper{
		Svc:     svc,
		Backoff: boCtx,
	}
}
