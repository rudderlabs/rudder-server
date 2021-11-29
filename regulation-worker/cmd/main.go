package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/custom"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/destination"
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
		Deleter: &delete.DeleteFacade{
			AM: &api.Mock_apiWorker{},
			BM: &batch.BatchManager{},
			CM: &custom.Mock_KVStoreWorker{},
		},
		DestDetail: &destination.DestMiddleware{
			Dest:    &backendconfig.WorkspaceConfig{},
			DestCat: &destination.DestCategory{},
		},
	}
	l := withLoop(svc)
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

func withLoop(svc service.JobSvc) *service.Looper {

	return &service.Looper{
		Svc: svc,
	}
}
