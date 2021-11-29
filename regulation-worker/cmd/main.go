package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/rudderlabs/rudder-server/config"
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
			WorkspaceID: config.GetEnv("workspaceID", "1001"),
			URLPrefix:   config.GetEnv("urlPrefix", "http://localhost:35359"),
		},
		Deleter: &delete.DeleteFacade{
			AM: &api.API{
				Client:           &http.Client{},
				DestTransformURL: config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"),
			},
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

func withLoop(svc service.JobSvc) *service.Looper {

	return &service.Looper{
		Svc: svc,
	}
}
