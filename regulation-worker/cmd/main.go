package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/kvstore"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/destination"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/initialize"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var pkgLogger = logger.NewLogger().Child("regulation-worker")

func main() {

	initialize.Init()
	backendconfig.Init()

	pkgLogger.Info("starting regulation-worker")
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

	dest := &destination.DestMiddleware{
		Dest: &backendconfig.SingleWorkspaceConfig{},
	}
	workspaceId, err := dest.GetWorkspaceId(ctx)
	if err != nil {
		panic("error while getting workspaceId")
	}

	svc := service.JobSvc{
		API: &client.JobAPI{
			Client:         &http.Client{Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)},
			URLPrefix:      config.MustGetEnv("CONFIG_BACKEND_URL"),
			WorkspaceToken: config.MustGetEnv("CONFIG_BACKEND_TOKEN"),
			WorkspaceID:    workspaceId,
		},
		DestDetail: dest,
		Deleter: delete.NewRouter(
			&kvstore.KVDeleteManager{},
			&batch.BatchManager{
				FMFactory: &filemanager.FileManagerFactoryT{},
			},
			&api.APIManager{
				Client:           &http.Client{Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)},
				DestTransformURL: config.MustGetEnv("DEST_TRANSFORM_URL"),
			}),
	}

	pkgLogger.Infof("calling looper with service: %v", svc)
	l := withLoop(svc)
	err = l.Loop(ctx)
	if err != nil {
		pkgLogger.Errorf("error: %v", err)
		panic(err)
	}

}

func withLoop(svc service.JobSvc) *service.Looper {
	return &service.Looper{
		Svc: svc,
	}
}
