package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rudderlabs/rudder-server/admin"
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
	admin.Init()
	if err := backendconfig.Setup(nil); err != nil {
		panic(fmt.Errorf("error while setting up backend config: %v", err))
	}
	dest := &destination.DestMiddleware{
		Dest: backendconfig.DefaultBackendConfig,
	}
	workspaceId, err := dest.GetWorkspaceId(ctx)
	if err != nil {
		panic(fmt.Errorf("error while getting workspaceId: %w", err))
	}

	svc := service.JobSvc{
		API: &client.JobAPI{
			Client:         &http.Client{Timeout: config.GetDuration("HttpClient.regulationWorker.timeout", 30, time.Second)},
			URLPrefix:      config.MustGetString("CONFIG_BACKEND_URL"),
			WorkspaceToken: config.MustGetString("CONFIG_BACKEND_TOKEN"),
			WorkspaceID:    workspaceId,
		},
		DestDetail: dest,
		Deleter: delete.NewRouter(
			&kvstore.KVDeleteManager{},
			&batch.BatchManager{
				FMFactory:  &filemanager.FileManagerFactoryT{},
				FilesLimit: config.GetInt("REGULATION_WORKER_FILES_LIMIT", 1000),
			},
			&api.APIManager{
				Client:           &http.Client{Timeout: config.GetDuration("HttpClient.regulationWorker.timeout", 30, time.Second)},
				DestTransformURL: config.MustGetString("DEST_TRANSFORM_URL"),
			}),
	}

	pkgLogger.Infof("calling looper with service: %v", svc)
	l := withLoop(svc)
	err = l.Loop(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		pkgLogger.Errorf("error: %v", err)
		panic(err)
	}
}

func withLoop(svc service.JobSvc) *service.Looper {
	return &service.Looper{
		Svc: svc,
	}
}
