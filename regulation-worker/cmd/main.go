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
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var pkgLogger = logger.NewLogger().Child("regulation-worker")

func main() {
	pkgLogger.Info("starting regulation-worker")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		<-c
		cancel()
		close(c)
	}()
	err := Run(ctx)
	if err != nil {
		pkgLogger.Errorf("error while running regulation worker: %v", err)
		os.Exit(1)

	}
}

func Run(ctx context.Context) error {
	admin.Init()
	stats.Default.Start(ctx)
	backendconfig.Init()
	if err := backendconfig.Setup(nil); err != nil {
		return fmt.Errorf("error while setting up backend config: %v", err)
	}
	dest := &destination.DestinationConfig{
		Dest: backendconfig.DefaultBackendConfig,
	}

	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("error while getting deployment type: %v", err)
	}
	pkgLogger.Infof("starting regulation worker in %v mode", deploymentType)
	backendconfig.DefaultBackendConfig.StartWithIDs(ctx, "")
	backendconfig.DefaultBackendConfig.WaitForConfig(ctx)
	identity := dest.Dest.Identity()
	rruntime.Go(func() {
		dest.BackendConfigSubscriber(ctx)
	})

	// setting up oauth
	OAuth := oauth.NewOAuthErrorHandler(backendconfig.DefaultBackendConfig, oauth.WithRudderFlow(oauth.RudderFlow_Delete))

	svc := service.JobSvc{
		API: &client.JobAPI{
			Client:    &http.Client{Timeout: config.GetDuration("HttpClient.regulationWorker.regulationManager.timeout", 60, time.Second)},
			URLPrefix: config.MustGetString("CONFIG_BACKEND_URL"),
			Identity:  identity,
			Mode:      deploymentType,
		},
		DestDetail: dest,
		Deleter: delete.NewRouter(
			&kvstore.KVDeleteManager{},
			&batch.BatchManager{
				FMFactory:  &filemanager.FileManagerFactoryT{},
				FilesLimit: config.GetInt("REGULATION_WORKER_FILES_LIMIT", 1000),
			},
			&api.APIManager{
				Client:                       &http.Client{Timeout: config.GetDuration("HttpClient.regulationWorker.transformer.timeout", 60, time.Second)},
				DestTransformURL:             config.MustGetString("DEST_TRANSFORM_URL"),
				OAuth:                        OAuth,
				MaxOAuthRefreshRetryAttempts: config.GetInt("RegulationWorker.oauth.maxRefreshRetryAttempts", 1),
			}),
	}

	pkgLogger.Infof("calling looper with service: %v", svc)
	l := withLoop(svc)
	err = misc.WithBugsnag(func() error {
		return l.Loop(ctx)
	})()
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("error: %v", err)
	}
	return nil
}

func withLoop(svc service.JobSvc) *service.Looper {
	return &service.Looper{
		Svc: svc,
	}
}
