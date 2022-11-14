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
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var pkgLogger = logger.NewLogger().Child("regulation-worker")

func init() {
	initialize.Init()
	backendconfig.Init()
	oauth.Init()
}

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
	err := misc.WithBugsnag(func() error {
		return Run(ctx)
	})()
	if err != nil {
		panic(err)
	}
}

func Run(ctx context.Context) error {
	admin.Init()

	if err := backendconfig.Setup(nil); err != nil {
		return fmt.Errorf("error while setting up backend config: %v", err)
	}

	// the above backendconfig.Setup() sets `DefaultBackendConfig` variable to `namespaceConfig` or `singleWorkspaceConfig` based on `DEPLOYMENT_TYPE` env.
	dest := &destination.DestMiddleware{
		Dest: backendconfig.DefaultBackendConfig,
	}
	// setting up oauth
	OAuth := oauth.NewOAuthErrorHandler(backendconfig.DefaultBackendConfig, oauth.WithRudderFlow(oauth.RudderFlow_Delete))

	svc := service.JobSvc{
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

	managerClient := &http.Client{Timeout: config.GetDuration("HttpClient.regulationWorker.regulationManager.timeout", 60, time.Second)}
	urlPrefix := config.MustGetString("CONFIG_BACKEND_URL")
	deploymentType := config.GetString("DEPLOYMENT_TYPE", string(deployment.DedicatedType))
	if deploymentType == string(deployment.MultiTenantType) {
		svc.API = &client.MultiTenantJobAPI{
			Client:         managerClient,
			URLPrefix:      urlPrefix,
			NamespaceID:    config.MustGetString("WORKSPACE_NAMESPACE"),
			NamespaceToken: config.MustGetString("HOSTED_SERVICE_SECRET"),
		}
	} else {
		workspaceId, err := dest.GetWorkspaceId(ctx)
		if err != nil {
			return fmt.Errorf("error while getting workspaceId: %w", err)
		}
		svc.API = &client.SingleTenantJobAPI{
			Client:         managerClient,
			URLPrefix:      urlPrefix,
			WorkspaceToken: config.MustGetString("CONFIG_BACKEND_TOKEN"),
			WorkspaceID:    workspaceId,
		}
	}

	pkgLogger.Infof("calling looper with service: %v", svc)
	l := withLoop(svc)
	err := l.Loop(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("error while running looper: %w", err)
	}
	return nil
}

func withLoop(svc service.JobSvc) *service.Looper {
	return &service.Looper{
		Svc: svc,
	}
}
