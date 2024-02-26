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

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/kvstore"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/destination"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"

	rudderSync "github.com/rudderlabs/rudder-go-kit/sync"
	oauthV2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	OAuthHttpClient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
)

var pkgLogger = logger.NewLogger().Child("regulation-worker")

func main() {
	pkgLogger.Info("Starting regulation-worker")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := Run(ctx)
	if ctx.Err() == nil {
		cancel()
	}
	if err != nil {
		pkgLogger.Errorf("Running regulation worker: %v", err)
		os.Exit(1)
	}
}

func Run(ctx context.Context) error {
	config.Set("Diagnostics.enableDiagnostics", false)

	stats.Default = stats.NewStats(config.Default, logger.Default, svcMetric.Instance,
		stats.WithServiceName("regulation-worker"),
	)
	if err := stats.Default.Start(ctx, rruntime.GoRoutineFactory); err != nil {
		return fmt.Errorf("failed to start stats: %w", err)
	}
	defer stats.Default.Stop()

	admin.Init()
	misc.Init()
	diagnostics.Init()
	backendconfig.Init()

	if err := backendconfig.Setup(nil); err != nil {
		return fmt.Errorf("setting up backend config: %w", err)
	}
	dest := &destination.DestinationConfig{
		Dest: backendconfig.DefaultBackendConfig,
	}

	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("getting deployment type: %w", err)
	}
	pkgLogger.Infof("Running regulation worker in %s mode", deploymentType)
	backendconfig.DefaultBackendConfig.StartWithIDs(ctx, "")
	backendconfig.DefaultBackendConfig.WaitForConfig(ctx)
	identity := backendconfig.DefaultBackendConfig.Identity()
	dest.Start(ctx)
	isOAuthV2RelVar := config.GetReloadableBoolVar(false, "RegulationWorker.oauthV2Enabled")
	regTimeoutReloadVar := config.GetReloadableDurationVar(60, time.Second, "HttpClient.regulationWorker.regulationManager.timeout")
	// setting up oauth
	OAuth := oauth.NewOAuthErrorHandler(backendconfig.DefaultBackendConfig, oauth.WithRudderFlow(oauth.RudderFlow_Delete))

	cli := &http.Client{
		Timeout: regTimeoutReloadVar.Load(),
		Transport: &http.Transport{
			DisableKeepAlives:   config.GetBool("Transformer.Client.disableKeepAlives", true),
			MaxConnsPerHost:     config.GetInt("Transformer.Client.maxHTTPConnections", 100),
			MaxIdleConnsPerHost: config.GetInt("Transformer.Client.maxHTTPIdleConnections", 10),
			IdleConnTimeout:     300 * time.Second,
		},
	}
	if isOAuthV2RelVar.Load() {
		cache := oauthV2.NewCache()
		oauthLock := rudderSync.NewPartitionRWLocker()
		optionalArgs := OAuthHttpClient.HttpClientOptionalArgs{
			Augmenter: extensions.HeaderAugmenter,
			Locker:    oauthLock,
		}
		cli = OAuthHttpClient.OAuthHttpClient(
			cli,
			oauthV2.RudderFlow(oauth.RudderFlow_Delete),
			&cache, backendconfig.DefaultBackendConfig,
			api.GetAuthErrorCategoryFromResponse, &optionalArgs,
		)
	}

	svc := service.JobSvc{
		API: &client.JobAPI{
			Client:    &http.Client{Timeout: config.GetDuration("HttpClient.regulationWorker.regulationManager.timeout", 60, time.Second)},
			URLPrefix: config.MustGetString("CONFIG_BACKEND_URL"),
			Identity:  identity,
		},
		DestDetail: dest,
		Deleter: delete.NewRouter(
			&kvstore.KVDeleteManager{},
			&batch.BatchManager{
				FMFactory:  filemanager.New,
				FilesLimit: config.GetInt("REGULATION_WORKER_FILES_LIMIT", 1000),
			},
			&api.APIManager{
				Client:                       cli,
				DestTransformURL:             config.MustGetString("DEST_TRANSFORM_URL"),
				OAuth:                        OAuth,
				IsOAuthV2Enabled:             isOAuthV2RelVar.Load(),
				MaxOAuthRefreshRetryAttempts: config.GetInt("RegulationWorker.oauth.maxRefreshRetryAttempts", 1),
				TransformerFeaturesService: transformer.NewFeaturesService(ctx, transformer.FeaturesServiceConfig{
					PollInterval:             config.GetDuration("Transformer.pollInterval", 1, time.Second),
					TransformerURL:           config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
					FeaturesRetryMaxAttempts: 10,
				}),
			}),
		MaxFailedAttempts: config.GetInt("REGULATION_DELETION_MAX_FAILED_ATTEMPTS", 4),
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
