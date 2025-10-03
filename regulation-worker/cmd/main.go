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
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
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
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"

	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	oauthv2http "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
)

var pkgLogger = logger.NewLogger().Child("regulation-worker")

func main() {
	pkgLogger.Infon("Starting regulation-worker")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := Run(ctx)
	if ctx.Err() == nil {
		cancel()
	}
	if err != nil {
		pkgLogger.Errorn("Running regulation worker", obskit.Error(err))
		os.Exit(1)
	}
}

func Run(ctx context.Context) error {
	config := config.Default
	config.Set("Diagnostics.enableDiagnostics", false)

	stats.Default = stats.NewStats(config, logger.Default, svcMetric.Instance,
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
	pkgLogger.Infon("Running regulation worker", logger.NewStringField("mode", string(deploymentType)))
	backendconfig.DefaultBackendConfig.StartWithIDs(ctx, "")
	backendconfig.DefaultBackendConfig.WaitForConfig(ctx)
	identity := backendconfig.DefaultBackendConfig.Identity()
	dest.Start(ctx)
	httpTimeout := config.GetDurationVar(60, time.Second, "HttpClient.regulationWorker.regulationManager.timeout")

	apiManagerHttpClient := createHTTPClient(config, httpTimeout)

	svc := service.JobSvc{
		API: &client.JobAPI{
			Client:    &http.Client{Timeout: httpTimeout},
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
				Client:                       apiManagerHttpClient,
				DestTransformURL:             config.MustGetString("DEST_TRANSFORM_URL"),
				MaxOAuthRefreshRetryAttempts: config.GetInt("RegulationWorker.oauth.maxRefreshRetryAttempts", 1),
				TransformerFeaturesService: transformer.NewFeaturesService(ctx, config, transformer.FeaturesServiceOptions{
					PollInterval:             config.GetDuration("Transformer.pollInterval", 10, time.Second),
					TransformerURL:           config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
					FeaturesRetryMaxAttempts: 10,
				}),
			}),
		MaxFailedAttempts: config.GetInt("REGULATION_DELETION_MAX_FAILED_ATTEMPTS", 4),
	}

	pkgLogger.Infon("calling looper with service")
	l := withLoop(svc)
	err = crash.Wrapper(func() error {
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

func createHTTPClient(conf *config.Config, httpTimeout time.Duration) *http.Client {
	cli := &http.Client{
		Timeout: httpTimeout,
		Transport: &http.Transport{
			DisableKeepAlives:   conf.GetBool("HttpClient.regulationWorker.regulationManager.disableKeepAlives", true),
			MaxConnsPerHost:     conf.GetInt("HttpClient.regulationWorker.regulationManager.maxHTTPConnections", 100),
			MaxIdleConnsPerHost: conf.GetInt("HttpClient.regulationWorker.regulationManager.maxHTTPIdleConnections", 10),
			IdleConnTimeout:     300 * time.Second,
		},
	}

	cache := oauthv2.NewOauthTokenCache()
	oauthLock := kitsync.NewPartitionRWLocker()
	optionalArgs := oauthv2http.HttpClientOptionalArgs{
		Augmenter: extensions.HeaderAugmenter,
		Locker:    oauthLock,
		Logger:    logger.NewLogger().Child("RegulationWorker"),
	}
	return oauthv2http.NewOAuthHttpClient(
		cli,
		common.RudderFlowDelete,
		&cache, backendconfig.DefaultBackendConfig,
		api.GetAuthErrorCategoryFromResponse, &optionalArgs,
	)
}
