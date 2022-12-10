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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/otel"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/kvstore"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/destination"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
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

	admin.Init()
	misc.Init()
	diagnostics.Init()
	backendconfig.Init()
	stats.Default.Start(ctx)
	defer stats.Default.Stop()

	// START - OpenTelemetry setup
	var (
		statsEnabled        = config.GetBool("enableStats", true)
		otelTracesEndpoint  = config.GetString("OpenTelemetry.Traces.Endpoint", "")
		otelMetricsEndpoint = config.GetString("OpenTelemetry.Metrics.Endpoint", "")
	)
	if statsEnabled && otelTracesEndpoint == "" && otelMetricsEndpoint == "" {
		pkgLogger.Warnf("No OpenTelemetry endpoints provided")
	} else if statsEnabled {
		otelResource, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("regulation-worker"),
			semconv.ServiceInstanceIDKey.String(config.GetString("INSTANCE_ID", "")),
			attribute.String("namespace", config.GetNamespaceIdentifier()),
			// @TODO add version? if yes then we can use otel.NewResource() directly
		))
		if err != nil {
			return fmt.Errorf("cannot create OpenTelemetry resource: %w", err)
		}

		var (
			otelManager otel.Manager
			otelOpts    = []otel.Option{otel.WithInsecureGRPC(), otel.WithLogger(pkgLogger.Child("otel"))}
		)
		if otelTracesEndpoint != "" {
			otelOpts = append(otelOpts, otel.WithTracerProvider(otelTracesEndpoint, otel.WithGlobalTracerProvider()))
		}
		if otelMetricsEndpoint != "" {
			otelOpts = append(otelOpts, otel.WithMeterProvider(otelMetricsEndpoint,
				otel.WithGlobalMeterProvider(),
				otel.WithMeterProviderExportsInterval(
					config.GetDuration("OpenTelemetry.Metrics.ExportInterval", 5, time.Second),
				),
			))
		}
		if _, _, err = otelManager.Setup(ctx, otelResource, otelOpts...); err != nil {
			return fmt.Errorf("OpenTelemetry setup failed: %w", err)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := otelManager.Shutdown(ctx); err != nil {
				pkgLogger.Warnf("OpenTelemetry shutdown failed: %v", err)
			}
		}()
	} else {
		pkgLogger.Warnf("Stats are disabled")
	}
	// END - OpenTelemetry setup

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

	// setting up oauth
	OAuth := oauth.NewOAuthErrorHandler(backendconfig.DefaultBackendConfig, oauth.WithRudderFlow(oauth.RudderFlow_Delete))

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
