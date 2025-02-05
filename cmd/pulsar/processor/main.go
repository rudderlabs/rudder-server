package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-schemas/go/stream"
	"github.com/rudderlabs/rudder-server/gateway"
)

const (
	serviceName = "processor"
)

var defaultHistogramBuckets = []float64{
	0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
	300 /* 5 mins */, 600 /* 10 mins */, 1800, /* 30 mins */
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	os.Exit(run(ctx))
}

func run(ctx context.Context) int {
	conf := config.Default
	logFactory := logger.NewFactory(conf)
	defer logFactory.Sync()
	log := logFactory.NewLogger().With(serviceName, conf.GetString("INSTANCE_ID", serviceName))

	statsOptions := []stats.Option{
		stats.WithServiceName(serviceName),
		// stats.WithServiceVersion(releaseInfo.Version), // TODO fix
		stats.WithDefaultHistogramBuckets(defaultHistogramBuckets),
	}
	stat := stats.NewStats(conf, logFactory, svcMetric.NewManager(), statsOptions...)
	defer stat.Stop()

	if err := stat.Start(ctx, stats.DefaultGoRoutineFactory); err != nil {
		log.Errorn("Failed to start Stats", obskit.Error(err))
		return 1
	}

	streamToJobsDBTransformer := gateway.NewStream2JobsDBTransformer(
		backendconfig.DefaultBackendConfig,
		stat,
		log.Withn(logger.NewStringField("component", "stream2jobsdb-transformer")),
	)

	return 0
}

type suppressUserHandlerNOP struct{}

func (s suppressUserHandlerNOP) GetSuppressedUser(workspaceID, userID, sourceID string) *model.Metadata {
	return nil
}
