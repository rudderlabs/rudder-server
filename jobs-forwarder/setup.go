package jobs_forwarder

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendConfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/forwarder"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

//go:generate mockgen -destination=../mocks/jobs-forwarder/mock_jobs_forwarder.go -package=mock_jobs_forwarder github.com/rudderlabs/rudder-server/jobs-forwarder Forwarder

type Forwarder interface {
	Start() error
	Stop()
}

func SetupJobsForwarder(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, client *pulsar.Client, backendConfig backendConfig.BackendConfig, log logger.Logger, conf *config.Config) (Forwarder, error) {
	return forwarder.NewJobsForwarder(ctx, g, schemaDB, client, conf, backendConfig, log)
}

func SetupAbortForwarder(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, log logger.Logger, conf *config.Config) (Forwarder, error) {
	return forwarder.NewNOOPForwarder(ctx, g, schemaDB, conf, log)
}
