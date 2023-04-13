package jobs_forwarder

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendConfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/forwarder"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/transientsource"
)

//go:generate mockgen -destination=../mocks/jobs-forwarder/mock_jobs_forwarder.go -package=mock_jobs_forwarder github.com/rudderlabs/rudder-server/jobs-forwarder Forwarder

type Forwarder interface {
	Start()
	Stop()
}

func Setup(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, transientSources transientsource.Service, backendConfig backendConfig.BackendConfig, log logger.Logger) (Forwarder, error) {
	config := config.New()
	forwarderEnabled := config.GetBool("JobsForwarder.enabled", false)
	if forwarderEnabled {
		return forwarder.NewJobsForwarder(ctx, g, schemaDB, config, transientSources, backendConfig, log)
	}
	return forwarder.NewNOOPForwarder(ctx, g, schemaDB, config, log)
}
