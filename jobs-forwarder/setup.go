package jobs_forwarder

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendConfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/forwarder/jobforwarder"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/forwarder/noopforwarder"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/transientsource"
)

type Forwarder interface {
	Start(ctx context.Context)
	Stop()
}

func Setup(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, transientSources transientsource.Service, backendConfig backendConfig.BackendConfig, log logger.Logger) (Forwarder, error) {
	config := config.New()
	forwarderEnabled := config.GetBool("JobsForwarder.enabled", false)
	if forwarderEnabled {
		return jobforwarder.New(ctx, g, schemaDB, transientSources, backendConfig, log)
	}
	return noopforwarder.New(ctx, g, schemaDB, log)
}
