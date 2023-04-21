package schema_forwarder

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendConfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/forwarder"
)

//go:generate mockgen -destination=../mocks/jobs-forwarder/mock_jobs_forwarder.go -package=mock_jobs_forwarder github.com/rudderlabs/rudder-server/schema-forwarder Forwarder

type Forwarder interface {
	Start() error
	Stop()
}

// NewJobsForwarder creates a new jobs forwarder that transforms and forwards jobs to pulsar
func NewForwarder(terminalErrFn func(error), schemaDB jobsdb.JobsDB, client *pulsar.Client, backendConfig backendConfig.BackendConfig, log logger.Logger, conf *config.Config, stat stats.Stats) Forwarder {
	return forwarder.NewJobsForwarder(terminalErrFn, schemaDB, client, conf, backendConfig, log, stat)
}

// NewAbortingForwarder creates a new aborting forwarder that marks jobs as aborted without trying to forward them
func NewAbortingForwarder(terminalErrFn func(error), schemaDB jobsdb.JobsDB, log logger.Logger, conf *config.Config, stat stats.Stats) Forwarder {
	return forwarder.NewAbortingForwarder(terminalErrFn, schemaDB, conf, log, stat)
}
