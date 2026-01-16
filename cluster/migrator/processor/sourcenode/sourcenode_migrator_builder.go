package sourcenode

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdclient"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// NewMigratorBuilder creates a new builder for sourcenode Migrator
func NewMigratorBuilder(nodeIndex int, nodeName string) *MigratorBuilder {
	return &MigratorBuilder{
		nodeIndex: nodeIndex,
		nodeName:  nodeName,
	}
}

// MigratorBuilder is a builder for sourcenode Migrator
type MigratorBuilder struct {
	nodeIndex int
	nodeName  string

	// dependencies
	config            *config.Config
	logger            logger.Logger
	stats             stats.Stats
	etcdClient        etcdclient.Client
	readerJobsDBs     []jobsdb.JobsDB
	shutdown          func()
	targetURLProvider func(targetNodeIndex int) (string, error)
}

// WithConfig sets the configuration for the Migrator
func (b *MigratorBuilder) WithConfig(config *config.Config) *MigratorBuilder {
	b.config = config
	return b
}

// WithLogger sets the logger for the Migrator
func (b *MigratorBuilder) WithLogger(logger logger.Logger) *MigratorBuilder {
	b.logger = logger
	return b
}

// WithStats sets the stats collector for the Migrator
func (b *MigratorBuilder) WithStats(stats stats.Stats) *MigratorBuilder {
	b.stats = stats
	return b
}

// WithEtcdClient sets the etcd client for the Migrator
func (b *MigratorBuilder) WithEtcdClient(etcdClient etcdclient.Client) *MigratorBuilder {
	b.etcdClient = etcdClient
	return b
}

// WithReaderJobsDBs sets the reader jobsdbs for the Migrator
func (b *MigratorBuilder) WithReaderJobsDBs(readerJobsDBs []jobsdb.JobsDB) *MigratorBuilder {
	b.readerJobsDBs = readerJobsDBs
	return b
}

// WithShutdown sets the shutdown function for the Migrator
func (b *MigratorBuilder) WithShutdown(shutdown func()) *MigratorBuilder {
	b.shutdown = shutdown
	return b
}

// WithTargetURLProvider sets the target URL provider for the Migrator
func (b *MigratorBuilder) WithTargetURLProvider(targetURLProvider func(targetNodeIndex int) (string, error)) *MigratorBuilder {
	b.targetURLProvider = targetURLProvider
	return b
}

// Build constructs the Migrator with the provided dependencies
func (b *MigratorBuilder) Build() (Migrator, error) {
	if b.config == nil {
		b.config = config.Default
	}
	if b.logger == nil {
		b.logger = logger.Default.NewLogger().Child("partitionmigration")
	}
	b.logger = b.logger.Withn(
		logger.NewIntField("nodeIndex", int64(b.nodeIndex)),
		logger.NewStringField("nodeName", b.nodeName),
	)
	if b.stats == nil {
		b.stats = stats.Default
	}
	if b.etcdClient == nil {
		return nil, fmt.Errorf("etcd client not provided")
	}
	if len(b.readerJobsDBs) == 0 {
		return nil, fmt.Errorf("reader jobsdbs not provided")
	}
	if b.shutdown == nil {
		return nil, fmt.Errorf("shutdown function not provided")
	}
	if b.targetURLProvider == nil {
		return nil, fmt.Errorf("target URL provider not provided")
	}

	m := &migrator{
		nodeIndex:            b.nodeIndex,
		nodeName:             b.nodeName,
		etcdClient:           b.etcdClient,
		readerJobsDBs:        b.readerJobsDBs,
		config:               b.config,
		logger:               b.logger,
		stats:                b.stats,
		shutdown:             b.shutdown,
		targetURLProvider:    b.targetURLProvider,
		pendingMigrationJobs: map[string]struct{}{},
	}
	m.c.readExcludeSleep = b.config.GetReloadableDurationVar(15, time.Second, "PartitionMigration.Processor.SourceNode.readExcludeSleep")
	m.c.waitForInProgressTimeout = b.config.GetReloadableDurationVar(5, time.Minute, "PartitionMigration.Processor.SourceNode.waitForInProgressTimeout")
	m.c.inProgressPollSleep = b.config.GetReloadableDurationVar(1, time.Second, "PartitionMigration.SourceNode.inProgressPollSleep")

	return m, nil
}
