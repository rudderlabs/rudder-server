package processor

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdclient"
	"github.com/rudderlabs/rudder-server/cluster/migrator/processor/sourcenode"
	"github.com/rudderlabs/rudder-server/cluster/migrator/processor/targetnode"
)

// NewProcessorPartitionMigratorBuilder creates a new builder for ProcessorPartitionMigrator
func NewProcessorPartitionMigratorBuilder(nodeIndex int, nodeName string) *ProcessorPartitionMigratorBuilder {
	return &ProcessorPartitionMigratorBuilder{
		nodeIndex: nodeIndex,
		nodeName:  nodeName,
	}
}

// ProcessorPartitionMigratorBuilder is a builder for ProcessorPartitionMigrator
type ProcessorPartitionMigratorBuilder struct {
	nodeIndex int
	nodeName  string

	// dependencies
	config         *config.Config
	logger         logger.Logger
	stats          stats.Stats
	etcdClient     etcdclient.Client
	sourceMigrator sourcenode.Migrator
	targetMigrator targetnode.Migrator
}

// WithConfig sets the configuration for the ProcessorPartitionMigrator
func (b *ProcessorPartitionMigratorBuilder) WithConfig(config *config.Config) *ProcessorPartitionMigratorBuilder {
	b.config = config
	return b
}

// WithLogger sets the logger for the ProcessorPartitionMigrator
func (b *ProcessorPartitionMigratorBuilder) WithLogger(logger logger.Logger) *ProcessorPartitionMigratorBuilder {
	b.logger = logger
	return b
}

// WithStats sets the stats collector for the ProcessorPartitionMigrator
func (b *ProcessorPartitionMigratorBuilder) WithStats(stats stats.Stats) *ProcessorPartitionMigratorBuilder {
	b.stats = stats
	return b
}

// WithEtcdClient sets the etcd client for the ProcessorPartitionMigrator
func (b *ProcessorPartitionMigratorBuilder) WithEtcdClient(etcdClient etcdclient.Client) *ProcessorPartitionMigratorBuilder {
	b.etcdClient = etcdClient
	return b
}

// WithSourceMigrator sets the source migrator for the ProcessorPartitionMigrator
func (b *ProcessorPartitionMigratorBuilder) WithSourceMigrator(sourceMigrator sourcenode.Migrator) *ProcessorPartitionMigratorBuilder {
	b.sourceMigrator = sourceMigrator
	return b
}

// WithTargetMigrator sets the target migrator for the ProcessorPartitionMigrator
func (b *ProcessorPartitionMigratorBuilder) WithTargetMigrator(targetMigrator targetnode.Migrator) *ProcessorPartitionMigratorBuilder {
	b.targetMigrator = targetMigrator
	return b
}

// Build constructs the ProcessorPartitionMigrator with the provided dependencies
func (b *ProcessorPartitionMigratorBuilder) Build() (ProcessorPartitionMigrator, error) {
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
	if b.sourceMigrator == nil {
		return nil, fmt.Errorf("source migrator not provided")
	}
	if b.targetMigrator == nil {
		return nil, fmt.Errorf("target migrator not provided")
	}

	return &processorPartitionMigrator{
		nodeIndex:      b.nodeIndex,
		nodeName:       b.nodeName,
		config:         b.config,
		logger:         b.logger,
		stats:          b.stats,
		etcdClient:     b.etcdClient,
		sourceMigrator: b.sourceMigrator,
		targetMigrator: b.targetMigrator,
	}, nil
}
