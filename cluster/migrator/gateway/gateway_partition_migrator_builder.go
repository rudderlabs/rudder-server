package migrator

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdclient"
)

// NewGatewayPartitionMigratorBuilder creates a new builder for GatewayPartitionMigrator
func NewGatewayPartitionMigratorBuilder(nodeIndex int, nodeName string) *GatewayPartitionMigratorBuilder {
	return &GatewayPartitionMigratorBuilder{
		nodeIndex: nodeIndex,
		nodeName:  nodeName,
	}
}

// GatewayPartitionMigratorBuilder is a builder for GatewayPartitionMigrator
type GatewayPartitionMigratorBuilder struct {
	nodeIndex int
	nodeName  string

	// dependencies
	config             *config.Config
	logger             logger.Logger
	stats              stats.Stats
	etcdClient         etcdclient.Client
	partitionRefresher PartitionRefresher
}

// WithConfig sets the configuration for the GatewayPartitionMigrator
func (b *GatewayPartitionMigratorBuilder) WithConfig(config *config.Config) *GatewayPartitionMigratorBuilder {
	b.config = config
	return b
}

// WithLogger sets the logger for the GatewayPartitionMigrator
func (b *GatewayPartitionMigratorBuilder) WithLogger(logger logger.Logger) *GatewayPartitionMigratorBuilder {
	b.logger = logger
	return b
}

// WithStats sets the stats collector for the GatewayPartitionMigrator
func (b *GatewayPartitionMigratorBuilder) WithStats(stats stats.Stats) *GatewayPartitionMigratorBuilder {
	b.stats = stats
	return b
}

// WithEtcdClient sets the etcd client for the GatewayPartitionMigrator
func (b *GatewayPartitionMigratorBuilder) WithEtcdClient(etcdClient etcdclient.Client) *GatewayPartitionMigratorBuilder {
	b.etcdClient = etcdClient
	return b
}

// WithPartitionRefresher sets the partition refresher for the GatewayPartitionMigrator
func (b *GatewayPartitionMigratorBuilder) WithPartitionRefresher(partitionRefresher PartitionRefresher) *GatewayPartitionMigratorBuilder {
	b.partitionRefresher = partitionRefresher
	return b
}

// Build constructs the GatewayPartitionMigrator with the provided dependencies
func (b *GatewayPartitionMigratorBuilder) Build() (PartitionMigrator, error) {
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
	if b.partitionRefresher == nil {
		return nil, fmt.Errorf("partition refresher not provided")
	}

	return &gatewayPartitionMigrator{
		nodeIndex:          b.nodeIndex,
		nodeName:           b.nodeName,
		config:             b.config,
		logger:             b.logger,
		stats:              b.stats,
		etcdClient:         b.etcdClient,
		partitionRefresher: b.partitionRefresher,
	}, nil
}
