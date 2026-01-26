package targetnode

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdclient"
	"github.com/rudderlabs/rudder-server/cluster/partitionbuffer"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// NewMigratorBuilder creates a new builder for targetnode Migrator
func NewMigratorBuilder(nodeIndex int, nodeName string) *MigratorBuilder {
	return &MigratorBuilder{
		nodeIndex: nodeIndex,
		nodeName:  nodeName,
	}
}

// MigratorBuilder is a builder for targetnode Migrator
type MigratorBuilder struct {
	nodeIndex int
	nodeName  string

	// dependencies
	config            *config.Config
	logger            logger.Logger
	stats             stats.Stats
	etcdClient        etcdclient.Client
	bufferedJobsDBs   [][]partitionbuffer.JobsDBPartitionBuffer
	unbufferedJobsDBs []jobsdb.JobsDB
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

// WithBufferedJobsDBs sets the buffered jobsdbs for the Migrator
func (b *MigratorBuilder) WithBufferedJobsDBs(bufferedJobsDBs [][]partitionbuffer.JobsDBPartitionBuffer) *MigratorBuilder {
	b.bufferedJobsDBs = bufferedJobsDBs
	return b
}

// WithUnbufferedJobsDBs sets the unbuffered jobsdbs for the Migrator
func (b *MigratorBuilder) WithUnbufferedJobsDBs(unbufferedJobsDBs []jobsdb.JobsDB) *MigratorBuilder {
	b.unbufferedJobsDBs = unbufferedJobsDBs
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
	if len(b.bufferedJobsDBs) == 0 {
		return nil, fmt.Errorf("buffered jobsdbs not provided")
	}
	if len(b.unbufferedJobsDBs) == 0 {
		return nil, fmt.Errorf("unbuffered jobsdbs not provided")
	}

	// build a map of unbuffered jobsdb identifiers for validation
	unbufferedIdentifiers := make(map[string]struct{}, len(b.unbufferedJobsDBs))
	for _, unbuffered := range b.unbufferedJobsDBs {
		unbufferedIdentifiers[unbuffered.Identifier()] = struct{}{}
		if _, ok := unbuffered.(partitionbuffer.JobsDBPartitionBuffer); ok {
			return nil, fmt.Errorf("unbuffered jobsdb %q should not implement the JobsDBPartitionBuffer interface", unbuffered.Identifier())
		}
	}

	// validate buffered jobsdbs: flatten and check identifiers match unbuffered
	bufferedCount := 0
	for i, bufferedGroup := range b.bufferedJobsDBs {
		if len(bufferedGroup) == 0 {
			return nil, fmt.Errorf("buffered jobsdbs group %d is empty", i)
		}
		for _, buffered := range bufferedGroup {
			bufferedCount++
			if _, ok := unbufferedIdentifiers[buffered.Identifier()]; !ok {
				return nil, fmt.Errorf("buffered jobsdb %q has no matching unbuffered jobsdb", buffered.Identifier())
			}
		}
	}
	if bufferedCount != len(b.unbufferedJobsDBs) {
		return nil, fmt.Errorf("buffered and unbuffered jobsdbs count mismatch: %d != %d", bufferedCount, len(b.unbufferedJobsDBs))
	}

	m := &migrator{
		nodeIndex:            b.nodeIndex,
		nodeName:             b.nodeName,
		etcdClient:           b.etcdClient,
		bufferedJobsDBs:      b.bufferedJobsDBs,
		unbufferedJobsDBs:    b.unbufferedJobsDBs,
		config:               b.config,
		logger:               b.logger,
		stats:                b.stats,
		pendingMigrationJobs: map[string]struct{}{},
	}

	return m, nil
}
