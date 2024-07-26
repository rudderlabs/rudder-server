package scylla

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type ScyllaDB struct {
	scylla         *gocql.Session
	conf           *config.Config
	stat           stats.Stats
	ttl            int
	batchSize      int
	createTableMap map[string]struct{}
}

func (d *ScyllaDB) Close() {
	d.scylla.Close()
}

func (d *ScyllaDB) Set(kv types.KeyValue) (bool, int64, error) {
	// Create the table if it doesn't exist
	if _, ok := d.createTableMap[kv.WorkspaceId]; !ok {
		err := d.scylla.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, value bigint) WITH bloom_filter_fp_chance = 0.005", kv.WorkspaceId)).Exec()
		if err != nil {
			return false, 0, err
		}
		d.createTableMap[kv.WorkspaceId] = struct{}{}
	}

	// Check if the key exists in the DB
	var value int64
	err := d.scylla.Query(fmt.Sprintf("SELECT value FROM %s WHERE id = ?", kv.WorkspaceId), kv.Key).Scan(&value)
	if err != nil && err != gocql.ErrNotFound {
		return false, 0, err
	}
	if err == gocql.ErrNotFound {
		return true, 0, nil
	}
	return false, kv.Value, nil
}

func (d *ScyllaDB) Commit(keys map[string]types.KeyValue) error {
	keysList := lo.PartitionBy(lo.Values(keys), func(kv types.KeyValue) string {
		return kv.WorkspaceId
	})
	for _, keys := range keysList {
		batches := lo.Chunk(keys, d.batchSize)
		for _, batch := range batches {
			scyllaBatch := d.scylla.NewBatch(gocql.LoggedBatch)
			for _, key := range batch {
				scyllaBatch.Entries = append(scyllaBatch.Entries, gocql.BatchEntry{
					Stmt: fmt.Sprintf("INSERT INTO %s (id) VALUES (?,?) USING TTL %d", key.WorkspaceId, d.ttl),
					Args: []interface{}{key.Key, key.Value},
				})
			}
			if err := d.scylla.ExecuteBatch(scyllaBatch); err != nil {
				return err
			}
		}
	}
	return nil
}

func New(conf *config.Config, stats stats.Stats) (*ScyllaDB, error) {
	cluster := gocql.NewCluster(conf.GetString("Scylla.Hosts", "localhost:9042"))
	cluster.Consistency = gocql.Quorum
	cluster.Keyspace = config.GetString("Scylla.Keyspace", "dedup")
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: config.GetInt("Scylla.NumRetries", 3),
		Min:        conf.GetDuration("Scylla.MinRetry", 100, time.Millisecond),
		Max:        conf.GetDuration("Scylla.MaxRetry", 2000, time.Second),
	}
	cluster.Timeout = conf.GetDuration("Scylla.Timeout", 10, time.Second)
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &ScyllaDB{
		scylla:    session,
		conf:      conf,
		stat:      stats,
		ttl:       conf.GetInt("Scylla.TTL", 1209600), // TTL is defaulted to seconds
		batchSize: conf.GetInt("Scylla.BatchSize", 100),
	}, nil
}
