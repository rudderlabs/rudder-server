package scylla

import (
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type ScyllaDB struct {
	scylla *gocql.Session
	conf   *config.Config
	stat   stats.Stats
	// Time to live in seconds for an entry in the DB
	ttl int
	// Maximum number of keys to commit in a single batch
	batchSize      int
	createTableMap map[string]struct{}

	cacheMu sync.Mutex
	cache   map[string]types.KeyValue
}

func (d *ScyllaDB) Close() {
	d.scylla.Close()
}

func (d *ScyllaDB) Get(kv types.KeyValue) (bool, int64, error) {
	// Create the table if it doesn't exist
	if _, ok := d.createTableMap[kv.WorkspaceId]; !ok {
		err := d.scylla.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, value bigint) WITH bloom_filter_fp_chance = 0.005", kv.WorkspaceId)).Exec()
		if err != nil {
			return false, 0, err
		}
		d.createTableMap[kv.WorkspaceId] = struct{}{}
	}

	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	// Check if the key exists in the cache
	// This is essential if we get the same key multiple times in the same batch
	// Since we are not committing the keys immediately, we need to keep track of the keys in the cache
	if previous, found := d.cache[kv.Key]; found {
		return false, previous.Value, nil
	}

	// Check if the key exists in the DB
	var value int64
	err := d.scylla.Query(fmt.Sprintf("SELECT value FROM %s WHERE id = ?", kv.WorkspaceId), kv.Key).Scan(&value)
	if err != nil && err != gocql.ErrNotFound {
		return false, 0, err
	}
	d.cache[kv.Key] = kv
	return false, kv.Value, nil
}

func (d *ScyllaDB) Commit(keys []string) error {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	kvs := make([]types.KeyValue, len(keys))
	for i, key := range keys {
		value, ok := d.cache[key]
		if !ok {
			return fmt.Errorf("key %v has not been previously set", key)
		}
		kvs[i] = types.KeyValue{Key: key, Value: value.Value, WorkspaceId: value.WorkspaceId}
	}
	keysList := lo.PartitionBy(kvs, func(kv types.KeyValue) string {
		return kv.WorkspaceId
	})
	for _, keysPerWorkspace := range keysList {
		batches := lo.Chunk(keysPerWorkspace, d.batchSize)
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
			for _, key := range batch {
				delete(d.cache, key.Key)
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
