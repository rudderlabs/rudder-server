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
	createTableMu  sync.Mutex
	createTableMap map[string]*sync.Once

	cacheMu  sync.Mutex
	cache    map[string]types.KeyValue
	keyspace string
}

func (d *ScyllaDB) Close() {
	d.scylla.Close()
}

func (d *ScyllaDB) Get(kv types.KeyValue) (bool, int64, error) {
	// Create the table if it doesn't exist
	var err error
	d.createTableMu.Lock()
	once, found := d.createTableMap[kv.WorkspaceId]
	if !found {
		d.createTableMap[kv.WorkspaceId] = &sync.Once{}
		once = d.createTableMap[kv.WorkspaceId]
	}
	d.createTableMu.Unlock()
	once.Do(func() {
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id text PRIMARY KEY,size bigint) WITH bloom_filter_fp_chance = 0.005;", d.keyspace, kv.WorkspaceId)
		err = d.scylla.Query(query).Exec()
	})
	if err != nil {
		return false, 0, fmt.Errorf("error creating table %s: %v", kv.WorkspaceId, err)
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
	err = d.scylla.Query(fmt.Sprintf("SELECT size FROM %s.%s WHERE id = ?", d.keyspace, kv.WorkspaceId), kv.Key).Scan(&value)
	if err != nil && err != gocql.ErrNotFound {
		return false, 0, fmt.Errorf("error getting key %s: %v", kv.Key, err)
	}
	exists := !(err == gocql.ErrNotFound)
	if !exists {
		d.cache[kv.Key] = kv
	}
	return !exists, kv.Value, nil
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
					Stmt: fmt.Sprintf("INSERT INTO %s.%s (id,size) VALUES (?,?) USING TTL %d", d.keyspace, key.WorkspaceId, d.ttl),
					Args: []interface{}{key.Key, key.Value},
				})
			}
			if err := d.scylla.ExecuteBatch(scyllaBatch); err != nil {
				return fmt.Errorf("error committing keys: %v", err)
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
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: conf.GetInt("Scylla.NumRetries", 3),
		Min:        conf.GetDuration("Scylla.MinRetry", 100, time.Millisecond),
		Max:        conf.GetDuration("Scylla.MaxRetry", 2000, time.Second),
	}
	cluster.Timeout = conf.GetDuration("Scylla.Timeout", 10, time.Second)
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	scylla := &ScyllaDB{
		scylla:         session,
		conf:           conf,
		keyspace:       conf.GetString("Scylla.Keyspace", "rudder"),
		stat:           stats,
		ttl:            conf.GetInt("Scylla.TTL", 1209600), // TTL is defaulted to seconds
		batchSize:      conf.GetInt("Scylla.BatchSize", 100),
		cache:          make(map[string]types.KeyValue),
		createTableMap: make(map[string]*sync.Once),
	}
	return scylla, nil
}
