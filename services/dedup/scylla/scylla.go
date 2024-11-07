package scylla

import (
	"errors"
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
	readBatchSize  int
	writeBatchSize int
	tableName      string

	cacheMu  sync.Mutex
	cache    map[string]types.KeyValue
	keyspace string
}

func (d *ScyllaDB) Close() {
	d.scylla.Close()
}

func (d *ScyllaDB) GetBatch(kvs []types.KeyValue) (map[types.KeyValue]bool, error) {
	defer d.stat.NewTaggedStat("dedup_get_batch_duration_seconds", stats.TimerType, stats.Tags{"mode": "scylla"}).RecordDuration()()
	// Prepare a map to store results for each job (true = accept, false = reject)
	results := make(map[types.KeyValue]bool)
	d.stat.NewTaggedStat("dedup_get_batch_size", stats.GaugeType, stats.Tags{"mode": "scylla"}).Gauge(len(kvs))
	// Group jobs by workspaceID for batch querying
	workspaceJobsMap := make(map[string][]types.KeyValue)
	d.cacheMu.Lock()

	for _, kv := range kvs {
		if _, found := d.cache[kv.Key]; found {
			results[kv] = false
			continue
		}
		d.cache[kv.Key] = kv
		results[kv] = true
		workspaceJobsMap[kv.WorkspaceID] = append(workspaceJobsMap[kv.WorkspaceID], kv)
	}
	d.cacheMu.Unlock()

	// Loop over each workspace and query all messageIDs for that workspace
	for workspaceID, workspaceJobs := range workspaceJobsMap {
		var messageIDs []string
		for _, job := range workspaceJobs {
			messageIDs = append(messageIDs, job.Key)
		}

		messageIDChunks := lo.Chunk(messageIDs, d.readBatchSize)
		for _, chunk := range messageIDChunks {
			// Query to get all jobIDs for the given workspaceID and messageIDs
			startTime := time.Now()
			query := fmt.Sprintf("SELECT id FROM %s.%q WHERE workspaceID = ? AND id IN ?", d.keyspace, d.tableName)
			iter := d.scylla.Query(query, workspaceID, chunk).Iter()

			var dbMessageID string
			for iter.Scan(&dbMessageID) {
				d.cacheMu.Lock()
				val, ok := d.cache[dbMessageID]
				if ok {
					results[val] = false
				}
				delete(d.cache, dbMessageID)
				d.cacheMu.Unlock()
			}
			if err := iter.Close(); err != nil {
				return nil, fmt.Errorf("error closing iterator: %v", err)
			}
			d.stat.NewTaggedStat("dedup_get_batch_query_duration_seconds", stats.TimerType, stats.Tags{"mode": "scylla"}).Since(startTime)
		}
	}

	return results, nil
}

func (d *ScyllaDB) Get(kv types.KeyValue) (bool, error) {
	defer d.stat.NewTaggedStat("dedup_get_duration_seconds", stats.TimerType, stats.Tags{"mode": "scylla"}).RecordDuration()()

	var err error
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	// Check if the key exists in the cache
	// This is essential if we get the same key multiple times in the same batch
	// Since we are not committing the keys immediately, we need to keep track of the keys in the cache
	if _, found := d.cache[kv.Key]; found {
		return false, nil
	}

	// Check if the key exists in the DB
	var id string
	err = d.scylla.Query(fmt.Sprintf("SELECT id FROM %s.%q WHERE id = ? and workspaceId = ?", d.keyspace, d.tableName), kv.Key, kv.WorkspaceID).Scan(&id)
	if err != nil && !errors.Is(err, gocql.ErrNotFound) {
		return false, fmt.Errorf("error getting key %s: %v", kv.Key, err)
	}
	exists := errors.Is(err, gocql.ErrNotFound)
	if exists {
		d.cache[kv.Key] = kv
	}
	return exists, nil
}

func (d *ScyllaDB) Commit(keys []string) error {
	defer d.stat.NewTaggedStat("dedup_commit_duration_seconds", stats.TimerType, stats.Tags{"mode": "scylla"}).RecordDuration()()

	d.cacheMu.Lock()
	kvs := make([]types.KeyValue, len(keys))
	for i, key := range keys {
		value, ok := d.cache[key]
		if !ok {
			d.cacheMu.Unlock()
			return fmt.Errorf("key %v has not been previously set", key)
		}
		kvs[i] = types.KeyValue{Key: key, WorkspaceID: value.WorkspaceID}
	}
	d.cacheMu.Unlock()
	batches := lo.Chunk(kvs, d.writeBatchSize)
	for _, batch := range batches {
		scyllaBatch := d.scylla.NewBatch(gocql.LoggedBatch)
		for _, key := range batch {
			scyllaBatch.Entries = append(scyllaBatch.Entries, gocql.BatchEntry{
				Stmt: fmt.Sprintf("INSERT INTO %s.%q (id,workspaceId) VALUES (?,?) USING TTL %d", d.keyspace, d.tableName, d.ttl),
				Args: []interface{}{key.Key, key.WorkspaceID},
			})
		}
		if err := d.scylla.ExecuteBatch(scyllaBatch); err != nil {
			return fmt.Errorf("error committing keys: %v", err)
		}
		d.cacheMu.Lock()
		for _, key := range batch {
			delete(d.cache, key.Key)
		}
		d.cacheMu.Unlock()
	}
	return nil
}

func New(conf *config.Config, stats stats.Stats) (*ScyllaDB, error) {
	cluster := gocql.NewCluster(conf.GetReloadableStringSliceVar([]string{"localhost:9042"}, "Scylla.Hosts").Load()...)
	cluster.Consistency = gocql.Quorum
	cluster.NumConns = conf.GetInt("Scylla.NumConns", 2)
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

	keySpace := conf.GetString("Scylla.Keyspace", "rudder")
	table := conf.GetString("Scylla.TableName", "dedup")

	err = session.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%q ( id text, workspaceId text, PRIMARY KEY (id, workspaceId)) WITH bloom_filter_fp_chance = 0.005;", keySpace, table)).Exec()
	if err != nil {
		return nil, err
	}

	scylla := &ScyllaDB{
		scylla:         session,
		conf:           conf,
		keyspace:       keySpace,
		stat:           stats,
		ttl:            conf.GetInt("Scylla.TTL", 864000), // TTL is defaulted to seconds
		readBatchSize:  conf.GetInt("Scylla.ReadBatchSize", 100),
		writeBatchSize: conf.GetInt("Scylla.WriteBatchSize", 100),
		tableName:      table,
		cache:          make(map[string]types.KeyValue),
	}
	return scylla, nil
}
