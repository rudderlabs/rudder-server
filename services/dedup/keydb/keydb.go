package keydb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type KeyDB struct {
	client *client.Client
	window config.ValueLoader[time.Duration]
	logger logger.Logger

	stats struct {
		getTimer stats.Timer
		setTimer stats.Timer
	}
}

func NewKeyDB(conf *config.Config, stat stats.Stats, log logger.Logger) (*Dedup, error) {
	dedupWindow := conf.GetReloadableDurationVar(3600, time.Second, "Dedup.dedupWindow", "Dedup.dedupWindowInS")
	c, err := client.NewClient(client.Config{
		Addresses:       conf.GetStringSlice("KeyDB.Dedup.Addresses", []string{"localhost:2379"}),
		TotalHashRanges: uint32(conf.GetInt("KeyDB.Dedup.TotalHashRanges", 128)),
		// TODO the client should support exponential backoff and circuit breakers
		RetryCount: conf.GetInt("KeyDB.Dedup.RetryCount", 60),
		RetryDelay: conf.GetDuration("KeyDB.Dedup.RetryDelay", 1, time.Second),
	})
	if err != nil {
		return nil, err
	}

	db := &KeyDB{
		client: c,
		window: dedupWindow,
		logger: log,
	}
	db.stats.getTimer = stat.NewTaggedStat("dedup_get_duration_seconds", stats.TimerType, stats.Tags{"mode": "keydb"})
	db.stats.setTimer = stat.NewTaggedStat("dedup_set_duration_seconds", stats.TimerType, stats.Tags{"mode": "keydb"})

	return &Dedup{
		keyDB:       db,
		uncommitted: make(map[string]struct{}),
	}, nil
}

func (d *KeyDB) Get(keys []string) (map[string]bool, error) {
	defer d.stats.getTimer.RecordDuration()()
	results := make(map[string]bool, len(keys))
	exist, err := d.client.Get(context.TODO(), keys)
	if err != nil {
		return nil, err
	}
	for i, key := range keys {
		if exist[i] {
			results[key] = true
		}
	}
	return results, err
}

func (d *KeyDB) Set(keys []string) error {
	defer d.stats.setTimer.RecordDuration()()
	items := make([]*proto.KeyWithTTL, len(keys))
	for i, key := range keys {
		items[i] = &proto.KeyWithTTL{
			Key:        key,
			TtlSeconds: uint64(d.window.Load().Seconds()),
		}
	}
	return d.client.Put(context.TODO(), items)
}

func (d *KeyDB) Close() {
	if d.client != nil {
		_ = d.client.Close()
	}
}

type Dedup struct {
	keyDB         *KeyDB
	uncommittedMu sync.RWMutex
	uncommitted   map[string]struct{}
}

func (d *Dedup) Allowed(batchKeys ...types.BatchKey) (map[types.BatchKey]bool, error) {
	result := make(map[types.BatchKey]bool, len(batchKeys))  // keys encountered for the first time
	seenInBatch := make(map[string]struct{}, len(batchKeys)) // keys already seen in the batch while iterating

	// figure out which keys need to be checked against the DB
	batchKeysToCheck := make([]types.BatchKey, 0, len(batchKeys)) // keys to check in the DB
	d.uncommittedMu.RLock()
	for _, batchKey := range batchKeys {
		// if the key is already seen in the batch, skip it
		if _, seen := seenInBatch[batchKey.Key]; seen {
			continue
		}

		seenInBatch[batchKey.Key] = struct{}{}

		if _, uncommitted := d.uncommitted[batchKey.Key]; uncommitted {
			continue
		}

		batchKeysToCheck = append(batchKeysToCheck, batchKey)
	}
	d.uncommittedMu.RUnlock()

	if len(batchKeysToCheck) > 0 {
		seenInDB, err := d.keyDB.Get(lo.Map(batchKeysToCheck, func(bk types.BatchKey, _ int) string { return bk.Key }))
		if err != nil {
			return nil, fmt.Errorf("getting keys from keydb: %w", err)
		}
		d.uncommittedMu.Lock()
		defer d.uncommittedMu.Unlock()
		for _, batchKey := range batchKeysToCheck {
			if !seenInDB[batchKey.Key] {
				if _, race := d.uncommitted[batchKey.Key]; !race { // if another goroutine managed to set this key, we should skip it
					result[batchKey] = true
					d.uncommitted[batchKey.Key] = struct{}{} // mark this key as uncommitted
				}
			}
		}
	}

	return result, nil
}

func (d *Dedup) Commit(keys []string) error {
	kvs := make([]types.BatchKey, len(keys))

	d.uncommittedMu.RLock()
	for i, key := range keys {
		if _, ok := d.uncommitted[key]; !ok {
			d.uncommittedMu.RUnlock()
			return fmt.Errorf("key %v has not been previously set", key)
		}
		kvs[i] = types.BatchKey{Key: key}
	}
	d.uncommittedMu.RUnlock()

	if err := d.keyDB.Set(keys); err != nil {
		return fmt.Errorf("setting keys in keydb: %w", err)
	}

	d.uncommittedMu.Lock()
	defer d.uncommittedMu.Unlock()
	for _, kv := range kvs {
		delete(d.uncommitted, kv.Key)
	}
	return nil
}

func (d *Dedup) Close() { d.keyDB.Close() }
