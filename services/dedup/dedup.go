//go:generate mockgen -destination=../../mocks/services/dedup/mock_dedup.go -package mock_dedup github.com/rudderlabs/rudder-server/services/dedup/types Dedup

package dedup

import (
	"fmt"
	"sync"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type dedup struct {
	db            types.DB
	uncommittedMu sync.RWMutex
	uncommitted   map[string]struct{}
}

type BatchKey = types.BatchKey

// SingleKey creates a BatchKey with index 0
func SingleKey(key string) BatchKey {
	return types.BatchKey{Key: key}
}

// New creates a new deduplication service. The service needs to be closed after use.
func New(conf *config.Config, stats stats.Stats, log logger.Logger) (types.Dedup, error) {
	db, err := NewDB(conf, stats, log)
	if err != nil {
		return nil, fmt.Errorf("create dedup DB: %w", err)
	}
	return &dedup{
		db:          db,
		uncommitted: make(map[string]struct{}),
	}, nil
}

func (d *dedup) Allowed(batchKeys ...types.BatchKey) (map[types.BatchKey]bool, error) {
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
		// if the key is already in the uncommitted list , skip it
		if _, uncommitted := d.uncommitted[batchKey.Key]; uncommitted {
			seenInBatch[batchKey.Key] = struct{}{}
			continue
		}
		seenInBatch[batchKey.Key] = struct{}{}
		batchKeysToCheck = append(batchKeysToCheck, batchKey)
	}
	d.uncommittedMu.RUnlock()

	if len(batchKeysToCheck) > 0 {
		seenInDB, err := d.db.Get(lo.Map(batchKeysToCheck, func(bk types.BatchKey, _ int) string { return bk.Key }))
		if err != nil {
			return nil, fmt.Errorf("getting keys from badger db: %w", err)
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

func (d *dedup) Commit(keys []string) error {
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

	if err := d.db.Set(keys); err != nil {
		return fmt.Errorf("setting keys in badger db: %w", err)
	}

	d.uncommittedMu.Lock()
	defer d.uncommittedMu.Unlock()
	for _, kv := range kvs {
		delete(d.uncommitted, kv.Key)
	}
	return nil
}

func (d *dedup) Close() {
	d.db.Close()
}
