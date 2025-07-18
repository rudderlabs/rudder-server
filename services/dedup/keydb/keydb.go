package keydb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/keydb/client"
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
	dedupWindow := conf.GetReloadableDurationVar(3600, time.Second, "KeyDB.Dedup.dedupWindow", "Dedup.dedupWindow", "Dedup.dedupWindowInS")
	nodeAddresses := conf.GetString("KeyDB.Dedup.Addresses", "")
	if len(nodeAddresses) == 0 {
		return nil, fmt.Errorf("keydb dedup: no node addresses provided")
	}
	c, err := client.NewClient(client.Config{
		Addresses:       strings.Split(nodeAddresses, ","),
		TotalHashRanges: uint32(conf.GetInt("KeyDB.Dedup.TotalHashRanges", 128)),
		// TODO the client should support exponential backoff and circuit breakers
		RetryCount: conf.GetInt("KeyDB.Dedup.RetryCount", 60),
		RetryDelay: conf.GetDuration("KeyDB.Dedup.RetryDelay", 1, time.Second),
	}, log.Child("keydb"))
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

	return &Dedup{keyDB: db}, nil
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
	return d.client.Put(context.TODO(), keys, d.window.Load())
}

func (d *KeyDB) Close() {
	if d.client != nil {
		_ = d.client.Close()
	}
}

type Dedup struct {
	keyDB *KeyDB
}

func (d *Dedup) Allowed(batchKeys ...types.BatchKey) (map[types.BatchKey]bool, error) {
	result := make(map[types.BatchKey]bool, len(batchKeys))
	seenInBatch := make(map[string]struct{}, len(batchKeys)) // keys already seen in the batch while iterating

	// figure out which keys need to be checked against the DB
	batchKeysToCheck := make([]types.BatchKey, 0, len(batchKeys)) // keys to check in the DB
	for _, batchKey := range batchKeys {
		// if the key is already seen in the batch, skip it
		if _, seen := seenInBatch[batchKey.Key]; seen {
			continue
		}

		seenInBatch[batchKey.Key] = struct{}{}
		batchKeysToCheck = append(batchKeysToCheck, batchKey)
	}

	if len(batchKeysToCheck) > 0 {
		seenInDB, err := d.keyDB.Get(lo.Map(batchKeysToCheck, func(bk types.BatchKey, _ int) string { return bk.Key }))
		if err != nil {
			return nil, fmt.Errorf("getting keys from keydb: %w", err)
		}
		for _, batchKey := range batchKeysToCheck {
			if !seenInDB[batchKey.Key] {
				result[batchKey] = true
			}
		}
	}

	return result, nil
}

func (d *Dedup) Commit(keys []string) error {
	if err := d.keyDB.Set(keys); err != nil {
		return fmt.Errorf("setting keys in keydb: %w", err)
	}
	return nil
}

func (d *Dedup) Close() { d.keyDB.Close() }
