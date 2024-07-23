//go:generate mockgen -destination=../../mocks/services/dedup/mock_dedup.go -package mock_dedup github.com/rudderlabs/rudder-server/services/dedup Dedup

package dedup

import (
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/services/dedup/badger"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type Mode string // skipcq: RVV-B0009

const (
	Badger Mode = "Badger"
	Scylla Mode = "Scylla"
	Dual   Mode = "Dual"
)

// New creates a new deduplication service. The service needs to be closed after use.
func New() Dedup {
	mode := Mode(config.GetString("Dedup.Mode", string(Badger)))
	switch mode {
	case Badger:
		return &dedup{
			badgerDB: badger.NewBadgerDB(badger.DefaultPath()),
			cache:    make(map[string]int64),
		}
	case Scylla:
		return nil
	case Dual:
		return nil
	default:
		return &dedup{
			badgerDB: badger.NewBadgerDB(badger.DefaultPath()),
			cache:    make(map[string]int64),
		}
	}
}

// Dedup is the interface for deduplication service
type Dedup interface {
	// Set returns [true] if it was the first time the key was encountered, otherwise it returns [false] along with the previous value
	Set(kv types.KeyValue) (bool, int64, error)

	// Commit commits a list of previously set keys to the DB
	Commit(keys []string) error

	// Close closes the deduplication service
	Close()
}

type dedup struct {
	badgerDB *badger.BadgerDB
	cacheMu  sync.Mutex
	cache    map[string]int64
}

func (d *dedup) Set(kv types.KeyValue) (bool, int64, error) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	if previous, found := d.cache[kv.Key]; found {
		return false, previous, nil
	}
	previous, found, err := d.badgerDB.Get(kv.Key)
	if err != nil {
		return false, 0, err
	}
	if !found {
		d.cache[kv.Key] = kv.Value
	}
	return !found, previous, nil
}

func (d *dedup) Commit(keys []string) error {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()

	kvs := make([]types.KeyValue, len(keys))
	for i, key := range keys {
		value, ok := d.cache[key]
		if !ok {
			return fmt.Errorf("key %v has not been previously set", key)
		}
		kvs[i] = types.KeyValue{Key: key, Value: value}
	}

	err := d.badgerDB.Set(kvs)
	if err == nil {
		for _, kv := range kvs {
			delete(d.cache, kv.Key)
		}
	}
	return err
}

func (d *dedup) Close() {
	d.badgerDB.Close()
}
