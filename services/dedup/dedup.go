//go:generate mockgen -destination=../../mocks/services/dedup/mock_dedup.go -package mock_dedup github.com/rudderlabs/rudder-server/services/dedup Dedup

package dedup

import (
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type OptFn func(*badgerDB)

// DefaultPath returns the default path for the deduplication service's badger DB
func DefaultPath() string {
	badgerPathName := "/badgerdbv4"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(`%v%v`, tmpDirPath, badgerPathName)
}

// New creates a new deduplication service. The service needs to be closed after use.
func New(path string) Dedup {
	dedupWindow := config.GetReloadableDurationVar(3600, time.Second, "Dedup.dedupWindow", "Dedup.dedupWindowInS")

	log := logger.NewLogger().Child("dedup")
	badgerOpts := badger.
		DefaultOptions(path).
		WithCompression(options.None).
		WithIndexCacheSize(16 << 20). // 16mb
		WithNumGoroutines(1).
		WithNumMemtables(config.GetInt("BadgerDB.numMemtable", 5)).
		WithValueThreshold(config.GetInt64("BadgerDB.valueThreshold", 1048576)).
		WithBlockCacheSize(0).
		WithNumVersionsToKeep(1).
		WithNumLevelZeroTables(config.GetInt("BadgerDB.numLevelZeroTables", 5)).
		WithNumLevelZeroTablesStall(config.GetInt("BadgerDB.numLevelZeroTablesStall", 15)).
		WithSyncWrites(config.GetBool("BadgerDB.syncWrites", false)).
		WithDetectConflicts(config.GetBool("BadgerDB.detectConflicts", false))

	db := &badgerDB{
		stats:  stats.Default,
		logger: loggerForBadger{log},
		path:   path,
		gcDone: make(chan struct{}),
		close:  make(chan struct{}),
		window: dedupWindow,
		opts:   badgerOpts,
	}
	db.start()
	return &dedup{
		badgerDB: db,
		cache:    make(map[string]int64),
	}
}

// Dedup is the interface for deduplication service
type Dedup interface {
	// Set returns [true] if it was the first time the key was encountered, otherwise it returns [false] along with the previous value
	Set(kv KeyValue) (bool, int64)

	// Commit commits a list of previously set keys to the DB
	Commit(keys []string) error

	// Close closes the deduplication service
	Close()
}
type KeyValue struct {
	Key   string
	Value int64
}

type dedup struct {
	badgerDB *badgerDB
	cacheMu  sync.Mutex
	cache    map[string]int64
}

func (d *dedup) Set(kv KeyValue) (bool, int64) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	if previous, found := d.cache[kv.Key]; found {
		return false, previous
	}
	previous, found := d.badgerDB.Get(kv.Key)
	if !found {
		d.cache[kv.Key] = kv.Value
	}
	return !found, previous
}

func (d *dedup) Commit(keys []string) error {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()

	kvs := make([]KeyValue, len(keys))
	for i, key := range keys {
		value, ok := d.cache[key]
		if !ok {
			return fmt.Errorf("key %v has not been previously set", key)
		}
		kvs[i] = KeyValue{Key: key, Value: value}
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

type loggerForBadger struct {
	logger.Logger
}

func (l loggerForBadger) Warningf(fmt string, args ...interface{}) {
	l.Warnf(fmt, args...)
}
