package badger

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
)

/*
loadCacheConfig sets the properties of the cache after reading it from the config file.
This gives a feature of hot readability as well.
*/
func (e *Cache[E]) loadCacheConfig() {
	config.RegisterDurationConfigVariable(30, &e.cleanupFreq, true, time.Second, "LiveEvent.cache."+e.origin+".clearFreq", "LiveEvent.cache.clearFreq") // default clearFreq is 30 seconds
	config.RegisterIntConfigVariable(100, &e.limiter, true, 1, "LiveEvent.cache."+e.origin+".limiter", "LiveEvent.cache.limiter")
	config.RegisterDurationConfigVariable(1, &e.ticker, false, time.Minute, "LiveEvent.cache."+e.origin+".GCTime", "LiveEvent.cache.GCTime")
	config.RegisterDurationConfigVariable(15, &e.queryTimeout, false, time.Second, "LiveEvent.cache."+e.origin+".queryTimeout", "LiveEvent.cache.queryTimeout")
	config.RegisterFloat64ConfigVariable(0.5, &e.gcDiscardRatio, false, "LiveEvent.cache."+e.origin+".gcDiscardRatio", "LiveEvent.cache.gcDiscardRatio")
	config.RegisterIntConfigVariable(5, &e.numMemtables, false, 1, "LiveEvent.cache."+e.origin+".NumMemtables", "LiveEvent.cache.NumMemtables")
	config.RegisterIntConfigVariable(5, &e.numLevelZeroTables, false, 1, "LiveEvent.cache."+e.origin+".NumLevelZeroTables", "LiveEvent.cache.NumLevelZeroTables")
	config.RegisterIntConfigVariable(15, &e.numLevelZeroTablesStall, false, 1, "LiveEvent.cache."+e.origin+".NumLevelZeroTablesStall", "LiveEvent.cache.NumLevelZeroTablesStall")
	// 512 bytes - prefer using Value Log over LSM tree
	config.RegisterInt64ConfigVariable(512, &e.valueThreshold, false, 1, "LiveEvent.cache."+e.origin+".ValueThreshold", "LiveEvent.cache.ValueThreshold")
	config.RegisterBoolConfigVariable(false, &e.syncWrites, false, "LiveEvent.cache."+e.origin+".SyncWrites", "LiveEvent.cache.SyncWrites")
}

/*
Cache is an in-memory cache. Each key-value pair stored in this cache have a TTL and one goroutine removes the
key-value pair form the cache which is older than TTL time.
*/
type Cache[E any] struct {
	limiter                 int
	path                    string
	origin                  string
	done                    chan struct{}
	closed                  chan struct{}
	ticker                  time.Duration
	queryTimeout            time.Duration
	cleanupFreq             time.Duration // TTL time on badgerDB
	gcDiscardRatio          float64
	numMemtables            int
	numLevelZeroTables      int
	numLevelZeroTablesStall int
	valueThreshold          int64
	syncWrites              bool
	db                      *badger.DB
	logger                  logger.Logger
}

type badgerLogger struct {
	logger.Logger
}

func (badgerLogger) Errorf(format string, a ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, format, a...)
}

func (badgerLogger) Warningf(format string, a ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, format, a...)
}

// Update writes the entries into badger db with a TTL
func (e *Cache[E]) Update(key string, value E) error {
	return e.db.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		entry := badger.NewEntry([]byte(key), data).WithTTL(e.cleanupFreq)
		return txn.SetEntry(entry)
	})
}

// Read fetches all the entries for a given key from badgerDB
func (e *Cache[E]) Read(key string) ([]E, error) {
	var values []E
	err := e.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = e.limiter
		itr := txn.NewKeyIterator([]byte(key), opts)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			if itr.Item().IsDeletedOrExpired() {
				break
			}
			var value E
			if err := itr.Item().Value(func(val []byte) error {
				return json.Unmarshal(val, &value)
			}); err == nil { // ignore unmarshal errors (old version of the data)
				values = append(values, value)
			}
			if len(values) >= e.limiter {
				break
			}
		}
		return nil
	})
	if err == nil {
		_ = e.db.Update(func(txn *badger.Txn) error {
			return txn.Delete([]byte(key))
		})
	}

	return lo.Reverse(values), err
}

func New[E any](origin string, logger logger.Logger, opts ...func(Cache[E])) (*Cache[E], error) {
	e := Cache[E]{
		origin: origin,
		logger: logger,
	}
	e.loadCacheConfig()
	badgerPathName := e.origin + "/cache/badgerdbv3"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		e.logger.Errorf("Unable to create tmp directory: %v", err)
		return nil, err
	}
	storagePath := path.Join(tmpDirPath, badgerPathName)
	e.path = storagePath
	e.done = make(chan struct{})
	e.closed = make(chan struct{})

	for _, opt := range opts {
		opt(e)
	}
	badgerOpts := badger.
		DefaultOptions(storagePath).
		WithLogger(badgerLogger{e.logger}).
		WithCompression(options.None).
		WithIndexCacheSize(16 << 20). // 16mb
		WithNumGoroutines(1).
		WithNumMemtables(e.numMemtables).
		WithValueThreshold(e.valueThreshold).
		WithBlockCacheSize(0).
		WithNumVersionsToKeep(e.limiter).
		WithNumLevelZeroTables(e.numLevelZeroTables).
		WithNumLevelZeroTablesStall(e.numLevelZeroTablesStall).
		WithSyncWrites(e.syncWrites)
	e.db, err = badger.Open(badgerOpts)
	if err != nil {
		e.logger.Errorf("Error while opening badgerDB: %v", err)
		return nil, err
	}
	rruntime.Go(func() {
		e.gcBadgerDB()
	})
	return &e, nil
}

func (e *Cache[E]) gcBadgerDB() {
	ticker := time.NewTicker(e.ticker)
	defer ticker.Stop()
	// One call would only result in removal of at max one log file.
	// As an optimization, you could also immediately re-run it whenever it returns nil error
	// (this is why `goto again` is used).
	for {
		select {
		case <-e.done:
			close(e.closed)
			return
		case <-ticker.C:
		again: // see https://dgraph.io/docs/badger/get-started/#garbage-collection
			err := e.db.RunValueLogGC(e.gcDiscardRatio)
			if err == nil {
				goto again
			}
		}
	}
}

func (e *Cache[E]) Stop() error {
	close(e.done)
	<-e.closed
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}
