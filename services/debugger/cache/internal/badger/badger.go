package badger

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/samber/lo/mutable"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

/*
loadCacheConfig sets the properties of the cache after reading it from the config file.
This gives a feature of hot readability as well.
*/
func (e *Cache[E]) loadCacheConfig() {
	e.ttl = config.GetReloadableDurationVar(5, time.Minute, "LiveEvent.cache."+e.origin+".clearFreq", "LiveEvent.cache.ttl")
	e.limiter = config.GetReloadableIntVar(100, 1, "LiveEvent.cache."+e.origin+".limiter", "LiveEvent.cache.limiter")
	e.ticker = config.GetDurationVar(1, time.Minute, "LiveEvent.cache."+e.origin+".GCTime", "LiveEvent.cache.GCTime")
	e.queryTimeout = config.GetDurationVar(15, time.Second, "LiveEvent.cache."+e.origin+".queryTimeout", "LiveEvent.cache.queryTimeout")
	e.gcDiscardRatio = config.GetFloat64Var(0.5, "LiveEvent.cache."+e.origin+".gcDiscardRatio", "LiveEvent.cache.gcDiscardRatio")
	e.numMemtables = config.GetIntVar(5, 1, "LiveEvent.cache."+e.origin+".NumMemtables", "LiveEvent.cache.NumMemtables")
	e.numLevelZeroTables = config.GetIntVar(5, 1, "LiveEvent.cache."+e.origin+".NumLevelZeroTables", "LiveEvent.cache.NumLevelZeroTables")
	e.numLevelZeroTablesStall = config.GetIntVar(15, 1, "LiveEvent.cache."+e.origin+".NumLevelZeroTablesStall", "LiveEvent.cache.NumLevelZeroTablesStall")
	// Using the maximum value threshold: (1 << 20) == 1048576 (1MB)
	e.valueThreshold = config.GetInt64Var(1<<20, 1, "LiveEvent.cache."+e.origin+".ValueThreshold", "LiveEvent.cache.ValueThreshold")
	e.syncWrites = config.GetBoolVar(false, "LiveEvent.cache."+e.origin+".SyncWrites", "LiveEvent.cache.SyncWrites")
	e.cleanupOnStartup = config.GetBoolVar(true, "LiveEvent.cache."+e.origin+".CleanupOnStartup", "LiveEvent.cache.CleanupOnStartup")
}

/*
Cache is an in-memory cache. Each key-value pair stored in this cache have a TTL and one goroutine removes the
key-value pair form the cache which is older than TTL time.
*/
type Cache[E any] struct {
	limiter                 config.ValueLoader[int]
	path                    string
	origin                  string
	done                    chan struct{}
	closed                  chan struct{}
	ticker                  time.Duration
	queryTimeout            time.Duration
	ttl                     config.ValueLoader[time.Duration]
	gcDiscardRatio          float64
	numMemtables            int
	numLevelZeroTables      int
	numLevelZeroTablesStall int
	valueThreshold          int64
	syncWrites              bool
	cleanupOnStartup        bool
	db                      *badger.DB
	logger                  logger.Logger
	stats                   stats.Stats
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
		entry := badger.NewEntry([]byte(key), data).WithTTL(e.ttl.Load())
		return txn.SetEntry(entry)
	})
}

// Read fetches all the entries for a given key from badgerDB
func (e *Cache[E]) Read(key string) ([]E, error) {
	var values []E
	err := e.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = e.limiter.Load()
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
			if len(values) >= e.limiter.Load() {
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
	mutable.Reverse(values)
	return values, err
}

func New[E any](origin string, log logger.Logger, stats stats.Stats, opts ...func(Cache[E])) (*Cache[E], error) {
	e := Cache[E]{
		origin: origin,
		logger: log,
		stats:  stats,
	}
	e.loadCacheConfig()
	badgerPathName := e.origin + "/cache/badgerdbv4"
	defer func() {
		// TODO : Remove this after badgerdb v2 is completely removed
		_ = os.RemoveAll(fmt.Sprintf(`%v%v`, e.origin, "/badgerdbv3"))
	}()
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		e.logger.Errorf("Unable to create tmp directory: %v", err)
		return nil, err
	}
	storagePath := path.Join(tmpDirPath, badgerPathName)
	if e.cleanupOnStartup {
		if err := os.RemoveAll(storagePath); err != nil {
			e.logger.Warnf("Unable to cleanup badgerDB storage path %q: %v", storagePath, err)
		}
	}
	e.path = storagePath
	e.done = make(chan struct{})
	e.closed = make(chan struct{})

	for _, opt := range opts {
		opt(e)
	}
	badgerOpts := badger.
		DefaultOptions(e.path).
		WithLogger(badgerLogger{e.logger}).
		WithCompression(options.None).
		WithIndexCacheSize(16 << 20). // 16mb
		WithNumGoroutines(1).
		WithNumMemtables(e.numMemtables).
		WithValueThreshold(e.valueThreshold).
		WithBlockCacheSize(0).
		WithNumVersionsToKeep(e.limiter.Load()).
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
		lsmSize, vlogSize, totSize, err := misc.GetBadgerDBUsage(e.db.Opts().Dir)
		if err != nil {
			e.logger.Errorf("Error while getting badgerDB usage: %v", err)
			continue
		}

		statName := fmt.Sprintf("liveevent-cache-%s", e.origin)
		e.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "lsm"}).Gauge(lsmSize)
		e.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "vlog"}).Gauge(vlogSize)
		e.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "total"}).Gauge(totSize)
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
