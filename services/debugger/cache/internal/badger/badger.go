package badger

import (
	"encoding/json"
	"errors"
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
	rsync "github.com/rudderlabs/rudder-server/utils/sync"
)

/*
loadCacheConfig sets the properties of the cache after reading it from the config file.
This gives a feature of hot readability as well.
*/
func (e *Cache[E]) loadCacheConfig() {
	config.RegisterDurationConfigVariable(30, &e.cleanupFreq, true, time.Second, "LiveEvent.cache.clearFreq") // default clearFreq is 15 seconds
	config.RegisterIntConfigVariable(100, &e.limiter, true, 1, "LiveEvent.cache.limiter")
	config.RegisterDurationConfigVariable(5, &e.ticker, false, time.Minute, "LiveEvent.cache.GCTime")
	config.RegisterDurationConfigVariable(15, &e.queryTimeout, false, time.Second, "LiveEvent.cache.queryTimeout")
	config.RegisterIntConfigVariable(3, &e.retries, false, 1, "LiveEvent.cache.retries")
}

/*
Cache is an in-memory cache. Each key-value pair stored in this cache have a TTL and one goroutine removes the
key-value pair form the cache which is older than TTL time.
*/
type Cache[E any] struct {
	plocker      *rsync.PartitionLocker
	limiter      int
	retries      int
	path         string
	origin       string
	done         chan struct{}
	closed       chan struct{}
	ticker       time.Duration
	queryTimeout time.Duration
	cleanupFreq  time.Duration // TTL time on badgerDB
	db           *badger.DB
	logger       logger.Logger
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
	e.plocker.Lock(key)
	defer e.plocker.Unlock(key)
	operation := func() error {
		return e.db.Update(func(txn *badger.Txn) error {
			res, err := txn.Get([]byte(key))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			var values []E
			if res != nil {
				if err = res.Value(func(val []byte) error {
					return json.Unmarshal(val, &values)
				}); err != nil {
					return err
				}
			}
			if len(values) >= e.limiter {
				values = values[len(values)-e.limiter+1:]
			}
			values = append(values, value)
			data, err := json.Marshal(values)
			if err != nil {
				return err
			}
			entry := badger.NewEntry([]byte(key), data).WithTTL(e.cleanupFreq)
			return txn.SetEntry(entry)
		})
	}
	var err error
	for i := 0; i < e.retries; i++ {
		if err = operation(); !errors.Is(err, badger.ErrConflict) {
			return err
		}
		e.logger.Warnf("Retrying update func because of ErrConflict %d", i+1)
	}
	return err
}

// Read fetches all the entries for a given key from badgerDB
func (e *Cache[E]) Read(key string) ([]E, error) {
	var values []E
	err := e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &values)
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return values, err
}

func New[E any](origin string, logger logger.Logger, opts ...func(Cache[E])) (*Cache[E], error) {
	e := Cache[E]{
		plocker: rsync.NewPartitionLocker(),
		origin:  origin,
		logger:  logger,
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
		WithNumMemtables(0).
		WithBlockCacheSize(0)

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
			err := e.db.RunValueLogGC(0.7)
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
