package embeddedcache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

/*
loadCacheConfig sets the properties of the cache after reading it from the config file.
This gives a feature of hot readability as well.
*/
func (e *EmbeddedCache[E]) loadCacheConfig() {
	config.RegisterDurationConfigVariable(30, &e.CleanupFreq, true, time.Second, "LiveEvent.cache.clearFreq") // default clearFreq is 15 seconds
	config.RegisterIntConfigVariable(100, &e.Limiter, true, 1, "LiveEvent.cache.limiter")
	config.RegisterDurationConfigVariable(5, &e.Ticker, false, time.Minute, "LiveEvent.cache.GCTime")
	config.RegisterDurationConfigVariable(15, &e.queryTimeout, false, time.Second, "LiveEvent.cache.queryTimeout")
	config.RegisterIntConfigVariable(3, &e.retries, false, 1, "LiveEvent.cache.retries")
}

/*
EmbeddedCache is an in-memory cache. Each key-value pair stored in this cache have a TTL and one goroutine removes the
key-value pair form the cache which is older than TTL time.
*/
type EmbeddedCache[E any] struct {
	Limiter      int
	retries      int
	path         string
	Origin       string
	done         chan struct{}
	closed       chan struct{}
	Ticker       time.Duration
	queryTimeout time.Duration
	CleanupFreq  time.Duration // TTL time on badgerDB
	once         sync.Once
	Db           *badger.DB
	Logger       logger.Logger
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
func (e *EmbeddedCache[E]) Update(key string, value E) {
	e.Init()
	txn := e.Db.NewTransaction(true)
	res := e.Read(key)
	if len(res) >= e.Limiter {
		res = res[len(res)-e.Limiter+1:]
	}
	res = append(res, value)
	data, err := json.Marshal(res)
	if err != nil {
		return
	}
	setBadgerEntry := func(ctx context.Context) error {
		entry := badger.NewEntry([]byte(key), data).WithTTL(e.CleanupFreq)
		if err := txn.SetEntry(entry); err == badger.ErrTxnTooBig {
			err = txn.Commit()
			if err != nil {
				return err
			}
			txn = e.Db.NewTransaction(true)
			err = txn.SetEntry(entry)
			if err != nil {
				return err
			}
		}
		return nil
	}
	_ = misc.RetryWith(context.TODO(), e.queryTimeout, e.retries, setBadgerEntry)
	_ = txn.Commit()
}

// Read fetches all the entries for a given key from badgerDB
func (e *EmbeddedCache[E]) Read(key string) []E {
	e.Init()
	read := func(ctx context.Context) ([]E, error) {
		var values []E
		err := e.Db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			prefix := []byte(key)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				err := item.Value(func(v []byte) error {
					err := json.Unmarshal(v, &values)
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return values, nil
	}

	res, _ := misc.QueryWithRetries(context.TODO(), e.queryTimeout, e.retries, read)

	return res
}

func (e *EmbeddedCache[E]) Init() {
	e.once.Do(func() {
		e.loadCacheConfig()
		badgerPathName := e.Origin + "/cache/badgerdbv3"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			e.Logger.Errorf("Unable to create tmp directory: %v", err)
			return
		}
		storagePath := path.Join(tmpDirPath, badgerPathName)
		e.path = storagePath
		e.done = make(chan struct{})
		e.closed = make(chan struct{})
		opts := badger.
			DefaultOptions(storagePath).
			WithLogger(badgerLogger{e.Logger}).
			WithCompression(options.None).
			WithIndexCacheSize(16 << 20). // 16mb
			WithNumGoroutines(1).
			WithNumMemtables(0).
			WithBlockCacheSize(0)

		e.Db, err = badger.Open(opts)
		if err != nil {
			e.Logger.Errorf("Error while opening badgerDB: %v", err)
			return
		}
		rruntime.Go(func() {
			e.gcBadgerDB()
		})
	})
}

func (e *EmbeddedCache[E]) gcBadgerDB() {
	ticker := time.NewTicker(e.Ticker)
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
			err := e.Db.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
	}
}

func (e *EmbeddedCache[E]) Stop() error {
	e.Init()
	close(e.done)
	<-e.closed
	if e.Db == nil {
		return nil
	}
	return e.Db.Close()
}
