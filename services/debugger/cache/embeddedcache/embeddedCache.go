package embeddedcache

import (
	"encoding/json"
	"fmt"
	"math/rand"
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

var delimiter = []byte("$$")

/*
loadCacheConfig sets the properties of the cache after reading it from the config file.
This gives a feature of hot readability as well.
*/
func (e *EmbeddedCache[E]) loadCacheConfig() {
	if e.cleanupFreq == 0 {
		config.RegisterDurationConfigVariable(30, &e.cleanupFreq, true, time.Second, "LiveEvent.cache.clearFreq") // default clearFreq is 15 seconds
	}
	if e.limiter == 0 {
		config.RegisterIntConfigVariable(100, &e.limiter, true, 1, "LiveEvent.cache.limiter")
	}
}

/*
EmbeddedCache is an in-memory cache. Each key-value pair stored in this cache have a TTL and one goroutine removes the
key-value pair form the cache which is older than TTL time.
*/
type EmbeddedCache[E any] struct {
	cleanupFreq time.Duration // TTL time on badgerDB
	limiter     int
	once        sync.Once
	db          *badger.DB
	dbL         sync.RWMutex
	Logger      logger.Logger
	path        string
	Origin      string
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

func (e *EmbeddedCache[E]) Update(key string, value E) {
	e.init()
	e.dbL.Lock()
	defer e.dbL.Unlock()
	txn := e.db.NewTransaction(true)
	byteKey, byteVal := keyPrefixValue(key, value, e.limiter)
	entry := badger.NewEntry(byteKey, byteVal).WithTTL(e.cleanupFreq)
	if err := txn.SetEntry(entry); err == badger.ErrTxnTooBig {
		err = txn.Commit()
		if err != nil {
			return
		}
		txn = e.db.NewTransaction(true)
		err = txn.SetEntry(entry)
		if err != nil {
			return
		}
	}
	_ = txn.Commit()
}

func (e *EmbeddedCache[E]) Read(key string) []E {
	e.init()
	e.dbL.RLock()
	defer e.dbL.RUnlock()
	var values []E
	err := e.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(key)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				var value E
				err := json.Unmarshal(v, &value)
				if err != nil {
					return err
				}
				values = append(values, value)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return values
}

func (e *EmbeddedCache[E]) init() {
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
		opts := badger.
			DefaultOptions(storagePath).
			WithLogger(badgerLogger{e.Logger}).
			WithCompression(options.None).
			WithIndexCacheSize(16 << 20). // 16mb
			WithNumGoroutines(1).
			WithNumMemtables(0).
			WithBlockCacheSize(0)

		e.db, err = badger.Open(opts)
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
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	// One call would only result in removal of at max one log file.
	// As an optimization, you could also immediately re-run it whenever it returns nil error
	// (this is why `goto again` is used).
	for {
		<-ticker.C
	again: // see https://dgraph.io/docs/badger/get-started/#garbage-collection
		err := e.db.RunValueLogGC(0.7)
		if err == nil {
			goto again
		}
	}
}

func keyPrefixValue[E any](key string, value E, limiter int) ([]byte, []byte) {
	prefixedKey := []byte(fmt.Sprintf("%s%s%v", key, delimiter, rand.Intn(limiter)))
	valueBytes, _ := json.Marshal(value)
	return prefixedKey, valueBytes
}

func (e *EmbeddedCache[E]) stop() error {
	err := e.db.Close()
	if err != nil {
		return err
	}
	return nil
}
