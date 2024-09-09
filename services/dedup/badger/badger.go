package badger

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type BadgerDB struct {
	stats    stats.Stats
	logger   loggerForBadger
	badgerDB *badger.DB
	window   config.ValueLoader[time.Duration]
	path     string
	opts     badger.Options
	once     sync.Once
	wg       sync.WaitGroup
	bgCtx    context.Context
	cancel   context.CancelFunc
}

// DefaultPath returns the default path for the deduplication service's badger DB
func DefaultPath() string {
	badgerPathName := "/badgerdbv4"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(`%v%v`, tmpDirPath, badgerPathName)
}

func NewBadgerDB(conf *config.Config, stats stats.Stats, path string) *Dedup {
	dedupWindow := conf.GetReloadableDurationVar(3600, time.Second, "Dedup.dedupWindow", "Dedup.dedupWindowInS")
	log := logger.NewLogger().Child("Dedup")
	badgerOpts := badger.
		DefaultOptions(path).
		WithCompression(options.None).
		WithIndexCacheSize(16 << 20). // 16mb
		WithNumGoroutines(1).
		WithNumMemtables(conf.GetInt("BadgerDB.numMemtable", 5)).
		WithValueThreshold(conf.GetInt64("BadgerDB.valueThreshold", 1048576)).
		WithBlockCacheSize(0).
		WithNumVersionsToKeep(1).
		WithNumLevelZeroTables(conf.GetInt("BadgerDB.numLevelZeroTables", 5)).
		WithNumLevelZeroTablesStall(conf.GetInt("BadgerDB.numLevelZeroTablesStall", 15)).
		WithSyncWrites(conf.GetBool("BadgerDB.syncWrites", false)).
		WithDetectConflicts(conf.GetBool("BadgerDB.detectConflicts", false))

	bgCtx, cancel := context.WithCancel(context.Background())
	db := &BadgerDB{
		stats:  stats,
		logger: loggerForBadger{log},
		path:   path,
		window: dedupWindow,
		opts:   badgerOpts,
		wg:     sync.WaitGroup{},
		bgCtx:  bgCtx,
		cancel: cancel,
	}
	return &Dedup{
		badgerDB: db,
		cache:    make(map[string]int64),
	}
}

func (d *BadgerDB) Get(key string) (int64, bool, error) {
	var payloadSize int64
	var found bool
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		if itemValue, err := item.ValueCopy(nil); err == nil {
			payloadSize, _ = strconv.ParseInt(string(itemValue), 10, 64)
			found = true
		}
		return nil
	})
	if err != nil && err != badger.ErrKeyNotFound {
		return 0, false, err
	}
	return payloadSize, found, nil
}

func (d *BadgerDB) Set(kvs []types.KeyValue) error {
	txn := d.badgerDB.NewTransaction(true)
	for _, message := range kvs {
		value := strconv.FormatInt(message.Value, 10)
		e := badger.NewEntry([]byte(message.Key), []byte(value)).WithTTL(d.window.Load())
		err := txn.SetEntry(e)
		if err == badger.ErrTxnTooBig {
			if err = txn.Commit(); err != nil {
				return err
			}
			txn = d.badgerDB.NewTransaction(true)
			if err = txn.SetEntry(e); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	return txn.Commit()
}

func (d *BadgerDB) Close() {
	d.cancel()
	d.wg.Wait()
	if d.badgerDB != nil {
		_ = d.badgerDB.Close()
	}
}

func (d *BadgerDB) init() error {
	var err error

	d.once.Do(func() {
		d.wg = sync.WaitGroup{}

		d.badgerDB, err = badger.Open(d.opts)
		if err != nil {
			return
		}
		d.wg.Add(1)
		rruntime.Go(func() {
			defer d.wg.Done()
			d.gcLoop()
		})
	})
	return err
}

func (d *BadgerDB) gcLoop() {
	for {
		select {
		case <-d.bgCtx.Done():
			_ = d.badgerDB.RunValueLogGC(0.5)
			return
		case <-time.After(5 * time.Minute):
		}
	again:
		if d.bgCtx.Err() != nil {
			return
		}
		// One call would only result in removal of at max one log file.
		// As an optimization, you could also immediately re-run it whenever it returns nil error
		// (this is why `goto again` is used).
		err := d.badgerDB.RunValueLogGC(0.5)
		if err == nil {
			goto again
		}
		lsmSize, vlogSize, totSize, err := misc.GetBadgerDBUsage(d.path)
		if err != nil {
			d.logger.Errorf("Error while getting badgerDB usage: %v", err)
			continue
		}
		statName := "dedup"
		d.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "lsm"}).Gauge(lsmSize)
		d.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "vlog"}).Gauge(vlogSize)
		d.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "total"}).Gauge(totSize)
	}
}

type Dedup struct {
	badgerDB *BadgerDB
	cacheMu  sync.Mutex
	cache    map[string]int64
}

func (d *Dedup) Get(kv types.KeyValue) (bool, int64, error) {
	err := d.badgerDB.init()
	if err != nil {
		return false, 0, err
	}

	d.cacheMu.Lock()
	previous, found := d.cache[kv.Key]
	d.cacheMu.Unlock()
	if found {
		return false, previous, nil
	}

	previous, found, err = d.badgerDB.Get(kv.Key)
	if err != nil {
		return false, 0, err
	}

	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	if !found { // still not in the cache, but it's in the DB so let's refresh the cache
		d.cache[kv.Key] = kv.Value
	}

	return !found, previous, nil
}

func (d *Dedup) Commit(keys []string) error {
	err := d.badgerDB.init()
	if err != nil {
		return err
	}
	kvs := make([]types.KeyValue, len(keys))
	d.cacheMu.Lock()
	for i, key := range keys {
		value, ok := d.cache[key]
		if !ok {
			d.cacheMu.Unlock()
			return fmt.Errorf("key %v has not been previously set", key)
		}
		kvs[i] = types.KeyValue{Key: key, Value: value}
	}
	d.cacheMu.Unlock()

	err = d.badgerDB.Set(kvs)
	if err != nil {
		return err
	}

	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	for _, kv := range kvs {
		delete(d.cache, kv.Key)
	}
	return nil
}

func (d *Dedup) Close() {
	d.badgerDB.Close()
}

type loggerForBadger struct {
	logger.Logger
}

func (l loggerForBadger) Warningf(fmt string, args ...interface{}) {
	l.Warnf(fmt, args...)
}
