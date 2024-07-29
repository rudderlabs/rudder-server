package badger

import (
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
	close    chan struct{}
	gcDone   chan struct{}
	path     string
	opts     badger.Options
	once     sync.Once
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

func NewBadgerDB(path string) *BadgerDB {
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

	db := &BadgerDB{
		stats:  stats.Default,
		logger: loggerForBadger{log},
		path:   path,
		gcDone: make(chan struct{}),
		close:  make(chan struct{}),
		window: dedupWindow,
		opts:   badgerOpts,
	}
	return db
}

func (d *BadgerDB) Get(key string) (int64, bool, error) {
	var payloadSize int64
	var found bool
	var err error
	err = d.init()
	if err != nil {
		return 0, false, err
	}
	err = d.badgerDB.View(func(txn *badger.Txn) error {
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
	err := d.init()
	if err != nil {
		return err
	}
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
	close(d.close)
	<-d.gcDone
	_ = d.badgerDB.Close()
}

func (d *BadgerDB) init() error {
	var err error

	d.once.Do(func() {
		d.badgerDB, err = badger.Open(d.opts)
		if err != nil {
			return
		}
		rruntime.Go(func() {
			d.gcLoop()
			close(d.gcDone)
		})
	})
	return err
}

func (d *BadgerDB) gcLoop() {
	for {
		select {
		case <-d.close:
			_ = d.badgerDB.RunValueLogGC(0.5)
			return
		case <-time.After(5 * time.Minute):
		}
	again:
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

type loggerForBadger struct {
	logger.Logger
}

func (l loggerForBadger) Warningf(fmt string, args ...interface{}) {
	l.Warnf(fmt, args...)
}
