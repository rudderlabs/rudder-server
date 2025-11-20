package badger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/rruntime"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/services/dedup/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type badgerDB struct {
	logger           loggerForBadger
	badgerDB         *badger.DB
	window           config.ValueLoader[time.Duration]
	path             string
	opts             badger.Options
	cleanupOnStartup bool

	wg     sync.WaitGroup
	bgCtx  context.Context
	cancel context.CancelFunc
	stats  struct {
		getTimer stats.Timer
		setTimer stats.Timer
		lsmSize  stats.Gauge
		vlogSize stats.Gauge
		totSize  stats.Gauge
	}
}

// DefaultPath returns the default path for the deduplication service's badger DB
func DefaultPath() string {
	badgerPathName := "/badgerdbv4"
	tmpDirPath, err := misc.GetTmpDir()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(`%v%v`, tmpDirPath, badgerPathName)
}

func NewBadgerDB(conf *config.Config, stat stats.Stats, path string) (types.DB, error) {
	dedupWindow := conf.GetReloadableDurationVar(3600, time.Second, "Dedup.dedupWindow", "Dedup.dedupWindowInS")
	log := logger.NewLogger().Child("Dedup")
	badgerOpts := badger.
		DefaultOptions(path).
		WithCompression(options.None).
		WithNumGoroutines(1).
		WithNumVersionsToKeep(1).
		WithIndexCacheSize(conf.GetInt64Var(16*bytesize.MB, 1, "BadgerDB.Dedup.indexCacheSize", "BadgerDB.indexCacheSize")).
		WithValueLogFileSize(conf.GetInt64Var(1*bytesize.MB, 1, "BadgerDB.Dedup.valueLogFileSize", "BadgerDB.valueLogFileSize")).
		WithBlockSize(conf.GetIntVar(int(4*bytesize.KB), 1, "BadgerDB.Dedup.blockSize", "BadgerDB.blockSize")).
		WithMemTableSize(conf.GetInt64Var(20*bytesize.MB, 1, "BadgerDB.Dedup.memTableSize", "BadgerDB.memTableSize")).
		WithNumMemtables(conf.GetIntVar(5, 1, "BadgerDB.Dedup.numMemtable", "BadgerDB.numMemtable")).
		WithNumLevelZeroTables(conf.GetIntVar(5, 1, "BadgerDB.Dedup.numLevelZeroTables", "BadgerDB.numLevelZeroTables")).
		WithNumLevelZeroTablesStall(conf.GetIntVar(10, 1, "BadgerDB.Dedup.numLevelZeroTablesStall", "BadgerDB.numLevelZeroTablesStall")).
		WithBaseTableSize(conf.GetInt64Var(1*bytesize.MB, 1, "BadgerDB.Dedup.baseTableSize", "BadgerDB.baseTableSize")).
		WithBaseLevelSize(conf.GetInt64Var(5*bytesize.MB, 1, "BadgerDB.Dedup.baseLevelSize", "BadgerDB.baseLevelSize")).
		WithLevelSizeMultiplier(conf.GetIntVar(10, 1, "BadgerDB.Dedup.levelSizeMultiplier", "BadgerDB.levelSizeMultiplier")).
		WithMaxLevels(conf.GetIntVar(7, 1, "BadgerDB.Dedup.maxLevels", "BadgerDB.maxLevels")).
		WithNumCompactors(conf.GetIntVar(4, 1, "BadgerDB.Dedup.numCompactors", "BadgerDB.numCompactors")).
		WithValueThreshold(conf.GetInt64Var(10*bytesize.B, 1, "BadgerDB.Dedup.valueThreshold", "BadgerDB.valueThreshold")).
		WithSyncWrites(conf.GetBoolVar(false, "BadgerDB.Dedup.syncWrites", "BadgerDB.syncWrites")).
		WithBlockCacheSize(conf.GetInt64Var(0, 1, "BadgerDB.Dedup.blockCacheSize", "BadgerDB.blockCacheSize")).
		WithDetectConflicts(conf.GetBoolVar(false, "BadgerDB.Dedup.detectConflicts", "BadgerDB.detectConflicts"))

	bgCtx, cancel := context.WithCancel(context.Background())
	db := &badgerDB{
		logger:           loggerForBadger{log},
		path:             path,
		window:           dedupWindow,
		opts:             badgerOpts,
		bgCtx:            bgCtx,
		cancel:           cancel,
		cleanupOnStartup: conf.GetBoolVar(false, "BadgerDB.Dedup.cleanupOnStartup", "BadgerDB.cleanupOnStartup"),
	}
	db.stats.getTimer = stat.NewTaggedStat("dedup_get_duration_seconds", stats.TimerType, stats.Tags{"mode": "badger"})
	db.stats.setTimer = stat.NewTaggedStat("dedup_set_duration_seconds", stats.TimerType, stats.Tags{"mode": "badger"})
	db.stats.lsmSize = stat.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": "dedup", "type": "lsm"})
	db.stats.vlogSize = stat.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": "dedup", "type": "vlog"})
	db.stats.totSize = stat.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": "dedup", "type": "total"})

	err := db.init()
	if err != nil {
		return nil, fmt.Errorf("initializing badger db: %w", err)
	}
	return db, nil
}

func (d *badgerDB) Get(keys []string) (map[string]bool, error) {
	defer d.stats.getTimer.RecordDuration()()
	results := make(map[string]bool, len(keys))
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			if _, err := txn.Get([]byte(key)); err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return err
			}
			results[key] = true
		}
		return nil
	})
	return results, err
}

func (d *badgerDB) Set(keys []string) error {
	defer d.stats.setTimer.RecordDuration()()
	wb := d.badgerDB.NewWriteBatch()
	defer wb.Cancel()
	for _, key := range keys {
		e := badger.NewEntry([]byte(key), nil).WithTTL(d.window.Load())
		if err := wb.SetEntry(e); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (d *badgerDB) Close() {
	d.cancel()
	d.wg.Wait()
	if d.badgerDB != nil {
		_ = d.badgerDB.Close()
	}
}

func (d *badgerDB) init() error {
	var err error
	if d.cleanupOnStartup {
		if err = os.RemoveAll(d.path); err != nil {
			err = fmt.Errorf("removing badger db directory: %w", err)
			return err
		}
	}
	openDB := func() (dbase *badger.DB, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic during badgerdb open: %v", r)
			}
		}()
		return badger.Open(d.opts)
	}
	d.badgerDB, err = openDB()
	if err != nil {
		// corrupted or incompatible db, clean up the directory and retry
		d.logger.Errorn("Error while opening dedup badger db, cleaning up the directory",
			obskit.Error(err),
		)
		if err = os.RemoveAll(d.opts.Dir); err != nil {
			err = fmt.Errorf("removing badger db directory: %w", err)
			return err
		}
		d.badgerDB, err = openDB()
		if err != nil {
			err = fmt.Errorf("opening badger db: %w", err)
			return err
		}
	}
	d.wg.Add(1)
	rruntime.Go(func() {
		defer d.wg.Done()
		d.gcLoop()
	})
	return err
}

func (d *badgerDB) gcLoop() {
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
		d.stats.lsmSize.Gauge(lsmSize)
		d.stats.vlogSize.Gauge(vlogSize)
		d.stats.totSize.Gauge(totSize)
	}
}

type loggerForBadger struct {
	logger.Logger
}

func (l loggerForBadger) Warningf(fmt string, args ...interface{}) {
	l.Warnf(fmt, args...)
}
