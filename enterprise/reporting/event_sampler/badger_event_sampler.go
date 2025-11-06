package event_sampler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type BadgerEventSampler struct {
	db     *badger.DB
	dbPath string
	module string
	mu     sync.Mutex
	ttl    config.ValueLoader[time.Duration]
	ctx    context.Context
	cancel context.CancelFunc
	logger badgerLogger
	wg     sync.WaitGroup
	sc     *StatsCollector
}

func GetPathName(module string) string {
	return "/" + module + "-badger"
}

func DefaultPath(pathName string) (string, error) {
	tmpDirPath, err := misc.GetTmpDir()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`%v%v`, tmpDirPath, pathName), nil
}

func NewBadgerEventSampler(
	ctx context.Context,
	module string,
	ttl config.ValueLoader[time.Duration],
	conf *config.Config,
	log logger.Logger,
	stats stats.Stats,
) (*BadgerEventSampler, error) {
	dbPath, err := DefaultPath(GetPathName(module))
	if err != nil || dbPath == "" {
		return nil, err
	}

	opts := badger.DefaultOptions(dbPath).
		WithLogger(badgerLogger{log}).
		WithCompression(options.None).
		WithNumGoroutines(1).
		WithNumVersionsToKeep(1).
		WithIndexCacheSize(conf.GetInt64Var(10*bytesize.MB, 1, "BadgerDB.EventSampler.indexCacheSize", "BadgerDB.indexCacheSize")).
		WithValueLogFileSize(conf.GetInt64Var(1*bytesize.MB, 1, "BadgerDB.EventSampler.valueLogFileSize", "BadgerDB.valueLogFileSize")).
		WithBlockSize(conf.GetIntVar(int(4*bytesize.KB), 1, "BadgerDB.EventSampler.blockSize", "BadgerDB.blockSize")).
		WithMemTableSize(conf.GetInt64Var(1*bytesize.MB, 1, "BadgerDB.EventSampler.memTableSize", "BadgerDB.memTableSize")).
		WithNumMemtables(conf.GetIntVar(5, 1, "BadgerDB.EventSampler.numMemtable", "BadgerDB.numMemtable")).
		WithNumLevelZeroTables(conf.GetIntVar(5, 1, "BadgerDB.EventSampler.numLevelZeroTables", "BadgerDB.numLevelZeroTables")).
		WithNumLevelZeroTablesStall(conf.GetIntVar(10, 1, "BadgerDB.EventSampler.numLevelZeroTablesStall", "BadgerDB.numLevelZeroTablesStall")).
		WithBaseTableSize(conf.GetInt64Var(200*bytesize.KB, 1, "BadgerDB.EventSampler.baseTableSize", "BadgerDB.baseTableSize")).
		WithBaseLevelSize(conf.GetInt64Var(1*bytesize.MB, 1, "BadgerDB.EventSampler.baseLevelSize", "BadgerDB.baseLevelSize")).
		WithLevelSizeMultiplier(conf.GetIntVar(10, 1, "BadgerDB.EventSampler.levelSizeMultiplier", "BadgerDB.levelSizeMultiplier")).
		WithMaxLevels(conf.GetIntVar(7, 1, "BadgerDB.EventSampler.maxLevels", "BadgerDB.maxLevels")).
		WithNumCompactors(conf.GetIntVar(4, 1, "BadgerDB.EventSampler.numCompactors", "BadgerDB.numCompactors")).
		WithValueThreshold(conf.GetInt64Var(10*bytesize.B, 1, "BadgerDB.EventSampler.valueThreshold", "BadgerDB.valueThreshold")).
		WithSyncWrites(conf.GetBoolVar(false, "BadgerDB.EventSampler.syncWrites", "BadgerDB.syncWrites")).
		WithBlockCacheSize(conf.GetInt64Var(0, 1, "BadgerDB.EventSampler.blockCacheSize", "BadgerDB.blockCacheSize")).
		WithDetectConflicts(conf.GetBoolVar(false, "BadgerDB.EventSampler.detectConflicts", "BadgerDB.detectConflicts"))

	if conf.GetBoolVar(false, "BadgerDB.EventSampler.cleanupOnStartup", "BadgerDB.cleanupOnStartup") {
		if err := os.RemoveAll(opts.Dir); err != nil {
			return nil, fmt.Errorf("removing badger db directory: %w", err)
		}
	}

	openDB := func() (dbase *badger.DB, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic during badgerdb open: %v", r)
			}
		}()
		return badger.Open(opts)
	}
	db, err := openDB()
	if err != nil {
		// corrupted or incompatible db, clean up the directory and retry
		log.Errorn("Error while opening event sampler badger db, cleaning up the directory",
			logger.NewStringField("module", module),
			obskit.Error(err),
		)
		if err := os.RemoveAll(opts.Dir); err != nil {
			return nil, fmt.Errorf("removing badger db directory: %w", err)
		}
		if db, err = openDB(); err != nil {
			return nil, fmt.Errorf("opening badger db: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	es := &BadgerEventSampler{
		db:     db,
		dbPath: dbPath,
		module: module,
		ttl:    ttl,
		ctx:    ctx,
		cancel: cancel,
		logger: badgerLogger{log},
		wg:     sync.WaitGroup{},
		sc:     NewStatsCollector(BadgerTypeEventSampler, module, stats),
	}
	es.wg.Add(1)
	rruntime.Go(func() {
		defer es.wg.Done()
		es.gcLoop()
	})

	return es, nil
}

func (es *BadgerEventSampler) Get(key string) (bool, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	start := time.Now()
	defer es.sc.RecordGetDuration(start)
	es.sc.RecordGet()

	var found bool

	err := es.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		found = item != nil
		return nil
	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return found, nil
}

func (es *BadgerEventSampler) Put(key string) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	start := time.Now()
	defer es.sc.RecordPutDuration(start)
	es.sc.RecordPut()

	return es.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), nil).WithTTL(es.ttl.Load())
		return txn.SetEntry(entry)
	})
}

func (es *BadgerEventSampler) gcLoop() {
	for {
		select {
		case <-es.ctx.Done():
			_ = es.db.RunValueLogGC(0.5)
			return
		case <-time.After(5 * time.Minute):
		}
	again:
		if es.ctx.Err() != nil {
			return
		}
		// One call would only result in removal of at max one log file.
		// As an optimization, you could also immediately re-run it whenever it returns nil error
		// (this is why `goto again` is used).
		err := es.db.RunValueLogGC(0.5)
		if err == nil {
			goto again
		}

		lsmSize, vlogSize, totSize, err := misc.GetBadgerDBUsage(es.dbPath)
		if err != nil {
			es.logger.Errorf("Error while getting %s BadgerDB usage: %v", es.module, err)
			continue
		}
		es.sc.RecordBadgerDBSize("lsm", lsmSize)
		es.sc.RecordBadgerDBSize("vlog", vlogSize)
		es.sc.RecordBadgerDBSize("total", totSize)
	}
}

func (es *BadgerEventSampler) Close() {
	es.cancel()
	es.wg.Wait()
	if es.db != nil {
		_ = es.db.Close()
	}
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
