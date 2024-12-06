package event_sampler

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type BadgerEventSampler struct {
	db     *badger.DB
	mu     sync.Mutex
	ttl    config.ValueLoader[time.Duration]
	ctx    context.Context
	cancel context.CancelFunc
}

func DefaultPath(pathName string) (string, error) {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`%v%v`, tmpDirPath, pathName), nil
}

func NewBadgerEventSampler(ctx context.Context, pathName string, ttl config.ValueLoader[time.Duration], conf *config.Config, log logger.Logger) (*BadgerEventSampler, error) {
	dbPath, err := DefaultPath(pathName)
	if err != nil || dbPath == "" {
		return nil, err
	}

	opts := badger.DefaultOptions(dbPath).
		WithLogger(badgerLogger{log}).
		WithCompression(options.None).
		WithIndexCacheSize(16 << 20). // 16mb
		WithNumGoroutines(1).
		WithBlockCacheSize(0).
		WithNumVersionsToKeep(1).
		WithNumMemtables(conf.GetInt("Reporting.eventSampling.badgerDB.numMemtable", 5)).
		WithValueThreshold(conf.GetInt64("Reporting.eventSampling.badgerDB.valueThreshold", 1048576)).
		WithNumLevelZeroTables(conf.GetInt("Reporting.eventSampling.badgerDB.numLevelZeroTables", 5)).
		WithNumLevelZeroTablesStall(conf.GetInt("Reporting.eventSampling.badgerDB.numLevelZeroTablesStall", 15)).
		WithSyncWrites(conf.GetBool("Reporting.eventSampling.badgerDB.syncWrites", false)).
		WithDetectConflicts(conf.GetBool("Reporting.eventSampling.badgerDB.detectConflicts", false))

	ctx, cancel := context.WithCancel(ctx)

	db, err := badger.Open(opts)

	es := &BadgerEventSampler{
		db:     db,
		ttl:    ttl,
		ctx:    ctx,
		cancel: cancel,
	}

	if err != nil {
		return nil, err
	}

	rruntime.Go(es.gcLoop)

	return es, nil
}

func (es *BadgerEventSampler) Get(key string) (bool, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	var found bool

	err := es.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		found = item != nil
		return nil
	})

	if err == badger.ErrKeyNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return found, nil
}

func (es *BadgerEventSampler) Put(key string) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	return es.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), []byte{1}).WithTTL(es.ttl.Load())
		return txn.SetEntry(entry)
	})
}

func (es *BadgerEventSampler) performGC() {
	// One call would only result in removal of at max one log file.
	// As an optimization, we can call it in a loop until it returns an error.
	for {
		select {
		case <-es.ctx.Done():
			return
		default:
			if err := es.db.RunValueLogGC(0.5); err != nil {
				return
			}
		}
	}
}

func (es *BadgerEventSampler) gcLoop() {
	for {
		select {
		case <-es.ctx.Done():
			return

		case <-time.After(5 * time.Minute):
			es.performGC()
		}
	}
}

func (es *BadgerEventSampler) Close() {
	es.cancel()
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
