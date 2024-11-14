package reporting

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
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type EventSampler struct {
	db     *badger.DB
	mu     sync.Mutex
	ttl    config.ValueLoader[time.Duration]
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func DefaultPath(pathName string) string {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(`%v%v`, tmpDirPath, pathName)
}

func NewEventSampler(pathName string, ttl config.ValueLoader[time.Duration], conf *config.Config, log logger.Logger) (*EventSampler, error) {
	dbPath := DefaultPath(pathName)
	opts := badger.DefaultOptions(dbPath).
		WithLogger(badgerLogger{log}).
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

	ctx, cancel := context.WithCancel(context.Background())

	db, err := badger.Open(opts)

	es := &EventSampler{
		db:     db,
		ttl:    ttl,
		ctx:    ctx,
		cancel: cancel,
		wg:     sync.WaitGroup{},
	}

	if err != nil {
		return nil, err
	}

	go es.gcLoop()

	return es, nil
}

func (es *EventSampler) Get(key []byte) (bool, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	var found bool

	err := es.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)

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

func (es *EventSampler) Put(key []byte) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	return es.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, []byte{1}).WithTTL(es.ttl.Load())
		return txn.SetEntry(entry)
	})
}

func (es *EventSampler) gcLoop() {
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
	}
}

func (es *EventSampler) Close() {
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
