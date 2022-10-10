//go:generate mockgen -destination=../../mocks/services/dedup/mock_dedup.go -package mock_dedup github.com/rudderlabs/rudder-server/services/dedup DedupI

package dedup

import (
	"fmt"
	"sort"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type DedupI interface {
	FindDuplicates(messageIDs []string, allMessageIDsSet map[string]struct{}) (duplicateIndexes []int)
	MarkProcessed(messageIDs []string) error
	PrintHistogram()
	Close()
}

var (
	dedupWindow  time.Duration
	memOptimized bool
	pkgLogger    logger.Logger
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("dedup")
}

func loadConfig() {
	// Dedup time window in hours
	config.RegisterDurationConfigVariable(3600, &dedupWindow, true, time.Second, []string{"Dedup.dedupWindow", "Dedup.dedupWindowInS"}...)
	config.RegisterBoolConfigVariable(true, &memOptimized, false, "Dedup.memOptimized")
}

type loggerForBadger struct {
	logger.Logger
}

func (l loggerForBadger) Warningf(fmt string, args ...interface{}) {
	l.Warnf(fmt, args...)
}

func DefaultRudderPath() string {
	badgerPathName := "/badgerdbv2"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(`%v%v`, tmpDirPath, badgerPathName)
}

type OptFn func(*DedupHandleT)

func FromConfig() OptFn {
	return func(dht *DedupHandleT) {
		dht.window = &dedupWindow
	}
}

func WithWindow(d time.Duration) OptFn {
	return func(dht *DedupHandleT) {
		dht.window = &d
	}
}

func WithClearDB() OptFn {
	return func(dht *DedupHandleT) {
		dht.clearDB = true
	}
}

type DedupHandleT struct {
	stats    stats.Stats
	logger   loggerForBadger
	badgerDB *badger.DB
	window   *time.Duration
	close    chan struct{}
	gcDone   chan struct{}
	path     string
	clearDB  bool
}

func New(path string, fns ...OptFn) *DedupHandleT {
	d := &DedupHandleT{
		path:   path,
		logger: loggerForBadger{logger.NewLogger().Child("dedup")},
		stats:  stats.Default,
		gcDone: make(chan struct{}),
		close:  make(chan struct{}),
		window: &dedupWindow,
	}
	for _, fn := range fns {
		fn(d)
	}
	d.openBadger()

	return d
}

func (d *DedupHandleT) openBadger() {
	var err error

	opts := badger.
		DefaultOptions(d.path).
		WithTruncate(true).
		WithLogger(d.logger).
		// Disable compression - Set options.Compression = options.None.
		// This means we won’t allocate memory for decompression
		// (this can be a lot in case of ZSTD decompression).
		// In our case, compression is not useful since we are storing messageIDs with high entropy.
		WithCompression(options.None)

	if memOptimized {
		// Memory usage optimizations:
		// Inspired by https://github.com/dgraph-io/badger/issues/1304#issuecomment-630078745
		// With modifications to ensure no performance degradation for dedup.
		opts.TableLoadingMode = options.FileIO
		opts.ValueLogLoadingMode = options.FileIO
		opts.NumMemtables = 3
		opts.MaxTableSize = 16 << 20
		opts.NumLevelZeroTables = 1
		opts.NumLevelZeroTablesStall = 2
		opts.KeepL0InMemory = false
	}
	d.badgerDB, err = badger.Open(opts)
	if err != nil {
		panic(err)
	}
	if d.clearDB {
		err = d.badgerDB.DropAll()
		if err != nil {
			panic(err)
		}
	}
	rruntime.Go(func() {
		d.gcBadgerDB()
		close(d.gcDone)
	})
}

func (d *DedupHandleT) PrintHistogram() {
	d.badgerDB.PrintHistogram(nil)
}

func (d *DedupHandleT) gcBadgerDB() {
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
	}
}

func (d *DedupHandleT) writeToBadger(messageIDs []string) error {
	txn := d.badgerDB.NewTransaction(true)
	for _, messageID := range messageIDs {
		e := badger.NewEntry([]byte(messageID), nil).WithTTL(*d.window)
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

// MarkProcessed persist messageIDs in Disk, with expiry time of dedupWindow
// Any message mark here will appear in FindDuplicates() if queried inside the dedupWindow
func (d *DedupHandleT) MarkProcessed(messageIDs []string) error {
	return d.writeToBadger(messageIDs)
}

func (d *DedupHandleT) FindDuplicates(messageIDs []string, allMessageIDsSet map[string]struct{}) (duplicateIndexes []int) {
	toRemoveMessageIndexesSet := make(map[int]struct{})
	// Dedup within events batch in a web request
	messageIDSet := make(map[string]struct{})

	// Eg messageIDs: [m1, m2, m3, m1, m1, m1]
	// Constructing a set out of messageIDs
	for _, messageID := range messageIDs {
		messageIDSet[messageID] = struct{}{}
	}
	// Eg messageIDSet: [m1, m2, m3]
	// In this loop it will remove from set for first occurrence and if not found in set it means it's a duplicate
	for idx, messageID := range messageIDs {
		if _, ok := messageIDSet[messageID]; ok {
			delete(messageIDSet, messageID)
		} else {
			toRemoveMessageIndexesSet[idx] = struct{}{}
		}
	}
	// Dedup within batch of batch jobs
	for idx, messageID := range messageIDs {
		if _, ok := allMessageIDsSet[messageID]; ok {
			toRemoveMessageIndexesSet[idx] = struct{}{}
		}
	}

	// Dedup with badgerDB
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		for idx, messageID := range messageIDs {
			_, err := txn.Get([]byte(messageID))
			if err != badger.ErrKeyNotFound {
				toRemoveMessageIndexesSet[idx] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	toRemoveMessageIndexes := make([]int, 0, len(toRemoveMessageIndexesSet))
	for k := range toRemoveMessageIndexesSet {
		toRemoveMessageIndexes = append(toRemoveMessageIndexes, k)
	}

	sort.Ints(toRemoveMessageIndexes)
	return toRemoveMessageIndexes
}

func (d *DedupHandleT) Close() {
	close(d.close)
	<-d.gcDone
	_ = d.badgerDB.Close()
}
