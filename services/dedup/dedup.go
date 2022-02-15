//go:generate mockgen -destination=../../mocks/services/dedup/mock_dedup.go -package mock_dedup github.com/rudderlabs/rudder-server/services/dedup DedupI

package dedup

import (
	"fmt"
	"sort"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type DedupI interface {
	FindDuplicates(messageIDs []string, allMessageIDsSet map[string]struct{}) (duplicateIndexes []int)
	MarkProcessed(messageIDs []string)
	PrintHistogram()
}

type DedupHandleT struct {
	stats    stats.Stats
	logger   loggerForBudger
	badgerDB *badger.DB
	window   *time.Duration
	close    chan struct{}
	gcDone   chan struct{}
	path     string
	clearDB  bool
}

var (
	dedupWindow time.Duration
	pkgLogger   logger.LoggerI
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("dedup")
}

func loadConfig() {
	// Dedup time window in hours
	config.RegisterDurationConfigVariable(time.Duration(3600), &dedupWindow, true, time.Second, []string{"Dedup.dedupWindow", "Dedup.dedupWindowInS"}...)
}

type loggerForBudger struct {
	logger.LoggerI
}

func (l loggerForBudger) Warningf(fmt string, args ...interface{}) {
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

func New(path string, fns ...OptFn) *DedupHandleT {
	d := &DedupHandleT{
		path:   path,
		logger: loggerForBudger{logger.NewLogger().Child("dedup")},
		stats:  stats.DefaultStats,
		gcDone: make(chan struct{}),
		close:  make(chan struct{}),
	}
	for _, fn := range fns {
		fn(d)
	}
	d.openBadger()

	return d
}

func (d *DedupHandleT) openBadger() {
	var err error
	d.badgerDB, err = badger.Open(badger.DefaultOptions(d.path).WithTruncate(true).WithLogger(d.logger))
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
			return
		case <-time.After(5 * time.Minute):
		}
		d.badgerDB.RunValueLogGC(0.5)
	}
}

func (d *DedupHandleT) writeToBadger(messageIDs []string) {
	err := d.badgerDB.Update(func(txn *badger.Txn) error {
		for _, messageID := range messageIDs {
			e := badger.NewEntry([]byte(messageID), nil).WithTTL(*d.window)
			if err := txn.SetEntry(e); err == badger.ErrTxnTooBig {
				_ = txn.Commit()
				txn = d.badgerDB.NewTransaction(true)
				_ = txn.SetEntry(e)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func (d *DedupHandleT) MarkProcessed(messageIDs []string) {
	d.writeToBadger(messageIDs)
}

func (d *DedupHandleT) FindDuplicates(messageIDs []string, allMessageIDsSet map[string]struct{}) (duplicateIndexes []int) {
	toRemoveMessageIndexesSet := make(map[int]struct{})
	//Dedup within events batch in a web request
	messageIDSet := make(map[string]struct{})

	// Eg messageIDs: [m1, m2, m3, m1, m1, m1]
	//Constructing a set out of messageIDs
	for _, messageID := range messageIDs {
		messageIDSet[messageID] = struct{}{}
	}
	// Eg messagIDSet: [m1, m2, m3]
	//In this loop it will remove from set for first occurance and if not found in set it means its a duplicate
	for idx, messageID := range messageIDs {
		if _, ok := messageIDSet[messageID]; ok {
			delete(messageIDSet, messageID)
		} else {
			toRemoveMessageIndexesSet[idx] = struct{}{}
		}
	}
	//Dedup within batch of batch jobs
	for idx, messageID := range messageIDs {
		if _, ok := allMessageIDsSet[messageID]; ok {
			toRemoveMessageIndexesSet[idx] = struct{}{}
		}
	}

	//Dedup with badgerDB
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
	d.badgerDB.Close()
}
