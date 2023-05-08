//go:generate mockgen -destination=../../mocks/services/dedup/mock_dedup.go -package mock_dedup github.com/rudderlabs/rudder-server/services/dedup Dedup

package dedup

import (
	"fmt"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type Message struct {
	ID   string
	Size int64
}

type Dedup interface {
	Get(messageID string) (int64, bool)
	MarkProcessed(messages []Message) error
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

type OptFn func(*dedup)

func FromConfig() OptFn {
	return func(dht *dedup) {
		dht.window = &dedupWindow
	}
}

func WithWindow(d time.Duration) OptFn {
	return func(dht *dedup) {
		dht.window = &d
	}
}

func WithClearDB() OptFn {
	return func(dht *dedup) {
		dht.clearDB = true
	}
}

type dedup struct {
	stats    stats.Stats
	logger   loggerForBadger
	badgerDB *badger.DB
	window   *time.Duration
	close    chan struct{}
	gcDone   chan struct{}
	path     string
	clearDB  bool
}

func New(path string, fns ...OptFn) *dedup {
	d := &dedup{
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

func (d *dedup) openBadger() {
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

func (d *dedup) PrintHistogram() {
	d.badgerDB.PrintHistogram(nil)
}

func (d *dedup) gcBadgerDB() {
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
		d.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "lsm"}).Gauge((lsmSize))
		d.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "vlog"}).Gauge((vlogSize))
		d.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "total"}).Gauge((totSize))

	}
}

func (d *dedup) writeToBadger(messages []Message) error {
	txn := d.badgerDB.NewTransaction(true)
	for _, message := range messages {
		value := strconv.FormatInt(message.Size, 10)
		e := badger.NewEntry([]byte(message.ID), []byte(value)).WithTTL(*d.window)
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
func (d *dedup) MarkProcessed(messages []Message) error {
	return d.writeToBadger(messages)
}

func (d *dedup) Get(messageID string) (int64, bool) {
	var value int64
	var found bool
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(messageID))
		if err != nil {
			return err
		}
		if itemValue, err := item.ValueCopy(nil); err == nil {
			value, err = strconv.ParseInt(string(itemValue), 10, 64)
			found = true
		}
		return nil
	})
	if err != nil && err != badger.ErrKeyNotFound {
		panic(err)
	}
	return value, found
}

func (d *dedup) Close() {
	close(d.close)
	<-d.gcDone
	_ = d.badgerDB.Close()
}
