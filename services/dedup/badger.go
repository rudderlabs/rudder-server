package dedup

import (
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type badgerDB struct {
	stats    stats.Stats
	logger   loggerForBadger
	badgerDB *badger.DB
	window   config.ValueLoader[time.Duration]
	close    chan struct{}
	gcDone   chan struct{}
	path     string
	opts     badger.Options
}

func (d *badgerDB) Get(key string) (int64, bool) {
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
		panic(err)
	}
	return payloadSize, found
}

func (d *badgerDB) Set(kvs []KeyValue) error {
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

func (d *badgerDB) Close() {
	close(d.close)
	<-d.gcDone
	_ = d.badgerDB.Close()
}

func (d *badgerDB) start() {
	var err error

	d.badgerDB, err = badger.Open(d.opts)
	if err != nil {
		panic(err)
	}
	rruntime.Go(func() {
		d.gcLoop()
		close(d.gcDone)
	})
}

func (d *badgerDB) gcLoop() {
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
