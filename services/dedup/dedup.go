package dedup

import (
	"fmt"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"sort"
	"time"
)

type DedupI interface {
	FindDuplicates(messageIDs []string, allMessageIDsSet map[string]struct{}) (duplicateIndexes []int)
	MarkProcessed(messageIDs []string)
}

type DedupHandleT struct {
	badgerDB *badger.DB
	stats    stats.Stats
}

var (
	dedupWindow time.Duration
	pkgLogger   logger.LoggerI
)

func loadConfig() {
	// Dedup time window in hours
	dedupWindow = config.GetDuration("Dedup.dedupWindowInS", time.Duration(86400))
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("dedup")
}
func (d *DedupHandleT) setup(clearDB *bool) {
	d.stats = stats.DefaultStats
	d.openBadger(clearDB)
}

func (d *DedupHandleT) openBadger(clearDB *bool) {
	var err error
	badgerPathName := "/badgerdbv2"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	path := fmt.Sprintf(`%v%v`, tmpDirPath, badgerPathName)
	d.badgerDB, err = badger.Open(badger.DefaultOptions(path))
	if err != nil {
		panic(err)
	}
	if *clearDB {
		err = d.badgerDB.DropAll()
		if err != nil {
			panic(err)
		}
	}
	rruntime.Go(func() {
		d.gcBadgerDB()
	})
}

func (d *DedupHandleT) gcBadgerDB() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
	again:
		err := d.badgerDB.RunValueLogGC(0.5)
		if err == nil {
			goto again
		}
	}
}

func (d *DedupHandleT) writeToBadger(messageIDs []string) {
	err := d.badgerDB.Update(func(txn *badger.Txn) error {
		for _, messageID := range messageIDs {
			e := badger.NewEntry([]byte(messageID), nil).WithTTL(dedupWindow * time.Second)
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
