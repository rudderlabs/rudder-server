package dedup

import (
	"fmt"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"sort"
	"time"
)

type DedupI interface {
	DedupJobs([]*jobsdb.JobT) []*jobsdb.JobT
}

type DedupHandleT struct {
	badgerDB  *badger.DB
	stats     stats.Stats
	gatewayDB jobsdb.JobsDB
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

func (d *DedupHandleT) setup(gatewayDB jobsdb.JobsDB, clearDB *bool) {
	d.stats = stats.DefaultStats
	d.gatewayDB = gatewayDB
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

func (d *DedupHandleT) dedupMessages(body *[]byte, messageIDs []string, writeKey string, sourceDupStats map[string]int) {
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
	count := 0
	for _, idx := range toRemoveMessageIndexes {
		pkgLogger.Debugf("Dropping event with duplicate messageId: %s", messageIDs[idx])
		misc.IncrementMapByKey(sourceDupStats, writeKey, 1)
		*body, err = sjson.DeleteBytes(*body, fmt.Sprintf(`batch.%v`, idx-count))
		if err != nil {
			panic(err)
		}
		count++
	}
}

func (d *DedupHandleT) updateSourceStats(sourceStats map[string]int, bucket string) {
	for sourceTag, count := range sourceStats {
		tags := map[string]string{
			"source": sourceTag,
		}
		sourceStatsD := d.stats.NewTaggedStat(bucket, stats.CountType, tags)
		sourceStatsD.Count(count)
	}
}

func addToSet(set map[string]struct{}, elements []string) {
	for _, element := range elements {
		set[element] = struct{}{}
	}
}

//Takes a job list, dedupes them in place and finally returns the modified list
//duplicated jobs are moved to succeded state, so that they are not picked in next turn
func (d *DedupHandleT) DedupJobs(jobList []*jobsdb.JobT) []*jobsdb.JobT {
	if len(jobList) > 0 {
		var dedupedJobList []*jobsdb.JobT
		var duplicateList []*jobsdb.JobT
		var sourceDupStats = make(map[string]int)

		allMessageIdsSet := make(map[string]struct{})

		for _, unprocessedJob := range jobList {
			body, err := unprocessedJob.EventPayload.MarshalJSON()
			if err != nil {
				pkgLogger.Infof("Marshalling job payload failed with : %s", err.Error())
				continue
			}
			result := gjson.GetBytes(body, "batch")
			var index int
			var reqMessageIDs []string
			result.ForEach(func(_, _ gjson.Result) bool {
				reqMessageIDs = append(reqMessageIDs, gjson.GetBytes(body, fmt.Sprintf(`batch.%v.messageId`, index)).String())
				index++
				return true // keep iterating
			})

			writeKey := gjson.GetBytes(body, "writeKey").Str
			d.dedupMessages(&body, reqMessageIDs, writeKey, sourceDupStats)
			if len(gjson.GetBytes(body, "batch").Array()) == 0 {
				duplicateList = append(duplicateList, unprocessedJob)
				continue
			}
			unprocessedJob.EventPayload.UnmarshalJSON(body)
			addToSet(allMessageIdsSet, reqMessageIDs)
			dedupedJobList = append(dedupedJobList, unprocessedJob)
		}
		if len(dedupedJobList) > 0 {
			messageIdsArr := make([]string, 0)
			for msgId := range allMessageIdsSet {
				messageIdsArr = append(messageIdsArr, msgId)
			}
			rruntime.Go(func() {
				d.writeToBadger(messageIdsArr)
			})
		}
		//Mark all as executing so next query doesn't pick it up
		var statusList []*jobsdb.JobStatusT
		for _, batchEvent := range duplicateList {
			newStatus := jobsdb.JobStatusT{
				JobID:         batchEvent.JobID,
				JobState:      jobsdb.Succeeded.State,
				AttemptNum:    1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: []byte(`{"success":"OK"}`),
			}
			statusList = append(statusList, &newStatus)
		}
		d.gatewayDB.UpdateJobStatus(statusList, []string{gateway.CustomVal}, nil)
		d.updateSourceStats(sourceDupStats, "processor.write_key_duplicate_events")
		return dedupedJobList
	}
	return nil
}
