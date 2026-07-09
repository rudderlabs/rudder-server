package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
)

// The struct fields need to be exposed to JSON package
type dataSetT struct {
	JobTable       string `json:"job"`
	JobStatusTable string `json:"status"`
	Index          string `json:"index"`
	ConsumersTable string `json:"consumers,omitempty"` // non-empty only when the registry table exists
}

func (ds dataSetT) String() string {
	return "JobTable=" + ds.JobTable + ",JobStatusTable=" + ds.JobStatusTable + ",Index=" + ds.Index
}

func (ds dataSetT) consumersRegistryTable() string {
	prefix := strings.TrimSuffix(ds.JobTable, "_jobs_"+ds.Index)
	return fmt.Sprintf("%s_consumers_%s", prefix, ds.Index)
}

type dataSetTList []dataSetT

func (l dataSetTList) String() string {
	sb := strings.Builder{}
	for i, ds := range l {
		if i > 0 {
			sb.WriteString(";")
		}
		sb.WriteString(ds.String())
	}
	return sb.String()
}

// dropDSEntry tracks a dataset to drop along with the dslist version that needs to be drained from readers before actually being able to drop the dataset
type dropDSEntry struct {
	ds        dataSetT // dataset to drop
	compacted bool     // true if the dataset is compacted, false if completed
	version   uint64   // the dslist version that needs to be drained from readers before dropping the dataset
}

type dataSetRangeT struct {
	minJobID int64
	maxJobID int64
	ds       dataSetT
}

func (ds dataSetRangeT) String() string {
	return "minJobID=" + strconv.FormatInt(ds.minJobID, 10) + ",maxJobID=" + strconv.FormatInt(ds.maxJobID, 10) + ",ds=" + ds.ds.String()
}

type dataSetRangeTList []dataSetRangeT

func (l dataSetRangeTList) String() string {
	sb := strings.Builder{}
	for i, ds := range l {
		if i > 0 {
			sb.WriteString(";")
		}
		sb.WriteString(ds.String())
	}
	return sb.String()
}

type dsRangeMinMax struct {
	minJobID sql.NullInt64
	maxJobID sql.NullInt64
}

/*
Utility function to return an ordered list of datasets (for tests)
*/
func (jd *Handle) getDSListSnapshot() dataSetTList {
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()
	list, _ := jd.dsList.snapshot()
	return list
}

// getLastDS returns the last dataset in the list. Caller must have the dsListLock readlocked
func (jd *Handle) getLastDS() dataSetT {
	list, _ := jd.dsList.snapshot()
	if len(list) == 0 {
		return dataSetT{}
	}
	return list[len(list)-1]
}

// doRefreshDSList refreshes the ds list from the database
func (jd *Handle) doRefreshDSList(l lock.LockToken, db sqlDbOrTx) (dataSetTList, error) {
	if l == nil {
		return nil, fmt.Errorf("cannot refresh DS list without a valid lock token")
	}
	datasetList, err := getDSList(jd, db, jd.tablePrefix)
	if err != nil {
		return nil, fmt.Errorf("getDSList %w", err)
	}
	// report table count metrics before shrinking the datasetList
	jd.statTableCount.Gauge(len(datasetList))

	// If the owner of this jobsdb is a writer, then shrinking datasetList to have only last dataset
	// which is being written to.
	// Writers only write to the last dataset and if this dataset is full, then create a new dataset.
	if jd.ownerType == Write {
		if len(datasetList) > 1 {
			datasetList = datasetList[len(datasetList)-1:]
		}
	}

	return datasetList, nil
}

// addCompletedDSToDropList adds the given datasets to the dropDSList and removes them from the dsList. Caller must have the dsListLock write-locked.
func (jd *Handle) addCompletedDSToDropList(ctx context.Context, l lock.LockToken, dsList ...dataSetT) error {
	if l == nil {
		return fmt.Errorf("cannot add to drop DS list without a valid lock token")
	}
	jd.dropDSListLock.Lock()
	defer jd.dropDSListLock.Unlock()
	currentList, currentRangeList := jd.dsList.snapshot()
	if len(dsList) == 0 {
		return nil
	}
	version := jd.dsList.currentVersion()
	previousDropDSList := append([]dropDSEntry(nil), jd.dropDSList...)
	existing := make(map[string]struct{}, len(jd.dropDSList)+len(dsList))
	for _, entry := range jd.dropDSList {
		existing[entry.ds.Index] = struct{}{}
	}
	var addedDSList []dataSetT
	for _, ds := range dsList {
		if _, ok := existing[ds.Index]; ok {
			continue
		}
		jd.dropDSList = append(jd.dropDSList, dropDSEntry{ds: ds, compacted: false, version: version})
		existing[ds.Index] = struct{}{}
		addedDSList = append(addedDSList, ds)
	}
	if len(addedDSList) == 0 {
		return nil
	}
	toDrop := lo.SliceToMap(jd.dropDSList, func(entry dropDSEntry) (string, struct{}) {
		return entry.ds.Index, struct{}{}
	})
	newList := lo.Filter(currentList, func(ds dataSetT, _ int) bool {
		_, ok := toDrop[ds.Index]
		return !ok
	})
	if err := jd.markPreDropDS(ctx, addedDSList...); err != nil {
		jd.dropDSList = previousDropDSList
		return err
	}
	jd.dsList.set(
		newList,
		lo.Filter(currentRangeList, func(dsRange dataSetRangeT, _ int) bool {
			_, ok := toDrop[dsRange.ds.Index]
			return !ok
		}))
	// update table count gauge after setting the new ds list
	jd.statTableCount.Gauge(len(newList))
	jd.dropNotifyPing()
	return nil
}

func (jd *Handle) dropNotifyPing() {
	select {
	case jd.dropNotify <- struct{}{}:
	default:
	}
}

func (jd *Handle) doRefreshDSRangeList(l lock.LockToken) error {
	return jd.doRefreshDSRangeListWithDB(l, jd.maintenanceDB())
}

// doRefreshDSRangeList first refreshes the DS list and then calculate the DS range list
func (jd *Handle) doRefreshDSRangeListWithDB(l lock.LockToken, db sqlDbOrTx) error {
	var prevMax int64

	// At this point we must have write-locked dsListLock
	dsList, err := jd.doRefreshDSList(l, db)
	if err != nil {
		return fmt.Errorf("refreshDSList %w", err)
	}
	var datasetRangeList dataSetRangeTList

	for idx := 0; idx < len(dsList)-1; idx++ {
		ds := dsList[idx]
		jd.assert(ds.Index != "", "ds.Index is empty")

		if _, ok := jd.dsRangeFuncMap[ds.Index]; !ok {
			getIndex := func() (sql.NullInt64, sql.NullInt64, error) {
				var minID, maxID sql.NullInt64
				sqlStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %q`, ds.JobTable)
				row := db.QueryRow(sqlStatement)
				if err := row.Scan(&minID, &maxID); err != nil {
					return sql.NullInt64{}, sql.NullInt64{}, fmt.Errorf("scanning min & max jobID %w", err)
				}
				jd.logger.Debugn(sqlStatement,
					logger.NewIntField("minID", minID.Int64), logger.NewIntField("maxID", maxID.Int64))
				return minID, maxID, nil
			}
			jd.dsRangeFuncMap[ds.Index] = sync.OnceValues(func() (dsRangeMinMax, error) {
				minID, maxID, err := getIndex()
				if err != nil {
					return dsRangeMinMax{}, fmt.Errorf("getIndex %w", err)
				}
				return dsRangeMinMax{
					minJobID: minID,
					maxJobID: maxID,
				}, nil
			})
		}
		minMax, err := jd.dsRangeFuncMap[ds.Index]()
		if err != nil {
			return err
		}
		minID, maxID := minMax.minJobID, minMax.maxJobID

		// We store ranges EXCEPT for
		// 1. the last element (which is being actively written to)
		// 2. Compaction target ds

		// Skipping asserts and updating prevMax if a ds is found to be empty
		// Happens if this function is called between addNewDS and populating data in two scenarios
		// Scenario-1: During internal compaction
		// Scenario-2: During scaleup scaledown
		if !minID.Valid || !maxID.Valid {
			continue
		}

		jd.assert(minID.Valid && maxID.Valid, fmt.Sprintf("minID.Valid: %v, maxID.Valid: %v. Either of them is false for table: %s", minID.Valid, maxID.Valid, ds.JobTable))
		jd.assert(idx == 0 || prevMax < minID.Int64, fmt.Sprintf("idx: %d != 0 and prevMax: %d >= minID.Int64: %v of table: %s", idx, prevMax, minID.Int64, ds.JobTable))
		datasetRangeList = append(datasetRangeList,
			dataSetRangeT{
				minJobID: minID.Int64,
				maxJobID: maxID.Int64,
				ds:       ds,
			})
		prevMax = maxID.Int64
	}
	jd.dsList.set(dsList, datasetRangeList)
	return nil
}

// acquireDSListForRead returns the dsList and dsRangeList. Caller should call the release function after done reading from the lists.
func (jd *Handle) acquireDSListForRead(ctx context.Context) (
	list dataSetTList, ranges dataSetRangeTList, release func(), err error,
) {
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		return nil, nil, nil, fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	list, ranges, _, release = jd.dsList.get()
	jd.dsListLock.RUnlock()
	return list, ranges, release, nil
}

func mapDSToLevel(ds dataSetT) (levelInt int, levelVals []int, err error) {
	indexStr := strings.Split(ds.Index, "_")
	// Currently we don't have a scenario where we need more than 3 levels.
	if len(indexStr) > 3 {
		err = fmt.Errorf("len(indexStr): %d > 3", len(indexStr))
		return levelInt, levelVals, err
	}
	for _, str := range indexStr {
		levelInt, err = strconv.Atoi(str)
		if err != nil {
			return levelInt, levelVals, err
		}
		levelVals = append(levelVals, levelInt)
	}
	return len(levelVals), levelVals, nil
}

func newDataSet(tablePrefix, dsIdx string) dataSetT {
	jobTable := fmt.Sprintf("%s_jobs_%s", tablePrefix, dsIdx)
	jobStatusTable := fmt.Sprintf("%s_job_status_%s", tablePrefix, dsIdx)
	return dataSetT{
		JobTable:       jobTable,
		JobStatusTable: jobStatusTable,
		Index:          dsIdx,
	}
}

func (jd *Handle) computeNewIdxForAppend(l lock.LockToken) string {
	dList, err := jd.doRefreshDSList(l, jd.maintenanceDB())
	jd.assertError(err)
	return jd.doComputeNewIdxForAppend(dList)
}

func (jd *Handle) doComputeNewIdxForAppend(dList []dataSetT) string {
	var newDSIdx string
	if len(dList) == 0 {
		newDSIdx = "1"
	} else {
		levels, levelVals, err := mapDSToLevel(dList[len(dList)-1])
		jd.assertError(err)
		// Last one can only be Level0
		jd.assert(levels == 1, fmt.Sprintf("levels:%d != 1", levels))
		newDSIdx = fmt.Sprintf("%d", levelVals[0]+1)
	}
	return newDSIdx
}

// GetMaxDSIndex returns max dataset index in the DB
func (jd *Handle) GetMaxDSIndex() (maxDSIndex int64) {
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()
	maxDSIndex, err := strconv.ParseInt(jd.getLastDS().Index, 10, 64)
	if err != nil {
		panic(err)
	}

	return maxDSIndex
}

func (jd *Handle) RefreshDSList(ctx context.Context) error {
	return jd.refreshDSListWithDB(ctx, jd.maintenanceDB())
}

// refreshDSListWithDB refreshes the list of datasets in memory if the database view of the list has changed.
func (jd *Handle) refreshDSListWithDB(ctx context.Context, db sqlDbOrTx) error {
	jd.logger.Debugn("Start", logger.NewStringField("operation", "refreshDSListLoop"))

	start := time.Now()
	var err error
	defer func() {
		jd.stats.NewTaggedStat("refresh_ds_loop", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix, "error": strconv.FormatBool(err != nil)}).Since(start)
	}()
	jd.dsListLock.RLock()
	previousLastDS := jd.getLastDS()
	jd.dsListLock.RUnlock()
	nextDS, err := getDSList(jd, db, jd.tablePrefix)
	if err != nil {
		return fmt.Errorf("getDSList: %w", err)
	}
	nextLastDS, _ := lo.Last(nextDS)

	if previousLastDS.Index == nextLastDS.Index {
		return nil
	}
	defer jd.stats.NewTaggedStat("refresh_ds_loop_lock", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix}).RecordDuration()()
	err = jd.dsListLock.WithLockInCtx(ctx, func(l lock.LockToken) error {
		return jd.doRefreshDSRangeListWithDB(l, db)
	})
	if err != nil {
		return fmt.Errorf("refreshDSRangeList: %w", err)
	}

	return nil
}
