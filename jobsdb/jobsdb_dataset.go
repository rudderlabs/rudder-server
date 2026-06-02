package jobsdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

// The struct fields need to be exposed to JSON package
type dataSetT struct {
	JobTable       string `json:"job"`
	JobStatusTable string `json:"status"`
	Index          string `json:"index"`
}

func (ds dataSetT) String() string {
	return "JobTable=" + ds.JobTable + ",JobStatusTable=" + ds.JobStatusTable + ",Index=" + ds.Index
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
// It returns the freshly published dsList snapshot (with the queued datasets filtered out) so callers can avoid an extra read.
func (jd *Handle) addCompletedDSToDropList(ctx context.Context, l lock.LockToken, dsList ...dataSetT) (dataSetTList, error) {
	if l == nil {
		return nil, fmt.Errorf("cannot add to drop DS list without a valid lock token")
	}
	jd.dropDSListLock.Lock()
	defer jd.dropDSListLock.Unlock()
	currentList, currentRangeList := jd.dsList.snapshot()
	if len(dsList) == 0 {
		return currentList, nil
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
		return currentList, nil
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
		return currentList, err
	}
	jd.dsList.set(
		newList,
		lo.Filter(currentRangeList, func(dsRange dataSetRangeT, _ int) bool {
			_, ok := toDrop[dsRange.ds.Index]
			return !ok
		}))

	jd.dropNotifyPing()
	return newList, nil
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
		// 2. Migration target ds

		// Skipping asserts and updating prevMax if a ds is found to be empty
		// Happens if this function is called between addNewDS and populating data in two scenarios
		// Scenario-1: During internal migrations
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

func (jd *Handle) checkIfFullDSInTx(tx *Tx, ds dataSetT) (bool, error) {
	var (
		minJobCreatedAt sql.NullTime
		tableSize       int64
		rowCount        int
	)

	sqlStatement := fmt.Sprintf(
		`with combinedResult as (
			SELECT
			(SELECT created_at FROM %[1]q ORDER BY job_id ASC LIMIT 1) AS minJobCreatedAt,
			(SELECT PG_TOTAL_RELATION_SIZE('%[1]s')) AS tableSize,
			(SELECT COUNT(*) FROM %[1]q) AS rowCount
		)
		SELECT minJobCreatedAt, tableSize, rowCount FROM combinedResult`,
		ds.JobTable,
	)
	row := tx.QueryRow(sqlStatement)
	err := row.Scan(&minJobCreatedAt, &tableSize, &rowCount)
	if err != nil {
		return false, err
	}
	if !minJobCreatedAt.Valid {
		return false, nil
	}

	if jd.conf.maxDSRetentionPeriod.Load() > 0 {
		if time.Since(minJobCreatedAt.Time) >= jd.conf.maxDSRetentionPeriod.Load() {
			return true, nil
		}
	}

	if tableSize >= jd.conf.maxTableSize.Load() {
		jd.logger.Infon(
			"[JobsDB] DS full in size",
			logger.NewStringField("ds", ds.String()),
			logger.NewIntField("rowCount", int64(rowCount)),
			logger.NewIntField("tableSize", tableSize),
		)
		return true, nil
	}

	if rowCount >= jd.conf.MaxDSSize.Load() {
		jd.logger.Infon(
			"[JobsDB] DS full by rows",
			logger.NewStringField("ds", ds.String()),
			logger.NewIntField("rowCount", int64(rowCount)),
			logger.NewIntField("tableSize", tableSize),
		)
		return true, nil
	}

	return false, nil
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

func (jd *Handle) addNewDS(ctx context.Context, l lock.LockToken, ds dataSetT) {
	err := jd.withMaintenanceTx(ctx, func(tx *Tx) error {
		dsList, err := jd.doRefreshDSList(l, tx.Tx)
		jd.assertError(err)
		return jd.addNewDSInTx(ctx, tx, l, dsList, ds)
	})
	jd.assertError(err)
	jd.assertError(jd.doRefreshDSRangeList(l))
}

// NOTE: If addNewDSInTx is directly called, make sure to explicitly call refreshDSRangeList(l) to update the DS list in cache, once transaction has completed.
func (jd *Handle) addNewDSInTx(ctx context.Context, tx *Tx, l lock.LockToken, dsList []dataSetT, ds dataSetT) error {
	defer jd.getTimerStat(
		"add_new_ds",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()
	if l == nil {
		return errors.New("nil ds list lock token provided")
	}
	jd.logger.Infon("Creating new DS", logger.NewStringField("ds", ds.String()))
	err := jd.createDSInTx(ctx, tx, ds)
	if err != nil {
		return err
	}
	err = jd.setSequenceNumberInTx(tx, l, dsList, ds.Index)
	if err != nil {
		return err
	}
	// Tracking time interval between new ds creations. Hence calling end before start
	if !jd.newDSCreationTime.IsZero() {
		jd.statNewDSPeriod.Since(jd.newDSCreationTime)
	}
	jd.newDSCreationTime = time.Now()

	return nil
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

func (jd *Handle) createDSInTx(ctx context.Context, tx *Tx, newDS dataSetT) error {
	// Mark the start of operation. If we crash somewhere here, we delete the
	// DS being added
	opPayload, err := jsonrs.Marshal(&journalOpPayloadT{To: newDS})
	if err != nil {
		return err
	}

	opID, err := jd.JournalMarkStartInTx(tx, addDSOperation, opPayload)
	if err != nil {
		return err
	}

	// Create the jobs and job_status tables
	if err = jd.createDSTablesInTx(ctx, tx, newDS); err != nil {
		return fmt.Errorf("creating DS tables %w", err)
	}
	if err = jd.createDSIndicesInTx(ctx, tx, newDS); err != nil {
		return fmt.Errorf("creating DS indices %w", err)
	}

	err = jd.journalMarkDoneInTx(tx, opID)
	if err != nil {
		return err
	}
	return nil
}

func (jd *Handle) createDSTablesInTx(ctx context.Context, tx *Tx, newDS dataSetT) error {
	var columnType payloadColumnType
	switch jd.conf.payloadColumnType {
	case JSONB:
		columnType = JSONB
	case BYTEA:
		columnType = BYTEA
	case TEXT:
		columnType = TEXT
	default:
		columnType = JSONB
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q (
		job_id BIGSERIAL PRIMARY KEY,
		workspace_id TEXT NOT NULL DEFAULT '',
		uuid UUID NOT NULL,
		user_id TEXT NOT NULL,
		partition_id TEXT NOT NULL DEFAULT '',
		parameters JSONB NOT NULL,
		custom_val VARCHAR(64) NOT NULL,
		event_payload `+string(columnType)+` NOT NULL,
		event_count INTEGER NOT NULL DEFAULT 1,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		expire_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());`, newDS.JobTable)); err != nil {
		return fmt.Errorf("creating %s: %w", newDS.JobTable, err)
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q (
		id BIGSERIAL,
		job_id BIGINT,
		job_state VARCHAR(64),
		attempt SMALLINT,
		exec_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		retry_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		error_code VARCHAR(32),
		error_response JSONB DEFAULT '{}'::JSONB,
		parameters JSONB DEFAULT '{}'::JSONB);`, newDS.JobStatusTable)); err != nil {
		return fmt.Errorf("creating %s: %w", newDS.JobStatusTable, err)
	}
	return nil
}

// createDSIndicesInTx creates the indices for the given dataset.
func (jd *Handle) createDSIndicesInTx(ctx context.Context, tx *Tx, newDS dataSetT) error {
	if err := jd.createDSJobIndicesInTx(ctx, tx, newDS); err != nil {
		return err
	}
	return jd.createDSStatusIndicesInTx(ctx, tx, newDS)
}

// createDSJobIndicesInTx creates the indices that live on the jobs table
func (jd *Handle) createDSJobIndicesInTx(ctx context.Context, tx *Tx, newDS dataSetT) error {
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_ws" ON %[1]q (workspace_id)`, newDS.JobTable)); err != nil {
		return fmt.Errorf("creating workspace_id index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_partid" ON %[1]q (partition_id)`, newDS.JobTable)); err != nil {
		return fmt.Errorf("creating partition_id index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_cv" ON %[1]q (custom_val)`, newDS.JobTable)); err != nil {
		return fmt.Errorf("creating custom_val index: %w", err)
	}
	for _, param := range cacheParameterFilters {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_%[2]s" ON %[1]q USING BTREE ((parameters->>'%[2]s'))`, newDS.JobTable, param)); err != nil {
			return fmt.Errorf("creating %s index: %w", param, err)
		}
	}
	return nil
}

// createDSStatusIndicesInTx creates the indices on the job status table plus the
// v_last view.
func (jd *Handle) createDSStatusIndicesInTx(ctx context.Context, tx *Tx, newDS dataSetT) error {
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_jid_id_js" ON %[1]q(job_id asc,id desc,job_state)`, newDS.JobStatusTable)); err != nil {
		return fmt.Errorf("adding job_id_id index: %w", err)
	}
	// index used for maxDSRetention during migration
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_id_js" ON %[1]q(id ,job_state) INCLUDE (exec_time)`, newDS.JobStatusTable)); err != nil {
		return fmt.Errorf("adding job_id_js index: %w", err)
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW "v_last_%[1]s" AS SELECT DISTINCT ON (job_id) * FROM %[1]q ORDER BY job_id ASC, id DESC`, newDS.JobStatusTable)); err != nil {
		return fmt.Errorf("create view: %w", err)
	}
	return nil
}

func (jd *Handle) setSequenceNumberInTx(tx *Tx, l lock.LockToken, dsList []dataSetT, newDSIdx string) error {
	if l == nil {
		return errors.New("nil ds list lock token provided")
	}

	var maxID sql.NullInt64

	// Now set the min JobID for the new DS just added to be 1 more than previous max
	if len(dsList) > 0 {
		sqlStatement := fmt.Sprintf(`SELECT MAX(job_id) FROM %q`, dsList[len(dsList)-1].JobTable)
		err := tx.QueryRowContext(context.TODO(), sqlStatement).Scan(&maxID)
		if err != nil {
			return err
		}

		newDSMin := maxID.Int64 + 1
		sqlStatement = fmt.Sprintf(`ALTER SEQUENCE "%[1]s_jobs_%[2]s_job_id_seq" MINVALUE %[3]d START %[3]d RESTART %[3]d`,
			jd.tablePrefix, newDSIdx, newDSMin)
		_, err = tx.ExecContext(context.TODO(), sqlStatement)
		if err != nil {
			return err
		}
	}
	return nil
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

func (jd *Handle) prepareAndExecStmtInTxAllowMissing(tx *sql.Tx, sqlStatement string) {
	const (
		savepointSql = "SAVEPOINT prepareAndExecStmtInTxAllowMissing"
		rollbackSql  = "ROLLBACK TO " + savepointSql
	)

	stmt, err := tx.Prepare(sqlStatement)
	jd.assertError(err)
	defer func() { _ = stmt.Close() }()

	_, err = tx.Exec(savepointSql)
	jd.assertError(err)

	_, err = stmt.Exec()
	if err != nil {
		var pqError *pq.Error
		ok := errors.As(err, &pqError)
		if ok && pqError.Code == ("42P01") {
			jd.logger.Infon("sql statement exec failed because table doesn't exist",
				logger.NewStringField("tablePrefix", jd.tablePrefix),
				logger.NewStringField("sqlStatement", sqlStatement),
			)
			_, err = tx.Exec(rollbackSql)
			jd.assertError(err)
		} else {
			jd.assertError(err)
		}
	}
}

func (jd *Handle) dropDS(ds dataSetT) error {
	return jd.dropDSWithCtx(context.Background(), ds)
}

func (jd *Handle) dropDSWithCtx(ctx context.Context, ds dataSetT) error {
	return jd.withMaintenanceTx(ctx, func(tx *Tx) error {
		return jd.dropDSInTx(tx, ds)
	})
}

func (jd *Handle) markPreDropDS(ctx context.Context, dsList ...dataSetT) error {
	return jd.withMaintenanceTx(ctx, func(tx *Tx) error {
		for _, ds := range dsList {
			if err := jd.markPreDropDSInTx(ctx, tx, ds); err != nil {
				return err
			}
		}
		return nil
	})
}

func (jd *Handle) markPreDropDSInTx(ctx context.Context, tx *Tx, ds dataSetT) error {
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`COMMENT ON TABLE %q IS '%s'`, ds.JobStatusTable, preDropTableComment)); err != nil {
		return fmt.Errorf("predrop comment %s: %w", ds.JobStatusTable, err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`COMMENT ON TABLE %q IS '%s'`, ds.JobTable, preDropTableComment)); err != nil {
		return fmt.Errorf("predrop comment %s: %w", ds.JobTable, err)
	}
	return nil
}

func (jd *Handle) cleanupPreDropTables(ctx context.Context) error {
	rows, err := jd.maintenanceDB().QueryContext(ctx, `SELECT c.relname
									FROM pg_catalog.pg_class c
									JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
									JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = 0
									WHERE n.nspname != 'pg_catalog'
										AND n.nspname != 'information_schema'
										AND c.relkind = 'r'
										AND d.description LIKE 'rudder:pre_drop:%'`)
	if err != nil {
		return fmt.Errorf("query pre-drop tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	datasets := make(map[string]dataSetT)
	for rows.Next() {
		var tableName string
		if err = rows.Scan(&tableName); err != nil {
			return fmt.Errorf("scan pre-drop table: %w", err)
		}
		jobTable, statusTable, ok := preDropDatasetTables(tableName)
		if !ok ||
			!strings.HasPrefix(jobTable, jd.tablePrefix+"_jobs_") ||
			!strings.HasPrefix(statusTable, jd.tablePrefix+"_job_status_") {
			continue
		}
		index := strings.TrimPrefix(jobTable, jd.tablePrefix+"_jobs_")
		datasets[index] = dataSetT{
			JobTable:       jobTable,
			JobStatusTable: statusTable,
			Index:          index,
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("iterate pre-drop tables: %w", err)
	}
	_ = rows.Close()

	indices := lo.Keys(datasets)
	sortDnumList(indices)
	for _, index := range indices {
		ds := datasets[index]
		jd.logger.Infon("dropping pre-drop dataset",
			logger.NewStringField("jobTable", ds.JobTable),
			logger.NewStringField("jobStatusTable", ds.JobStatusTable),
		)
		if err = jd.withMaintenanceTx(ctx, func(tx *Tx) error {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q CASCADE`, ds.JobStatusTable)); err != nil {
				return fmt.Errorf("drop %s: %w", ds.JobStatusTable, err)
			}
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q CASCADE`, ds.JobTable)); err != nil {
				return fmt.Errorf("drop %s: %w", ds.JobTable, err)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// dropDS drops a dataset
func (jd *Handle) dropDSInTx(tx *Tx, ds dataSetT) error {
	var err error
	if _, err = tx.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %q CASCADE`, ds.JobStatusTable)); err != nil {
		return err
	}
	if _, err = tx.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %q CASCADE`, ds.JobTable)); err != nil {
		return err
	}
	jd.postDropDs(ds)
	return nil
}

// Drop a dataset and ignore if a table is missing
func (jd *Handle) dropDSForRecovery(ds dataSetT) {
	var sqlStatement string
	var err error
	tx, err := jd.maintenanceDB().Begin()
	jd.assertError(err)
	sqlStatement = fmt.Sprintf(`LOCK TABLE %q IN ACCESS EXCLUSIVE MODE;`, ds.JobStatusTable)
	jd.prepareAndExecStmtInTxAllowMissing(tx, sqlStatement)

	sqlStatement = fmt.Sprintf(`LOCK TABLE %q IN ACCESS EXCLUSIVE MODE;`, ds.JobTable)
	jd.prepareAndExecStmtInTxAllowMissing(tx, sqlStatement)

	sqlStatement = fmt.Sprintf(`DROP TABLE IF EXISTS %q CASCADE`, ds.JobStatusTable)
	jd.prepareAndExecStmtInTx(tx, sqlStatement)
	sqlStatement = fmt.Sprintf(`DROP TABLE IF EXISTS %q CASCADE`, ds.JobTable)
	jd.prepareAndExecStmtInTx(tx, sqlStatement)
	err = tx.Commit()
	jd.assertError(err)
}

func (jd *Handle) postDropDs(ds dataSetT) {
	jd.noResultsCache.InvalidateDataset(ds.Index)

	// Tracking time interval between drop ds operations. Hence calling end before start
	if !jd.dsDropTime.IsZero() {
		jd.statDropDSPeriod.Since(jd.dsDropTime)
	}
	jd.dsDropTime = time.Now()
}

func (jd *Handle) dropAllDS(l lock.LockToken) error {
	var err error
	dList, err := getDSList(jd, jd.dbHandle, jd.tablePrefix)
	if err != nil {
		return fmt.Errorf("getDSList: %w", err)
	}
	if err := jd.WithTx(context.Background(), func(tx *Tx) error {
		for _, ds := range dList {
			if err = jd.dropDSInTx(tx, ds); err != nil {
				return fmt.Errorf("dropDS: %w", err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("withTx: %w", err)
	}

	// Update the lists
	if err = jd.doRefreshDSRangeListWithDB(l, jd.dbHandle); err != nil {
		return fmt.Errorf("refreshDSRangeList: %w", err)
	}
	return nil
}

func setReadonlyDsInTx(ctx context.Context, tx *Tx, latestDS dataSetT) error {
	sqlStatement := fmt.Sprintf(
		`CREATE TRIGGER readonlyTableTrg
		BEFORE INSERT
		ON %q
		FOR EACH STATEMENT
		EXECUTE PROCEDURE %s;`, latestDS.JobTable, pgReadonlyTableExceptionFuncName)
	_, err := tx.ExecContext(ctx, sqlStatement)
	return err
}

// RefreshDSList refreshes the list of datasets in memory if the database view of the list has changed.
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
