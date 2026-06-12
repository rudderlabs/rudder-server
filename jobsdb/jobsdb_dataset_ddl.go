package jobsdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

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
	if jd.isStatNewDSPeriodInitialized {
		jd.statNewDSPeriod.Since(jd.newDSCreationTime)
	}
	jd.newDSCreationTime = time.Now()
	jd.isStatNewDSPeriodInitialized = true

	return nil
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
		expire_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		consumers TEXT[] NOT NULL DEFAULT '{""}');`, newDS.JobTable)); err != nil {
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
		parameters JSONB DEFAULT '{}'::JSONB,
		consumer TEXT NOT NULL DEFAULT '');`, newDS.JobStatusTable)); err != nil {
		return fmt.Errorf("creating %s: %w", newDS.JobStatusTable, err)
	}

	// Record the dataset's creation time as a comment on the jobs table, so that
	// compaction can tell how recently a dataset was created (see checkIfCompactDS).
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`COMMENT ON TABLE %q IS '%s'`, newDS.JobTable, dsCreatedAtComment(time.Now()))); err != nil {
		return fmt.Errorf("commenting creation time on %s: %w", newDS.JobTable, err)
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
	if jd.conf.multiConsumer {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_consumers" ON %[1]q USING GIN (consumers)`, newDS.JobTable)); err != nil {
			return fmt.Errorf("creating consumers GIN index: %w", err)
		}
	}
	return nil
}

// createDSStatusIndicesInTx creates the indices on the job status table plus the
// v_last view (single-consumer) or v_last_c_ view (multi-consumer).
func (jd *Handle) createDSStatusIndicesInTx(ctx context.Context, tx *Tx, newDS dataSetT) error {
	// retention index — consumer-agnostic, always created
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_id_js" ON %[1]q(id ,job_state) INCLUDE (exec_time)`, newDS.JobStatusTable)); err != nil {
		return fmt.Errorf("adding id_js index: %w", err)
	}

	if jd.conf.multiConsumer {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_jid_c_id_js" ON %[1]q (job_id ASC, consumer, id DESC, job_state)`, newDS.JobStatusTable)); err != nil {
			return fmt.Errorf("adding jid_c_id_js index: %w", err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW "v_last_c_%[1]s" AS SELECT DISTINCT ON (job_id, consumer) * FROM %[1]q ORDER BY job_id ASC, consumer, id DESC`, newDS.JobStatusTable)); err != nil {
			return fmt.Errorf("creating v_last_c view: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_jid_id_js" ON %[1]q(job_id asc,id desc,job_state)`, newDS.JobStatusTable)); err != nil {
			return fmt.Errorf("adding jid_id_js index: %w", err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW "v_last_%[1]s" AS SELECT DISTINCT ON (job_id) * FROM %[1]q ORDER BY job_id ASC, id DESC`, newDS.JobStatusTable)); err != nil {
			return fmt.Errorf("creating v_last view: %w", err)
		}
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

// dropDSInTx drops a dataset
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
	jd.execStmtInTxAllowMissing(tx, sqlStatement)

	sqlStatement = fmt.Sprintf(`LOCK TABLE %q IN ACCESS EXCLUSIVE MODE;`, ds.JobTable)
	jd.execStmtInTxAllowMissing(tx, sqlStatement)

	sqlStatement = fmt.Sprintf(`DROP TABLE IF EXISTS %q CASCADE`, ds.JobStatusTable)
	jd.execStmtInTx(tx, sqlStatement)
	sqlStatement = fmt.Sprintf(`DROP TABLE IF EXISTS %q CASCADE`, ds.JobTable)
	jd.execStmtInTx(tx, sqlStatement)
	err = tx.Commit()
	jd.assertError(err)
}

func (jd *Handle) postDropDs(ds dataSetT) {
	jd.noResultsCache.InvalidateDataset(ds.Index)

	// Tracking time interval between drop ds operations. Hence calling end before start
	if jd.isStatDropDSPeriodInitialized {
		jd.statDropDSPeriod.Since(jd.dsDropTime)
	}
	jd.dsDropTime = time.Now()
	jd.isStatDropDSPeriodInitialized = true
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
