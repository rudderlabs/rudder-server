package jobsdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cenkalti/backoff"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func (jd *HandleT) isBackupEnabled() bool {
	return jd.conf.backup.masterBackupEnabled && jd.conf.backup.instanceBackupEnabled
}

func (jd *HandleT) IsMasterBackupEnabled() bool {
	return jd.conf.backup.masterBackupEnabled
}

func (jd *HandleT) backupDSLoop(ctx context.Context) {
	sleepMultiplier := time.Duration(1)

	jd.logger.Info("BackupDS loop is running")

	for {
		select {
		case <-time.After(sleepMultiplier * jd.conf.backup.backupCheckSleepDuration):
			if !jd.isBackupEnabled() {
				jd.logger.Debugf("backupDSLoop backup disabled %s", jd.tablePrefix)
				continue
			}
		case <-ctx.Done():
			return
		}
		loop := func() error {
			jd.logger.Debugf("backupDSLoop backup enabled %s", jd.tablePrefix)
			backupDSRange, err := jd.getBackupDSRange(ctx)
			if err != nil {
				return fmt.Errorf("[JobsDB] :: Failed to get backup dataset range. Err: %w", err)
			}
			// check if non-empty dataset is present to back up
			// else continue
			sleepMultiplier = 1
			if (dataSetRangeT{} == *backupDSRange) {
				// sleep for more duration if no dataset is found
				sleepMultiplier = 6
				return nil
			}

			backupDS := backupDSRange.ds

			opPayload, err := json.Marshal(&backupDS)
			jd.assertError(err)

			if err := jd.WithTx(func(tx *Tx) error {
				opID, err := jd.JournalMarkStartInTx(tx, backupDSOperation, opPayload)
				if err != nil {
					return fmt.Errorf("mark start of backup operation: %w", err)
				}
				if err := jd.backupDS(ctx, backupDSRange); err != nil {
					return fmt.Errorf("backup dataset: %w", err)
				}
				if err := jd.journalMarkDoneInTx(tx, opID); err != nil {
					return fmt.Errorf("mark end of backup operation: %w", err)
				}
				return nil
			}); err != nil {
				return err
			}

			return jd.WithTx(func(tx *Tx) error {
				// drop dataset after successfully uploading both jobs and jobs_status to s3
				opID, err := jd.JournalMarkStartInTx(tx, backupDropDSOperation, opPayload)
				if err != nil {
					return fmt.Errorf("mark start of drop backup operation: %w", err)
				}
				// Currently, we retry uploading a table for some time & if it fails. We only drop that table & not all `pre_drop` tables.
				// So, in situation when new table creation rate is more than drop. We will still have pipe up issue.
				// An easy way to fix this is, if at any point of time exponential retry fails then instead of just dropping that particular
				// table drop all subsequent `pre_drop` table. As, most likely the upload of rest of the table will also fail with the same error.
				if err := jd.dropDSInTx(tx, backupDS); err != nil {
					return fmt.Errorf(" drop dataset: %w", err)
				}
				if err := jd.journalMarkDoneInTx(tx, opID); err != nil {
					return fmt.Errorf("mark end of drop backup operation: %w", err)
				}
				return nil
			})
		}
		if err := loop(); err != nil && ctx.Err() == nil {
			if !jd.conf.skipMaintenanceError {
				panic(err)
			}
			jd.logger.Errorf("[JobsDB] :: Failed to backup dataset. Err: %s", err.Error())
		}

	}
}

// backupDS writes both jobs and job_staus table to JOBS_BACKUP_STORAGE_PROVIDER
func (jd *HandleT) backupDS(ctx context.Context, backupDSRange *dataSetRangeT) error {
	if err := jd.WithTx(func(tx *Tx) error {
		_, err := jd.cleanStatusTable(ctx, tx, backupDSRange.ds.JobStatusTable, false)
		return err
	}); err != nil {
		return fmt.Errorf("error while cleaning status table: %w", err)
	}
	if jd.conf.backup.FailedOnly {
		if err := jd.failedOnlyBackup(ctx, backupDSRange); err != nil {
			return fmt.Errorf("error while backing up failed jobs: %w", err)
		}
		return nil
	}
	if err := jd.completeBackup(ctx, backupDSRange); err != nil {
		return fmt.Errorf("error while backing up complete jobs: %w", err)
	}
	return nil
}

func (jd *HandleT) uploadDumps(ctx context.Context, dumps map[string]string) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(jd.config.GetInt("JobsDB.JobsBackupUploadWorkers", 100))
	for workspaceID, filePath := range dumps {
		wrkId := workspaceID
		path := filePath
		operation := func() error {
			if err := jd.uploadTableDump(ctx, wrkId, path); err != nil {
				jd.logger.Errorf("[JobsDB] :: Failed to upload workspaceId %v. Error: %s", wrkId, err.Error())
				stats.Default.NewTaggedStat("backup_ds_failed", stats.CountType, stats.Tags{"customVal": jd.tablePrefix, "workspaceId": wrkId}).Increment()
				return err
			}
			return nil
		}
		g.Go(misc.WithBugsnag(func() error {
			return backoff.RetryNotify(operation, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(jd.config.GetInt("JobsDB.backup.maxRetries", 100))), func(err error, d time.Duration) {
				jd.logger.Errorf("[JobsDB] :: Retrying upload workspaceId %v. Error: %s", wrkId, err.Error())
			})
		}))
	}
	return g.Wait()
}

func (jd *HandleT) failedOnlyBackup(ctx context.Context, backupDSRange *dataSetRangeT) error {
	tableName := backupDSRange.ds.JobStatusTable

	getRowCount := func(ctx context.Context) (totalCount int64, err error) {
		countStmt := fmt.Sprintf(`SELECT COUNT(*) from %q where job_state in ('%s', '%s')`, tableName, Failed.State, Aborted.State)
		if err = jd.dbHandle.QueryRowContext(ctx, countStmt).Scan(&totalCount); err != nil {
			return 0, fmt.Errorf("error while getting row count: %w", err)
		}
		return totalCount, nil
	}

	totalCount, err := getRowCount(ctx)
	if err != nil {
		return err
	}

	if totalCount == 0 {
		return nil
	}

	jd.logger.Infof("[JobsDB] :: Backing up table (failed only backup): %v", tableName)

	start := time.Now()
	getFileName := func(workspaceID string) (string, error) {
		backupPathDirName := "/rudder-s3-dumps/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			return "", err
		}
		pathPrefix := strings.TrimPrefix(tableName, preDropTablePrefix)
		return fmt.Sprintf(`%v%v_%v.%v.gz`, tmpDirPath+backupPathDirName, pathPrefix, Aborted.State, workspaceID), nil
	}

	dumps, err := jd.createTableDumps(ctx, getFailedOnlyBackupQueryFn(backupDSRange, jd.conf.backup.backupRowsBatchSize, jd.conf.backup.backupMaxTotalPayloadSize), getFileName, totalCount)
	if err != nil {
		return fmt.Errorf("error while creating table dump: %w", err)
	}
	defer func() {
		for _, filePath := range dumps {
			_ = os.Remove(filePath)
		}
	}()
	err = jd.uploadDumps(ctx, dumps)
	if err != nil {
		jd.logger.Errorf("[JobsDB] :: Failed to upload dumps for table: %s. Error: %v", tableName, err)
		return nil
	}
	stats.Default.NewTaggedStat("total_TableDump_TimeStat", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix}).Since(start)
	return nil
}

func (jd *HandleT) backupJobsTable(ctx context.Context, backupDSRange *dataSetRangeT) error {
	tableName := backupDSRange.ds.JobTable

	getRowCount := func(ctx context.Context) (totalCount int64, err error) {
		countStmt := fmt.Sprintf(`SELECT COUNT(*) from %q`, tableName)
		if err = jd.dbHandle.QueryRowContext(ctx, countStmt).Scan(&totalCount); err != nil {
			return 0, fmt.Errorf("error while getting row count: %w", err)
		}
		return totalCount, nil
	}

	totalCount, err := getRowCount(ctx)
	if err != nil {
		return err
	}

	if totalCount == 0 {
		return nil
	}

	jd.logger.Infof("[JobsDB] :: Backing up table (jobs table): %v", tableName)

	start := time.Now()

	getFileName := func(workspaceID string) (string, error) {
		backupPathDirName := "/rudder-s3-dumps/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			return "", err
		}
		pathPrefix := strings.TrimPrefix(tableName, preDropTablePrefix)
		return fmt.Sprintf(`%v%v.%v.%v.%v.%v.%v.gz`,
			tmpDirPath+backupPathDirName,
			pathPrefix,
			backupDSRange.minJobID,
			backupDSRange.maxJobID,
			backupDSRange.startTime,
			backupDSRange.endTime,
			workspaceID,
		), nil
	}

	dumps, err := jd.createTableDumps(ctx, getJobsBackupQueryFn(backupDSRange, jd.conf.backup.backupRowsBatchSize, jd.conf.backup.backupMaxTotalPayloadSize), getFileName, totalCount)
	if err != nil {
		return fmt.Errorf("error while creating table dump: %w", err)
	}
	defer func() {
		for _, filePath := range dumps {
			_ = os.Remove(filePath)
		}
	}()
	err = jd.uploadDumps(ctx, dumps)
	if err != nil {
		jd.logger.Errorf("[JobsDB] :: Failed to upload dumps for table: %s. Error: %v", tableName, err)
		return nil
	}

	// Do not record stat in error case as error case time might be low and skew stats
	stats.Default.NewTaggedStat("total_TableDump_TimeStat", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix}).Since(start)
	return nil
}

func (jd *HandleT) backupStatusTable(ctx context.Context, backupDSRange *dataSetRangeT) error {
	tableName := backupDSRange.ds.JobStatusTable

	getRowCount := func(ctx context.Context) (totalCount int64, err error) {
		countStmt := fmt.Sprintf(`SELECT COUNT(*) from %q`, tableName)
		if err = jd.dbHandle.QueryRowContext(ctx, countStmt).Scan(&totalCount); err != nil {
			return 0, fmt.Errorf("error while getting row count: %w", err)
		}
		return totalCount, nil
	}

	totalCount, err := getRowCount(ctx)
	if err != nil {
		return err
	}

	if totalCount == 0 {
		return nil
	}

	jd.logger.Infof("[JobsDB] :: Backing up table (status table): %v", tableName)

	start := time.Now()

	getFileName := func(workspaceID string) (string, error) {
		backupPathDirName := "/rudder-s3-dumps/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			return "", err
		}
		pathPrefix := strings.TrimPrefix(tableName, preDropTablePrefix)
		return fmt.Sprintf(`%v%v.%v.gz`, tmpDirPath+backupPathDirName, pathPrefix, workspaceID), nil
	}

	dumps, err := jd.createTableDumps(ctx, getStatusBackupQueryFn(backupDSRange, jd.conf.backup.backupRowsBatchSize), getFileName, totalCount)
	if err != nil {
		return fmt.Errorf("error while creating table dump: %w", err)
	}
	defer func() {
		for _, filePath := range dumps {
			_ = os.Remove(filePath)
		}
	}()
	err = jd.uploadDumps(ctx, dumps)
	if err != nil {
		jd.logger.Errorf("[JobsDB] :: Failed to upload dumps for table: %s. Error: %v", tableName, err)
		return nil
	}

	// Do not record stat in error case as error case time might be low and skew stats
	stats.Default.NewTaggedStat("total_TableDump_TimeStat", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix}).Since(start)
	return nil
}

func (jd *HandleT) completeBackup(ctx context.Context, backupDSRange *dataSetRangeT) error {
	if err := jd.backupJobsTable(ctx, backupDSRange); err != nil {
		return err
	}
	if err := jd.backupStatusTable(ctx, backupDSRange); err != nil {
		return err
	}
	return nil
}

func (jd *HandleT) removeTableJSONDumps() {
	backupPathDirName := "/rudder-s3-dumps/"
	tmpDirPath, err := misc.CreateTMPDIR()
	jd.assertError(err)
	files, err := filepath.Glob(fmt.Sprintf("%v%v_job*", tmpDirPath+backupPathDirName, jd.tablePrefix))
	jd.assertError(err)
	for _, f := range files {
		err = os.Remove(f)
		jd.assertError(err)
	}
}

func getFailedOnlyBackupQueryFn(backupDSRange *dataSetRangeT, backupRowsBatchSize, backupMaxTotalPayloadSize int64) func(int64) string {
	return func(offSet int64) string {
		return fmt.Sprintf(
			`SELECT
			failed_jobs.workspace_id,
			json_build_object(
				'job_id', failed_jobs.job_id,
				'workspace_id',failed_jobs.workspace_id,
				'uuid',failed_jobs.uuid,
				'user_id',failed_jobs.user_id,
				'parameters',failed_jobs.parameters,
				'custom_val',failed_jobs.custom_val,
				'event_payload',failed_jobs.event_payload,
				'event_count',failed_jobs.event_count,
				'created_at',failed_jobs.created_at,
				'expire_at',failed_jobs.expire_at,
				'id',failed_jobs.id,
				'job_id',failed_jobs.status_job_id,
				'job_state',failed_jobs.job_state,
				'attempt',failed_jobs.attempt,
				'exec_time',failed_jobs.exec_time,
				'retry_time',failed_jobs.retry_time,
				'error_code',failed_jobs.error_code,
				'error_response',failed_jobs.error_response,
				'parameters',failed_jobs.status_parameters
			)
		FROM
			(
			SELECT
				*
			FROM
				(
				SELECT *,
				sum(
				pg_column_size(jobs.event_payload)
				) OVER (
				ORDER BY
					jobs.custom_val,
					jobs.status_job_id,
					jobs.exec_time
				) AS running_payload_size,
				ROW_NUMBER()
				OVER (
				ORDER BY
					jobs.custom_val,
					jobs.status_job_id,
					jobs.exec_time
				) AS row_num
				FROM
					(
					SELECT
						job.job_id,
						job.workspace_id,
						job.uuid,
						job.user_id,
						job.parameters,
						job.custom_val,
						job.event_payload,
						job.event_count,
						job.created_at,
						job.expire_at,
						job_status.id,
						job_status.job_id AS status_job_id,
						job_status.job_state,
						job_status.attempt,
						job_status.exec_time,
						job_status.retry_time,
						job_status.error_code,
						job_status.error_response,
						job_status.parameters AS status_parameters
					FROM
						%[1]q "job_status"
						INNER JOIN %[2]q "job" ON job_status.job_id = job.job_id
					WHERE
						job_status.job_state IN ('%[3]s', '%[4]s')
					ORDER BY
					job.custom_val,
						job_status.job_id,
						job_status.exec_time ASC
					LIMIT
						%[5]d
					OFFSET
						%[6]d
					) jobs
				) subquery
			WHERE
				subquery.running_payload_size <= %[7]d OR subquery.row_num = 1
			) AS failed_jobs
	  `, backupDSRange.ds.JobStatusTable, backupDSRange.ds.JobTable, Failed.State, Aborted.State, backupRowsBatchSize, offSet, backupMaxTotalPayloadSize)
	}
}

func getJobsBackupQueryFn(backupDSRange *dataSetRangeT, backupRowsBatchSize, backupMaxTotalPayloadSize int64) func(int64) string {
	return func(offSet int64) string {
		return fmt.Sprintf(`
			SELECT
				dump_table.workspace_id,
				jsonb_build_object(
					'job_id', dump_table.job_id,
					'workspace_id', dump_table.workspace_id,
					'uuid', dump_table.uuid,
					'user_id', dump_table.user_id,
					'parameters', dump_table.parameters,
					'custom_val', dump_table.custom_val,
					'event_payload', dump_table.event_payload,
					'event_count', dump_table.event_count,
					'created_at', dump_table.created_at,
					'expire_at', dump_table.expire_at
				)
		  	FROM
				(
				SELECT
					*
				FROM
					(
						SELECT
							*,
							sum(
							pg_column_size(jobs.event_payload)
							) OVER (
							ORDER BY
								jobs.job_id
							) AS running_payload_size,
							ROW_NUMBER()
							OVER (
							ORDER BY
								job_id ASC
							) AS row_num
						FROM
							(
							SELECT
								*
							FROM
								%[1]q job
							ORDER BY
								job_id ASC
							LIMIT
								%[2]d
							OFFSET
								%[3]d
							) jobs
					) subquery
				WHERE
					subquery.running_payload_size <= %[4]d OR subquery.row_num = 1
			) AS dump_table
			`, backupDSRange.ds.JobTable, backupRowsBatchSize, offSet, backupMaxTotalPayloadSize)
	}
}

func getStatusBackupQueryFn(backupDSRange *dataSetRangeT, backupRowsBatchSize int64) func(int64) string {
	return func(offSet int64) string {
		return fmt.Sprintf(`
			SELECT
				dump_table.workspace_id,
			 	json_build_object(
					'id', dump_table.id,
			 		'job_id', dump_table.job_id,
				 	'job_state', dump_table.job_state,
				 	'attempt', dump_table.attempt,
			 		'exec_time', dump_table.exec_time,
			 		'retry_time', dump_table.retry_time,
			 		'error_code', dump_table.error_code,
			 		'error_response', dump_table.error_response,
			 		'parameters', dump_table.parameters
	)
			FROM
				(
				SELECT
					job_status.*, job.workspace_id
				FROM
				(
					%[1]q "job_status"
					INNER JOIN %[2]q "job" ON job_status.job_id = job.job_id
				)
				ORDER BY
					job_status.job_id ASC
				LIMIT
					%[3]d
				OFFSET
					%[4]d
				)
				AS dump_table
			`, backupDSRange.ds.JobStatusTable, backupDSRange.ds.JobTable, backupRowsBatchSize, offSet)
	}
}

func (jd *HandleT) createTableDumps(ctx context.Context, queryFunc func(int64) string, pathFunc func(string) (string, error), totalCount int64) (map[string]string, error) {
	defer jd.getTimerStat(
		"table_FileDump_TimeStat",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()
	filesWriter := fileuploader.NewGzMultiFileWriter()

	var offset int64
	dumps := make(map[string]string)
	writeBackupToGz := func() error {
		stmt := queryFunc(offset)
		var rawJSONRows json.RawMessage
		var workspaceID string
		rows, err := jd.dbHandle.QueryContext(ctx, stmt)
		if err != nil {
			return fmt.Errorf("error while getting rows: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			err = rows.Scan(&workspaceID, &rawJSONRows)
			if err != nil {
				return fmt.Errorf("scanning row failed with error : %w", err)
			}
			preferences, err := jd.conf.backup.fileUploaderProvider.GetStoragePreferences(workspaceID)
			if errors.Is(err, fileuploader.NoStorageForWorkspaceError) {
				offset++
				jd.logger.Infof("getting storage preferences failed with error: %w for workspaceID: %s", err, workspaceID)
				continue
			}
			if err != nil {
				return fmt.Errorf("getting storage preferences failed with error: %w for workspaceID: %s", err, workspaceID)
			}
			if !preferences.Backup(jd.tablePrefix) {
				offset++
				jd.logger.Infof("Skipping backup for workspace: %s. Preferences: %v and tablePrefix: %s", workspaceID, preferences, jd.tablePrefix)
				continue
			}
			rawJSONRows = append(rawJSONRows, '\n') // appending '\n'
			path, err := pathFunc(workspaceID)
			if err != nil {
				return fmt.Errorf("error while getting path: %w", err)
			}
			_, err = filesWriter.Write(path, rawJSONRows)
			if err != nil {
				return fmt.Errorf("writing gz file %q: %w", path, err)
			}
			if _, ok := dumps[workspaceID]; !ok {
				dumps[workspaceID] = path
			}
			offset++
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error while iterating rows: %w", err)
		}
		return nil
	}

	for {
		if err := writeBackupToGz(); err != nil {
			return nil, err
		}
		if offset >= totalCount {
			break
		}
	}

	err := filesWriter.Close()
	if err != nil {
		return dumps, err
	}
	return dumps, nil
}

func (jd *HandleT) uploadTableDump(ctx context.Context, workspaceID, path string) error {
	defer jd.getTimerStat(
		"fileUpload_TimeStat",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening gz file %q: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	pathPrefixes := make([]string, 0)
	// For empty path prefix, don't need to add anything to the array
	if jd.conf.backup.PathPrefix != "" {
		pathPrefixes = append(pathPrefixes, jd.conf.backup.PathPrefix, jd.config.GetString("INSTANCE_ID", "1"))
	} else {
		pathPrefixes = append(pathPrefixes, jd.config.GetString("INSTANCE_ID", "1"))
	}

	var output filemanager.UploadedFile
	output, err = jd.backupUploadWithExponentialBackoff(ctx, file, workspaceID, pathPrefixes...)
	if err != nil {
		jd.logger.Errorf("[JobsDB] :: Failed to upload table dump for workspaceId %s. Error: %s", workspaceID, err.Error())
		return err
	}
	jd.logger.Infof("[JobsDB] :: Backed up table at %s for workspaceId %s", output.Location, workspaceID)
	return nil
}

func (jd *HandleT) backupUploadWithExponentialBackoff(ctx context.Context, file *os.File, workspaceID string, pathPrefixes ...string) (filemanager.UploadedFile, error) {
	// get a file uploader
	fileUploader, err := jd.conf.backup.fileUploaderProvider.GetFileManager(workspaceID)
	if err != nil {
		return filemanager.UploadedFile{}, err
	}
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = jd.conf.backup.maxBackupRetryTime
	boRetries := backoff.WithMaxRetries(bo, uint64(jd.config.GetInt64("MAX_BACKOFF_RETRIES", 3)))
	boCtx := backoff.WithContext(boRetries, ctx)

	var output filemanager.UploadedFile
	backup := func() error {
		output, err = fileUploader.Upload(ctx, file, pathPrefixes...)
		return err
	}

	err = backoff.Retry(backup, boCtx)
	return output, err
}

func (jd *HandleT) getBackupDSRange(ctx context.Context) (*dataSetRangeT, error) {
	var backupDS dataSetT
	var backupDSRange dataSetRangeT

	// Read the table names from PG
	tableNames, err := getAllTableNames(jd.dbHandle)
	if err != nil {
		return nil, fmt.Errorf("getAllTableNames: %w", err)
	}
	// We check for job_status because that is renamed after job
	var dnumList []string
	for _, t := range tableNames {
		if strings.HasPrefix(t, preDropTablePrefix+jd.tablePrefix+"_jobs_") {
			dnum := t[len(preDropTablePrefix+jd.tablePrefix+"_jobs_"):]
			dnumList = append(dnumList, dnum)
			continue
		}
	}
	if len(dnumList) == 0 {
		return &backupDSRange, nil
	}
	jd.statPreDropTableCount.Gauge(len(dnumList))

	sortDnumList(dnumList)

	backupDS = dataSetT{
		JobTable:       fmt.Sprintf("%s%s_jobs_%s", preDropTablePrefix, jd.tablePrefix, dnumList[0]),
		JobStatusTable: fmt.Sprintf("%s%s_job_status_%s", preDropTablePrefix, jd.tablePrefix, dnumList[0]),
		Index:          dnumList[0],
	}

	var minID, maxID sql.NullInt64
	jobIDSQLStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) from %q`, backupDS.JobTable)
	row := jd.dbHandle.QueryRowContext(ctx, jobIDSQLStatement)
	err = row.Scan(&minID, &maxID)
	if err != nil {
		return nil, fmt.Errorf("getting min and max job_id: %w", err)
	}

	var minCreatedAt, maxCreatedAt time.Time
	jobTimeSQLStatement := fmt.Sprintf(`SELECT MIN(created_at), MAX(created_at) from %q`, backupDS.JobTable)
	row = jd.dbHandle.QueryRowContext(ctx, jobTimeSQLStatement)
	err = row.Scan(&minCreatedAt, &maxCreatedAt)
	if err != nil {
		return nil, fmt.Errorf("getting min and max created_at: %w", err)
	}

	backupDSRange = dataSetRangeT{
		minJobID:  minID.Int64,
		maxJobID:  maxID.Int64,
		startTime: minCreatedAt.UnixNano() / int64(time.Millisecond),
		endTime:   maxCreatedAt.UnixNano() / int64(time.Millisecond),
		ds:        backupDS,
	}
	return &backupDSRange, nil
}
