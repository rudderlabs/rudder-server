package jobsdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	backup "github.com/rudderlabs/rudder-server/services/backup"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/sync/errgroup"
)

// backupSettings is for capturing the backup
// configuration from the config/env files to
// instantiate jobdb correctly
type backupSettings struct {
	instanceBackupEnabled bool
	FailedOnly            bool
	PathPrefix            string
}

type WorkerT struct {
	workerId           int
	dbHandle           *sql.DB
	logger             logger.Logger
	maxBackupRetryTime time.Duration
	job                *WorkerJob
}

type WorkerJob struct {
	id           int
	workspaceID  string
	fileUploader filemanager.FileManager
}

func (b *backupSettings) isBackupEnabled() bool {
	return masterBackupEnabled && b.instanceBackupEnabled && config.GetString("JOBS_BACKUP_BUCKET", "") != ""
}

func IsMasterBackupEnabled() bool {
	return masterBackupEnabled
}

// NewWorker creates a new worker
func (jd *HandleT) NewWorker(id int) *WorkerT {
	return &WorkerT{
		workerId:           id,
		dbHandle:           jd.dbHandle,
		logger:             jd.logger.Child(fmt.Sprintf("worker-%d", id)),
		maxBackupRetryTime: jd.maxBackupRetryTime,
	}
}

func (jd *HandleT) backupDSLoop(ctx context.Context) {
	sleepMultiplier := time.Duration(1)

	jd.logger.Info("BackupDS loop is running")

	for {
		select {
		case <-time.After(sleepMultiplier * backupCheckSleepDuration):
			if !jd.BackupSettings.isBackupEnabled() {
				jd.logger.Debugf("backupDSLoop backup disabled %s", jd.tablePrefix)
				continue
			}
		case <-ctx.Done():
			return
		}
		jd.logger.Debugf("backupDSLoop backup enabled %s", jd.tablePrefix)
		backupDSRange := jd.getBackupDSRange()
		// check if non-empty dataset is present to back up
		// else continue
		sleepMultiplier = 1
		if (dataSetRangeT{} == *backupDSRange) {
			// sleep for more duration if no dataset is found
			sleepMultiplier = 6
			continue
		}

		backupDS := backupDSRange.ds

		opPayload, err := json.Marshal(&backupDS)
		jd.assertError(err)

		opID := jd.JournalMarkStart(backupDSOperation, opPayload)
		err = jd.backupDS(ctx, backupDSRange)
		if err != nil {
			stats.Default.NewTaggedStat("backup_ds_failed", stats.CountType, stats.Tags{"customVal": jd.tablePrefix, "provider": config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3")}).Increment()
			jd.logger.Errorf("[JobsDB] :: Failed to backup jobs table %v. Err: %v", backupDSRange.ds.JobStatusTable, err)
		}
		jd.JournalMarkDone(opID)

		// drop dataset after successfully uploading both jobs and jobs_status to s3
		opID = jd.JournalMarkStart(backupDropDSOperation, opPayload)
		// Currently, we retry uploading a table for some time & if it fails. We only drop that table & not all `pre_drop` tables.
		// So, in situation when new table creation rate is more than drop. We will still have pipe up issue.
		// An easy way to fix this is, if at any point of time exponential retry fails then instead of just dropping that particular
		// table drop all subsequent `pre_drop` table. As, most likely the upload of rest of the table will also fail with the same error.
		jd.mustDropDS(backupDS)
		jd.JournalMarkDone(opID)
	}
}

func (jd *HandleT) getAllWorkspaces(jobTable string) ([]string, error) {
	var workspaces []string
	query := fmt.Sprintf(`SELECT DISTINCT workspace_id FROM %s`, jobTable)
	rows, err := jd.dbHandle.Query(query)
	if err != nil {
		return workspaces, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var workspace string
		err = rows.Scan(&workspace)
		if err != nil {
			return workspaces, err
		}
		workspaces = append(workspaces, workspace)
	}
	return workspaces, nil
}

// backupDS writes both jobs and job_staus table to JOBS_BACKUP_STORAGE_PROVIDER
func (jd *HandleT) backupDS(ctx context.Context, backupDSRange *dataSetRangeT) error {
	if jd.backupWorkers == nil {
		// create workers
		noOfWorkers := config.GetInt("MULTITENANT_JOBS_BACKUP_WORKERS", 64)
		jd.backupWorkers = make(chan *WorkerT, noOfWorkers)
		for i := 0; i < noOfWorkers; i++ {
			jd.backupWorkers <- jd.NewWorker(i)
		}
	}

	err := jd.cleanStatusTable(backupDSRange)
	if err != nil {
		return fmt.Errorf("error while cleaning status table: %w", err)
	}

	if jd.storageSettings.StoragePreferences == nil {
		jd.storageSettings.StoragePreferences = make(backup.StoragePreferences)
	}

	if jd.BackupSettings.FailedOnly {
		if err = jd.failedOnlyBackup(ctx, backupDSRange); err != nil {
			return fmt.Errorf("error while backing up failed jobs: %w", err)
		}
		return nil
	}

	err = jd.completeBackup(ctx, backupDSRange)
	if err != nil {
		return fmt.Errorf("error while backing up complete jobs: %w", err)
	}
	return nil
}

func (jd *HandleT) cleanStatusTable(backupDSRange *dataSetRangeT) error {
	_, err := jd.dbHandle.Exec(
		fmt.Sprintf(`
		DELETE FROM %[1]q
		where id
		IN (
			SELECT id 
			FROM (
				SELECT id, RANK()
				OVER(
					PARTITION BY job_id 
					ORDER BY id DESC
					)
				as rank 
				from %[1]q
			)
			as inner_table 
			where rank > 2
		);`, backupDSRange.ds.JobStatusTable))
	return err
}

func (jd *HandleT) failedOnlyBackup(ctx context.Context, backupDSRange *dataSetRangeT) error {
	tableName := backupDSRange.ds.JobStatusTable
	g, _ := errgroup.WithContext(ctx)

	getRowCount := func(workspaceID string) (totalCount int64, err error) {
		countStmt := fmt.Sprintf(`SELECT COUNT(job_status.*) from %q "job_status" INNER JOIN %q "job" ON job_status.job_id = job.job_id WHERE job_status.job_state in ('%s', '%s') AND job.workspace_id='%s'`, backupDSRange.ds.JobStatusTable, backupDSRange.ds.JobTable, Failed.State, Aborted.State, workspaceID)
		if err = jd.dbHandle.QueryRow(countStmt).Scan(&totalCount); err != nil {
			return 0, fmt.Errorf("error while getting row count: %w", err)
		}
		return totalCount, nil
	}

	jd.logger.Infof("[JobsDB] :: Backing up table: %v", tableName)

	start := time.Now()
	getFileName := func(workspaceID string) (string, error) {
		backupPathDirName := "/rudder-s3-dumps/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			return "", err
		}
		pathPrefix = strings.TrimPrefix(tableName, preDropTablePrefix)
		return fmt.Sprintf(`%v%v_%v.%v.gz`, tmpDirPath+backupPathDirName, pathPrefix, Aborted.State, workspaceID), nil
	}

	workspaces, err := jd.getAllWorkspaces(backupDSRange.ds.JobTable)
	if err != nil {
		return err
	}

	for i := 0; i < len(workspaces); i++ {
		workspace := workspaces[i]
		totalCount, err := getRowCount(workspace)
		if err != nil {
			return err
		}

		if totalCount == 0 {
			jd.logger.Infof("[JobsDB] :: Skipping backup for workspace/table since there are no rows: %s/%s", workspace, tableName)
			return nil
		}
		fileUploader, err := jd.getBackupFileUploader(ctx, workspace)
		if err != nil {
			return err
		}
		backupWorker := <-jd.backupWorkers
		backupWorker.job = &WorkerJob{
			id:           i,
			workspaceID:  workspace,
			fileUploader: fileUploader,
		}
		g.Go(misc.WithBugsnag(func() error {
			defer func() {
				jd.backupWorkers <- backupWorker
			}()

			if storagePreferences, ok := jd.storageSettings.StoragePreferences[workspace]; ok && !jd.shouldBackUp(storagePreferences) {
				jd.logger.Infof("Skipping backup for workspace/tablePrefix: %s/%s", backupWorker.job.workspaceID, jd.tablePrefix)
				return nil
			}
			path, err := getFileName(backupWorker.job.workspaceID)
			if err != nil {
				return fmt.Errorf("error while getting file name: %w", err)
			}

			err = backupWorker.createTableDump(getFailedOnlyBackupQueryFn(backupDSRange, workspace), jd.tablePrefix, path, totalCount)
			if err != nil {
				return fmt.Errorf("error while creating table dump: %w", err)
			}
			defer func() { _ = os.Remove(path) }()

			err = backupWorker.uploadTableDump(ctx, jd.tablePrefix, jd.BackupSettings.PathPrefix, path)
			if err != nil {
				jd.logger.Errorf("[JobsDB] :: Failed to upload table %v", tableName)
				return err
			}
			return nil
		}))
	}
	err = g.Wait()
	if err != nil {
		return err
	}


	stats.Default.NewTaggedStat("total_TableDump_TimeStat", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix}).Since(start)
	return nil
}

func (jd *HandleT) backupJobsTable(ctx context.Context, backupDSRange *dataSetRangeT) error {
	tableName := backupDSRange.ds.JobTable
	g, _ := errgroup.WithContext(ctx)

	getRowCount := func(workspaceID string) (totalCount int64, err error) {
		countStmt := fmt.Sprintf(`SELECT COUNT(job.*) from %q "job" WHERE job.workspace_id='%s'`, tableName, workspaceID)
		if err = jd.dbHandle.QueryRow(countStmt).Scan(&totalCount); err != nil {
			return 0, fmt.Errorf("error while getting row count: %w", err)
		}
		return totalCount, nil
	}

	jd.logger.Infof("[JobsDB] :: Backing up table: %v", tableName)

	start := time.Now()

	getFileName := func(workspaceID string) (string, error) {
		backupPathDirName := "/rudder-s3-dumps/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			return "", err
		}
		pathPrefix = strings.TrimPrefix(tableName, preDropTablePrefix)
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

	workspaces, err := jd.getAllWorkspaces(backupDSRange.ds.JobTable)
	if err != nil {
		return err
	}

	for i := 0; i < len(workspaces); i++ {
		workspace := workspaces[i]
		totalCount, err := getRowCount(workspace)
		if err != nil {
			return err
		}

		if totalCount == 0 {
			jd.logger.Infof("[JobsDB] :: Skipping backup for workspace/table since there are no rows: %s/%s", workspace, tableName)
			return nil
		}
		fileUploader, err := jd.getBackupFileUploader(ctx, workspace)
		if err != nil {
			return err
		}
		backupWorker := <-jd.backupWorkers
		backupWorker.job = &WorkerJob{
			id:           i,
			workspaceID:  workspace,
			fileUploader: fileUploader,
		}
		g.Go(misc.WithBugsnag(func() error {
			defer func() {
				jd.backupWorkers <- backupWorker
			}()

			if storagePreferences, ok := jd.storageSettings.StoragePreferences[workspace]; ok && !jd.shouldBackUp(storagePreferences) {
				jd.logger.Infof("Skipping backup for workspace/tablePrefix: %s/%s", backupWorker.job.workspaceID, jd.tablePrefix)
				return nil
			}

			path, err := getFileName(backupWorker.job.workspaceID)
			if err != nil {
				return fmt.Errorf("error while getting file name: %w", err)
			}

			err = backupWorker.createTableDump(getJobsBackupQueryFn(backupDSRange, workspace), jd.tablePrefix, path, totalCount)
			if err != nil {
				return fmt.Errorf("error while creating table dump: %w", err)
			}
			defer func() { _ = os.Remove(path) }()

			err = backupWorker.uploadTableDump(ctx, jd.tablePrefix, jd.BackupSettings.PathPrefix, path)
			if err != nil {
				jd.logger.Errorf("[JobsDB] :: Failed to upload table %v", tableName)
				return err
			}
			return nil
		}))
	}
	err = g.Wait()
	if err != nil {
		return err
	}

	// Do not record stat in error case as error case time might be low and skew stats
	stats.Default.NewTaggedStat("total_TableDump_TimeStat", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix}).Since(start)
	return nil
}

func (jd *HandleT) backupStatusTable(ctx context.Context, backupDSRange *dataSetRangeT) error {
	tableName := backupDSRange.ds.JobStatusTable
	g, _ := errgroup.WithContext(ctx)

	getRowCount := func(workspaceID string) (totalCount int64, err error) {
		countStmt := fmt.Sprintf(`SELECT COUNT(job_status.*) from %q "job_status" INNER JOIN %q "job" ON job_status.job_id = job.job_id WHERE job.workspace_id='%s'`, backupDSRange.ds.JobStatusTable, backupDSRange.ds.JobTable, workspaceID)
		if err = jd.dbHandle.QueryRow(countStmt).Scan(&totalCount); err != nil {
			return 0, fmt.Errorf("error while getting row count: %w", err)
		}
		return totalCount, nil
	}

	jd.logger.Infof("[JobsDB] :: Backing up table: %v", tableName)

	start := time.Now()

	getFileName := func(workspaceID string) (string, error) {
		backupPathDirName := "/rudder-s3-dumps/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			return "", err
		}
		pathPrefix = strings.TrimPrefix(tableName, preDropTablePrefix)
		return fmt.Sprintf(`%v%v.%v.gz`, tmpDirPath+backupPathDirName, pathPrefix, workspaceID), nil
	}

	workspaces, err := jd.getAllWorkspaces(backupDSRange.ds.JobTable)
	if err != nil {
		return err
	}

	for i := 0; i < len(workspaces); i++ {
		workspace := workspaces[i]
		totalCount, err := getRowCount(workspace)
		if err != nil {
			return err
		}

		if totalCount == 0 {
			jd.logger.Infof("[JobsDB] :: Skipping backup for workspace/table since there are no rows: %s/%s", workspace, tableName)
			return nil
		}
		backupWorker := <-jd.backupWorkers
		fileUploader, err := jd.getBackupFileUploader(ctx, workspace)
		if err != nil {
			return err
		}
		backupWorker.job = &WorkerJob{
			id:           i,
			workspaceID:  workspace,
			fileUploader: fileUploader,
		}
		g.Go(misc.WithBugsnag(func() error {
			defer func() {
				jd.backupWorkers <- backupWorker
			}()

			if storagePreferences, ok := jd.storageSettings.StoragePreferences[workspace]; ok && !jd.shouldBackUp(storagePreferences) {
				jd.logger.Infof("Skipping backup for workspace/tablePrefix: %s/%s", backupWorker.job.workspaceID, jd.tablePrefix)
				return nil
			}

			path, err := getFileName(backupWorker.job.workspaceID)
			if err != nil {
				return fmt.Errorf("error while getting file name: %w", err)
			}

			err = backupWorker.createTableDump(getStatusBackupQueryFn(backupDSRange, workspace), jd.tablePrefix, path, totalCount)
			if err != nil {
				return fmt.Errorf("error while creating table dump: %w", err)
			}
			defer func() { _ = os.Remove(path) }()

			err = backupWorker.uploadTableDump(ctx, jd.tablePrefix, jd.BackupSettings.PathPrefix, path)
			if err != nil {
				jd.logger.Errorf("[JobsDB] :: Failed to upload table %v", tableName)
				return err
			}
			return nil
		}))
	}
	err = g.Wait()
	if err != nil {
		return err
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

// getFileUploader get a file uploader
func (jd *HandleT) getBackupFileUploader(ctx context.Context, workspaceID string) (filemanager.FileManager, error) {
	var err error
	var fileUploader filemanager.FileManager
	if bucket, ok := jd.storageSettings.StorageBucket[workspaceID]; ok {
		fileUploader, err = filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
			Provider: bucket.Type,
			Config:   bucket.Config,
		})
	} else {
		fileUploader, err = filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
			Provider: config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
			Config:   filemanager.GetProviderConfigForBackupsFromEnv(ctx),
		})
	}
	return fileUploader, err
}

func getFailedOnlyBackupQueryFn(backupDSRange *dataSetRangeT, workspaceID string) func(int64) string {
	return func(offSet int64) string {
		return fmt.Sprintf(
			`SELECT
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
						job_status.job_state IN ('%[3]s', '%[4]s') AND job.workspace_id = '%[8]s'
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
	  `, backupDSRange.ds.JobStatusTable, backupDSRange.ds.JobTable, Failed.State, Aborted.State, backupRowsBatchSize, offSet, backupMaxTotalPayloadSize, workspaceID)
	}
}

func getJobsBackupQueryFn(backupDSRange *dataSetRangeT, workspaceID string) func(int64) string {
	return func(offSet int64) string {
		return fmt.Sprintf(`
			SELECT
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
							WHERE
								job.workspace_id = '%[5]s'
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
			`, backupDSRange.ds.JobTable, backupRowsBatchSize, offSet, backupMaxTotalPayloadSize, workspaceID)
	}
}

func getStatusBackupQueryFn(backupDSRange *dataSetRangeT, workspaceID string) func(int64) string {
	return func(offSet int64) string {
		return fmt.Sprintf(`
			SELECT
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
					job_status.*
				FROM
				(
					%[1]q "job_status"
					INNER JOIN %[4]q "job" ON job_status.job_id = job.job_id
				)
				WHERE
					job.workspace_id = '%[5]s'
				ORDER BY
					job_status.job_id ASC
				LIMIT
					%[2]d
				OFFSET
					%[3]d
				)
				AS dump_table
			`, backupDSRange.ds.JobStatusTable, backupRowsBatchSize, offSet, backupDSRange.ds.JobTable, workspaceID)
	}
}

func (worker *WorkerT) createTableDump(queryFunc func(int64) string, tablePrefix, path string, totalCount int64) error {
	tableFileDumpTimeStat := stats.Default.NewTaggedStat("table_FileDump_TimeStat", stats.TimerType, stats.Tags{"customVal": tablePrefix})
	tableFileDumpTimeStat.Start()

	err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return err
	}

	gzWriter, err := misc.CreateGZ(path)
	if err != nil {
		return fmt.Errorf("creating gz file %q: %w", path, err)
	}
	var offset int64
	writeBackupToGz := func() error {
		stmt := queryFunc(offset)
		var rawJSONRows json.RawMessage
		rows, err := worker.dbHandle.Query(stmt)
		if err != nil {
			return fmt.Errorf("error while getting rows: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			err = rows.Scan(&rawJSONRows)
			if err != nil {
				return fmt.Errorf("scanning row failed with error : %w", err)
			}
			rawJSONRows = append(rawJSONRows, '\n') // appending '\n'
			_, err = gzWriter.Write(rawJSONRows)
			if err != nil {
				return fmt.Errorf("writing gz file %q: %w", path, err)
			}
			offset++
		}
		return nil
	}

	for {
		if err := writeBackupToGz(); err != nil {
			return err
		}
		if offset >= totalCount {
			break
		}
	}

	if err := gzWriter.CloseGZ(); err != nil {
		return fmt.Errorf("closing gz file %q: %w", path, err)
	}
	tableFileDumpTimeStat.End()
	return nil
}

func (worker *WorkerT) uploadTableDump(ctx context.Context, tablePrefix, pathPrefix, path string) error {
	fileUploadTimeStat := stats.Default.NewTaggedStat("fileUpload_TimeStat", stats.TimerType, stats.Tags{"customVal": tablePrefix})
	fileUploadTimeStat.Start()

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening gz file %q: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	pathPrefixes := make([]string, 0)
	// For empty path prefix, don't need to add anything to the array
	if pathPrefix != "" {
		pathPrefixes = append(pathPrefixes, pathPrefix, config.GetString("INSTANCE_ID", "1"))
	} else {
		pathPrefixes = append(pathPrefixes, config.GetString("INSTANCE_ID", "1"))
	}

	var output filemanager.UploadOutput
	output, err = worker.backupUploadWithExponentialBackoff(ctx, file, pathPrefixes...)
	if err != nil {
		storageProvider := config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
		worker.logger.Errorf("[JobsDB] :: Failed to upload table dump to %s. Error: %s", storageProvider, err.Error())
		return err
	}
	worker.logger.Infof("[JobsDB] :: Backed up table at %v", output.Location)
	fileUploadTimeStat.End()
	return nil
}

func (jd *HandleT) shouldBackUp(storagePreferences backendconfig.StoragePreferences) bool {
	return (storagePreferences.GatewayDumps && jd.tablePrefix == "gw") || (storagePreferences.ProcErrors && jd.tablePrefix == "proc_error")
}

func (worker *WorkerT) backupUploadWithExponentialBackoff(ctx context.Context, file *os.File, pathPrefixes ...string) (filemanager.UploadOutput, error) {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = worker.maxBackupRetryTime
	boRetries := backoff.WithMaxRetries(bo, uint64(config.GetInt64("MAX_BACKOFF_RETRIES", 3)))
	boCtx := backoff.WithContext(boRetries, ctx)

	var output filemanager.UploadOutput
	var err error
	backup := func() error {
		output, err = worker.job.fileUploader.Upload(ctx, file, pathPrefixes...)
		return err
	}

	err = backoff.Retry(backup, boCtx)
	return output, err
}

func (jd *HandleT) getBackupDSRange() *dataSetRangeT {
	var backupDS dataSetT
	var backupDSRange dataSetRangeT

	// Read the table names from PG
	tableNames := mustGetAllTableNames(jd, jd.dbHandle)

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
		return &backupDSRange
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
	row := jd.dbHandle.QueryRow(jobIDSQLStatement)
	err := row.Scan(&minID, &maxID)
	jd.assertError(err)

	var minCreatedAt, maxCreatedAt time.Time
	jobTimeSQLStatement := fmt.Sprintf(`SELECT MIN(created_at), MAX(created_at) from %q`, backupDS.JobTable)
	row = jd.dbHandle.QueryRow(jobTimeSQLStatement)
	err = row.Scan(&minCreatedAt, &maxCreatedAt)
	jd.assertError(err)

	backupDSRange = dataSetRangeT{
		minJobID:  minID.Int64,
		maxJobID:  maxID.Int64,
		startTime: minCreatedAt.UnixNano() / int64(time.Millisecond),
		endTime:   maxCreatedAt.UnixNano() / int64(time.Millisecond),
		ds:        backupDS,
	}
	return &backupDSRange
}
