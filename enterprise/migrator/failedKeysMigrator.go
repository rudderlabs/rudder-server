package migrator

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/enterprise/pathfinder"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type FailedKeysMigratorT struct {
	failedEventsManager router.FailedEventsManagerI
	dbHandle            *sql.DB
	fileManager         filemanager.FileManager
	fromClusterVersion  int
	toClusterVersion    int
	importer            *failedKeysImporterT
	exporter            *failedKeysExporterT
	migrator            MigratorT
	logger              logger.LoggerI
	guardConcurrency    chan struct{}
}

var (
	sourceNodeID    string
	destNodeID      string
	uniqueConstaint string
	maxConcurrency  int
)

type failedKeysExporterT struct {
	logger                          logger.LoggerI
	migrator                        *FailedKeysMigratorT
	statVal                         string
	eventStat                       stats.RudderStats
	pf                              pathfinder.ClusterStateT
	uploadWorkers                   map[string]*fkUploadWorkerT
	uploadWorkersLock               sync.RWMutex
	nonExportedJobsCountByTaskRunID map[string]int64
	doesTRIDHaveJobsToMigrateMap    map[string]bool
	TaskRunIDs                      []string
}

type fkUploadWorkerT struct {
	channel   chan []*router.FailedEventRowT
	taskRunID string
}

func NewFailedKeysMigrator(mode string, failedEventsManager router.FailedEventsManagerI, pf pathfinder.ClusterStateT) FailedKeysMigratorT {
	failedKeysMigrator := FailedKeysMigratorT{}
	sourceNodeID = misc.GetNodeID()

	uniqueConstaint = fmt.Sprintf(`failed_keys_%d_%d_%s`, GetMigratingFromVersion(), GetMigratingToVersion(), jobsdb.UniqueConstraintSuffix)
	config.RegisterIntConfigVariable(10, &maxConcurrency, false, 1, "Migrator.FailedKeys.MaxConcurrency")
	failedKeysMigrator.guardConcurrency = make(chan struct{}, maxConcurrency)

	failedKeysMigrator.FailedKeysMigratorSetup(failedEventsManager, mode)

	switch mode {
	case db.EXPORT:
		destNodeID = pf.GetNodeFromUserID(sourceNodeID).ID
		failedKeysMigrator.exporter = &failedKeysExporterT{}
		failedKeysMigrator.exporter.TaskRunIDs = failedKeysMigrator.GetTaskRunsIds()
		failedKeysMigrator.exporter.SetupFKExporter(&failedKeysMigrator, pf)
	case db.IMPORT:
		failedKeysMigrator.importer = &failedKeysImporterT{}
		failedKeysMigrator.importer.SetupFKImporter(&failedKeysMigrator)
	}

	return failedKeysMigrator
}

func (failedKeysMigrator *FailedKeysMigratorT) FailedKeysMigratorSetup(failedEventsManager router.FailedEventsManagerI, migrationMode string) {
	failedKeysMigrator.logger = pkgLogger.Child(`failed_keys_migrator`)
	pkgLogger.Info("Migrator: Setting up migrator for Failed Keys")

	failedKeysMigrator.migrator = MigratorT{}
	failedKeysMigrator.migrator.fromClusterVersion = GetMigratingFromVersion()
	failedKeysMigrator.migrator.toClusterVersion = GetMigratingToVersion()
	failedKeysMigrator.fileManager = failedKeysMigrator.migrator.setupFileManager()

	failedKeysMigrator.failedEventsManager = failedEventsManager
	failedKeysMigrator.fromClusterVersion = GetMigratingFromVersion()
	failedKeysMigrator.toClusterVersion = GetMigratingToVersion()
	failedKeysMigrator.dbHandle = failedEventsManager.GetDBHandle()
	failedKeysMigrator.SetupFailedKeyMigrationCheckpointTables(failedKeysMigrator.fromClusterVersion, failedKeysMigrator.toClusterVersion)
}

func (migrator *FailedKeysMigratorT) SetupFailedKeyMigrationCheckpointTables(fromVersion, toVersion int) {
	tableName := fmt.Sprintf(`failed_keys_%d_%d_%s`, fromVersion, toVersion, jobsdb.MigrationCheckpointSuffix)
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			migration_type TEXT NOT NULL,
		from_node TEXT NOT NULL,
		to_node TEXT NOT NULL,
		jobs_count BIGINT NOT NULL,
		file_location TEXT,
		status TEXT,
		task_run_id TEXT NOT NULL,
		time_stamp TIMESTAMP NOT NULL DEFAULT NOW(),
		CONSTRAINT %s UNIQUE(migration_type, from_node, to_node, file_location)
		);`, tableName, uniqueConstaint)

	_, err := migrator.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
	pkgLogger.Infof("[[ failed_keys-migrator ]] %s table created", tableName)
}

func (migrator *FailedKeysMigratorT) GetTaskRunsIds() []string {
	var TRIDs []string

	stmt, err := migrator.dbHandle.Prepare(`select tablename from pg_catalog.pg_tables where tablename like 'failed_keys_%';`)
	if err != nil {
		panic(err)
	}
	rows, err := stmt.Query()
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var tbName string
		err = rows.Scan(&tbName)
		if err != nil {
			panic(err)
		}
		if !strings.HasSuffix(tbName, jobsdb.MigrationCheckpointSuffix) {
			TRIDs = append(TRIDs, tbName[len(`failed_keys_`):])
		}
	}

	return TRIDs
}

func (migrator *FailedKeysMigratorT) getURI(uri string) string {
	return fmt.Sprintf("/%s%s", `failed_keys`, uri)
}

func (migrator *FailedKeysMigratorT) GetMigrationCheckpoints(migrationType jobsdb.MigrationOp, status string) map[string][]jobsdb.MigrationCheckpointT {
	checkpoints := make(map[string][]jobsdb.MigrationCheckpointT)

	tableName := fmt.Sprintf(`failed_keys_%d_%d_%s`, migrator.fromClusterVersion, migrator.toClusterVersion, jobsdb.MigrationCheckpointSuffix)
	queryString := fmt.Sprintf(`select * from %[1]s where migration_type = $1 and status = '%[2]s'`, tableName, status)
	stmt, err := migrator.dbHandle.Prepare(queryString)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(migrationType)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		migrationCheckpoint := jobsdb.MigrationCheckpointT{}
		var taskRunID string
		err = rows.Scan(&migrationCheckpoint.ID, &migrationCheckpoint.MigrationType, &migrationCheckpoint.FromNode,
			&migrationCheckpoint.ToNode, &migrationCheckpoint.JobsCount, &migrationCheckpoint.FileLocation, &migrationCheckpoint.Status,
			&taskRunID, &migrationCheckpoint.TimeStamp)
		if err != nil {
			panic(err)
		}
		if _, ok := checkpoints[taskRunID]; !ok {
			checkpoints[taskRunID] = make([]jobsdb.MigrationCheckpointT, 0)
		}
		checkpoints[taskRunID] = append(checkpoints[taskRunID], migrationCheckpoint)
	}

	return checkpoints
}

func (migrator *FailedKeysMigratorT) checkpoint(checkpoint jobsdb.MigrationCheckpointT, trID string, txn *sql.Tx) {
	var sqlStatement string
	// var checkpointType string
	tableName := fmt.Sprintf(`failed_keys_%d_%d_%s`, migrator.fromClusterVersion, migrator.toClusterVersion, jobsdb.MigrationCheckpointSuffix)

	// checking if a checkpoint exists for a (task_run_id and from_node) combination -> so an importing pod can get notifications for a trID from more than one exporting_pod.
	checkForCheckpointStatement := fmt.Sprintf(`select exists(select * from %[1]s where task_run_id='%[2]s' and from_node='%[3]s')`, tableName, trID, checkpoint.FromNode)
	stmt, err := txn.Prepare(checkForCheckpointStatement)
	if err != nil {
		txn.Rollback()
		panic(err)
	}
	existsQueryResult := stmt.QueryRow()
	if err != nil {
		panic(err)
	}
	var exists bool
	err = existsQueryResult.Scan(&exists)
	if err != nil {
		panic(err)
	}

	if exists {
		sqlStatement = fmt.Sprintf(`UPDATE %s SET status = $1 WHERE id = $2 RETURNING id`, tableName)
	} else {
		sqlStatement = fmt.Sprintf(`INSERT INTO %s (migration_type, from_node, to_node, jobs_count, file_location, status, task_run_id, time_stamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT ON CONSTRAINT %s DO UPDATE SET status=EXCLUDED.status RETURNING id`, tableName, uniqueConstaint)
	}

	migrator.logger.Debugf("[[ failed_keys-migrator ]] Checkpointing with query : %s with migrationCheckpoint %+v", sqlStatement, checkpoint)

	stmt, err = txn.Prepare(sqlStatement)
	if err != nil {
		txn.Rollback()
		panic(err)
	}
	if exists {
		_, err = stmt.Exec(checkpoint.Status, checkpoint.ID)
	} else {
		_, err = stmt.Exec(checkpoint.MigrationType,
			checkpoint.FromNode,
			checkpoint.ToNode,
			checkpoint.JobsCount,
			checkpoint.FileLocation,
			checkpoint.Status,
			trID,
			time.Now(),
		)
	}
	if err != nil {
		panic(err)
	}
}

/*
	EXPORT
*/

func (exp *failedKeysExporterT) exportStatusHandler() bool {
	return exp.isExportDone()
}

func (exp *failedKeysExporterT) SetupFKExporter(migrator *FailedKeysMigratorT, pf pathfinder.ClusterStateT) {
	exp.logger = pkgLogger.Child("export")
	exp.crashRecover()
	exp.migrator = migrator
	exp.statVal = fmt.Sprintf("%s-exporter", "failed_keys")
	exp.eventStat = stats.NewStat("fk_export_events", stats.GaugeType)

	exp.pf = pf

	exp.uploadWorkers = make(map[string]*fkUploadWorkerT)
	exp.uploadWorkersLock = sync.RWMutex{}

	exp.nonExportedJobsCountByTaskRunID = make(map[string]int64)
	exp.doesTRIDHaveJobsToMigrateMap = make(map[string]bool)

	rruntime.Go(func() {
		exp.export()
	})
}

func (exp *failedKeysExporterT) export() {
	queryStat := stats.NewTaggedStat("export_main", stats.TimerType, stats.Tags{"migrationType": exp.statVal})
	queryStat.Start()
	defer queryStat.End()

	if exp.isExportDone() {
		return
	}

	// readfromcheckpointAndNotify
	rruntime.Go(func() {
		exp.readFromCheckpointAndNotify()
	})

	exp.eventStat.Gauge(1)
	exp.logger.Info("[[ failed_keys-Export-migrator ]] export loop is starting")
	exportedCheckpointsForTRID := exp.migrator.GetMigrationCheckpoints(jobsdb.ExportOp, jobsdb.Exported)
	notifiedCheckpointsForTRID := exp.migrator.GetMigrationCheckpoints(jobsdb.ExportOp, jobsdb.Notified)
	for _, trID := range exp.TaskRunIDs {
		// this check is done so that if the pod restarts, we don't want upload process to repeat again
		if len(exportedCheckpointsForTRID[trID])+len(notifiedCheckpointsForTRID[trID]) <= 0 {
			taskRunID := trID
			rruntime.Go(func() {
				exp.fetchAndDump(taskRunID)
			})
		}
	}
}

func (exp *failedKeysExporterT) readFromCheckpointAndNotify() {
	notifiedCheckpoints := make(map[string]map[int64]jobsdb.MigrationCheckpointT)
	destURL, err := exp.pf.GetConnectionStringForNodeID(destNodeID)
	if err != nil {
		panic(err)
	}
	for {
		time.Sleep(workLoopSleepDuration)
		checkpoints := exp.migrator.GetMigrationCheckpoints(jobsdb.ExportOp, jobsdb.Exported)
		for trID, taskCheckpoints := range checkpoints {
			if _, ok := notifiedCheckpoints[trID]; !ok {
				notifiedCheckpoints[trID] = make(map[int64]jobsdb.MigrationCheckpointT)
			}
			for _, checkpoint := range taskCheckpoints {
				_, found := notifiedCheckpoints[trID][checkpoint.ID]
				if !found {
					_, statusCode, err := misc.MakeRetryablePostRequest(destURL, exp.migrator.getURI(notificationURI), checkpoint)
					if err == nil && statusCode == 200 {
						exp.logger.Infof("[[ failed_keys-Export-migrator ]] Notified destination node %s to download and import file from %s.", checkpoint.ToNode, checkpoint.FileLocation)
						exp.notifyCheckpoint(checkpoint, trID)
						notifiedCheckpoints[trID][checkpoint.ID] = checkpoint
					}
					exp.logger.Errorf("[[ failed_keys-Export-migrator ]] Failed to Notify: %s, Checkpoint: %+v, Error: %v, status: %d", destURL, checkpoint, err, statusCode)
				}
			}
		}
		if exp.isExportDone() {
			break
		}
	}
	exp.eventStat.Gauge(1)
}

func (exp *failedKeysExporterT) notifyCheckpoint(checkpoint jobsdb.MigrationCheckpointT, trID string) {
	checkpoint.Status = jobsdb.Notified
	txn, err := exp.migrator.dbHandle.Begin()
	if err != nil {
		panic(err)
	}
	exp.migrator.checkpoint(checkpoint, trID, txn)
	txn.Commit()
}

func (exp *failedKeysExporterT) fetchAndDump(trID string) {
	failedKeys := exp.migrator.failedEventsManager.FetchFailedRecordIDs(trID)
	exp.logger.Infof("[[ failed_keys-Export-migrator ]] Received failed keys for node:%s taskRunID:%s to be written to file and upload it", destNodeID, trID)

	dumpChannel := exp.getDumpChannelForTask(trID)
	dumpChannel <- failedKeys
}

func (exp *failedKeysExporterT) crashRecover() {
	// Cleaning up residue files in localExportTmpDirName folder.
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	os.RemoveAll(tmpDirPath + localExportTmpDirName)
}

func (exp *failedKeysExporterT) upload(exportFilePath string) filemanager.UploadOutput {
	queryStat := stats.NewTaggedStat("upload_time", stats.TimerType, stats.Tags{"migrationType": exp.statVal})
	queryStat.Start()
	defer queryStat.End()

	file, err := os.Open(exportFilePath)
	if err != nil {
		panic(err)
	}
	var uploadOutput filemanager.UploadOutput

	operation := func() error {
		var uploadError error
		uploadOutput, uploadError = exp.migrator.fileManager.Upload(context.TODO(), file)
		return uploadError
	}

	for {
		err := backoff.RetryNotify(operation, backoff.NewExponentialBackOff(), func(err error, t time.Duration) {
			exp.logger.Errorf("[[ failed_keys-Export-migrator ]] Failed to upload file: %s with error: %s, retrying after %v",
				file.Name(),
				err.Error(),
				t)
		})
		if err == nil {
			break
		}
		exp.logger.Errorf("[[ failed_keys-Export-migrator ]] Failed to export file to %s", uploadOutput.Location)
	}

	err = file.Close()
	if err != nil {
		panic(err)
	}
	exp.logger.Infof("[[ failed_keys-Export-migrator ]] Uploaded an export file to %s", uploadOutput.Location)
	return uploadOutput
}

func (exp *failedKeysExporterT) updateCheckpoint(uploadOutput filemanager.UploadOutput, jobsCount int, trID string) {
	txn, err := exp.migrator.dbHandle.Begin()
	if err != nil {
		panic(err)
	}
	migrationCheckpoint := jobsdb.NewMigrationCheckpoint(jobsdb.ExportOp, misc.GetNodeID(), destNodeID, int64(jobsCount), uploadOutput.Location, jobsdb.Exported, 0)
	exp.migrator.checkpoint(migrationCheckpoint, trID, txn)
	txn.Commit()
}

func (exp *failedKeysExporterT) isExportDone() bool {
	migrationStates := exp.migrator.GetMigrationCheckpoints(jobsdb.ExportOp, jobsdb.Notified)
	totalStates := make([]jobsdb.MigrationCheckpointT, 0)
	for _, state := range migrationStates {
		totalStates = append(totalStates, state...)
	}
	return len(totalStates) == len(exp.TaskRunIDs)
}

func (exp *failedKeysExporterT) getDumpChannelForTask(trID string) chan []*router.FailedEventRowT {
	exp.migrator.guardConcurrency <- struct{}{}
	if _, ok := exp.uploadWorkers[trID]; !ok {
		channel := make(chan []*router.FailedEventRowT, 1)
		uploadWorker := &fkUploadWorkerT{
			channel:   channel,
			taskRunID: trID,
		}
		exp.uploadWorkersLock.Lock()
		exp.uploadWorkers[trID] = uploadWorker
		exp.uploadWorkersLock.Unlock()
		rruntime.Go(func() {
			exp.uploadWorkerProcess(uploadWorker)
		})
	}
	return exp.uploadWorkers[trID].channel
}

func (exp *failedKeysExporterT) uploadWorkerProcess(uploadWorkerT *fkUploadWorkerT) {
	waitStat := stats.NewTaggedStat("upload_worker_wait_time", stats.TimerType, stats.Tags{"migrationType": exp.statVal})
	waitStat.Start()
	failedKeys := <-uploadWorkerT.channel
	waitStat.End()

	// add queryStats
	contentSlice := make([][]byte, len(failedKeys))
	for idx, record := range failedKeys {
		m, err := json.Marshal(record)
		if err != nil {
			panic(err)
		}

		contentSlice[idx] = m
	}

	content := bytes.Join(contentSlice[:], []byte("\n"))

	// fileName only contains the taskRunID because anyway the tables are shifted to one node only
	// and all the records can be aggregated together.
	exportFileName := fmt.Sprintf(`%s_%s_%s.gz`,
		misc.GetNodeID(),
		`failed_keys`,
		uploadWorkerT.taskRunID,
	)

	exportFilePath := writeContentToFile(content, exportFileName)
	uploadOutput := exp.upload(exportFilePath)
	// checkpoint update with uploadoutput.Location
	exp.updateCheckpoint(uploadOutput, len(failedKeys), uploadWorkerT.taskRunID)

	os.Remove(exportFilePath)
	<-exp.migrator.guardConcurrency
}

/*
	Import
*/

type failedKeysImporterT struct {
	logger          logger.LoggerI
	migrator        *FailedKeysMigratorT
	statVal         string
	eventStat       stats.RudderStats
	importWaitGroup sync.WaitGroup
}

func (imp *failedKeysImporterT) SetupFKImporter(migrator *FailedKeysMigratorT) {
	imp.logger = pkgLogger.Child("fkimport")
	imp.crashRecover()
	imp.migrator = migrator
	imp.statVal = `failed_keys-importer`
	imp.eventStat = stats.NewStat("import-events", stats.GaugeType)
	imp.logger.Info(`[[ failed_keys-Import-Migrator ]] setup`)
	imp.importWaitGroup = sync.WaitGroup{}

	rruntime.Go(func() {
		imp.readFromCheckpointAndTriggerImport()
	})
}

func (imp *failedKeysImporterT) crashRecover() {
	// Cleaning up residue files in `localImportTmpDirName` folder.
	tmpDirPath, _ := misc.CreateTMPDIR()
	os.RemoveAll(tmpDirPath + localImportTmpDirName)
}

func (imp *failedKeysImporterT) importHandler(migrationCheckpoint jobsdb.MigrationCheckpointT) error {
	queryStat := stats.NewTaggedStat("import_notify_latency", stats.TimerType, stats.Tags{"migrationType": imp.statVal})
	queryStat.Start()
	defer queryStat.End()

	imp.logger.Infof("[[ %s-Import-migrator ]] Request received to import %s", `failed_keys`, migrationCheckpoint.FileLocation)
	if migrationCheckpoint.MigrationType != jobsdb.ExportOp {
		errorString := fmt.Sprintf("[[ failed_keys-Import-migrator ]] Wrong migration event received. Only export type events are expected. migrationType: %s, migrationCheckpoint: %v", migrationCheckpoint.MigrationType, migrationCheckpoint)
		imp.logger.Error(errorString)
		return errors.New(errorString)
	}

	if migrationCheckpoint.ToNode != misc.GetNodeID() {
		errorString := fmt.Sprintf("[[ failed_keys-Import-migrator ]] Wrong migration event received. Supposed to be sent to %s. Received by %s instead, migrationCheckpoint: %v", migrationCheckpoint.ToNode, misc.GetNodeID(), migrationCheckpoint)
		imp.logger.Error(errorString)
		return errors.New(errorString)
	}

	migrationCheckpoint.MigrationType = jobsdb.ImportOp
	migrationCheckpoint.ID = 0
	migrationCheckpoint.Status = jobsdb.PreparedForImport
	migrationCheckpoint.TimeStamp = time.Now()

	// get TaskRunID
	fileLocation := migrationCheckpoint.FileLocation
	idx := strings.Index(fileLocation, `failed_keys_`)
	trID := fileLocation[idx+len(`failed_keys_`) : len(fileLocation)-3]

	txn, err := imp.migrator.dbHandle.Begin()
	if err != nil {
		panic(err)
	}
	imp.migrator.checkpoint(migrationCheckpoint, trID, txn)
	txn.Commit()

	imp.eventStat.Gauge(2)
	imp.logger.Debug("[[ failed_keys-Import-migrator ]] Ack: %v", migrationCheckpoint)
	return nil
}

func (imp *failedKeysImporterT) readFromCheckpointAndTriggerImport() {
	importTriggeredCheckpoints := make(map[string]map[int64]jobsdb.MigrationCheckpointT)
	for {
		time.Sleep(workLoopSleepDuration)

		checkpoints := imp.migrator.GetMigrationCheckpoints(jobsdb.ImportOp, jobsdb.PreparedForImport)
		for taskRunID, trcheckpoints := range checkpoints {
			_, trFound := importTriggeredCheckpoints[taskRunID]
			if !trFound {
				importTriggeredCheckpoints[taskRunID] = make(map[int64]jobsdb.MigrationCheckpointT)
			}
			for _, trcheckpoint := range trcheckpoints {
				_, found := importTriggeredCheckpoints[taskRunID][trcheckpoint.ID]
				if !found {
					trID := taskRunID
					importCheckpoint := trcheckpoint
					imp.migrator.guardConcurrency <- struct{}{}
					rruntime.Go(func() {
						imp.importWaitGroup.Add(1)
						imp.importProcess(trID, importCheckpoint)
					})
					importTriggeredCheckpoints[taskRunID][trcheckpoint.ID] = trcheckpoint
				}
			}
		}
		imp.importWaitGroup.Wait()
	}
}

func (imp *failedKeysImporterT) importProcess(trID string, checkpoint jobsdb.MigrationCheckpointT) {
	jsonPath := imp.download(checkpoint.FileLocation)

	recordsList, err := imp.getRecordsListFromFile(jsonPath)
	if err != nil {
		panic(err)
	}
	imp.storeRecordsAndCheckpoint(recordsList, checkpoint, trID)
	imp.logger.Infof("[[ failed_keys-Import-migrator ]] Done importing file %s", jsonPath)

	os.Remove(jsonPath)
	imp.importWaitGroup.Done()
	<-imp.migrator.guardConcurrency
}

func (imp *failedKeysImporterT) storeRecordsAndCheckpoint(recordsList []*router.FailedEventRowT, checkpoint jobsdb.MigrationCheckpointT, trID string) {
	recordsMap := make(map[string][]*router.FailedEventRowT)
	recordsMap[trID] = recordsList

	txn, err := imp.migrator.dbHandle.Begin()
	if err != nil {
		panic(err)
	}
	imp.migrator.failedEventsManager.SaveFailedRecordIDs(recordsMap, txn)
	imp.logger.Infof(`[[ failed_keys-Import-migrator ]] %d records written to failed_keys_%s table, checkpoint updation and transaction commit pending`, len(recordsList), trID)
	checkpoint.Status = jobsdb.Imported
	imp.migrator.checkpoint(checkpoint, trID, txn)
	imp.logger.Infof(`[[ failed_keys-Import-migrator ]] import checkpoint: %d updated`, checkpoint.ID)
	txn.Commit()
	imp.logger.Infof(`[[ failed_keys-Import-migrator ]] import transaction complete for TaskRunID: %s`, trID)
}

func (imp *failedKeysImporterT) download(fileLocation string) string {
	queryStat := stats.NewTaggedStat("download_time", stats.TimerType, stats.Tags{"migrationType": imp.statVal})
	queryStat.Start()
	defer queryStat.End()

	tmpDirPath, err := misc.CreateTMPDIR()

	filePathSlice := strings.Split(fileLocation, "/")
	fileName := filePathSlice[len(filePathSlice)-1]
	jsonPath := fmt.Sprintf("%v%v.json", tmpDirPath+localImportTmpDirName, fileName)

	err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	jsonFile, err := os.Create(jsonPath)
	if err != nil {
		panic(err)
	}

	operation := func() error {
		downloadError := imp.migrator.fileManager.Download(context.TODO(), jsonFile, imp.migrator.fileManager.GetDownloadKeyFromFileLocation(fileLocation))
		return downloadError
	}

	for {
		err := backoff.Retry(operation, backoff.NewExponentialBackOff())
		if err == nil {
			break
		}
		imp.logger.Errorf("[[ failed_keys-Import-migrator ]] Failed to download file %s", fileLocation)
	}

	jsonFile.Close()
	imp.logger.Infof("[[ failed_keys-Import-migrator ]] Downloaded an import file %s", fileLocation)

	return jsonPath
}

func (imp *failedKeysImporterT) getRecordsListFromFile(jsonPath string) ([]*router.FailedEventRowT, error) {
	var recordsList []*router.FailedEventRowT
	file, err := os.Open(jsonPath)
	imp.logger.Info(err)
	if err != nil {
		return recordsList, err
	}

	imp.logger.Infof("[[ failed_keys-Import-migrator ]] Parsing the file:%s for import and passing it to jobsDb", jsonPath)

	reader, err := gzip.NewReader(file)
	if err != nil {
		imp.logger.Errorf(`[[ failed_keys-Import-migrator ]] %s`, err.Error())
		return recordsList, err
	}

	sc := bufio.NewScanner(reader)

	maxCapacity := 1024 * 1024
	buf := make([]byte, maxCapacity)
	sc.Buffer(buf, maxCapacity)

	for sc.Scan() {
		lineBytes := sc.Bytes()
		record, err := imp.processSingleLine(lineBytes)
		imp.logger.Error(err)
		if err != nil {
			return recordsList, err
		}
		recordsList = append(recordsList, &record)
	}

	reader.Close()
	file.Close()
	return recordsList, sc.Err()
}

func (imp *failedKeysImporterT) processSingleLine(line []byte) (router.FailedEventRowT, error) {
	record := router.FailedEventRowT{}
	err := json.Unmarshal(line, &record)
	if err != nil {
		return router.FailedEventRowT{}, err
	}
	return record, nil
}

func (imp *failedKeysImporterT) importStatusHandler() bool {
	return imp.isImportDone()
}

func (imp *failedKeysImporterT) isImportDone() bool {
	checkpointsPreparedForImportMap := imp.migrator.GetMigrationCheckpoints(jobsdb.ImportOp, jobsdb.PreparedForImport)
	var checkpoints []jobsdb.MigrationCheckpointT
	for _, TRcheckpoints := range checkpointsPreparedForImportMap {
		checkpoints = append(checkpoints, TRcheckpoints...)
	}
	return len(checkpoints) == 0
}
