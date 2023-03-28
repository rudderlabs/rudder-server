package warehouse

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"golang.org/x/sync/errgroup"
)

const (
	STATS_WORKER_IDLE_TIME                  = "worker_idle_time"
	STATS_WORKER_CLAIM_PROCESSING_TIME      = "worker_claim_processing_time"
	STATS_WORKER_CLAIM_PROCESSING_FAILED    = "worker_claim_processing_failed"
	STATS_WORKER_CLAIM_PROCESSING_SUCCEEDED = "worker_claim_processing_succeeded"
	TAG_WORKERID                            = "workerId"
)

const (
	WorkerProcessingDownloadStagingFileFailed = "worker_processing_download_staging_file_failed"
)

// JobRun Temporary store for processing staging file to load file
type JobRun struct {
	job                  Payload
	stagingFilePath      string
	uuidTS               time.Time
	outputFileWritersMap map[string]encoding.LoadFileWriter
	tableEventCountMap   map[string]int
	stagingFileReader    *gzip.Reader
	whIdentifier         string
	stats                stats.Stats
}

func (jobRun *JobRun) setStagingFileReader() (reader *gzip.Reader, endOfFile bool) {
	job := jobRun.job
	pkgLogger.Debugf("Starting read from downloaded staging file: %s", job.StagingFileLocation)
	stagingFile, err := os.Open(jobRun.stagingFilePath)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error opening file using os.Open at path:%s downloaded from %s", jobRun.stagingFilePath, job.StagingFileLocation)
		panic(err)
	}
	reader, err = gzip.NewReader(stagingFile)
	if err != nil {
		if err.Error() == "EOF" {
			return nil, true
		}
		pkgLogger.Errorf("[WH]: Error reading file using gzip.NewReader at path:%s downloaded from %s", jobRun.stagingFilePath, job.StagingFileLocation)
		panic(err)
	}

	jobRun.stagingFileReader = reader
	return reader, false
}

/*
 * Get download path for the job. Also creates missing directories for this path
 */
func (jobRun *JobRun) setStagingFileDownloadPath(index int) (filePath string) {
	job := jobRun.job
	dirName := fmt.Sprintf(`/%s/_%s/`, misc.RudderWarehouseJsonUploadsTmp, strconv.Itoa(index))
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to create tmp DIR")
		panic(err)
	}
	filePath = tmpDirPath + dirName + fmt.Sprintf(`%s_%s/`, job.DestinationType, job.DestinationID) + job.StagingFileLocation
	err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	jobRun.stagingFilePath = filePath
	return filePath
}

func (job *Payload) sendDownloadStagingFileFailedStat() {
	tags := []warehouseutils.Tag{
		{
			Name:  "destID",
			Value: job.DestinationID,
		},
		{
			Name:  "destType",
			Value: job.DestinationType,
		},
	}
	warehouseutils.NewCounterStat(WorkerProcessingDownloadStagingFileFailed, tags...).Increment()
}

// Get fileManager
func (job *Payload) getFileManager(config interface{}, useRudderStorage bool) (filemanager.FileManager, error) {
	storageProvider := warehouseutils.ObjectStorageType(job.DestinationType, config, useRudderStorage)
	fileManager, err := filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:                    storageProvider,
			Config:                      config,
			UseRudderStorage:            useRudderStorage,
			RudderStoragePrefixOverride: job.RudderStoragePrefix,
			WorkspaceID:                 job.WorkspaceID,
		}),
	})
	return fileManager, err
}

/*
 * Download Staging file for the job
 * If error occurs with the current config and current revision is different from staging revision
 * We retry with the staging revision config if it is present
 */
func (jobRun *JobRun) downloadStagingFile() error {
	job := jobRun.job
	downloadTask := func(config interface{}, useRudderStorage bool) (err error) {
		filePath := jobRun.stagingFilePath
		file, err := os.Create(filePath)
		if err != nil {
			panic(err)
		}

		downloader, err := job.getFileManager(config, useRudderStorage)
		if err != nil {
			pkgLogger.Errorf("[WH]: Failed to initialize downloader")
			return err
		}

		downloadStart := time.Now()

		err = downloader.Download(context.TODO(), file, job.StagingFileLocation)
		if err != nil {
			pkgLogger.Errorf("[WH]: Failed to download file")
			return err
		}
		file.Close()
		jobRun.timerStat("download_staging_file_time").Since(downloadStart)

		fi, err := os.Stat(filePath)
		if err != nil {
			pkgLogger.Errorf("[WH]: Error getting file size of downloaded staging file: ", err)
			return err
		}
		fileSize := fi.Size()
		pkgLogger.Debugf("[WH]: Downloaded staging file %s size:%v", job.StagingFileLocation, fileSize)
		return
	}

	err := downloadTask(job.DestinationConfig, job.UseRudderStorage)
	if err != nil {
		if PickupStagingConfiguration(&job) {
			pkgLogger.Infof("[WH]: Starting processing staging file with revision config for StagingFileID: %d, DestinationRevisionID: %s, StagingDestinationRevisionID: %s, whIdentifier: %s", job.StagingFileID, job.DestinationRevisionID, job.StagingDestinationRevisionID, jobRun.whIdentifier)
			err = downloadTask(job.StagingDestinationConfig, job.StagingUseRudderStorage)
			if err != nil {
				job.sendDownloadStagingFileFailedStat()
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func PickupStagingConfiguration(job *Payload) bool {
	return job.StagingDestinationRevisionID != job.DestinationRevisionID && job.StagingDestinationConfig != nil
}

func (job *Payload) getDiscardsTable() string {
	return warehouseutils.ToProviderCase(job.DestinationType, warehouseutils.DiscardsTable)
}

func (jobRun *JobRun) getLoadFilePath(tableName string) string {
	job := jobRun.job
	randomness := misc.FastUUID().String()
	return strings.TrimSuffix(jobRun.stagingFilePath, "json.gz") + tableName + fmt.Sprintf(`.%s`, randomness) + fmt.Sprintf(`.%s`, warehouseutils.GetLoadFileFormat(job.DestinationType))
}

func (job *Payload) getColumnName(columnName string) string {
	return warehouseutils.ToProviderCase(job.DestinationType, columnName)
}

type loadFileUploadJob struct {
	tableName  string
	outputFile encoding.LoadFileWriter
}

type loadFileUploadOutput struct {
	TableName             string
	Location              string
	TotalRows             int
	ContentLength         int64
	StagingFileID         int64
	DestinationRevisionID string
	UseRudderStorage      bool
}

func (jobRun *JobRun) uploadLoadFilesToObjectStorage() ([]loadFileUploadOutput, error) {
	job := jobRun.job
	uploader, err := job.getFileManager(job.DestinationConfig, job.UseRudderStorage)
	if err != nil {
		return []loadFileUploadOutput{}, err
	}
	// var loadFileIDs []int64
	var loadFileUploadOutputs []loadFileUploadOutput

	// take the first staging file id in upload
	// TODO: support multiple staging files in one upload
	stagingFileId := jobRun.job.StagingFileID

	loadFileOutputChan := make(chan loadFileUploadOutput, len(jobRun.outputFileWritersMap))
	loadFileUploadTimer := jobRun.timerStat("load_file_upload_time")
	uploadJobChan := make(chan *loadFileUploadJob, len(jobRun.outputFileWritersMap))
	// close chan to avoid memory leak ranging over it
	defer close(uploadJobChan)
	uploadErrorChan := make(chan error, numLoadFileUploadWorkers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < numLoadFileUploadWorkers; i++ {
		go func(ctx context.Context) {
			for uploadJob := range uploadJobChan {
				select {
				case <-ctx.Done():
					pkgLogger.Debugf("context is cancelled, stopped processing load file for staging file ids %s ", stagingFileId)
					return // stop further processing
				default:
					tableName := uploadJob.tableName
					loadFileUploadStart := time.Now()
					uploadOutput, err := jobRun.uploadLoadFileToObjectStorage(uploader, uploadJob.outputFile, tableName)
					if err != nil {
						uploadErrorChan <- err
						return
					}
					loadFileUploadTimer.Since(loadFileUploadStart)
					loadFileStats, err := os.Stat(uploadJob.outputFile.GetLoadFile().Name())
					if err != nil {
						uploadErrorChan <- err
						return
					}
					loadFileOutputChan <- loadFileUploadOutput{
						TableName:             tableName,
						Location:              uploadOutput.Location,
						ContentLength:         loadFileStats.Size(),
						TotalRows:             jobRun.tableEventCountMap[tableName],
						StagingFileID:         stagingFileId,
						DestinationRevisionID: job.DestinationRevisionID,
						UseRudderStorage:      job.UseRudderStorage,
					}
				}
			}
		}(ctx)
	}
	// Create upload jobs
	go func() {
		for tableName, loadFile := range jobRun.outputFileWritersMap {
			uploadJobChan <- &loadFileUploadJob{tableName: tableName, outputFile: loadFile}
		}
	}()

	// Wait for response
	for {
		select {
		case loadFileOutput := <-loadFileOutputChan:
			loadFileUploadOutputs = append(loadFileUploadOutputs, loadFileOutput)
			if len(loadFileUploadOutputs) == len(jobRun.outputFileWritersMap) {
				return loadFileUploadOutputs, nil
			}
		case err := <-uploadErrorChan:
			pkgLogger.Errorf("received error while uploading load file to bucket for staging file ids %s, cancelling the context: err %v", stagingFileId, err)
			return []loadFileUploadOutput{}, err
		case <-time.After(slaveUploadTimeout):
			return []loadFileUploadOutput{}, fmt.Errorf("load files upload timed out for staging file idsx: %v", stagingFileId)
		}
	}
}

func (jobRun *JobRun) uploadLoadFileToObjectStorage(uploader filemanager.FileManager, uploadFile encoding.LoadFileWriter, tableName string) (filemanager.UploadOutput, error) {
	job := jobRun.job
	file, err := os.Open(uploadFile.GetLoadFile().Name()) // opens file in read mode
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to Open File: %s", uploadFile.GetLoadFile().Name())
		return filemanager.UploadOutput{}, err
	}
	defer file.Close()
	pkgLogger.Debugf("[WH]: %s: Uploading load_file to %s for table: %s with staging_file id: %v", job.DestinationType, warehouseutils.ObjectStorageType(job.DestinationType, job.DestinationConfig, job.UseRudderStorage), tableName, job.StagingFileID)
	var uploadLocation filemanager.UploadOutput
	if misc.Contains(warehouseutils.TimeWindowDestinations, job.DestinationType) {
		uploadLocation, err = uploader.Upload(context.TODO(), file, warehouseutils.GetTablePathInObjectStorage(jobRun.job.DestinationNamespace, tableName), job.LoadFilePrefix)
	} else {
		uploadLocation, err = uploader.Upload(context.TODO(), file, config.GetString("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects"), tableName, job.SourceID, getBucketFolder(job.UniqueLoadGenID, tableName))
	}
	return uploadLocation, err
}

// Sort columns per table to maintain same order in load file (needed in case of csv load file)
func (job *Payload) getSortedColumnMapForAllTables() map[string][]string {
	sortedTableColumnMap := make(map[string][]string)

	for tableName, columnMap := range job.UploadSchema {
		sortedTableColumnMap[tableName] = []string{}
		for k := range columnMap {
			sortedTableColumnMap[tableName] = append(sortedTableColumnMap[tableName], k)
		}
		sort.Strings(sortedTableColumnMap[tableName])
	}
	return sortedTableColumnMap
}

func (jobRun *JobRun) GetWriter(tableName string) (encoding.LoadFileWriter, error) {
	writer, ok := jobRun.outputFileWritersMap[tableName]
	if !ok {
		var err error
		outputFilePath := jobRun.getLoadFilePath(tableName)
		if jobRun.job.LoadFileType == warehouseutils.LOAD_FILE_TYPE_PARQUET {
			writer, err = encoding.CreateParquetWriter(jobRun.job.UploadSchema[tableName], outputFilePath, jobRun.job.DestinationType)
		} else {
			writer, err = misc.CreateGZ(outputFilePath)
		}
		if err != nil {
			return nil, err
		}
		jobRun.outputFileWritersMap[tableName] = writer
		jobRun.tableEventCountMap[tableName] = 0
	}
	return writer, nil
}

func (jobRun *JobRun) cleanup() {
	if jobRun.stagingFileReader != nil {
		err := jobRun.stagingFileReader.Close()
		if err != nil {
			pkgLogger.Errorf("[WH]: Failed to close staging file: %v", err)
		}
	}

	if jobRun.stagingFilePath != "" {
		misc.RemoveFilePaths(jobRun.stagingFilePath)
	}
	if jobRun.outputFileWritersMap != nil {
		for _, writer := range jobRun.outputFileWritersMap {
			misc.RemoveFilePaths(writer.GetLoadFile().Name())
		}
	}
}

func (event *BatchRouterEvent) GetColumnInfo(columnName string) (columnInfo warehouseutils.ColumnInfo, ok bool) {
	columnVal, ok := event.Data[columnName]
	if !ok {
		return warehouseutils.ColumnInfo{}, false
	}
	columnType, ok := event.Metadata.Columns[columnName]
	if !ok {
		return warehouseutils.ColumnInfo{}, false
	}
	return warehouseutils.ColumnInfo{Value: columnVal, Type: columnType}, true
}

//
// This function is triggered when warehouse-master creates a new entry in wh_uploads table
// This is executed in the context of the warehouse-slave/worker and does the following:
//
// 1. Download the Staging file into a tmp directory
// 2. Transform the staging file into multiple load files (One file per output table)
// 3. Uploads these load files to Object storage
// 4. Save entries for the generated load files in wh_load_files table
// 5. Delete the staging and load files from tmp directory
//

func processStagingFile(job Payload, workerIndex int) (loadFileUploadOutputs []loadFileUploadOutput, err error) {
	processStartTime := time.Now()
	jobRun := JobRun{
		job:          job,
		whIdentifier: warehouseutils.GetWarehouseIdentifier(job.DestinationType, job.SourceID, job.DestinationID),
		stats:        stats.Default,
	}

	defer jobRun.counterStat("staging_files_processed", Tag{Name: "worker_id", Value: strconv.Itoa(workerIndex)}).Count(1)
	defer func() {
		jobRun.timerStat("staging_files_total_processing_time", Tag{Name: "worker_id", Value: strconv.Itoa(workerIndex)}).Since(processStartTime)
	}()
	defer jobRun.cleanup()

	pkgLogger.Debugf("[WH]: Starting processing staging file: %v at %s for %s", job.StagingFileID, job.StagingFileLocation, jobRun.whIdentifier)

	jobRun.setStagingFileDownloadPath(workerIndex)

	// This creates the file, so on successful creation remove it
	err = jobRun.downloadStagingFile()
	if err != nil {
		return loadFileUploadOutputs, err
	}

	sortedTableColumnMap := job.getSortedColumnMapForAllTables()

	reader, endOfFile := jobRun.setStagingFileReader()
	if endOfFile {
		// If empty file, return nothing
		return loadFileUploadOutputs, nil
	}
	scanner := bufio.NewScanner(reader)
	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := maxStagingFileReadBufferCapacityInK * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	// read from staging file and write a separate load file for each table in warehouse
	jobRun.outputFileWritersMap = make(map[string]encoding.LoadFileWriter)
	jobRun.tableEventCountMap = make(map[string]int)
	jobRun.uuidTS = timeutil.Now()

	// Initialize Discards Table
	discardsTable := job.getDiscardsTable()
	jobRun.tableEventCountMap[discardsTable] = 0

	processingStart := time.Now()

	lineBytesCounter := 0
	var interfaceSliceSample []interface{}
	for {
		ok := scanner.Scan()
		if !ok {
			scanErr := scanner.Err()
			if scanErr != nil {
				pkgLogger.Errorf("WH: Error in scanner reading line from staging file: %v", scanErr)
			}
			break
		}

		lineBytes := scanner.Bytes()
		lineBytesCounter += len(lineBytes)
		var batchRouterEvent BatchRouterEvent
		err := json.Unmarshal(lineBytes, &batchRouterEvent)
		if err != nil {
			pkgLogger.Errorf("[WH]: Failed to unmarshal JSON line to batchrouter event: %+v", batchRouterEvent)
			continue
		}

		tableName := batchRouterEvent.Metadata.Table
		columnData := batchRouterEvent.Data

		if job.DestinationType == warehouseutils.S3_DATALAKE && len(sortedTableColumnMap[tableName]) > columnCountLimitMap[warehouseutils.S3_DATALAKE] {
			pkgLogger.Errorf("[WH]: Huge staging file columns : columns in upload schema: %v for StagingFileID: %v", len(sortedTableColumnMap[tableName]), job.StagingFileID)
			return nil, fmt.Errorf("staging file schema limit exceeded for stagingFileID: %d, actualCount: %d", job.StagingFileID, len(sortedTableColumnMap[tableName]))
		}

		// Create separate load file for each table
		writer, err := jobRun.GetWriter(tableName)
		if err != nil {
			return nil, err
		}

		eventLoader := encoding.GetNewEventLoader(job.DestinationType, job.LoadFileType, writer)
		for _, columnName := range sortedTableColumnMap[tableName] {
			if eventLoader.IsLoadTimeColumn(columnName) {
				timestampFormat := eventLoader.GetLoadTimeFormat(columnName)
				eventLoader.AddColumn(job.getColumnName(columnName), job.UploadSchema[tableName][columnName], jobRun.uuidTS.Format(timestampFormat))
				continue
			}
			columnInfo, ok := batchRouterEvent.GetColumnInfo(columnName)
			if !ok {
				eventLoader.AddEmptyColumn(columnName)
				continue
			}
			columnType := columnInfo.Type
			columnVal := columnInfo.Value

			if job.DestinationType == warehouseutils.CLICKHOUSE {
				switch columnType {
				case "boolean":
					newColumnVal := 0
					if k, ok := columnVal.(bool); ok {
						if k {
							newColumnVal = 1
						}
					}
					columnVal = newColumnVal
				case "array(boolean)":
					if boolValue, ok := columnVal.([]interface{}); ok {
						newColumnVal := make([]interface{}, len(boolValue))

						for i, value := range boolValue {
							if k, v := value.(bool); k && v {
								newColumnVal[i] = 1
							} else {
								newColumnVal[i] = 0
							}
						}

						columnVal = newColumnVal
					}
				}
			}

			if model.SchemaType(columnType) == model.IntDataType || model.SchemaType(columnType) == model.BigIntDataType {
				floatVal, ok := columnVal.(float64)
				if !ok {
					eventLoader.AddEmptyColumn(columnName)
					continue
				}
				columnVal = int(floatVal)
			}

			dataTypeInSchema, ok := job.UploadSchema[tableName][columnName]
			violatedConstraints := ViolatedConstraints(job.DestinationType, &batchRouterEvent, columnName)
			if ok && ((columnType != dataTypeInSchema) || (violatedConstraints.IsViolated)) {
				newColumnVal, convError := HandleSchemaChange(
					model.SchemaType(dataTypeInSchema),
					model.SchemaType(columnType),
					columnVal,
				)
				if convError != nil || violatedConstraints.IsViolated {
					if violatedConstraints.IsViolated {
						eventLoader.AddColumn(columnName, job.UploadSchema[tableName][columnName], violatedConstraints.ViolatedIdentifier)
					} else {
						eventLoader.AddEmptyColumn(columnName)
					}

					discardWriter, err := jobRun.GetWriter(discardsTable)
					if err != nil {
						return nil, err
					}
					// add discardWriter to outputFileWritersMap
					jobRun.outputFileWritersMap[discardsTable] = discardWriter

					err = jobRun.handleDiscardTypes(tableName, columnName, columnVal, columnData, violatedConstraints, discardWriter)

					if err != nil {
						pkgLogger.Errorf("[WH]: Failed to write to discards: %v", err)
					}
					jobRun.tableEventCountMap[discardsTable]++
					continue
				}
				columnVal = newColumnVal
			}

			// Special handling for JSON arrays
			// TODO: Will this work for both BQ and RS?
			if reflect.TypeOf(columnVal) == reflect.TypeOf(interfaceSliceSample) {
				marshalledVal, err := json.Marshal(columnVal)
				if err != nil {
					pkgLogger.Errorf("[WH]: Error in marshalling []interface{} columnVal: %v", err)
					eventLoader.AddEmptyColumn(columnName)
					continue
				}
				columnVal = string(marshalledVal)
			}

			eventLoader.AddColumn(columnName, job.UploadSchema[tableName][columnName], columnVal)
		}

		// Completed parsing all columns, write single event to the file
		err = eventLoader.Write()
		if err != nil {
			pkgLogger.Errorf("[WH]: Failed to write event: %v", err)
			return loadFileUploadOutputs, err
		}
		jobRun.tableEventCountMap[tableName]++
	}
	jobRun.timerStat("process_staging_file_time").Since(processingStart)

	pkgLogger.Debugf("[WH]: Process %v bytes from downloaded staging file: %s", lineBytesCounter, job.StagingFileLocation)
	jobRun.counterStat("bytes_processed_in_staging_file").Count(lineBytesCounter)
	for _, loadFile := range jobRun.outputFileWritersMap {
		err = loadFile.Close()
		if err != nil {
			pkgLogger.Errorf("Error while closing load file %s : %v", loadFile.GetLoadFile().Name(), err)
		}
	}
	loadFileUploadOutputs, err = jobRun.uploadLoadFilesToObjectStorage()
	return loadFileUploadOutputs, err
}

func processClaimedUploadJob(claimedJob pgnotifier.Claim, workerIndex int) {
	claimProcessTimeStart := time.Now()
	defer func() {
		warehouseutils.NewTimerStat(STATS_WORKER_CLAIM_PROCESSING_TIME, warehouseutils.Tag{Name: TAG_WORKERID, Value: fmt.Sprintf("%d", workerIndex)}).Since(claimProcessTimeStart)
	}()
	handleErr := func(err error, claim pgnotifier.Claim) {
		pkgLogger.Errorf("[WH]: Error processing claim: %v", err)
		response := pgnotifier.ClaimResponse{
			Err: err,
		}
		warehouseutils.NewCounterStat(STATS_WORKER_CLAIM_PROCESSING_FAILED, warehouseutils.Tag{Name: TAG_WORKERID, Value: strconv.Itoa(workerIndex)}).Increment()
		notifier.UpdateClaimedEvent(&claimedJob, &response)
	}

	var job Payload
	err := json.Unmarshal(claimedJob.Payload, &job)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}
	job.BatchID = claimedJob.BatchID
	pkgLogger.Infof(`Starting processing staging-file:%v from claim:%v`, job.StagingFileID, claimedJob.ID)
	loadFileOutputs, err := processStagingFile(job, workerIndex)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}
	job.Output = loadFileOutputs
	output, err := json.Marshal(job)
	response := pgnotifier.ClaimResponse{
		Err:     err,
		Payload: output,
	}
	if response.Err != nil {
		warehouseutils.NewCounterStat(STATS_WORKER_CLAIM_PROCESSING_FAILED, warehouseutils.Tag{Name: TAG_WORKERID, Value: strconv.Itoa(workerIndex)}).Increment()
	} else {
		warehouseutils.NewCounterStat(STATS_WORKER_CLAIM_PROCESSING_SUCCEEDED, warehouseutils.Tag{Name: TAG_WORKERID, Value: strconv.Itoa(workerIndex)}).Increment()
	}
	notifier.UpdateClaimedEvent(&claimedJob, &response)
}

type AsyncJobRunResult struct {
	Result bool
	Id     string
}

func runAsyncJob(asyncjob jobs.AsyncJobPayload) (AsyncJobRunResult, error) {
	warehouse, err := getDestinationFromConnectionMap(asyncjob.DestinationID, asyncjob.SourceID)
	if err != nil {
		return AsyncJobRunResult{Id: asyncjob.Id, Result: false}, err
	}
	destType := warehouse.Destination.DestinationDefinition.Name
	whManager, err := manager.NewWarehouseOperations(destType)
	if err != nil {
		return AsyncJobRunResult{Id: asyncjob.Id, Result: false}, err
	}
	whasyncjob := &jobs.WhAsyncJob{}

	var metadata warehouseutils.DeleteByMetaData
	err = json.Unmarshal(asyncjob.MetaData, &metadata)
	if err != nil {
		return AsyncJobRunResult{Id: asyncjob.Id, Result: false}, err
	}
	whManager.Setup(warehouse, whasyncjob)
	defer whManager.Cleanup()
	tableNames := []string{asyncjob.TableName}
	if asyncjob.AsyncJobType == "deletebyjobrunid" {
		pkgLogger.Info("[WH-Jobs]: Running DeleteByJobRunID on slave worker")

		params := warehouseutils.DeleteByParams{
			SourceId:  asyncjob.SourceID,
			TaskRunId: metadata.TaskRunId,
			JobRunId:  metadata.JobRunId,
			StartTime: metadata.StartTime,
		}
		err = whManager.DeleteBy(tableNames, params)
	}
	asyncJobRunResult := AsyncJobRunResult{
		Result: err == nil,
		Id:     asyncjob.Id,
	}
	return asyncJobRunResult, err
}

func processClaimedAsyncJob(claimedJob pgnotifier.Claim) {
	pkgLogger.Infof("[WH-Jobs]: Got request for processing Async Job with Batch ID %s", claimedJob.BatchID)
	handleErr := func(err error, claim pgnotifier.Claim) {
		pkgLogger.Errorf("[WH]: Error processing claim: %v", err)
		response := pgnotifier.ClaimResponse{
			Err: err,
		}
		notifier.UpdateClaimedEvent(&claimedJob, &response)
	}
	var job jobs.AsyncJobPayload
	err := json.Unmarshal(claimedJob.Payload, &job)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}
	result, err := runAsyncJob(job)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}

	marshalledResult, err := json.Marshal(result)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}
	response := pgnotifier.ClaimResponse{
		Err:     err,
		Payload: marshalledResult,
	}
	notifier.UpdateClaimedEvent(&claimedJob, &response)
}

func setupSlave(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	slaveID := misc.FastUUID().String()
	jobNotificationChannel := notifier.Subscribe(ctx, slaveID, noOfSlaveWorkerRoutines)
	for workerIdx := 0; workerIdx <= noOfSlaveWorkerRoutines-1; workerIdx++ {
		idx := workerIdx
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			// create tags and timers
			workerIdleTimer := warehouseutils.NewTimerStat(STATS_WORKER_IDLE_TIME, warehouseutils.Tag{Name: TAG_WORKERID, Value: fmt.Sprintf("%d", idx)})
			workerIdleTimeStart := time.Now()
			for claimedJob := range jobNotificationChannel {
				workerIdleTimer.Since(workerIdleTimeStart)
				pkgLogger.Infof("[WH]: Successfully claimed job:%v by slave worker-%v-%v & job type %s", claimedJob.ID, idx, slaveID, claimedJob.JobType)

				if claimedJob.JobType == jobs.AsyncJobType {
					processClaimedAsyncJob(claimedJob)
				} else {
					processClaimedUploadJob(claimedJob, idx)
				}

				pkgLogger.Infof("[WH]: Successfully processed job:%v by slave worker-%v-%v", claimedJob.ID, idx, slaveID)
				workerIdleTimeStart = time.Now()
			}
			return nil
		}))
	}
	g.Go(misc.WithBugsnagForWarehouse(func() error {
		return notifier.RunMaintenanceWorker(ctx)
	}))
	return g.Wait()
}

func (jobRun *JobRun) handleDiscardTypes(tableName, columnName string, columnVal interface{}, columnData Data, violatedConstraints *ConstraintsViolation, discardWriter encoding.LoadFileWriter) error {
	job := jobRun.job
	rowID, hasID := columnData[job.getColumnName("id")]
	receivedAt, hasReceivedAt := columnData[job.getColumnName("received_at")]
	if violatedConstraints.IsViolated {
		if !hasID {
			rowID = violatedConstraints.ViolatedIdentifier
			hasID = true
		}
		if !hasReceivedAt {
			receivedAt = time.Now().Format(misc.RFC3339Milli)
			hasReceivedAt = true
		}
	}
	if hasID && hasReceivedAt {
		eventLoader := encoding.GetNewEventLoader(job.DestinationType, job.LoadFileType, discardWriter)
		eventLoader.AddColumn("column_name", warehouseutils.DiscardsSchema["column_name"], columnName)
		eventLoader.AddColumn("column_value", warehouseutils.DiscardsSchema["column_value"], fmt.Sprintf("%v", columnVal))
		eventLoader.AddColumn("received_at", warehouseutils.DiscardsSchema["received_at"], receivedAt)
		eventLoader.AddColumn("row_id", warehouseutils.DiscardsSchema["row_id"], rowID)
		eventLoader.AddColumn("table_name", warehouseutils.DiscardsSchema["table_name"], tableName)
		if eventLoader.IsLoadTimeColumn("uuid_ts") {
			timestampFormat := eventLoader.GetLoadTimeFormat("uuid_ts")
			eventLoader.AddColumn("uuid_ts", warehouseutils.DiscardsSchema["uuid_ts"], jobRun.uuidTS.Format(timestampFormat))
		}
		if eventLoader.IsLoadTimeColumn("loaded_at") {
			timestampFormat := eventLoader.GetLoadTimeFormat("loaded_at")
			eventLoader.AddColumn("loaded_at", "datetime", jobRun.uuidTS.Format(timestampFormat))
		}

		err := eventLoader.Write()
		if err != nil {
			pkgLogger.Errorf("[WH]: Failed to write event to discards table: %v", err)
			return err
		}
	}
	return nil
}
