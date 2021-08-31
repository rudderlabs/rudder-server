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
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
)

// Temporary store for processing staging file to load file
type JobRunT struct {
	job                  PayloadT
	stagingFilePaths     map[int64]string
	uuidTS               time.Time
	outputFileWritersMap map[string]warehouseutils.LoadFileWriterI
	tableEventCountMap   map[string]int
	stagingFileReaders   map[int64]*gzip.Reader
	whIdentifier         string
}

func (jobRun *JobRunT) setStagingFileReader(stagingFile *StagingFileEntryT) (reader *gzip.Reader, endOfFile bool) {
	pkgLogger.Debugf("Starting read from downloaded staging file: %s", stagingFile.Location)
	stagingFilePath := jobRun.stagingFilePaths[stagingFile.ID]
	rawf, err := os.Open(stagingFilePath)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error opening file using os.Open at path:%s downloaded from %s", stagingFilePath, stagingFile.Location)
		panic(err)
	}
	reader, err = gzip.NewReader(rawf)
	if err != nil {
		if err.Error() == "EOF" {
			return nil, true
		}
		pkgLogger.Errorf("[WH]: Error reading file using gzip.NewReader at path:%s downloaded from %s", stagingFilePath, stagingFile.Location)
		panic(err)
	}

	jobRun.stagingFileReaders[stagingFile.ID] = reader
	return reader, false
}

/*
 * Get download path for the job. Also creates missing directories for this path
 */
func (jobRun *JobRunT) setStagingFileDownloadPath(stagingFile *StagingFileEntryT) (filePath string) {
	job := jobRun.job
	dirName := "/rudder-warehouse-json-uploads-tmp/"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to create tmp DIR")
		panic(err)
	}
	filePath = tmpDirPath + dirName + fmt.Sprintf(`%s_%s/`, job.DestinationType, job.DestinationID) + stagingFile.Location
	err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	jobRun.stagingFilePaths[stagingFile.ID] = filePath
	return filePath
}

// Get fileManager
func (job *PayloadT) getFileManager() (filemanager.FileManager, error) {
	storageProvider := warehouseutils.ObjectStorageType(job.DestinationType, job.DestinationConfig, job.UseRudderStorage)
	fileManager, err := filemanager.New(&filemanager.SettingsT{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:                    storageProvider,
			Config:                      job.DestinationConfig,
			UseRudderStorage:            job.UseRudderStorage,
			RudderStoragePrefixOverride: job.RudderStoragePrefix,
		}),
	})
	return fileManager, err
}

/*
 * Download Staging file for the job
 */
func (jobRun *JobRunT) downloadStagingFile(stagingFile *StagingFileEntryT) error {
	filePath := jobRun.stagingFilePaths[stagingFile.ID]
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}

	downloader, err := jobRun.job.getFileManager()
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to initialize downloader")
		return err
	}

	timer := jobRun.timerStat("download_staging_file_time")
	timer.Start()

	err = downloader.Download(file, stagingFile.Location)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to download file")
		return err
	}
	file.Close()
	timer.End()

	fi, err := os.Stat(filePath)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error getting file size of downloaded staging file: ", err)
		return err
	}
	fileSize := fi.Size()
	pkgLogger.Debugf("[WH]: Downloaded staging file %s size:%v", stagingFile.Location, fileSize)

	return nil
}

func (job *PayloadT) getDiscardsTable() string {
	return warehouseutils.ToProviderCase(job.DestinationType, warehouseutils.DiscardsTable)
}

func (jobRun *JobRunT) getLoadFilePath(tableName string) string {
	job := jobRun.job
	randomness := uuid.NewV4().String()
	return tableName + fmt.Sprintf(`.%s`, randomness) + fmt.Sprintf(`.%s`, getLoadFileFormat(job.DestinationType))
}

func (job *PayloadT) getColumnName(columnName string) string {
	return warehouseutils.ToProviderCase(job.DestinationType, columnName)
}

type loadFileUploadJob struct {
	tableName  string
	outputFile warehouseutils.LoadFileWriterI
}

type loadFileUploadOutputT struct {
	TableName string
	Location  string
	TotalRows int
	Metadata  loadFileMetadataT
	UploadID  int64
}

type loadFileMetadataT struct {
	ContentLength      int64
	StartStagingFileID int64
	EndStagingFileID   int64
}

func (jobRun *JobRunT) uploadLoadFilesToObjectStorage() ([]loadFileUploadOutputT, error) {
	job := jobRun.job
	uploader, err := job.getFileManager()
	if err != nil {
		return []loadFileUploadOutputT{}, err
	}
	// var loadFileIDs []int64
	var loadFileUploadOutputs []loadFileUploadOutputT

	var stagingFileIds = jobRun.job.StagingFiles.IDs

	loadFileOutputChan := make(chan loadFileUploadOutputT, len(jobRun.outputFileWritersMap))
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
					pkgLogger.Debugf("context is cancelled, stopped processing load file for staging file ids %v ", stagingFileIds)
					return // stop further processing
				default:
					tableName := uploadJob.tableName
					uploadOutput, err := jobRun.uploadLoadFileToObjectStorage(uploader, uploadJob.outputFile, tableName)
					if err != nil {
						uploadErrorChan <- err
						return
					}
					loadFileStats, err := os.Stat(uploadJob.outputFile.GetLoadFile().Name())
					if err != nil {
						uploadErrorChan <- err
						return
					}
					loadFileOutputChan <- loadFileUploadOutputT{
						TableName: tableName,
						Location:  uploadOutput.Location,
						Metadata: loadFileMetadataT{
							ContentLength:      loadFileStats.Size(),
							StartStagingFileID: stagingFileIds[0],
							EndStagingFileID:   stagingFileIds[len(stagingFileIds)-1],
						},
						TotalRows: jobRun.tableEventCountMap[tableName],
						UploadID:  job.UploadID,
					}
				}

			}
		}(ctx)
	}
	// Create upload jobs
	for tableName, loadFile := range jobRun.outputFileWritersMap {
		uploadJobChan <- &loadFileUploadJob{tableName: tableName, outputFile: loadFile}
	}

	// Wait for response
	for {
		select {
		case loadFileOutput := <-loadFileOutputChan:
			loadFileUploadOutputs = append(loadFileUploadOutputs, loadFileOutput)
			if len(loadFileUploadOutputs) == len(jobRun.outputFileWritersMap) {
				return loadFileUploadOutputs, nil
			}
		case err := <-uploadErrorChan:
			pkgLogger.Errorf("received error while uploading load file to bucket for staging file ids %v, cancelling the context: err %v", stagingFileIds, err)
			return []loadFileUploadOutputT{}, err
		case <-time.After(slaveUploadTimeout):
			return []loadFileUploadOutputT{}, fmt.Errorf("Load files upload timed out for staging file idsx: %v", stagingFileIds)
		}
	}
}

func (jobRun *JobRunT) uploadLoadFileToObjectStorage(uploader filemanager.FileManager, uploadFile warehouseutils.LoadFileWriterI, tableName string) (filemanager.UploadOutput, error) {
	job := jobRun.job
	file, err := os.Open(uploadFile.GetLoadFile().Name()) // opens file in read mode
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to Open File: %s", uploadFile.GetLoadFile().Name())
		return filemanager.UploadOutput{}, err
	}
	defer file.Close()
	pkgLogger.Debugf("[WH]: %s: Uploading load_file to %s for table: %s with staging_file ids: %v", job.DestinationType, warehouseutils.ObjectStorageType(job.DestinationType, job.DestinationConfig, job.UseRudderStorage), tableName, job.StagingFiles.IDs)
	var uploadLocation filemanager.UploadOutput
	if job.DestinationType == "S3_DATALAKE" {
		uploadLocation, err = uploader.Upload(file, warehouseutils.GetTablePathInObjectStorage(jobRun.job.DestinationNamespace, tableName), job.LoadFilePrefix)
	} else {
		uploadLocation, err = uploader.Upload(file, config.GetEnv("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects"), tableName, job.SourceID, getBucketFolder(job.UniqueLoadGenID, tableName))
	}
	return uploadLocation, err
}

// Sort columns per table to maintain same order in load file (needed in case of csv load file)
func (job *PayloadT) getSortedColumnMapForAllTables() map[string][]string {
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

func (jobRun *JobRunT) GetWriter(tableName string) (warehouseutils.LoadFileWriterI, error) {
	writer, ok := jobRun.outputFileWritersMap[tableName]
	if !ok {
		var err error
		outputFilePath := jobRun.getLoadFilePath(tableName)
		if jobRun.job.LoadFileType == warehouseutils.LOAD_FILE_TYPE_PARQUET {
			writer, err = warehouseutils.CreateParquetWriter(jobRun.job.UploadSchema[tableName], outputFilePath, jobRun.job.DestinationType)
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

func (jobRun *JobRunT) cleanup() {
	if jobRun.stagingFileReaders != nil {
		for _, stagingFileReader := range jobRun.stagingFileReaders {
			err := stagingFileReader.Close()
			if err != nil {
				pkgLogger.Errorf("[WH]: Failed to close staging file: %v", err)
			}
		}
	}

	if jobRun.stagingFilePaths != nil {
		for _, stagingFilePath := range jobRun.stagingFilePaths {
			err := os.Remove(stagingFilePath)
			if err != nil {
				pkgLogger.Errorf("[WH]: Failed to remove staging file: %v", err)
			}
		}
	}
	if jobRun.outputFileWritersMap != nil {
		for _, writer := range jobRun.outputFileWritersMap {
			os.Remove(writer.GetLoadFile().Name())
		}
	}
}

func (event *BatchRouterEventT) getColumnInfo(columnName string) (columnInfo ColumnInfoT, ok bool) {
	columnVal, ok := event.Data[columnName]
	if !ok {
		return ColumnInfoT{}, false
	}
	columnType, ok := event.Metadata.Columns[columnName]
	if !ok {
		return ColumnInfoT{}, false
	}
	return ColumnInfoT{ColumnVal: columnVal, ColumnType: columnType}, true
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

func processStagingFile(job PayloadT, workerIndex int) (loadFileUploadOutputs []loadFileUploadOutputT, err error) {

	jobRun := JobRunT{
		job:                  job,
		whIdentifier:         warehouseutils.GetWarehouseIdentifier(job.DestinationType, job.SourceID, job.DestinationID),
		tableEventCountMap:   map[string]int{},
		outputFileWritersMap: make(map[string]warehouseutils.LoadFileWriterI),
		stagingFilePaths:     map[int64]string{},
		stagingFileReaders:   make(map[int64]*gzip.Reader),
	}

	defer jobRun.counterStat("staging_files_processed", tag{name: "worker_id", value: strconv.Itoa(workerIndex)}).Count(1)
	defer jobRun.cleanup()

	pkgLogger.Debugf("[WH]: Starting processing staging files: %v for %s", job.StagingFiles.IDs, jobRun.whIdentifier)

	for _, stagingFile := range job.StagingFiles.Batch {
		jobRun.setStagingFileDownloadPath(stagingFile)

		// This creates the file, so on successful creation remove it
		err = jobRun.downloadStagingFile(stagingFile)
		if err != nil {
			return loadFileUploadOutputs, err
		}

		sortedTableColumnMap := job.getSortedColumnMapForAllTables()

		reader, endOfFile := jobRun.setStagingFileReader(stagingFile)
		if endOfFile {
			// If empty file, skip processing current staging file
			continue
		}
		scanner := bufio.NewScanner(reader)
		// default scanner buffer maxCapacity is 64K
		// set it to higher value to avoid read stop on read size error
		maxCapacity := maxStagingFileReadBufferCapacityInK * 1024
		buf := make([]byte, maxCapacity)
		scanner.Buffer(buf, maxCapacity)

		// read from staging file and write a separate load file for each table in warehouse
		jobRun.uuidTS = timeutil.Now()
		misc.PrintMemUsage()

		// Initilize Discards Table
		discardsTable := job.getDiscardsTable()
		jobRun.tableEventCountMap[discardsTable] = 0

		timer := jobRun.timerStat("process_staging_file_time")
		timer.Start()

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
			var batchRouterEvent BatchRouterEventT
			err := json.Unmarshal(lineBytes, &batchRouterEvent)
			if err != nil {
				pkgLogger.Errorf("[WH]: Failed to unmarshal JSON line to batchrouter event: %+v", batchRouterEvent)
				continue
			}

			tableName := batchRouterEvent.Metadata.Table
			columnData := batchRouterEvent.Data

			// Create separate load file for each table
			writer, err := jobRun.GetWriter(tableName)
			if err != nil {
				return nil, err
			}
			eventLoader := warehouseutils.GetNewEventLoader(job.DestinationType, job.LoadFileType, writer)
			for _, columnName := range sortedTableColumnMap[tableName] {
				if eventLoader.IsLoadTimeColumn(columnName) {
					timestampFormat := eventLoader.GetLoadTimeFomat(columnName)
					eventLoader.AddColumn(job.getColumnName(columnName), job.UploadSchema[tableName][columnName], jobRun.uuidTS.Format(timestampFormat))
					continue
				}
				columnInfo, ok := batchRouterEvent.getColumnInfo(columnName)
				if !ok {
					eventLoader.AddEmptyColumn(columnName)
					continue
				}
				columnType := columnInfo.ColumnType
				columnVal := columnInfo.ColumnVal

				if columnType == "int" || columnType == "bigint" {
					floatVal, ok := columnVal.(float64)
					if !ok {
						eventLoader.AddEmptyColumn(columnName)
						continue
					}
					columnVal = int(floatVal)
				}

				dataTypeInSchema, ok := job.UploadSchema[tableName][columnName]
				if ok && columnType != dataTypeInSchema {
					newColumnVal, ok := handleSchemaChange(dataTypeInSchema, columnType, columnVal)
					if !ok {
						eventLoader.AddEmptyColumn(columnName)

						discardWriter, err := jobRun.GetWriter(discardsTable)
						if err != nil {
							return nil, err
						}
						err = jobRun.handleDiscardTypes(tableName, columnName, columnVal, columnData, discardWriter)

						if err != nil {
							pkgLogger.Errorf("[WH]: Failed to write to discards: %v", err)
						}
						jobRun.tableEventCountMap[discardsTable]++
						continue
					}
					if newColumnVal == nil {
						eventLoader.AddEmptyColumn(columnName)
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

		timer.End()
		misc.PrintMemUsage()

		pkgLogger.Debugf("[WH]: Process %v bytes from downloaded staging file: %s", lineBytesCounter, stagingFile.Location)
		jobRun.counterStat("bytes_processed_in_staging_file").Count(lineBytesCounter)
	}

	for _, loadFile := range jobRun.outputFileWritersMap {
		err = loadFile.Close()
		if err != nil {
			pkgLogger.Errorf("Error while closing load file %s : %v", loadFile.GetLoadFile().Name(), err)
		}
	}
	loadFileUploadOutputs, err = jobRun.uploadLoadFilesToObjectStorage()
	return loadFileUploadOutputs, err
}

func freeWorker(workerIdx int) {
	slaveWorkerRoutineBusy[workerIdx] = false
	pkgLogger.Debugf("[WH]: Setting free slave worker %d: %v", workerIdx, slaveWorkerRoutineBusy)
}

func claim(workerIdx int, slaveID string) (claimedJob pgnotifier.ClaimT, claimed bool) {
	pkgLogger.Debugf("[WH]: Attempting to claim job by slave worker-%v-%v", workerIdx, slaveID)
	workerID := warehouseutils.GetSlaveWorkerId(workerIdx, slaveID)
	claimedJob, claimed = notifier.Claim(workerID)
	return
}

func processClaimedJob(claimedJob pgnotifier.ClaimT, workerIndex int) {
	handleErr := func(err error, claim pgnotifier.ClaimT) {
		pkgLogger.Errorf("[WH]: Error processing claim: %v", err)
		response := pgnotifier.ClaimResponseT{
			Err: err,
		}
		claim.ClaimResponseChan <- response
	}

	var job PayloadT
	err := json.Unmarshal(claimedJob.Payload, &job)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}
	job.BatchID = claimedJob.BatchID
	pkgLogger.Infof(`Starting processing staging-files:%v from claim:%v`, job.StagingFiles.IDs, claimedJob.ID)
	loadFileOutputs, err := processStagingFile(job, workerIndex)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}
	job.Output = loadFileOutputs
	output, err := json.Marshal(job)
	response := pgnotifier.ClaimResponseT{
		Err:     err,
		Payload: output,
	}
	claimedJob.ClaimResponseChan <- response
}

func setupSlave() {
	slaveWorkerRoutineBusy = make([]bool, noOfSlaveWorkerRoutines)
	slaveID := uuid.NewV4().String()
	rruntime.Go(func() {
		jobNotificationChannel, err := notifier.Subscribe(StagingFilesPGNotifierChannel, 2*noOfSlaveWorkerRoutines)
		if err != nil {
			panic(err)
		}
		for {
			ev := <-jobNotificationChannel
			pkgLogger.Debugf("[WH]: Notification recieved, event: %v, workers: %v", ev, slaveWorkerRoutineBusy)
			for workerIdx := 0; workerIdx <= noOfSlaveWorkerRoutines-1; workerIdx++ {
				if !slaveWorkerRoutineBusy[workerIdx] {
					slaveWorkerRoutineBusy[workerIdx] = true
					idx := workerIdx
					claimedJob, claimed := claim(idx, slaveID)
					if !claimed {
						freeWorker(idx)
						break
					}
					pkgLogger.Infof("[WH]: Successfully claimed job:%v by slave worker-%v-%v", claimedJob.ID, idx, slaveID)
					rruntime.Go(func() {
						processClaimedJob(claimedJob, idx)
						freeWorker(idx)
					})
					break
				}
			}
		}
	})
}

func (jobRun *JobRunT) handleDiscardTypes(tableName string, columnName string, columnVal interface{}, columnData DataT, discardWriter warehouseutils.LoadFileWriterI) error {
	job := jobRun.job
	rowID, hasID := columnData[job.getColumnName("id")]
	receivedAt, hasReceivedAt := columnData[job.getColumnName("received_at")]
	if hasID && hasReceivedAt {
		eventLoader := warehouseutils.GetNewEventLoader(job.DestinationType, job.LoadFileType, discardWriter)
		eventLoader.AddColumn("column_name", warehouseutils.DiscardsSchema["column_name"], columnName)
		eventLoader.AddColumn("column_value", warehouseutils.DiscardsSchema["column_value"], fmt.Sprintf("%v", columnVal))
		eventLoader.AddColumn("received_at", warehouseutils.DiscardsSchema["received_at"], receivedAt)
		eventLoader.AddColumn("row_id", warehouseutils.DiscardsSchema["row_id"], rowID)
		eventLoader.AddColumn("table_name", warehouseutils.DiscardsSchema["table_name"], tableName)
		if eventLoader.IsLoadTimeColumn("uuid_ts") {
			timestampFormat := eventLoader.GetLoadTimeFomat("uuid_ts")
			eventLoader.AddColumn("uuid_ts", warehouseutils.DiscardsSchema["uuid_ts"], jobRun.uuidTS.Format(timestampFormat))
		}
		if eventLoader.IsLoadTimeColumn("loaded_at") {
			timestampFormat := eventLoader.GetLoadTimeFomat("loaded_at")
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
