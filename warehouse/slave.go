package warehouse

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/atomic"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	statsWorkerIdleTime                 = "worker_idle_time"
	statsWorkerClaimProcessingTime      = "worker_claim_processing_time"
	statsWorkerClaimProcessingFailed    = "worker_claim_processing_failed"
	statsWorkerClaimProcessingSucceeded = "worker_claim_processing_succeeded"

	tagWorkerid = "workerId"

	workerProcessingDownloadStagingFileFailed = "worker_processing_download_staging_file_failed"
)

type uploadProcessingResult struct {
	result uploadResult
	err    error
}

type uploadResult struct {
	TableName             string
	Location              string
	TotalRows             int
	ContentLength         int64
	StagingFileID         int64
	DestinationRevisionID string
	UseRudderStorage      bool
}

type asyncJobRunResult struct {
	Result bool
	Id     string
}

// jobRun Temporary store for processing staging file to load file
type jobRun struct {
	job                  Payload
	stagingFilePath      string
	uuidTS               time.Time
	outputFileWritersMap map[string]encoding.LoadFileWriter
	tableEventCountMap   map[string]int
	stagingFileReader    *gzip.Reader
	whIdentifier         string
	since                func(time.Time) time.Duration
	logger               logger.Logger

	stats                         stats.Stats
	uploadTimeStat                stats.Measurement
	totalUploadTimeStat           stats.Measurement
	downloadStagingFileStat       stats.Measurement
	processingStagingFileStat     stats.Measurement
	bytesProcessedStagingFileStat stats.Measurement

	config struct {
		numLoadFileUploadWorkers int
		slaveUploadTimeout       time.Duration
		loadObjectFolder         string
	}
}

func newJobRun(job Payload, conf *config.Config, log logger.Logger, stat stats.Stats) jobRun {
	jr := jobRun{
		job:          job,
		whIdentifier: warehouseutils.GetWarehouseIdentifier(job.DestinationType, job.SourceID, job.DestinationID),
		stats:        stat,
		since:        time.Since,
		logger:       log,
	}

	jr.config.numLoadFileUploadWorkers = conf.GetInt("Warehouse.numLoadFileUploadWorkers", 8)
	jr.config.slaveUploadTimeout = conf.GetDuration("Warehouse.slaveUploadTimeout", 10, conf.GetDuration("Warehouse.slaveUploadTimeoutInMin", 10, time.Minute))
	jr.config.loadObjectFolder = conf.GetString("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects")

	jr.uploadTimeStat = jr.timerStat("load_file_upload_time")
	jr.totalUploadTimeStat = jr.timerStat("load_file_total_upload_time")
	jr.downloadStagingFileStat = jr.timerStat("download_staging_file_time")
	jr.processingStagingFileStat = jr.timerStat("process_staging_file_time")
	jr.bytesProcessedStagingFileStat = jr.counterStat("bytes_processed_in_staging_file")

	return jr
}

func (jr *jobRun) setStagingFileReader() (reader *gzip.Reader, endOfFile bool) {
	job := jr.job
	jr.logger.Debugf("Starting read from downloaded staging file: %s", job.StagingFileLocation)
	stagingFile, err := os.Open(jr.stagingFilePath)
	if err != nil {
		jr.logger.Errorf("[WH]: Error opening file using os.Open at path:%s downloaded from %s", jr.stagingFilePath, job.StagingFileLocation)
		panic(err)
	}
	reader, err = gzip.NewReader(stagingFile)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, true
		}
		jr.logger.Errorf("[WH]: Error reading file using gzip.NewReader at path:%s downloaded from %s", jr.stagingFilePath, job.StagingFileLocation)
		panic(err)
	}

	jr.stagingFileReader = reader
	return reader, false
}

// setStagingFilePath Get download path for the job. Also creates missing directories for this path
func (jr *jobRun) setStagingFilePath(index int) (filePath string) {
	job := jr.job
	dirName := fmt.Sprintf(`/%s/_%s/`, misc.RudderWarehouseJsonUploadsTmp, strconv.Itoa(index))
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		jr.logger.Errorf("[WH]: Failed to create tmp DIR")
		panic(err)
	}
	filePath = tmpDirPath + dirName + fmt.Sprintf(`%s_%s/`, job.DestinationType, job.DestinationID) + job.StagingFileLocation
	err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	jr.stagingFilePath = filePath
	return filePath
}

// downloadStagingFile Download Staging file for the job
// If error occurs with the current config and current revision is different from staging revision
// We retry with the staging revision config if it is present
func (jr *jobRun) downloadStagingFile(ctx context.Context) error {
	job := jr.job
	downloadTask := func(config interface{}, useRudderStorage bool) (err error) {
		filePath := jr.stagingFilePath
		file, err := os.Create(filePath)
		if err != nil {
			panic(err)
		}

		downloader, err := job.fileManager(config, useRudderStorage)
		if err != nil {
			jr.logger.Errorf("[WH]: Failed to initialize downloader")
			return err
		}

		downloadStart := time.Now()

		err = downloader.Download(ctx, file, job.StagingFileLocation)
		if err != nil {
			jr.logger.Errorf("[WH]: Failed to download file")
			return err
		}
		file.Close()

		jr.downloadStagingFileStat.Since(downloadStart)

		fi, err := os.Stat(filePath)
		if err != nil {
			jr.logger.Errorf("[WH]: Error getting file size of downloaded staging file: ", err)
			return err
		}
		fileSize := fi.Size()
		jr.logger.Debugf("[WH]: Downloaded staging file %s size:%v", job.StagingFileLocation, fileSize)
		return
	}

	err := downloadTask(job.DestinationConfig, job.UseRudderStorage)
	if err != nil {
		if PickupStagingConfiguration(&job) {
			jr.logger.Infof("[WH]: Starting processing staging file with revision config for StagingFileID: %d, DestinationRevisionID: %s, StagingDestinationRevisionID: %s, whIdentifier: %s", job.StagingFileID, job.DestinationRevisionID, job.StagingDestinationRevisionID, jr.whIdentifier)
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

func (jr *jobRun) getLoadFilePath(tableName string) string {
	job := jr.job
	randomness := misc.FastUUID().String()
	return strings.TrimSuffix(jr.stagingFilePath, "json.gz") + tableName + fmt.Sprintf(`.%s`, randomness) + fmt.Sprintf(`.%s`, warehouseutils.GetLoadFileFormat(job.LoadFileType))
}

// uploadLoadFiles returns the upload output for each file uploaded to object storage
func (jr *jobRun) uploadLoadFiles(ctx context.Context) ([]uploadResult, error) {
	ctx, cancel := context.WithTimeout(ctx, jr.config.slaveUploadTimeout)
	defer cancel()

	uploader, err := jr.job.fileManager(jr.job.DestinationConfig, jr.job.UseRudderStorage)
	if err != nil {
		return nil, fmt.Errorf("creating uploader: %w", err)
	}

	var totalUploadTime atomic.Duration

	defer func() {
		if err == nil {
			jr.totalUploadTimeStat.SendTiming(totalUploadTime.Load())
		}
	}()

	uploadLoadFile := func(
		ctx context.Context,
		uploadFile encoding.LoadFileWriter,
		tableName string,
	) (filemanager.UploadedFile, error) {
		file, err := os.Open(uploadFile.GetLoadFile().Name())
		if err != nil {
			return filemanager.UploadedFile{}, fmt.Errorf("opening file %s: %w", uploadFile.GetLoadFile().Name(), err)
		}
		defer func() { _ = file.Close() }()

		var uploadLocation filemanager.UploadedFile

		uploadStart := time.Now()

		if slices.Contains(warehouseutils.TimeWindowDestinations, jr.job.DestinationType) {
			uploadLocation, err = uploader.Upload(
				ctx,
				file,
				warehouseutils.GetTablePathInObjectStorage(jr.job.DestinationNamespace, tableName),
				jr.job.LoadFilePrefix,
			)
		} else {
			uploadLocation, err = uploader.Upload(
				ctx,
				file,
				jr.config.loadObjectFolder,
				tableName,
				jr.job.SourceID,
				getBucketFolder(jr.job.UniqueLoadGenID, tableName),
			)
		}
		jr.uploadTimeStat.SendTiming(jr.since(uploadStart))

		return uploadLocation, err
	}

	process := func() <-chan *uploadProcessingResult {
		processStream := make(chan *uploadProcessingResult, len(jr.outputFileWritersMap))

		g, groupCtx := errgroup.WithContext(ctx)
		g.SetLimit(jr.config.numLoadFileUploadWorkers)

		go func() {
			defer close(processStream)

			for tableName, uploadFile := range jr.outputFileWritersMap {
				tableName := tableName
				uploadFile := uploadFile

				g.Go(func() error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						loadFileUploadStart := time.Now()

						uploadOutput, err := uploadLoadFile(
							groupCtx,
							uploadFile,
							tableName,
						)
						if err != nil {
							return fmt.Errorf("uploading load file: %w", err)
						}

						totalUploadTime.Add(jr.since(loadFileUploadStart))

						loadFileStats, err := os.Stat(uploadFile.GetLoadFile().Name())
						if err != nil {
							return fmt.Errorf("getting load file stats: %w", err)
						}

						processStream <- &uploadProcessingResult{
							result: uploadResult{
								TableName:             tableName,
								Location:              uploadOutput.Location,
								ContentLength:         loadFileStats.Size(),
								TotalRows:             jr.tableEventCountMap[tableName],
								StagingFileID:         jr.job.StagingFileID,
								DestinationRevisionID: jr.job.DestinationRevisionID,
								UseRudderStorage:      jr.job.UseRudderStorage,
							},
						}
						return nil
					}
				})
			}

			if err := g.Wait(); err != nil {
				processStream <- &uploadProcessingResult{err: err}
			}
		}()

		return processStream
	}

	processStream := process()
	output := make([]uploadResult, 0, len(jr.outputFileWritersMap))

	for processedJob := range processStream {
		if err := processedJob.err; err != nil {
			return nil, fmt.Errorf("uploading load file to object storage: %w", err)
		}

		output = append(output, processedJob.result)
	}
	if len(output) != len(jr.outputFileWritersMap) {
		return nil, fmt.Errorf("matching number of load file upload outputs: expected %d, got %d", len(jr.outputFileWritersMap), len(output))
	}

	return output, nil
}

func (jr *jobRun) GetWriter(tableName string) (encoding.LoadFileWriter, error) {
	writer, ok := jr.outputFileWritersMap[tableName]
	if !ok {
		var err error
		outputFilePath := jr.getLoadFilePath(tableName)
		if jr.job.LoadFileType == warehouseutils.LoadFileTypeParquet {
			writer, err = encoding.CreateParquetWriter(jr.job.UploadSchema[tableName], outputFilePath, jr.job.DestinationType)
		} else {
			writer, err = misc.CreateGZ(outputFilePath)
		}
		if err != nil {
			return nil, err
		}
		jr.outputFileWritersMap[tableName] = writer
		jr.tableEventCountMap[tableName] = 0
	}
	return writer, nil
}

func (jr *jobRun) cleanup() {
	if jr.stagingFileReader != nil {
		err := jr.stagingFileReader.Close()
		if err != nil {
			jr.logger.Errorf("[WH]: Failed to close staging file: %v", err)
		}
	}

	if jr.stagingFilePath != "" {
		misc.RemoveFilePaths(jr.stagingFilePath)
	}
	if jr.outputFileWritersMap != nil {
		for _, writer := range jr.outputFileWritersMap {
			misc.RemoveFilePaths(writer.GetLoadFile().Name())
		}
	}
}

func (jr *jobRun) handleDiscardTypes(tableName, columnName string, columnVal interface{}, columnData Data, violatedConstraints *ConstraintsViolation, discardWriter encoding.LoadFileWriter) error {
	job := jr.job
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
			eventLoader.AddColumn("uuid_ts", warehouseutils.DiscardsSchema["uuid_ts"], jr.uuidTS.Format(timestampFormat))
		}
		if eventLoader.IsLoadTimeColumn("loaded_at") {
			timestampFormat := eventLoader.GetLoadTimeFormat("loaded_at")
			eventLoader.AddColumn("loaded_at", "datetime", jr.uuidTS.Format(timestampFormat))
		}

		err := eventLoader.Write()
		if err != nil {
			jr.logger.Errorf("[WH]: Failed to write event to discards table: %v", err)
			return err
		}
	}
	return nil
}

func (job *Payload) getDiscardsTable() string {
	return warehouseutils.ToProviderCase(job.DestinationType, warehouseutils.DiscardsTable)
}

func (job *Payload) getColumnName(columnName string) string {
	return warehouseutils.ToProviderCase(job.DestinationType, columnName)
}

// getSortedColumnMapForAllTables Sort columns per table to maintain same order in load file (needed in case of csv load file)
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
	warehouseutils.NewCounterStat(workerProcessingDownloadStagingFileFailed, tags...).Increment()
}

func (job *Payload) fileManager(config interface{}, useRudderStorage bool) (filemanager.FileManager, error) {
	storageProvider := warehouseutils.ObjectStorageType(job.DestinationType, config, useRudderStorage)
	fileManager, err := filemanager.New(&filemanager.Settings{
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

func setupSlave(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	slaveID := misc.FastUUID().String()
	jobNotificationChannel := notifier.Subscribe(ctx, slaveID, noOfSlaveWorkerRoutines)
	for workerIdx := 0; workerIdx <= noOfSlaveWorkerRoutines-1; workerIdx++ {
		idx := workerIdx
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			// create tags and timers
			workerIdleTimer := warehouseutils.NewTimerStat(statsWorkerIdleTime, warehouseutils.Tag{Name: tagWorkerid, Value: fmt.Sprintf("%d", idx)})
			workerIdleTimeStart := time.Now()
			for claimedJob := range jobNotificationChannel {
				workerIdleTimer.Since(workerIdleTimeStart)
				pkgLogger.Infof("[WH]: Successfully claimed job:%v by slave worker-%v-%v & job type %s", claimedJob.ID, idx, slaveID, claimedJob.JobType)

				if claimedJob.JobType == jobs.AsyncJobType {
					processClaimedAsyncJob(ctx, claimedJob)
				} else {
					processClaimedUploadJob(ctx, claimedJob, idx)
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

func processClaimedUploadJob(ctx context.Context, claimedJob pgnotifier.Claim, workerIndex int) {
	claimProcessTimeStart := time.Now()
	defer func() {
		warehouseutils.NewTimerStat(statsWorkerClaimProcessingTime, warehouseutils.Tag{Name: tagWorkerid, Value: fmt.Sprintf("%d", workerIndex)}).Since(claimProcessTimeStart)
	}()
	handleErr := func(err error, claim pgnotifier.Claim) {
		pkgLogger.Errorf("[WH]: Error processing claim: %v", err)
		response := pgnotifier.ClaimResponse{
			Err: err,
		}
		warehouseutils.NewCounterStat(statsWorkerClaimProcessingFailed, warehouseutils.Tag{Name: tagWorkerid, Value: strconv.Itoa(workerIndex)}).Increment()
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
	loadFileOutputs, err := processStagingFile(ctx, job, workerIndex)
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
		warehouseutils.NewCounterStat(statsWorkerClaimProcessingFailed, warehouseutils.Tag{Name: tagWorkerid, Value: strconv.Itoa(workerIndex)}).Increment()
	} else {
		warehouseutils.NewCounterStat(statsWorkerClaimProcessingSucceeded, warehouseutils.Tag{Name: tagWorkerid, Value: strconv.Itoa(workerIndex)}).Increment()
	}
	notifier.UpdateClaimedEvent(&claimedJob, &response)
}

// This function is triggered when warehouse-master creates a new entry in wh_uploads table
// This is executed in the context of the warehouse-slave/worker and does the following:
//
// 1. Download the Staging file into a tmp directory
// 2. Transform the staging file into multiple load files (One file per output table)
// 3. Uploads these load files to Object storage
// 4. Save entries for the generated load files in wh_load_files table
// 5. Delete the staging and load files from tmp directory
func processStagingFile(ctx context.Context, job Payload, workerIndex int) (loadFileUploadOutputs []uploadResult, err error) {
	processStartTime := time.Now()

	jr := newJobRun(job, config.Default, pkgLogger, stats.Default)

	defer func() {
		jr.counterStat("staging_files_processed", warehouseutils.Tag{Name: "worker_id", Value: strconv.Itoa(workerIndex)}).Count(1)
		jr.timerStat("staging_files_total_processing_time", warehouseutils.Tag{Name: "worker_id", Value: strconv.Itoa(workerIndex)}).Since(processStartTime)
		jr.cleanup()
	}()

	jr.logger.Debugf("[WH]: Starting processing staging file: %v at %s for %s", job.StagingFileID, job.StagingFileLocation, jr.whIdentifier)

	jr.setStagingFilePath(workerIndex)

	// This creates the file, so on successful creation remove it
	err = jr.downloadStagingFile(ctx)
	if err != nil {
		return loadFileUploadOutputs, err
	}

	sortedTableColumnMap := job.getSortedColumnMapForAllTables()

	reader, endOfFile := jr.setStagingFileReader()
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
	jr.outputFileWritersMap = make(map[string]encoding.LoadFileWriter)
	jr.tableEventCountMap = make(map[string]int)
	jr.uuidTS = timeutil.Now()

	// Initialize Discards Table
	discardsTable := job.getDiscardsTable()
	jr.tableEventCountMap[discardsTable] = 0

	processingStart := time.Now()

	lineBytesCounter := 0
	var interfaceSliceSample []interface{}
	for {
		ok := scanner.Scan()
		if !ok {
			scanErr := scanner.Err()
			if scanErr != nil {
				jr.logger.Errorf("WH: Error in scanner reading line from staging file: %v", scanErr)
			}
			break
		}

		lineBytes := scanner.Bytes()
		lineBytesCounter += len(lineBytes)
		var batchRouterEvent BatchRouterEvent
		err := json.Unmarshal(lineBytes, &batchRouterEvent)
		if err != nil {
			jr.logger.Errorf("[WH]: Failed to unmarshal JSON line to batchrouter event: %+v", batchRouterEvent)
			continue
		}

		tableName := batchRouterEvent.Metadata.Table
		columnData := batchRouterEvent.Data

		if job.DestinationType == warehouseutils.S3Datalake && len(sortedTableColumnMap[tableName]) > columnCountLimitMap[warehouseutils.S3Datalake] {
			jr.logger.Errorf("[WH]: Huge staging file columns : columns in upload schema: %v for StagingFileID: %v", len(sortedTableColumnMap[tableName]), job.StagingFileID)
			return nil, fmt.Errorf("staging file schema limit exceeded for stagingFileID: %d, actualCount: %d", job.StagingFileID, len(sortedTableColumnMap[tableName]))
		}

		// Create separate load file for each table
		writer, err := jr.GetWriter(tableName)
		if err != nil {
			return nil, err
		}

		eventLoader := encoding.GetNewEventLoader(job.DestinationType, job.LoadFileType, writer)
		for _, columnName := range sortedTableColumnMap[tableName] {
			if eventLoader.IsLoadTimeColumn(columnName) {
				timestampFormat := eventLoader.GetLoadTimeFormat(columnName)
				eventLoader.AddColumn(job.getColumnName(columnName), job.UploadSchema[tableName][columnName], jr.uuidTS.Format(timestampFormat))
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
				newColumnVal, convError := handleSchemaChange(
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

					discardWriter, err := jr.GetWriter(discardsTable)
					if err != nil {
						return nil, err
					}
					// add discardWriter to outputFileWritersMap
					jr.outputFileWritersMap[discardsTable] = discardWriter

					err = jr.handleDiscardTypes(tableName, columnName, columnVal, columnData, violatedConstraints, discardWriter)

					if err != nil {
						jr.logger.Errorf("[WH]: Failed to write to discards: %v", err)
					}
					jr.tableEventCountMap[discardsTable]++
					continue
				}
				columnVal = newColumnVal
			}

			// Special handling for JSON arrays
			// TODO: Will this work for both BQ and RS?
			if reflect.TypeOf(columnVal) == reflect.TypeOf(interfaceSliceSample) {
				marshalledVal, err := json.Marshal(columnVal)
				if err != nil {
					jr.logger.Errorf("[WH]: Error in marshalling []interface{} columnVal: %v", err)
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
			jr.logger.Errorf("[WH]: Failed to write event: %v", err)
			return loadFileUploadOutputs, err
		}
		jr.tableEventCountMap[tableName]++
	}
	jr.processingStagingFileStat.Since(processingStart)

	jr.logger.Debugf("[WH]: Process %v bytes from downloaded staging file: %s", lineBytesCounter, job.StagingFileLocation)
	jr.bytesProcessedStagingFileStat.Count(lineBytesCounter)

	for _, loadFile := range jr.outputFileWritersMap {
		err = loadFile.Close()
		if err != nil {
			jr.logger.Errorf("Error while closing load file %s : %v", loadFile.GetLoadFile().Name(), err)
		}
	}
	loadFileUploadOutputs, err = jr.uploadLoadFiles(ctx)
	return loadFileUploadOutputs, err
}

func processClaimedAsyncJob(ctx context.Context, claimedJob pgnotifier.Claim) {
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
	result, err := runAsyncJob(ctx, job)
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

func runAsyncJob(ctx context.Context, asyncjob jobs.AsyncJobPayload) (asyncJobRunResult, error) {
	warehouse, err := getDestinationFromSlaveConnectionMap(asyncjob.DestinationID, asyncjob.SourceID)
	if err != nil {
		return asyncJobRunResult{Id: asyncjob.Id, Result: false}, err
	}
	destType := warehouse.Destination.DestinationDefinition.Name
	whManager, err := manager.NewWarehouseOperations(destType)
	if err != nil {
		return asyncJobRunResult{Id: asyncjob.Id, Result: false}, err
	}
	whasyncjob := &jobs.WhAsyncJob{}

	var metadata warehouseutils.DeleteByMetaData
	err = json.Unmarshal(asyncjob.MetaData, &metadata)
	if err != nil {
		return asyncJobRunResult{Id: asyncjob.Id, Result: false}, err
	}
	whManager.SetConnectionTimeout(warehouseutils.GetConnectionTimeout(
		destType, warehouse.Destination.ID,
	))
	err = whManager.Setup(ctx, warehouse, whasyncjob)
	if err != nil {
		return asyncJobRunResult{Id: asyncjob.Id, Result: false}, err
	}
	defer whManager.Cleanup(ctx)
	tableNames := []string{asyncjob.TableName}
	if asyncjob.AsyncJobType == "deletebyjobrunid" {
		pkgLogger.Info("[WH-Jobs]: Running DeleteByJobRunID on slave worker")

		params := warehouseutils.DeleteByParams{
			SourceId:  asyncjob.SourceID,
			TaskRunId: metadata.TaskRunId,
			JobRunId:  metadata.JobRunId,
			StartTime: metadata.StartTime,
		}
		err = whManager.DeleteBy(ctx, tableNames, params)
	}
	asyncJobRunResult := asyncJobRunResult{
		Result: err == nil,
		Id:     asyncjob.Id,
	}
	return asyncJobRunResult, err
}
