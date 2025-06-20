package slave

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/constraints"
	"github.com/rudderlabs/rudder-server/warehouse/utils/types"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	appConfig "github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type basePayload struct {
	BatchID                      string                 `json:"batch_id"`
	UploadID                     int64                  `json:"upload_id"`
	UploadSchema                 model.Schema           `json:"upload_schema"`
	WorkspaceID                  string                 `json:"workspace_id"`
	SourceID                     string                 `json:"source_id"`
	SourceName                   string                 `json:"source_name"`
	DestinationID                string                 `json:"destination_id"`
	DestinationName              string                 `json:"destination_name"`
	DestinationType              string                 `json:"destination_type"`
	DestinationNamespace         string                 `json:"destination_namespace"`
	DestinationRevisionID        string                 `json:"destination_revision_id"`
	StagingDestinationRevisionID string                 `json:"staging_destination_revision_id"`
	DestinationConfig            map[string]interface{} `json:"destination_config"`
	StagingDestinationConfig     interface{}            `json:"staging_destination_config"`
	UseRudderStorage             bool                   `json:"use_rudder_storage"`
	StagingUseRudderStorage      bool                   `json:"staging_use_rudder_storage"`
	UniqueLoadGenID              string                 `json:"unique_load_gen_id"`
	RudderStoragePrefix          string                 `json:"rudder_storage_prefix"`
	Output                       []uploadResult         `json:"output"`
	LoadFilePrefix               string                 `json:"load_file_prefix"`
	LoadFileType                 string                 `json:"load_file_type"`
}

// payload represents the job payload for upload type jobs
type payload struct {
	basePayload
	StagingFileID       int64  `json:"staging_file_id"`
	StagingFileLocation string `json:"staging_file_location"`
}

// payloadV2 represents the job payload for upload_v2 type jobs
type payloadV2 struct {
	basePayload
	StagingFiles []stagingFileInfo `json:"staging_files"`
}

// stagingFileInfo contains information about a staging file
type stagingFileInfo struct {
	ID       int64  `json:"id"`
	Location string `json:"location"`
}

func (p *basePayload) discardsTable() string {
	return warehouseutils.ToProviderCase(p.DestinationType, warehouseutils.DiscardsTable)
}

func (p *basePayload) columnName(columnName string) string {
	return warehouseutils.ToProviderCase(p.DestinationType, columnName)
}

// sortedColumnMapForAllTables Sort columns per table to maintain same order in load file (needed in case of csv load file)
func (p *basePayload) sortedColumnMapForAllTables() map[string][]string {
	return lo.MapValues(p.UploadSchema, func(value model.TableSchema, key string) []string {
		columns := lo.Keys(value)
		sort.Strings(columns)
		return columns
	})
}

func (p *basePayload) fileManager(config interface{}, useRudderStorage bool) (filemanager.FileManager, error) {
	storageProvider := warehouseutils.ObjectStorageType(p.DestinationType, config, useRudderStorage)
	fileManager, err := filemanager.New(&filemanager.Settings{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:                    storageProvider,
			Config:                      config,
			UseRudderStorage:            useRudderStorage,
			RudderStoragePrefixOverride: p.RudderStoragePrefix,
			WorkspaceID:                 p.WorkspaceID,
		}),
		Conf: appConfig.Default,
	})
	return fileManager, err
}

func (p *basePayload) pickupStagingConfiguration() bool {
	return p.StagingDestinationRevisionID != p.DestinationRevisionID && p.StagingDestinationConfig != nil
}

// jobRun Temporary store for processing staging file to load file
type jobRun struct {
	job                    basePayload
	workerIdx              int
	uuidTS                 time.Time
	outputFileWritersMap   map[string]encoding.LoadFileWriter
	outputFileWritersMapMu sync.RWMutex // To prevent concurrent access to the outputFileWritersMap

	tableEventCountMap   map[string]int
	tableEventCountMapMu sync.RWMutex // To prevent concurrent access to the tableEventCountMap

	identifier string

	since           func(time.Time) time.Duration
	logger          logger.Logger
	encodingFactory *encoding.Factory

	now func() time.Time

	stats               stats.Stats
	conf                *appConfig.Config
	uploadTimeStat      stats.Timer
	totalUploadTimeStat stats.Timer

	// Staging file related stats are thread safe
	// so no need to move them to stagingFileProcessor struct
	downloadStagingFileStat        stats.Timer
	processingStagingFileStat      stats.Timer
	bytesProcessedStagingFileStat  stats.Counter
	bytesDownloadedStagingFileStat stats.Counter
	downloadStagingFileFailedStat  stats.Counter
	stagingFileDuplicateEvents     stats.Counter

	config struct {
		numLoadFileUploadWorkers int
		slaveUploadTimeout       time.Duration
		loadObjectFolder         string
	}

	stagingFilePaths   map[int64]string
	stagingFilePathsMu sync.RWMutex // To prevent concurrent access to the stagingFilePaths

	// tableWriterMutexes ensures thread-safe writes to tables by providing exclusive access
	// to each table's writer. When a writer acquires a table's mutex, other writers for that
	// same table must wait until the lock is released.
	tableWriterMutexes   map[string]*sync.Mutex
	tableWriterMutexesMu sync.Mutex // To prevent concurrent access to the tableWriterMutexes

	// Function to download staging file, can be overridden in tests
	downloadStagingFile func(ctx context.Context, stagingFileInfo stagingFileInfo) error
}

func newJobRun(job basePayload, workerIdx int, conf *appConfig.Config, log logger.Logger, stat stats.Stats, encodingFactory *encoding.Factory) *jobRun {
	jr := &jobRun{
		job:                  job,
		workerIdx:            workerIdx,
		identifier:           warehouseutils.GetWarehouseIdentifier(job.DestinationType, job.SourceID, job.DestinationID),
		stats:                stat,
		conf:                 conf,
		since:                time.Since,
		logger:               log,
		now:                  timeutil.Now,
		encodingFactory:      encodingFactory,
		uuidTS:               timeutil.Now(),
		outputFileWritersMap: make(map[string]encoding.LoadFileWriter),
		tableEventCountMap:   make(map[string]int),
		stagingFilePaths:     make(map[int64]string),
		tableWriterMutexes:   make(map[string]*sync.Mutex),
	}

	jr.downloadStagingFile = func(ctx context.Context, stagingFileInfo stagingFileInfo) error {
		doTask := func(config interface{}, useRudderStorage bool) error {
			var file *os.File
			var err error

			stagingFilePath, err := jr.path(stagingFileInfo)
			if err != nil {
				return fmt.Errorf("getting staging file path: %w", err)
			}

			if file, err = os.Create(stagingFilePath); err != nil {
				return fmt.Errorf("creating file at path:%s downloaded from %s: %w",
					stagingFilePath,
					stagingFileInfo.Location,
					err,
				)
			}

			downloader, err := jr.job.fileManager(config, useRudderStorage)
			if err != nil {
				return fmt.Errorf("creating file manager: %w", err)
			}

			downloadStart := jr.now()
			if err = downloader.Download(ctx, file, stagingFileInfo.Location); err != nil {
				return fmt.Errorf("downloading staging file from %s: %w", stagingFileInfo.Location, err)
			}
			if err = file.Close(); err != nil {
				return fmt.Errorf("closing file after download: %w", err)
			}

			jr.downloadStagingFileStat.Since(downloadStart)

			fileInfo, err := os.Stat(stagingFilePath)
			if err != nil {
				return fmt.Errorf("file size of downloaded staging file: %w", err)
			}

			jr.bytesDownloadedStagingFileStat.Count(int(fileInfo.Size()))

			return nil
		}
		if err := doTask(jr.job.DestinationConfig, jr.job.UseRudderStorage); err != nil {
			if !jr.job.pickupStagingConfiguration() {
				return fmt.Errorf("downloading staging file: %w", err)
			}

			jr.logger.Infon("[WH]: Starting processing staging file with revision config",
				logger.NewField("stagingFileID", stagingFileInfo.ID),
				logger.NewField("destinationRevisionID", jr.job.DestinationRevisionID),
				logger.NewField("stagingDestinationRevisionID", jr.job.StagingDestinationRevisionID),
				logger.NewField("identifier", jr.identifier),
			)

			if err := doTask(jr.job.StagingDestinationConfig, jr.job.StagingUseRudderStorage); err != nil {
				jr.downloadStagingFileFailedStat.Increment()
				return err
			}
		}
		return nil
	}

	jr.config.slaveUploadTimeout = conf.GetDurationVar(10, time.Minute, "Warehouse.slaveUploadTimeout", "Warehouse.slaveUploadTimeoutInMin")
	jr.config.numLoadFileUploadWorkers = conf.GetInt("Warehouse.numLoadFileUploadWorkers", 8)
	jr.config.loadObjectFolder = conf.GetString("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects")

	jr.uploadTimeStat = jr.timerStat("load_file_upload_time")
	jr.totalUploadTimeStat = jr.timerStat("load_file_total_upload_time")
	jr.downloadStagingFileStat = jr.timerStat("download_staging_file_time")
	jr.processingStagingFileStat = jr.timerStat("process_staging_file_time")
	jr.bytesProcessedStagingFileStat = jr.counterStat("bytes_processed_in_staging_file")
	jr.bytesDownloadedStagingFileStat = jr.counterStat("bytes_downloaded_in_staging_file")
	jr.downloadStagingFileFailedStat = jr.stats.NewTaggedStat("worker_processing_download_staging_file_failed", stats.CountType, stats.Tags{
		"module":   "warehouse",
		"destID":   jr.job.DestinationID,
		"destType": jr.job.DestinationType,
	})
	jr.stagingFileDuplicateEvents = jr.counterStat("duplicate_events_in_staging_file")

	return jr
}

func (jr *jobRun) buildTags(extraTags ...warehouseutils.Tag) stats.Tags {
	tags := stats.Tags{
		"module":      "warehouse",
		"destType":    jr.job.DestinationType,
		"warehouseID": misc.GetTagName(jr.job.DestinationID, jr.job.SourceName, jr.job.DestinationName, misc.TailTruncateStr(jr.job.SourceID, 6)),
		"workspaceId": jr.job.WorkspaceID,
		"destID":      jr.job.DestinationID,
		"sourceID":    jr.job.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return tags
}

func (jr *jobRun) timerStat(name string, extraTags ...warehouseutils.Tag) stats.Timer {
	return jr.stats.NewTaggedStat(name, stats.TimerType, jr.buildTags(extraTags...))
}

func (jr *jobRun) counterStat(name string, extraTags ...warehouseutils.Tag) stats.Counter {
	return jr.stats.NewTaggedStat(name, stats.CountType, jr.buildTags(extraTags...))
}

// Returns the path where the staging file is/will be downloaded
func (jr *jobRun) path(stagingFileInfo stagingFileInfo) (string, error) {
	jr.stagingFilePathsMu.RLock()
	path, exists := jr.stagingFilePaths[stagingFileInfo.ID]
	jr.stagingFilePathsMu.RUnlock()
	if exists {
		return path, nil
	}

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", fmt.Errorf("creating tmp dir: %w", err)
	}

	dirName := "/" + misc.RudderWarehouseJsonUploadsTmp + "/" + "_" + strconv.Itoa(jr.workerIdx) + "/"
	filePath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s/`, jr.job.DestinationType, jr.job.DestinationID) + stagingFileInfo.Location
	if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return "", fmt.Errorf("creating staging file directory: %w", err)
	}

	jr.stagingFilePathsMu.Lock()
	jr.stagingFilePaths[stagingFileInfo.ID] = filePath
	jr.stagingFilePathsMu.Unlock()
	return filePath, nil
}

// loadFilePath generates a unique path for a load file based on the staging file path.
// Every call to this function will generate a new path even if the same staging file is used.
func (jr *jobRun) loadFilePath(stagingFileInfo stagingFileInfo) (string, error) {
	stagingFilePath, err := jr.path(stagingFileInfo)
	if err != nil {
		return "", fmt.Errorf("getting staging file path: %w", err)
	}

	return fmt.Sprintf("%s.%s.%s.%s",
		strings.TrimSuffix(stagingFilePath, ".json.gz"),
		jr.job.SourceID,
		misc.FastUUID().String(),
		warehouseutils.GetLoadFileFormat(jr.job.LoadFileType),
	), nil
}

// uploadLoadFiles returns the upload output for each file uploaded to object storage
func (jr *jobRun) uploadLoadFiles(ctx context.Context, modifier func(result uploadResult) uploadResult) ([]uploadResult, error) {
	ctx, cancel := context.WithTimeout(ctx, jr.config.slaveUploadTimeout)
	defer cancel()

	uploader, err := jr.job.fileManager(jr.job.DestinationConfig, jr.job.UseRudderStorage)
	if err != nil {
		return nil, fmt.Errorf("creating uploader: %w", err)
	}

	var totalUploadTime atomic.Duration

	defer func() {
		jr.totalUploadTimeStat.SendTiming(totalUploadTime.Load())
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

		uploadStart := jr.now()
		defer func() {
			jr.uploadTimeStat.SendTiming(jr.since(uploadStart))
		}()

		if slices.Contains(warehouseutils.TimeWindowDestinations, jr.job.DestinationType) {
			return uploader.Upload(
				ctx,
				file,
				warehouseutils.GetTablePathInObjectStorage(jr.job.DestinationNamespace, tableName),
				jr.job.LoadFilePrefix,
			)
		}
		return uploader.Upload(
			ctx,
			file,
			jr.config.loadObjectFolder,
			tableName,
			jr.job.SourceID,
			jr.bucketFolder(jr.job.UniqueLoadGenID, tableName),
		)
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
						loadFileUploadStart := jr.now()

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

						result := uploadResult{
							TableName:             tableName,
							Location:              uploadOutput.Location,
							TotalRows:             jr.tableEventCountMap[tableName],
							ContentLength:         loadFileStats.Size(),
							DestinationRevisionID: jr.job.DestinationRevisionID,
							UseRudderStorage:      jr.job.UseRudderStorage,
						}

						processStream <- &uploadProcessingResult{
							result: modifier(result),
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

func (jr *jobRun) bucketFolder(batchID, tableName string) string {
	return batchID + "-" + tableName
}

// reader should be called only if the staging file has been downloaded
func (jr *jobRun) reader(stagingFileInfo stagingFileInfo) (*gzip.Reader, error) {
	var stagingFile *os.File
	stagingFilePath, err := jr.path(stagingFileInfo)
	if err != nil {
		return nil, fmt.Errorf("getting staging file path: %w", err)
	}
	if stagingFile, err = os.Open(stagingFilePath); err != nil {
		return nil, fmt.Errorf("opening file at path:%s downloaded from %s: %w", stagingFilePath, stagingFileInfo.Location, err)
	}
	return gzip.NewReader(stagingFile)
}

// writer returns a writer for the table and an unlock function that MUST be called when done using the writer
// If two goroutines request a writer for the same table, they will block on the mutex until the first goroutine is done writing
func (jr *jobRun) writer(tableName string, stagingFileInfo stagingFileInfo) (encoding.LoadFileWriter, func(), error) {
	// Get or create mutex for this table
	jr.tableWriterMutexesMu.Lock()
	tableMutex, exists := jr.tableWriterMutexes[tableName]
	if !exists {
		tableMutex = &sync.Mutex{}
		jr.tableWriterMutexes[tableName] = tableMutex
	}
	jr.tableWriterMutexesMu.Unlock()

	// Lock the specific table mutex to ensure exclusive access
	tableMutex.Lock()

	jr.outputFileWritersMapMu.RLock()
	writer, exists := jr.outputFileWritersMap[tableName]
	jr.outputFileWritersMapMu.RUnlock()
	if exists {
		return writer, tableMutex.Unlock, nil
	}

	outputFilePath, err := jr.loadFilePath(stagingFileInfo)
	if err != nil {
		tableMutex.Unlock()
		return nil, nil, fmt.Errorf("failed to get output file path for table %s: %w", tableName, err)
	}

	writer, err = jr.encodingFactory.NewLoadFileWriter(jr.job.LoadFileType, outputFilePath, jr.job.UploadSchema[tableName], jr.job.DestinationType)
	if err != nil {
		tableMutex.Unlock()
		return nil, nil, fmt.Errorf("creating new writer for table %s: %w", tableName, err)
	}

	// Initialize event count for this table if not already initialized
	jr.tableEventCountMapMu.Lock()
	if _, exists := jr.tableEventCountMap[tableName]; !exists {
		jr.tableEventCountMap[tableName] = 0
	}
	jr.tableEventCountMapMu.Unlock()

	jr.outputFileWritersMapMu.Lock()
	jr.outputFileWritersMap[tableName] = writer
	jr.outputFileWritersMapMu.Unlock()

	return writer, tableMutex.Unlock, nil
}

func (jr *jobRun) cleanup() {
	// cleanup staging files
	for _, path := range jr.stagingFilePaths {
		misc.RemoveFilePaths(path)
	}

	// cleanup load files
	for _, writer := range jr.outputFileWritersMap {
		misc.RemoveFilePaths(writer.GetLoadFile().Name())
	}
}

func (jr *jobRun) closeLoadFiles() {
	for _, writer := range jr.outputFileWritersMap {
		if err := writer.Close(); err != nil {
			jr.logger.Errorf("Error while closing load file %s : %v", writer.GetLoadFile().Name(), err)
		}
	}
}

func (jr *jobRun) handleDiscardTypes(tableName, columnName string, columnVal interface{}, columnData types.Data, violatedConstraints *constraints.Violation, discardWriter encoding.LoadFileWriter, reason string) error {
	rowID, hasID := columnData[jr.job.columnName("id")]
	receivedAt, hasReceivedAt := columnData[jr.job.columnName("received_at")]

	if violatedConstraints.IsViolated {
		if !hasID {
			rowID = violatedConstraints.ViolatedIdentifier
			hasID = true
		}
		if !hasReceivedAt {
			receivedAt = jr.now().Format(misc.RFC3339Milli)
			hasReceivedAt = true
		}
	}
	if hasID && hasReceivedAt {
		eventLoader := jr.encodingFactory.NewEventLoader(discardWriter, jr.job.LoadFileType, jr.job.DestinationType)
		eventLoader.AddColumn("column_name", warehouseutils.DiscardsSchema["column_name"], columnName)
		eventLoader.AddColumn("column_value", warehouseutils.DiscardsSchema["column_value"], fmt.Sprintf("%v", columnVal))
		eventLoader.AddColumn("reason", warehouseutils.DiscardsSchema["reason"], reason)
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

		if err := eventLoader.Write(); err != nil {
			return fmt.Errorf("writing event to discards table: %w", err)
		}
	}
	return nil
}

func (jr *jobRun) incrementEventCount(tableName string) {
	jr.tableEventCountMapMu.Lock()
	jr.tableEventCountMap[tableName]++
	jr.tableEventCountMapMu.Unlock()
}
