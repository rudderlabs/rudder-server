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
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/constraints"
	"github.com/rudderlabs/rudder-server/warehouse/utils/types"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type payload struct {
	BatchID                      string
	UploadID                     int64
	StagingFileID                int64
	StagingFileLocation          string
	UploadSchema                 model.Schema
	WorkspaceID                  string
	SourceID                     string
	SourceName                   string
	DestinationID                string
	DestinationName              string
	DestinationType              string
	DestinationNamespace         string
	DestinationRevisionID        string
	StagingDestinationRevisionID string
	DestinationConfig            map[string]interface{}
	StagingDestinationConfig     interface{}
	UseRudderStorage             bool
	StagingUseRudderStorage      bool
	UniqueLoadGenID              string
	RudderStoragePrefix          string
	Output                       []uploadResult
	LoadFilePrefix               string // prefix for the load file name
	LoadFileType                 string
}

func (p *payload) discardsTable() string {
	return warehouseutils.ToProviderCase(p.DestinationType, warehouseutils.DiscardsTable)
}

func (p *payload) columnName(columnName string) string {
	return warehouseutils.ToProviderCase(p.DestinationType, columnName)
}

// sortedColumnMapForAllTables Sort columns per table to maintain same order in load file (needed in case of csv load file)
func (p *payload) sortedColumnMapForAllTables() map[string][]string {
	return lo.MapValues(p.UploadSchema, func(value model.TableSchema, key string) []string {
		columns := lo.Keys(value)
		sort.Strings(columns)
		return columns
	})
}

func (p *payload) fileManager(config interface{}, useRudderStorage bool) (filemanager.FileManager, error) {
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
	})
	return fileManager, err
}

func (p *payload) pickupStagingConfiguration() bool {
	return p.StagingDestinationRevisionID != p.DestinationRevisionID && p.StagingDestinationConfig != nil
}

// jobRun Temporary store for processing staging file to load file
type jobRun struct {
	job                  payload
	stagingFilePath      string
	uuidTS               time.Time
	outputFileWritersMap map[string]encoding.LoadFileWriter
	tableEventCountMap   map[string]int
	stagingFileReader    *gzip.Reader
	identifier           string
	since                func(time.Time) time.Duration
	logger               logger.Logger
	encodingFactory      *encoding.Factory

	now func() time.Time

	stats                          stats.Stats
	conf                           *config.Config
	uploadTimeStat                 stats.Measurement
	totalUploadTimeStat            stats.Measurement
	downloadStagingFileStat        stats.Measurement
	processingStagingFileStat      stats.Measurement
	bytesProcessedStagingFileStat  stats.Measurement
	bytesDownloadedStagingFileStat stats.Measurement
	downloadStagingFileFailedStat  stats.Measurement

	config struct {
		numLoadFileUploadWorkers int
		slaveUploadTimeout       time.Duration
		loadObjectFolder         string
	}
}

func newJobRun(job payload, conf *config.Config, log logger.Logger, stat stats.Stats, encodingFactory *encoding.Factory) jobRun {
	jr := jobRun{
		job:             job,
		identifier:      warehouseutils.GetWarehouseIdentifier(job.DestinationType, job.SourceID, job.DestinationID),
		stats:           stat,
		conf:            conf,
		since:           time.Since,
		logger:          log,
		now:             timeutil.Now,
		encodingFactory: encodingFactory,
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

	jr.outputFileWritersMap = make(map[string]encoding.LoadFileWriter)
	jr.tableEventCountMap = make(map[string]int)

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

func (jr *jobRun) timerStat(name string, extraTags ...warehouseutils.Tag) stats.Measurement {
	return jr.stats.NewTaggedStat(name, stats.TimerType, jr.buildTags(extraTags...))
}

func (jr *jobRun) counterStat(name string, extraTags ...warehouseutils.Tag) stats.Measurement {
	return jr.stats.NewTaggedStat(name, stats.CountType, jr.buildTags(extraTags...))
}

// getStagingFilePath Get download path for the job. Also creates missing directories for this path
func (jr *jobRun) getStagingFilePath(index int) (string, error) {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", fmt.Errorf("creating tmp dir: %w", err)
	}

	dirName := "/" + misc.RudderWarehouseJsonUploadsTmp + "/" + "_" + strconv.Itoa(index) + "/"
	filePath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s/`, jr.job.DestinationType, jr.job.DestinationID) + jr.job.StagingFileLocation
	if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return "", fmt.Errorf("creating staging file directory: %w", err)
	}

	return filePath, nil
}

// downloadStagingFile Download Staging file for the job
// If error occurs with the current config and current revision is different from staging revision
// We retry with the staging revision config if it is present
func (jr *jobRun) downloadStagingFile(ctx context.Context) error {
	doTask := func(config interface{}, useRudderStorage bool) error {
		var file *os.File
		var err error

		if file, err = os.Create(jr.stagingFilePath); err != nil {
			return fmt.Errorf("creating file at path:%s downloaded from %s: %w",
				jr.stagingFilePath,
				jr.job.StagingFileLocation,
				err,
			)
		}

		downloader, err := jr.job.fileManager(config, useRudderStorage)
		if err != nil {
			return fmt.Errorf("creating file manager: %w", err)
		}

		downloadStart := jr.now()
		if err = downloader.Download(ctx, file, jr.job.StagingFileLocation); err != nil {
			return fmt.Errorf("downloading staging file from %s: %w", jr.job.StagingFileLocation, err)
		}
		if err = file.Close(); err != nil {
			return fmt.Errorf("closing file after download: %w", err)
		}

		jr.downloadStagingFileStat.Since(downloadStart)

		fileInfo, err := os.Stat(jr.stagingFilePath)
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

		jr.logger.Infof("[WH]: Starting processing staging file with revision config for StagingFileID: %d, DestinationRevisionID: %s, StagingDestinationRevisionID: %s, identifier: %s",
			jr.job.StagingFileID,
			jr.job.DestinationRevisionID,
			jr.job.StagingDestinationRevisionID,
			jr.identifier,
		)

		if err := doTask(jr.job.StagingDestinationConfig, jr.job.StagingUseRudderStorage); err != nil {
			jr.downloadStagingFileFailedStat.Increment()
			return err
		}
	}
	return nil
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

func (jr *jobRun) bucketFolder(batchID, tableName string) string {
	return batchID + "-" + tableName
}

func (jr *jobRun) reader() (*gzip.Reader, error) {
	var stagingFile *os.File
	var err error
	var reader *gzip.Reader

	if stagingFile, err = os.Open(jr.stagingFilePath); err != nil {
		return nil, fmt.Errorf("opening file at path:%s downloaded from %s: %w", jr.stagingFilePath, jr.job.StagingFileLocation, err)
	}

	if reader, err = gzip.NewReader(stagingFile); err != nil {
		return nil, fmt.Errorf("creating gzip reader at path:%s downloaded from %s: %w", jr.stagingFilePath, jr.job.StagingFileLocation, err)
	}
	return reader, nil
}

func (jr *jobRun) writer(tableName string) (encoding.LoadFileWriter, error) {
	if writer, ok := jr.outputFileWritersMap[tableName]; ok {
		return writer, nil
	}

	outputFilePath := jr.loadFilePath()

	writer, err := jr.encodingFactory.NewLoadFileWriter(jr.job.LoadFileType, outputFilePath, jr.job.UploadSchema[tableName], jr.job.DestinationType)
	if err != nil {
		return nil, err
	}

	jr.outputFileWritersMap[tableName] = writer
	jr.tableEventCountMap[tableName] = 0

	return writer, nil
}

func (jr *jobRun) loadFilePath() string {
	return fmt.Sprintf("%s.%s.%s.%s",
		strings.TrimSuffix(jr.stagingFilePath, ".json.gz"),
		jr.job.SourceID,
		misc.FastUUID().String(),
		warehouseutils.GetLoadFileFormat(jr.job.LoadFileType),
	)
}

func (jr *jobRun) cleanup() {
	if jr.stagingFileReader != nil {
		err := jr.stagingFileReader.Close()
		if err != nil {
			jr.logger.Warnf("[WH]: Failed to close staging file: %v", err)
		}
	}

	if jr.stagingFilePath != "" {
		misc.RemoveFilePaths(jr.stagingFilePath)
	}

	for _, writer := range jr.outputFileWritersMap {
		misc.RemoveFilePaths(writer.GetLoadFile().Name())
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
