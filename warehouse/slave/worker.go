package slave

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	"github.com/rudderlabs/rudder-server/warehouse/constraints"
	"github.com/rudderlabs/rudder-server/warehouse/utils/types"

	"github.com/rudderlabs/rudder-server/services/notifier"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	integrationsconfig "github.com/rudderlabs/rudder-server/warehouse/integrations/config"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/source"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"golang.org/x/sync/errgroup"
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
	StagingFileID         int64 // Only used for upload jobs
	UploadID              int64 // Only used for upload_v2 jobs
	DestinationRevisionID string
	UseRudderStorage      bool
}

type worker struct {
	conf               *config.Config
	log                logger.Logger
	statsFactory       stats.Stats
	notifier           slaveNotifier
	bcManager          *bcm.BackendConfigManager
	constraintsManager *constraints.Manager
	encodingFactory    *encoding.Factory
	workerIdx          int
	activeJobId        atomic.Int64
	refreshClaimJitter time.Duration

	config struct {
		maxStagingFileReadBufferCapacityInK config.ValueLoader[int]
		maxConcurrentStagingFiles           config.ValueLoader[int]
		claimRefreshInterval                config.ValueLoader[time.Duration]
		enableNotifierHeartbeat             config.ValueLoader[bool]
	}
	stats struct {
		workerIdleTime                 stats.Timer
		workerClaimProcessingSucceeded stats.Counter
		workerClaimProcessingFailed    stats.Counter
		workerClaimProcessingTime      stats.Timer
	}
}

type dedupKey struct {
	tableName string
	idValue   string
}

func newWorker(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	notifier slaveNotifier,
	bcManager *bcm.BackendConfigManager,
	constraintsManager *constraints.Manager,
	encodingFactory *encoding.Factory,
	workerIdx int,
) *worker {
	s := &worker{}

	s.conf = conf
	s.log = logger
	s.statsFactory = statsFactory
	s.notifier = notifier
	s.bcManager = bcManager
	s.constraintsManager = constraintsManager
	s.encodingFactory = encodingFactory
	s.workerIdx = workerIdx

	s.config.maxStagingFileReadBufferCapacityInK = s.conf.GetReloadableIntVar(10240, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
	// Increasing maxConcurrentStagingFiles config would also require increasing the memory requests for the slave pods
	s.config.maxConcurrentStagingFiles = s.conf.GetReloadableIntVar(10, 1, "Warehouse.maxStagingFilesInUploadV2Job")
	s.config.claimRefreshInterval = s.conf.GetReloadableDurationVar(30, time.Second, "Warehouse.claimRefreshIntervalInS")
	s.config.enableNotifierHeartbeat = s.conf.GetReloadableBoolVar(false, "Warehouse.enableNotifierHeartbeat")

	tags := stats.Tags{
		"module":   "warehouse",
		"workerId": strconv.Itoa(workerIdx),
	}
	s.stats.workerIdleTime = s.statsFactory.NewTaggedStat("worker_idle_time", stats.TimerType, tags)
	s.stats.workerClaimProcessingTime = s.statsFactory.NewTaggedStat("worker_claim_processing_time", stats.TimerType, tags)
	s.stats.workerClaimProcessingSucceeded = s.statsFactory.NewTaggedStat("worker_claim_processing_succeeded", stats.CountType, tags)
	s.stats.workerClaimProcessingFailed = s.statsFactory.NewTaggedStat("worker_claim_processing_failed", stats.CountType, tags)
	s.refreshClaimJitter = time.Duration(rand.Int63n(5)) * time.Second // Random jitter between [0-5) seconds
	return s
}

func (w *worker) start(ctx context.Context, notificationChan <-chan *notifier.ClaimJob, slaveID string) {
	workerIdleTimeStart := time.Now()

	if w.config.enableNotifierHeartbeat.Load() {
		refreshCtx, refreshCancel := context.WithCancel(ctx)
		defer refreshCancel()
		go w.runClaimRefresh(refreshCtx)
	}

	for {
		select {
		case <-ctx.Done():
			w.log.Infon("Slave worker is shutting down",
				logger.NewField("workerIdx", w.workerIdx),
				logger.NewField("slaveId", slaveID),
			)
			return
		case claimedJob, ok := <-notificationChan:
			if !ok {
				return
			}
			w.stats.workerIdleTime.Since(workerIdleTimeStart)

			w.log.Debugn("Successfully claimed job by slave worker",
				logger.NewField("jobId", claimedJob.Job.ID),
				logger.NewField("workerIdx", w.workerIdx),
				logger.NewField("slaveId", slaveID),
				logger.NewField("jobType", claimedJob.Job.Type),
			)

			// Set active job ID
			w.activeJobId.Store(claimedJob.Job.ID)

			switch claimedJob.Job.Type {
			case notifier.JobTypeAsync:
				w.processClaimedSourceJob(ctx, claimedJob)
			default:
				w.processClaimedUploadJob(ctx, claimedJob)
			}

			w.log.Infon("Successfully processed job",
				logger.NewField("jobId", claimedJob.Job.ID),
				logger.NewField("workerIdx", w.workerIdx),
				logger.NewField("slaveId", slaveID),
			)
			// Clear active job ID after processing
			w.activeJobId.Store(0)

			workerIdleTimeStart = time.Now()
		}
	}
}

// run claimRefresh periodically to make sure that job is not orphaned
func (w *worker) runClaimRefresh(ctx context.Context) {
	baseInterval := w.config.claimRefreshInterval.Load()
	// Distribute claim refresh requests across workers by adding random delay
	// This will prevent database load spikes from synchronized refresh attempts
	ticker := time.NewTicker(baseInterval + w.refreshClaimJitter)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.activeJobId.Load() != 0 {
				if err := w.notifier.RefreshClaim(ctx, w.activeJobId.Load()); err != nil {
					w.log.Errorf("Failed to refresh claim for job %d: %v", w.activeJobId.Load(), err)
				}
			}
		}
	}
}

// processClaimedUploadJob processes a claimed upload job from the notifier.
// It supports both regular upload jobs (one staging file per job) and upload_v2 jobs (multiple staging files per job).
func (w *worker) processClaimedUploadJob(ctx context.Context, claimedJob *notifier.ClaimJob) {
	w.stats.workerClaimProcessingTime.RecordDuration()()

	handleErr := func(err error, claimedJob *notifier.ClaimJob) {
		w.stats.workerClaimProcessingFailed.Increment()

		w.notifier.UpdateClaim(ctx, claimedJob, &notifier.ClaimJobResponse{
			Err: err,
		})
	}

	var (
		jobJSON []byte
		err     error
	)

	switch claimedJob.Job.Type {
	case notifier.JobTypeUploadV2:
		var job payloadV2
		if err = jsonrs.Unmarshal(claimedJob.Job.Payload, &job); err != nil {
			handleErr(err, claimedJob)
			return
		}
		job.BatchID = claimedJob.Job.BatchID
		w.log.Infon("Starting processing staging-files from claim",
			logger.NewField("jobId", claimedJob.Job.ID),
		)
		job.Output, err = w.processMultiStagingFiles(ctx, &job)
		if err != nil {
			handleErr(err, claimedJob)
			return
		}
		jobJSON, err = jsonrs.Marshal(job)
	default:
		var job payload
		if err = jsonrs.Unmarshal(claimedJob.Job.Payload, &job); err != nil {
			handleErr(err, claimedJob)
			return
		}
		job.BatchID = claimedJob.Job.BatchID
		w.log.Infon("Starting processing staging-file from claim",
			logger.NewField("stagingFileID", job.StagingFileID),
			logger.NewField("jobId", claimedJob.Job.ID),
		)
		job.Output, err = w.processStagingFile(ctx, &job)
		if err != nil {
			handleErr(err, claimedJob)
			return
		}
		jobJSON, err = jsonrs.Marshal(job)
	}

	if err != nil {
		handleErr(err, claimedJob)
		return
	}

	w.stats.workerClaimProcessingSucceeded.Increment()

	w.notifier.UpdateClaim(ctx, claimedJob, &notifier.ClaimJobResponse{
		Payload: jobJSON,
	})
}

// processSingleStagingFile processes a single staging file and writes its data to the appropriate load files.
// It handles downloading, reading, and processing the file contents.
func (w *worker) processSingleStagingFile(
	ctx context.Context,
	jr *jobRun,
	job *basePayload,
	stagingFile stagingFileInfo,
) error {
	processingStart := jr.now()
	var err error

	if err := jr.downloadStagingFile(ctx, stagingFile); err != nil {
		return fmt.Errorf("downloading staging file %d: %w", stagingFile.ID, err)
	}
	var stagingFileReader io.ReadCloser

	if stagingFileReader, err = jr.reader(stagingFile); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return fmt.Errorf("creating reader for staging file %d: %w", stagingFile.ID, err)
	}
	defer func() {
		if err := stagingFileReader.Close(); err != nil {
			jr.logger.Errorf("Error closing staging file reader: %v", err)
		}
	}()

	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := w.config.maxStagingFileReadBufferCapacityInK.Load() * 1024

	bufScanner := bufio.NewScanner(stagingFileReader)
	bufScanner.Buffer(make([]byte, maxCapacity), maxCapacity)

	var lineBytesCounter int
	var interfaceSliceSample []interface{}
	columnCountLimitMap := integrationsconfig.ColumnCountLimitMap(jr.conf)
	discardsTable := job.discardsTable()
	sortedTableColumnMap := job.sortedColumnMapForAllTables()

	tableIDColumnSet := make(map[dedupKey]struct{})
	duplicateCount := 0

	for {
		ok := bufScanner.Scan()
		if !ok {
			if scanErr := bufScanner.Err(); scanErr != nil {
				jr.logger.Errorf("WH: Error in scanner reading line from staging file: %v", scanErr)
			}
			break
		}

		lineBytes := bufScanner.Bytes()
		lineBytesCounter += len(lineBytes)

		var (
			batchRouterEvent types.BatchRouterEvent
			writer           encoding.LoadFileWriter
			releaseWriter    func()
		)

		if err := jsonrs.Unmarshal(lineBytes, &batchRouterEvent); err != nil {
			jr.logger.Warnn("Failed to unmarshal line from staging file to BatchRouterEvent",
				logger.NewIntField("stagingFileID", stagingFile.ID),
				obskit.Error(err),
			)
			continue
		}

		tableName := batchRouterEvent.Metadata.Table
		columnData := batchRouterEvent.Data

		if job.DestinationType == warehouseutils.S3Datalake && len(sortedTableColumnMap[tableName]) > columnCountLimitMap[warehouseutils.S3Datalake] {
			return fmt.Errorf("staging file schema limit exceeded for stagingFileID: %d, actualCount: %d, maxAllowedCount: %d",
				stagingFile.ID,
				len(sortedTableColumnMap[tableName]),
				columnCountLimitMap[warehouseutils.S3Datalake],
			)
		}

		// Create separate load file for each table
		if writer, releaseWriter, err = jr.writer(tableName, stagingFile); err != nil {
			return err
		}

		eventLoader := w.encodingFactory.NewEventLoader(writer, job.LoadFileType, job.DestinationType)

		// Duplicate detection by id column
		iDVal, ok := columnData[job.columnName("id")]
		if ok {
			iDStr, ok := iDVal.(string)
			if ok {
				dedupKey := dedupKey{tableName: tableName, idValue: iDStr}
				if _, exists := tableIDColumnSet[dedupKey]; exists {
					duplicateCount++
				} else {
					tableIDColumnSet[dedupKey] = struct{}{}
				}
			}
		}

		for _, columnName := range sortedTableColumnMap[tableName] {
			if eventLoader.IsLoadTimeColumn(columnName) {
				timestampFormat := eventLoader.GetLoadTimeFormat(columnName)
				eventLoader.AddColumn(job.columnName(columnName), job.UploadSchema[tableName][columnName], jr.uuidTS.Format(timestampFormat))
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
				case model.BooleanDataType:
					newColumnVal := 0

					if k, ok := columnVal.(bool); ok {
						if k {
							newColumnVal = 1
						}
					}

					columnVal = newColumnVal
				case model.ArrayOfBooleanDataType:
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

			if columnType == model.IntDataType || columnType == model.BigIntDataType {
				floatVal, ok := columnVal.(float64)
				if !ok {
					eventLoader.AddEmptyColumn(columnName)
					continue
				}
				columnVal = int(floatVal)
			}

			dataTypeInSchema, ok := job.UploadSchema[tableName][columnName]

			violatedConstraints := w.constraintsManager.ViolatedConstraints(job.DestinationType, &batchRouterEvent, columnName)

			if ok && ((columnType != dataTypeInSchema) || (violatedConstraints.IsViolated)) {
				newColumnVal, convError := HandleSchemaChange(
					dataTypeInSchema,
					columnType,
					columnVal,
				)

				if convError != nil || violatedConstraints.IsViolated {
					var reason string
					if violatedConstraints.IsViolated {
						reason = violatedConstraints.Reason
					} else if convError != nil {
						reason = convError.Error()
					} else {
						reason = "unknown reason"
					}

					if violatedConstraints.IsViolated {
						eventLoader.AddColumn(columnName, job.UploadSchema[tableName][columnName], violatedConstraints.ViolatedIdentifier)
					} else {
						eventLoader.AddEmptyColumn(columnName)
					}

					discardWriter, releaseDiscardWriter, err := jr.writer(discardsTable, stagingFile)
					if err != nil {
						return err
					}

					err = jr.handleDiscardTypes(tableName, columnName, columnVal, columnData, violatedConstraints, discardWriter, reason)
					if err != nil {
						jr.logger.Errorf("Failed to write to discards: %v", err)
					}
					releaseDiscardWriter()

					jr.incrementEventCount(discardsTable)
					continue
				}

				columnVal = newColumnVal
			}

			// Special handling for JSON arrays
			// TODO: Will this work for both BQ and RS?
			if reflect.TypeOf(columnVal) == reflect.TypeOf(interfaceSliceSample) {
				marshalledVal, err := jsonrs.Marshal(columnVal)
				if err != nil {
					eventLoader.AddEmptyColumn(columnName)
					continue
				}

				columnVal = string(marshalledVal)
			}

			eventLoader.AddColumn(columnName, job.UploadSchema[tableName][columnName], columnVal)
		}

		if err = eventLoader.Write(); err != nil {
			return err
		}
		releaseWriter()

		jr.incrementEventCount(tableName)
	}

	// After processing all lines, increment the metric for duplicates
	if duplicateCount > 0 {
		jr.stagingFileDuplicateEvents.Count(duplicateCount)
		jr.logger.Infon("Found duplicate events in staging file",
			logger.NewField("stagingFileID", stagingFile.ID),
			logger.NewIntField("duplicateEvents", int64(duplicateCount)),
		)
	}

	jr.logger.Debugn("Process bytes from downloaded staging file",
		logger.NewField("bytes", lineBytesCounter),
		logger.NewField("location", stagingFile.Location),
	)
	jr.processingStagingFileStat.Since(processingStart)
	jr.bytesProcessedStagingFileStat.Count(lineBytesCounter)

	return nil
}

func (w *worker) processStagingFile(ctx context.Context, job *payload) ([]uploadResult, error) {
	processStartTime := time.Now()

	jr := newJobRun(job.basePayload, w.workerIdx, w.conf, w.log, w.statsFactory, w.encodingFactory)

	w.log.Debugn("Starting processing staging file",
		logger.NewField("stagingFileID", job.StagingFileID),
		logger.NewField("stagingFileLocation", job.StagingFileLocation),
		logger.NewField("identifier", jr.identifier),
	)

	defer func() {
		jr.counterStat("staging_files_processed", warehouseutils.Tag{Name: "worker_id", Value: strconv.Itoa(w.workerIdx)}).Count(1)
		jr.timerStat("staging_files_total_processing_time", warehouseutils.Tag{Name: "worker_id", Value: strconv.Itoa(w.workerIdx)}).Since(processStartTime)
		jr.cleanup()
	}()

	// Initialize Discards Table
	discardsTable := job.discardsTable()
	jr.tableEventCountMap[discardsTable] = 0

	// Process the staging file
	if err := w.processSingleStagingFile(ctx, jr, &job.basePayload, stagingFileInfo{
		ID:       job.StagingFileID,
		Location: job.StagingFileLocation,
	}); err != nil {
		return nil, err
	}

	jr.closeLoadFiles()
	return jr.uploadLoadFiles(ctx, func(result uploadResult) uploadResult {
		result.StagingFileID = job.StagingFileID
		return result
	})
}

func (w *worker) processMultiStagingFiles(ctx context.Context, job *payloadV2) ([]uploadResult, error) {
	processStartTime := time.Now()

	// Create a jobRun for all staging files
	jr := newJobRun(job.basePayload, w.workerIdx, w.conf, w.log, w.statsFactory, w.encodingFactory)

	defer func() {
		jr.counterStat("staging_files_processed", warehouseutils.Tag{Name: "worker_id", Value: strconv.Itoa(w.workerIdx)}).Count(len(job.StagingFiles))
		jr.timerStat("staging_files_total_processing_time", warehouseutils.Tag{Name: "worker_id", Value: strconv.Itoa(w.workerIdx)}).Since(processStartTime)
		jr.cleanup()
	}()

	// Initialize Discards Table
	discardsTable := job.discardsTable()
	jr.tableEventCountMap[discardsTable] = 0

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(w.config.maxConcurrentStagingFiles.Load())

	for _, stagingFile := range job.StagingFiles {
		g.Go(func() error {
			w.log.Infon("Processing staging-file for upload_v2 job",
				logger.NewField("stagingFileID", stagingFile.ID),
			)
			if err := w.processSingleStagingFile(gCtx, jr, &job.basePayload, stagingFile); err != nil {
				return fmt.Errorf("processing staging file %d: %w", stagingFile.ID, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	jr.closeLoadFiles()
	return jr.uploadLoadFiles(ctx, func(result uploadResult) uploadResult {
		result.UploadID = job.UploadID
		return result
	})
}

func (w *worker) processClaimedSourceJob(ctx context.Context, claimedJob *notifier.ClaimJob) {
	handleErr := func(err error, claimedJob *notifier.ClaimJob) {
		w.log.Errorf("Error processing claim: %v", err)

		w.notifier.UpdateClaim(ctx, claimedJob, &notifier.ClaimJobResponse{
			Err: err,
		})
	}

	var (
		job source.NotifierRequest
		err error
	)

	if err := jsonrs.Unmarshal(claimedJob.Job.Payload, &job); err != nil {
		handleErr(err, claimedJob)
		return
	}

	err = w.runSourceJob(ctx, job)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}

	jobResultJSON, err := jsonrs.Marshal(source.NotifierResponse{
		ID: job.ID,
	})
	if err != nil {
		handleErr(err, claimedJob)
		return
	}

	w.notifier.UpdateClaim(ctx, claimedJob, &notifier.ClaimJobResponse{
		Payload: jobResultJSON,
	})
}

func (w *worker) runSourceJob(ctx context.Context, sourceJob source.NotifierRequest) error {
	warehouse, err := w.destinationFromSlaveConnectionMap(sourceJob.DestinationID, sourceJob.SourceID)
	if err != nil {
		return fmt.Errorf("getting warehouse: %w", err)
	}

	integrationsManager, err := manager.NewWarehouseOperations(warehouse.Destination.DestinationDefinition.Name, w.conf, w.log, w.statsFactory)
	if err != nil {
		return fmt.Errorf("getting integrations manager: %w", err)
	}

	integrationsManager.SetConnectionTimeout(warehouseutils.GetConnectionTimeout(
		warehouse.Destination.DestinationDefinition.Name,
		warehouse.Destination.ID,
	))

	err = integrationsManager.Setup(ctx, warehouse, &source.Uploader{})
	if err != nil {
		return fmt.Errorf("setting up integrations manager: %w", err)
	}
	defer integrationsManager.Cleanup(ctx)

	var metadata warehouseutils.DeleteByMetaData
	if err = jsonrs.Unmarshal(sourceJob.MetaData, &metadata); err != nil {
		return fmt.Errorf("unmarshalling metadata: %w", err)
	}

	switch sourceJob.JobType {
	case model.SourceJobTypeDeleteByJobRunID.String():
		err = integrationsManager.DeleteBy(ctx, []string{sourceJob.TableName}, warehouseutils.DeleteByParams{
			SourceId:  sourceJob.SourceID,
			TaskRunId: metadata.TaskRunId,
			JobRunId:  metadata.JobRunId,
			StartTime: metadata.StartTime,
		})
	default:
		err = errors.New("invalid sourceJob type")
	}
	return err
}

func (w *worker) destinationFromSlaveConnectionMap(destinationId, sourceId string) (model.Warehouse, error) {
	if destinationId == "" || sourceId == "" {
		return model.Warehouse{}, errors.New("invalid Parameters")
	}

	sourceMap, ok := w.bcManager.ConnectionSourcesMap(destinationId)
	if !ok {
		return model.Warehouse{}, errors.New("invalid Destination Id")
	}

	conn, ok := sourceMap[sourceId]
	if !ok {
		return model.Warehouse{}, errors.New("invalid Source Id")
	}

	return conn, nil
}

// HandleSchemaChange checks if the existing column type is compatible with the new column type
func HandleSchemaChange(existingDataType, currentDataType model.SchemaType, value any) (any, error) {
	var (
		newColumnVal any
		err          error
	)

	if existingDataType == model.StringDataType || existingDataType == model.TextDataType {
		// only stringify if the previous type is non-string/text/json
		if currentDataType != model.StringDataType && currentDataType != model.TextDataType && currentDataType != model.JSONDataType {
			newColumnVal = fmt.Sprintf("%v", value)
		} else {
			newColumnVal = value
		}
	} else if (currentDataType == model.IntDataType || currentDataType == model.BigIntDataType) && existingDataType == model.FloatDataType {
		intVal, ok := value.(int)
		if !ok {
			err = fmt.Errorf("incompatible schema conversion from %v to %v", existingDataType, currentDataType)
		} else {
			newColumnVal = float64(intVal)
		}
	} else if currentDataType == model.FloatDataType && (existingDataType == model.IntDataType || existingDataType == model.BigIntDataType) {
		floatVal, ok := value.(float64)
		if !ok {
			err = fmt.Errorf("incompatible schema conversion from %v to %v", existingDataType, currentDataType)
		} else {
			newColumnVal = int(floatVal)
		}
	} else if existingDataType == model.JSONDataType {
		var interfaceSliceSample []any
		if currentDataType == model.IntDataType || currentDataType == model.FloatDataType || currentDataType == model.BooleanDataType {
			newColumnVal = fmt.Sprintf("%v", value)
		} else if reflect.TypeOf(value) == reflect.TypeOf(interfaceSliceSample) {
			newColumnVal = value
		} else {
			newColumnVal = strconv.Quote(fmt.Sprintf("%v", value))
		}
	} else {
		err = fmt.Errorf("incompatible schema conversion from %v to %v", existingDataType, currentDataType)
	}

	return newColumnVal, err
}
