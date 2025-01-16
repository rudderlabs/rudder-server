package slave

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

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
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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

type worker struct {
	conf               *config.Config
	log                logger.Logger
	statsFactory       stats.Stats
	notifier           slaveNotifier
	bcManager          *bcm.BackendConfigManager
	constraintsManager *constraints.Manager
	encodingFactory    *encoding.Factory
	workerIdx          int

	config struct {
		maxStagingFileReadBufferCapacityInK config.ValueLoader[int]
	}
	stats struct {
		workerIdleTime                 stats.Measurement
		workerClaimProcessingSucceeded stats.Measurement
		workerClaimProcessingFailed    stats.Measurement
		workerClaimProcessingTime      stats.Measurement
	}
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

	tags := stats.Tags{
		"module":   "warehouse",
		"workerId": strconv.Itoa(workerIdx),
	}
	s.stats.workerIdleTime = s.statsFactory.NewTaggedStat("worker_idle_time", stats.TimerType, tags)
	s.stats.workerClaimProcessingTime = s.statsFactory.NewTaggedStat("worker_claim_processing_time", stats.TimerType, tags)
	s.stats.workerClaimProcessingSucceeded = s.statsFactory.NewTaggedStat("worker_claim_processing_succeeded", stats.CountType, tags)
	s.stats.workerClaimProcessingFailed = s.statsFactory.NewTaggedStat("worker_claim_processing_failed", stats.CountType, tags)
	return s
}

func (w *worker) start(ctx context.Context, notificationChan <-chan *notifier.ClaimJob, slaveID string) {
	workerIdleTimeStart := time.Now()

	for {
		select {
		case <-ctx.Done():
			w.log.Infof("Slave worker-%d-%s is shutting down", w.workerIdx, slaveID)
			return
		case claimedJob, ok := <-notificationChan:
			if !ok {
				return
			}
			w.stats.workerIdleTime.Since(workerIdleTimeStart)

			w.log.Debugf("Successfully claimed job:%d by slave worker-%d-%s & job type %s",
				claimedJob.Job.ID,
				w.workerIdx,
				slaveID,
				claimedJob.Job.Type,
			)

			switch claimedJob.Job.Type {
			case notifier.JobTypeAsync:
				w.processClaimedSourceJob(ctx, claimedJob)
			default:
				w.processClaimedUploadJob(ctx, claimedJob)
			}

			w.log.Infof("Successfully processed job:%d by slave worker-%d-%s",
				claimedJob.Job.ID,
				w.workerIdx,
				slaveID,
			)

			workerIdleTimeStart = time.Now()
		}
	}
}

func (w *worker) processClaimedUploadJob(ctx context.Context, claimedJob *notifier.ClaimJob) {
	w.stats.workerClaimProcessingTime.RecordDuration()()

	handleErr := func(err error, claimedJob *notifier.ClaimJob) {
		w.stats.workerClaimProcessingFailed.Increment()

		w.notifier.UpdateClaim(ctx, claimedJob, &notifier.ClaimJobResponse{
			Err: err,
		})
	}

	var (
		job     payload
		jobJSON []byte
		err     error
	)

	if err = json.Unmarshal(claimedJob.Job.Payload, &job); err != nil {
		handleErr(err, claimedJob)
		return
	}

	w.log.Infof(`Starting processing staging-file:%v from claim:%v`, job.StagingFileID, claimedJob.Job.ID)

	job.BatchID = claimedJob.Job.BatchID
	job.Output, err = w.processStagingFile(ctx, job)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}

	if jobJSON, err = json.Marshal(job); err != nil {
		handleErr(err, claimedJob)
		return
	}

	w.stats.workerClaimProcessingSucceeded.Increment()

	w.notifier.UpdateClaim(ctx, claimedJob, &notifier.ClaimJobResponse{
		Payload: jobJSON,
	})
}

// This function is triggered when warehouse-master creates a new entry in wh_uploads table
// This is executed in the context of the warehouse-slave/worker and does the following:
//
// 1. Download the Staging file into a tmp directory
// 2. Transform the staging file into multiple load files (One file per output table)
// 3. Uploads these load files to Object storage
// 4. Save entries for the generated load files in wh_load_files table
// 5. Delete the staging and load files from tmp directory
func (w *worker) processStagingFile(ctx context.Context, job payload) ([]uploadResult, error) {
	processStartTime := time.Now()

	jr := newJobRun(job, w.conf, w.log, w.statsFactory, w.encodingFactory)

	w.log.Debugf("Starting processing staging file: %v at %s for %s",
		job.StagingFileID,
		job.StagingFileLocation,
		jr.identifier,
	)

	defer func() {
		jr.counterStat("staging_files_processed", warehouseutils.Tag{Name: "worker_id", Value: strconv.Itoa(w.workerIdx)}).Count(1)
		jr.timerStat("staging_files_total_processing_time", warehouseutils.Tag{Name: "worker_id", Value: strconv.Itoa(w.workerIdx)}).Since(processStartTime)

		jr.cleanup()
	}()

	var (
		err                  error
		lineBytesCounter     int
		interfaceSliceSample []interface{}
	)

	if jr.stagingFilePath, err = jr.getStagingFilePath(w.workerIdx); err != nil {
		return nil, err
	}
	if err = jr.downloadStagingFile(ctx); err != nil {
		return nil, err
	}
	if jr.stagingFileReader, err = jr.reader(); errors.Is(err, io.EOF) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	jr.uuidTS = jr.now()

	// Initialize Discards Table
	discardsTable := job.discardsTable()
	jr.tableEventCountMap[discardsTable] = 0

	processingStart := jr.now()
	sortedTableColumnMap := job.sortedColumnMapForAllTables()

	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := w.config.maxStagingFileReadBufferCapacityInK.Load() * 1024

	bufScanner := bufio.NewScanner(jr.stagingFileReader)
	bufScanner.Buffer(make([]byte, maxCapacity), maxCapacity)

	columnCountLimitMap := integrationsconfig.ColumnCountLimitMap(jr.conf)

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
		)

		if err := json.Unmarshal(lineBytes, &batchRouterEvent); err != nil {
			jr.logger.Warnn("Failed to unmarshal line from staging file to BatchRouterEvent",
				logger.NewIntField("stagingFileID", job.StagingFileID),
				obskit.Error(err),
			)
			continue
		}

		tableName := batchRouterEvent.Metadata.Table
		columnData := batchRouterEvent.Data

		if job.DestinationType == warehouseutils.S3Datalake && len(sortedTableColumnMap[tableName]) > columnCountLimitMap[warehouseutils.S3Datalake] {
			return nil, fmt.Errorf("staging file schema limit exceeded for stagingFileID: %d, actualCount: %d",
				job.StagingFileID,
				len(sortedTableColumnMap[tableName]),
			)
		}

		// Create separate load file for each table
		if writer, err = jr.writer(tableName); err != nil {
			return nil, err
		}

		eventLoader := w.encodingFactory.NewEventLoader(writer, job.LoadFileType, job.DestinationType)

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

					jr.outputFileWritersMap[discardsTable], err = jr.writer(discardsTable)
					if err != nil {
						return nil, err
					}

					err = jr.handleDiscardTypes(tableName, columnName, columnVal, columnData, violatedConstraints, jr.outputFileWritersMap[discardsTable], reason)
					if err != nil {
						jr.logger.Errorf("Failed to write to discards: %v", err)
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
					eventLoader.AddEmptyColumn(columnName)
					continue
				}

				columnVal = string(marshalledVal)
			}

			eventLoader.AddColumn(columnName, job.UploadSchema[tableName][columnName], columnVal)
		}

		if err = eventLoader.Write(); err != nil {
			return nil, err
		}

		jr.tableEventCountMap[tableName]++
	}

	jr.logger.Debugf("Process %v bytes from downloaded staging file: %s", lineBytesCounter, job.StagingFileLocation)

	jr.processingStagingFileStat.Since(processingStart)
	jr.bytesProcessedStagingFileStat.Count(lineBytesCounter)

	for _, loadFile := range jr.outputFileWritersMap {
		if err = loadFile.Close(); err != nil {
			jr.logger.Errorf("Error while closing load file %s : %v", loadFile.GetLoadFile().Name(), err)
		}
	}

	uploadsResults, err := jr.uploadLoadFiles(ctx)
	if err != nil {
		return nil, err
	}

	return uploadsResults, err
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

	if err := json.Unmarshal(claimedJob.Job.Payload, &job); err != nil {
		handleErr(err, claimedJob)
		return
	}

	err = w.runSourceJob(ctx, job)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}

	jobResultJSON, err := json.Marshal(source.NotifierResponse{
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
	if err = json.Unmarshal(sourceJob.MetaData, &metadata); err != nil {
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
			newColumnVal = fmt.Sprintf(`"%v"`, value)
		}
	} else {
		err = fmt.Errorf("incompatible schema conversion from %v to %v", existingDataType, currentDataType)
	}

	return newColumnVal, err
}
