package warehouse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	integrationsconfig "github.com/rudderlabs/rudder-server/warehouse/integrations/config"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	errIncompatibleSchemaConversion = errors.New("incompatible schema conversion")
	errSchemaConversionNotSupported = errors.New("schema conversion not supported")
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
	Result bool   `json:"Result"`
	ID     string `json:"Id"`
}

type slaveWorker struct {
	conf               *config.Config
	log                logger.Logger
	statsFactory       stats.Stats
	notifier           slaveNotifier
	bcManager          *backendConfigManager
	constraintsManager *constraintsManager
	encodingFactory    *encoding.Factory
	workerIdx          int

	config struct {
		maxStagingFileReadBufferCapacityInK int
	}
	stats struct {
		workerIdleTime                 stats.Measurement
		workerClaimProcessingSucceeded stats.Measurement
		workerClaimProcessingFailed    stats.Measurement
		workerClaimProcessingTime      stats.Measurement
	}
}

func newSlaveWorker(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	notifier slaveNotifier,
	bcManager *backendConfigManager,
	constraintsManager *constraintsManager,
	encodingFactory *encoding.Factory,
	workerIdx int,
) *slaveWorker {
	s := &slaveWorker{}

	s.conf = conf
	s.log = logger
	s.statsFactory = statsFactory
	s.notifier = notifier
	s.bcManager = bcManager
	s.constraintsManager = constraintsManager
	s.encodingFactory = encodingFactory
	s.workerIdx = workerIdx

	conf.RegisterIntConfigVariable(10240, &s.config.maxStagingFileReadBufferCapacityInK, true, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")

	tags := stats.Tags{
		"module":   moduleName,
		"workerId": strconv.Itoa(workerIdx),
	}
	s.stats.workerIdleTime = s.statsFactory.NewTaggedStat("worker_idle_time", stats.TimerType, tags)
	s.stats.workerClaimProcessingTime = s.statsFactory.NewTaggedStat("worker_claim_processing_time", stats.TimerType, tags)
	s.stats.workerClaimProcessingSucceeded = s.statsFactory.NewTaggedStat("worker_claim_processing_succeeded", stats.CountType, tags)
	s.stats.workerClaimProcessingFailed = s.statsFactory.NewTaggedStat("worker_claim_processing_failed", stats.CountType, tags)
	return s
}

func (sw *slaveWorker) start(ctx context.Context, notificationChan <-chan pgnotifier.Claim, slaveID string) {
	workerIdleTimeStart := time.Now()

	for {
		select {
		case <-ctx.Done():
			sw.log.Infof("[WH]: Slave worker-%d-%s is shutting down", sw.workerIdx, slaveID)
			return
		case claimedJob := <-notificationChan:
			sw.stats.workerIdleTime.Since(workerIdleTimeStart)

			sw.log.Debugf("[WH]: Successfully claimed job:%d by slave worker-%d-%s & job type %s",
				claimedJob.ID,
				sw.workerIdx,
				slaveID,
				claimedJob.JobType,
			)

			switch claimedJob.JobType {
			case jobs.AsyncJobType:
				sw.processClaimedAsyncJob(ctx, claimedJob)
			default:
				sw.processClaimedUploadJob(ctx, claimedJob)
			}

			sw.log.Infof("[WH]: Successfully processed job:%d by slave worker-%d-%s",
				claimedJob.ID,
				sw.workerIdx,
				slaveID,
			)

			workerIdleTimeStart = time.Now()
		}
	}
}

func (sw *slaveWorker) processClaimedUploadJob(ctx context.Context, claimedJob pgnotifier.Claim) {
	sw.stats.workerClaimProcessingTime.RecordDuration()()

	handleErr := func(err error, claim pgnotifier.Claim) {
		sw.stats.workerClaimProcessingFailed.Increment()

		sw.notifier.UpdateClaimedEvent(&claim, &pgnotifier.ClaimResponse{
			Err: err,
		})
	}

	var (
		job     payload
		jobJSON []byte
		err     error
	)

	if err = json.Unmarshal(claimedJob.Payload, &job); err != nil {
		handleErr(err, claimedJob)
		return
	}

	sw.log.Infof(`Starting processing staging-file:%v from claim:%v`, job.StagingFileID, claimedJob.ID)

	job.BatchID = claimedJob.BatchID
	job.Output, err = sw.processStagingFile(ctx, job)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}

	if jobJSON, err = json.Marshal(job); err != nil {
		handleErr(err, claimedJob)
		return
	}

	sw.stats.workerClaimProcessingSucceeded.Increment()

	sw.notifier.UpdateClaimedEvent(&claimedJob, &pgnotifier.ClaimResponse{
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
func (sw *slaveWorker) processStagingFile(ctx context.Context, job payload) ([]uploadResult, error) {
	processStartTime := time.Now()

	jr := newJobRun(job, sw.conf, sw.log, sw.statsFactory, sw.encodingFactory)

	sw.log.Debugf("[WH]: Starting processing staging file: %v at %s for %s",
		job.StagingFileID,
		job.StagingFileLocation,
		jr.identifier,
	)

	defer func() {
		jr.counterStat("staging_files_processed", warehouseutils.Tag{Name: "worker_id", Value: strconv.Itoa(sw.workerIdx)}).Count(1)
		jr.timerStat("staging_files_total_processing_time", warehouseutils.Tag{Name: "worker_id", Value: strconv.Itoa(sw.workerIdx)}).Since(processStartTime)

		jr.cleanup()
	}()

	var (
		err                  error
		lineBytesCounter     int
		interfaceSliceSample []interface{}
	)

	if jr.stagingFilePath, err = jr.getStagingFilePath(sw.workerIdx); err != nil {
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
	maxCapacity := sw.config.maxStagingFileReadBufferCapacityInK * 1024

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
			batchRouterEvent BatchRouterEvent
			writer           encoding.LoadFileWriter
		)

		if err := json.Unmarshal(lineBytes, &batchRouterEvent); err != nil {
			jr.logger.Errorf("[WH]: Failed to unmarshal JSON line to batchrouter event: %+v", batchRouterEvent)
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

		eventLoader := sw.encodingFactory.NewEventLoader(writer, job.LoadFileType, job.DestinationType)

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
				case string(model.BooleanDataType):
					newColumnVal := 0

					if k, ok := columnVal.(bool); ok {
						if k {
							newColumnVal = 1
						}
					}

					columnVal = newColumnVal
				case string(model.ArrayOfBooleanDatatype):
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

			violatedConstraints := sw.constraintsManager.violatedConstraints(job.DestinationType, &batchRouterEvent, columnName)

			if ok && ((columnType != dataTypeInSchema) || (violatedConstraints.isViolated)) {
				newColumnVal, convError := handleSchemaChange(
					model.SchemaType(dataTypeInSchema),
					model.SchemaType(columnType),
					columnVal,
				)

				if convError != nil || violatedConstraints.isViolated {
					if violatedConstraints.isViolated {
						eventLoader.AddColumn(columnName, job.UploadSchema[tableName][columnName], violatedConstraints.violatedIdentifier)
					} else {
						eventLoader.AddEmptyColumn(columnName)
					}

					jr.outputFileWritersMap[discardsTable], err = jr.writer(discardsTable)
					if err != nil {
						return nil, err
					}

					err = jr.handleDiscardTypes(tableName, columnName, columnVal, columnData, violatedConstraints, jr.outputFileWritersMap[discardsTable])
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

	jr.logger.Debugf("[WH]: Process %v bytes from downloaded staging file: %s", lineBytesCounter, job.StagingFileLocation)

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

func (sw *slaveWorker) processClaimedAsyncJob(ctx context.Context, claimedJob pgnotifier.Claim) {
	handleErr := func(err error, claim pgnotifier.Claim) {
		sw.log.Errorf("[WH]: Error processing claim: %v", err)

		sw.notifier.UpdateClaimedEvent(&claimedJob, &pgnotifier.ClaimResponse{
			Err: err,
		})
	}

	var (
		job jobs.AsyncJobPayload
		err error
	)

	if err := json.Unmarshal(claimedJob.Payload, &job); err != nil {
		handleErr(err, claimedJob)
		return
	}

	jobResult, err := sw.runAsyncJob(ctx, job)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}

	jobResultJSON, err := json.Marshal(jobResult)
	if err != nil {
		handleErr(err, claimedJob)
		return
	}

	sw.notifier.UpdateClaimedEvent(&claimedJob, &pgnotifier.ClaimResponse{
		Payload: jobResultJSON,
	})
}

func (sw *slaveWorker) runAsyncJob(ctx context.Context, asyncjob jobs.AsyncJobPayload) (asyncJobRunResult, error) {
	result := asyncJobRunResult{
		ID:     asyncjob.Id,
		Result: false,
	}

	warehouse, err := sw.destinationFromSlaveConnectionMap(asyncjob.DestinationID, asyncjob.SourceID)
	if err != nil {
		return result, err
	}

	integrationsManager, err := manager.NewWarehouseOperations(warehouse.Destination.DestinationDefinition.Name, sw.conf, sw.log, sw.statsFactory)
	if err != nil {
		return result, err
	}

	integrationsManager.SetConnectionTimeout(warehouseutils.GetConnectionTimeout(
		warehouse.Destination.DestinationDefinition.Name,
		warehouse.Destination.ID,
	))

	err = integrationsManager.Setup(ctx, warehouse, &jobs.WhAsyncJob{})
	if err != nil {
		return result, err
	}
	defer integrationsManager.Cleanup(ctx)

	var metadata warehouseutils.DeleteByMetaData
	if err = json.Unmarshal(asyncjob.MetaData, &metadata); err != nil {
		return result, err
	}

	switch asyncjob.AsyncJobType {
	case "deletebyjobrunid":
		err = integrationsManager.DeleteBy(ctx, []string{asyncjob.TableName}, warehouseutils.DeleteByParams{
			SourceId:  asyncjob.SourceID,
			TaskRunId: metadata.TaskRunId,
			JobRunId:  metadata.JobRunId,
			StartTime: metadata.StartTime,
		})
	default:
		err = errors.New("invalid AsyncJobType")
	}
	if err != nil {
		return result, err
	}

	result.Result = true

	return result, nil
}

func (sw *slaveWorker) destinationFromSlaveConnectionMap(destinationId, sourceId string) (model.Warehouse, error) {
	if destinationId == "" || sourceId == "" {
		return model.Warehouse{}, errors.New("invalid Parameters")
	}

	sourceMap, ok := sw.bcManager.ConnectionSourcesMap(destinationId)
	if !ok {
		return model.Warehouse{}, errors.New("invalid Destination Id")
	}

	conn, ok := sourceMap[sourceId]
	if !ok {
		return model.Warehouse{}, errors.New("invalid Source Id")
	}

	return conn, nil
}

// handleSchemaChange checks if the existing column type is compatible with the new column type
func handleSchemaChange(existingDataType, currentDataType model.SchemaType, value any) (any, error) {
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
			err = errIncompatibleSchemaConversion
		} else {
			newColumnVal = float64(intVal)
		}
	} else if currentDataType == model.FloatDataType && (existingDataType == model.IntDataType || existingDataType == model.BigIntDataType) {
		floatVal, ok := value.(float64)
		if !ok {
			err = errIncompatibleSchemaConversion
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
		err = errSchemaConversionNotSupported
	}

	return newColumnVal, err
}
