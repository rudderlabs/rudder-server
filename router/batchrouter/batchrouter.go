package batchrouter

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	destinationConnectionTester "github.com/rudderlabs/rudder-server/services/destination-connection-tester"
	"github.com/rudderlabs/rudder-server/warehouse"
	"github.com/thoas/go-funk"

	"github.com/rudderlabs/rudder-server/services/diagnostics"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/destination-debugger"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	jobQueryBatchSize                  int
	mainLoopSleep, diagnosisTickerTime time.Duration
	uploadFreqInS                      int64
	configSubscriberLock               sync.RWMutex
	objectStorageDestinations          []string
	warehouseDestinations              []string
	inProgressMap                      map[string]bool
	inProgressMapLock                  sync.RWMutex
	lastExecMap                        map[string]int64
	lastExecMapLock                    sync.RWMutex
	uploadedRawDataJobsCache           map[string]map[string]bool
	warehouseURL                       string
	warehouseMode                      string
	warehouseServiceFailedTime         time.Time
	warehouseServiceFailedTimeLock     sync.RWMutex
	warehouseServiceMaxRetryTimeinHr   time.Duration
	encounteredMergeRuleMap            map[string]map[string]bool
	encounteredMergeRuleMapLock        sync.RWMutex
	pkgLogger                          logger.LoggerI
	Diagnostics                        diagnostics.DiagnosticsI = diagnostics.Diagnostics
)

type HandleT struct {
	destType                string
	destinationsMap         map[string]*BatchDestinationT // destinationID -> destination
	netHandle               *http.Client
	processQ                chan BatchDestinationT
	jobsDB                  *jobsdb.HandleT
	errorDB                 jobsdb.JobsDB
	isEnabled               bool
	batchRequestsMetricLock sync.RWMutex
	diagnosisTicker         *time.Ticker
	batchRequestsMetric     []batchRequestMetric
	logger                  logger.LoggerI
	noOfWorkers             int
	maxFailedCountForJob    int
	retryTimeWindow         time.Duration
}

type BatchDestinationT struct {
	Destination backendconfig.DestinationT
	Sources     []backendconfig.SourceT
}

type ObjectStorageT struct {
	Config          map[string]interface{}
	Key             string
	Provider        string
	DestinationID   string
	DestinationType string
}

func (brt *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		brt.destinationsMap = map[string]*BatchDestinationT{}
		allSources := config.Data.(backendconfig.ConfigT)
		for _, source := range allSources.Sources {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.DestinationDefinition.Name == brt.destType {
						if _, ok := brt.destinationsMap[destination.ID]; !ok {
							brt.destinationsMap[destination.ID] = &BatchDestinationT{Destination: destination, Sources: []backendconfig.SourceT{}}
						}
						brt.destinationsMap[destination.ID].Sources = append(brt.destinationsMap[destination.ID].Sources, source)

						// initialize map to track encountered anonymousIds for a warehouse destination
						if warehouseutils.IDResolutionEnabled() && misc.ContainsString(warehouseutils.IdentityEnabledWarehouses, brt.destType) {
							identifier := connectionString(DestinationT{Destination: destination, Source: source})
							encounteredMergeRuleMapLock.Lock()
							if _, ok := encounteredMergeRuleMap[identifier]; !ok {
								encounteredMergeRuleMap[identifier] = make(map[string]bool)
							}
							encounteredMergeRuleMapLock.Unlock()
						}

						if val, ok := destination.Config["testConnection"].(bool); ok && val && misc.ContainsString(objectStorageDestinations, destination.DestinationDefinition.Name) {
							destination := destination
							rruntime.Go(func() {
								testResponse := destinationConnectionTester.TestBatchDestinationConnection(destination)
								destinationConnectionTester.UploadDestinationConnectionTesterResponse(testResponse, destination.ID)
							})
						}
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

type batchRequestMetric struct {
	batchRequestSuccess int
	batchRequestFailed  int
}

type StorageUploadOutput struct {
	Config         map[string]interface{}
	Key            string
	LocalFilePaths []string
	JournalOpID    int64
	Error          error
	FirstEventAt   string
	LastEventAt    string
	TotalEvents    int
}

type ErrorResponseT struct {
	Error string
}

func sendDestStatusStats(batchDestination *DestinationT, jobStateCounts map[string]map[string]int, destType string, isWarehouse bool) {
	tags := map[string]string{
		"module":        "batch_router",
		"destType":      destType,
		"isWarehouse":   fmt.Sprintf("%t", isWarehouse),
		"destinationId": misc.GetTagName(batchDestination.Destination.ID, batchDestination.Destination.Name),
		"sourceId":      misc.GetTagName(batchDestination.Source.ID, batchDestination.Source.Name),
	}

	for jobState, countByAttemptMap := range jobStateCounts {
		for attempt, count := range countByAttemptMap {
			tags["job_state"] = jobState
			tags["attempt_number"] = attempt
			if count > 0 {
				stats.NewTaggedStat("event_status", stats.CountType, tags).Count(count)
			}
		}
	}
}

func (brt *HandleT) copyJobsToStorage(provider string, batchJobs BatchJobsT, makeJournalEntry bool, isWarehouse bool) StorageUploadOutput {
	var localTmpDirName string
	if isWarehouse {
		localTmpDirName = "/rudder-warehouse-staging-uploads/"
	} else {
		localTmpDirName = "/rudder-raw-data-destination-logs/"
	}

	uuid := uuid.NewV4()
	brt.logger.Debugf("BRT: Starting logging to %s", provider)

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	path := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v.%v", time.Now().Unix(), batchJobs.BatchDestination.Source.ID, uuid))

	gzipFilePath := fmt.Sprintf(`%v.gz`, path)
	err = os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	gzWriter, err := misc.CreateGZ(gzipFilePath)
	if err != nil {
		panic(err)
	}

	var dedupedIDMergeRuleJobs int
	eventsFound := false
	identifier := connectionString(*batchJobs.BatchDestination)
	for _, job := range batchJobs.Jobs {
		// do not add to staging file if the event is a rudder_identity_merge_rules record
		// and has been previously added to it
		if isWarehouse && warehouseutils.IDResolutionEnabled() && gjson.GetBytes(job.EventPayload, "metadata.isMergeRule").Bool() {
			mergeProp1 := gjson.GetBytes(job.EventPayload, "metadata.mergePropOne").String()
			mergeProp2 := gjson.GetBytes(job.EventPayload, "metadata.mergePropTwo").String()
			ruleIdentifier := fmt.Sprintf(`%s::%s`, mergeProp1, mergeProp2)
			encounteredMergeRuleMapLock.Lock()
			if _, ok := encounteredMergeRuleMap[identifier][ruleIdentifier]; ok {
				encounteredMergeRuleMapLock.Unlock()
				dedupedIDMergeRuleJobs++
				continue
			} else {
				encounteredMergeRuleMap[identifier][ruleIdentifier] = true
				encounteredMergeRuleMapLock.Unlock()
			}
		}

		eventID := gjson.GetBytes(job.EventPayload, "messageId").String()
		var ok bool
		interruptedEventsMap, isDestInterrupted := uploadedRawDataJobsCache[batchJobs.BatchDestination.Destination.ID]
		if isDestInterrupted {
			if _, ok = interruptedEventsMap[eventID]; !ok {
				eventsFound = true
				gzWriter.WriteGZ(fmt.Sprintf(`%s`, job.EventPayload) + "\n")
			}
		} else {
			eventsFound = true
			gzWriter.WriteGZ(fmt.Sprintf(`%s`, job.EventPayload) + "\n")
		}
	}
	gzWriter.CloseGZ()
	if !eventsFound {
		brt.logger.Infof("BRT: No events in this batch for upload to %s. Events are either de-deuplicated or skipped", provider)
		return StorageUploadOutput{
			LocalFilePaths: []string{gzipFilePath},
		}
	}
	// assumes events from warehouse have receivedAt in metadata
	var firstEventAt, lastEventAt string
	if isWarehouse {
		firstEventAtStr := gjson.GetBytes(batchJobs.Jobs[0].EventPayload, "metadata.receivedAt").String()
		lastEventAtStr := gjson.GetBytes(batchJobs.Jobs[len(batchJobs.Jobs)-1].EventPayload, "metadata.receivedAt").String()

		// received_at set in rudder-server has timezone component
		// whereas first_event_at column in wh_staging_files is of type 'timestamp without time zone'
		// convert it to UTC before saving to wh_staging_files
		firstEventAtWithTimeZone, err := time.Parse(misc.RFC3339Milli, firstEventAtStr)
		if err != nil {
			brt.logger.Errorf(`BRT: Unable to parse receivedAt in RFC3339Milli format from eventPayload: %v. Error: %v`, firstEventAtStr, err)
		}
		lastEventAtWithTimeZone, err := time.Parse(misc.RFC3339Milli, lastEventAtStr)
		if err != nil {
			brt.logger.Errorf(`BRT: Unable to parse receivedAt in RFC3339Milli format from eventPayload: %v. Error: %v`, lastEventAtStr, err)
		}

		firstEventAt = firstEventAtWithTimeZone.UTC().Format(time.RFC3339)
		lastEventAt = lastEventAtWithTimeZone.UTC().Format(time.RFC3339)
	} else {
		firstEventAt = gjson.GetBytes(batchJobs.Jobs[0].EventPayload, "receivedAt").String()
		lastEventAt = gjson.GetBytes(batchJobs.Jobs[len(batchJobs.Jobs)-1].EventPayload, "receivedAt").String()
	}

	brt.logger.Debugf("BRT: Logged to local file: %v", gzipFilePath)
	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: provider,
		Config:   misc.GetObjectStorageConfig(provider, batchJobs.BatchDestination.Destination.Config),
	})
	if err != nil {
		panic(err)
	}

	outputFile, err := os.Open(gzipFilePath)
	if err != nil {
		panic(err)
	}

	brt.logger.Debugf("BRT: Starting upload to %s", provider)

	var keyPrefixes []string
	if isWarehouse {
		keyPrefixes = []string{config.GetEnv("WAREHOUSE_STAGING_BUCKET_FOLDER_NAME", "rudder-warehouse-staging-logs"), batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006")}
	} else {
		keyPrefixes = []string{config.GetEnv("DESTINATION_BUCKET_FOLDER_NAME", "rudder-logs"), batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006")}
	}

	_, fileName := filepath.Split(gzipFilePath)
	var (
		opID      int64
		opPayload json.RawMessage
	)
	if !isWarehouse {
		opPayload, _ = json.Marshal(&ObjectStorageT{
			Config:          batchJobs.BatchDestination.Destination.Config,
			Key:             strings.Join(append(keyPrefixes, fileName), "/"),
			Provider:        provider,
			DestinationID:   batchJobs.BatchDestination.Destination.ID,
			DestinationType: batchJobs.BatchDestination.Destination.DestinationDefinition.Name,
		})
		opID = brt.jobsDB.JournalMarkStart(jobsdb.RawDataDestUploadOperation, opPayload)
	}
	uploadOutput, err := uploader.Upload(outputFile, keyPrefixes...)

	if err != nil {
		brt.logger.Errorf("BRT: Error uploading to %s: Error: %v", provider, err)
		return StorageUploadOutput{
			Error:          err,
			JournalOpID:    opID,
			LocalFilePaths: []string{gzipFilePath},
		}
	}

	return StorageUploadOutput{
		Config:         batchJobs.BatchDestination.Destination.Config,
		Key:            uploadOutput.ObjectName,
		LocalFilePaths: []string{gzipFilePath},
		JournalOpID:    opID,
		FirstEventAt:   firstEventAt,
		LastEventAt:    lastEventAt,
		TotalEvents:    len(batchJobs.Jobs) - dedupedIDMergeRuleJobs,
	}
}

func (brt *HandleT) postToWarehouse(batchJobs BatchJobsT, output StorageUploadOutput) (err error) {
	schemaMap := make(map[string]map[string]interface{})
	for _, job := range batchJobs.Jobs {
		var payload map[string]interface{}
		err := json.Unmarshal(job.EventPayload, &payload)
		if err != nil {
			panic(err)
		}
		var ok bool
		tableName, ok := payload["metadata"].(map[string]interface{})["table"].(string)
		if !ok {
			brt.logger.Errorf(`BRT: tableName not found in event metadata: %v`, payload["metadata"])
			return nil
		}
		if _, ok = schemaMap[tableName]; !ok {
			schemaMap[tableName] = make(map[string]interface{})
		}
		columns := payload["metadata"].(map[string]interface{})["columns"].(map[string]interface{})
		for columnName, columnType := range columns {
			if _, ok := schemaMap[tableName][columnName]; !ok {
				schemaMap[tableName][columnName] = columnType
			} else {
				// this condition is required for altering string to text. if schemaMap[tableName][columnName] has string and in the next job if it has text type then we change schemaMap[tableName][columnName] to text
				if columnType == "text" && schemaMap[tableName][columnName] == "string" {
					schemaMap[tableName][columnName] = columnType
				}
			}
		}
	}
	payload := warehouseutils.StagingFileT{
		Schema: schemaMap,
		BatchDestination: warehouseutils.DestinationT{
			Source:      batchJobs.BatchDestination.Source,
			Destination: batchJobs.BatchDestination.Destination,
		},
		Location:     output.Key,
		FirstEventAt: output.FirstEventAt,
		LastEventAt:  output.LastEventAt,
		TotalEvents:  output.TotalEvents,
	}

	jsonPayload, err := json.Marshal(&payload)
	if err != nil {
		brt.logger.Errorf("BRT: Failed to marshal WH staging file payload error:%v", err)
	}
	uri := fmt.Sprintf(`%s/v1/process`, warehouseURL)
	_, err = brt.netHandle.Post(uri, "application/json; charset=utf-8",
		bytes.NewBuffer(jsonPayload))
	if err != nil {
		brt.logger.Errorf("BRT: Failed to route staging file URL to warehouse service@%v, error:%v", uri, err)
	} else {
		brt.logger.Infof("BRT: Routed successfully staging file URL to warehouse service@%v", uri)
	}
	return
}

func (brt *HandleT) setJobStatus(batchJobs BatchJobsT, isWarehouse bool, err error, postToWarehouseErr bool) {
	var (
		batchJobState string
		errorResp     []byte
	)
	var abortedEvents []*jobsdb.JobT
	var batchReqMetric batchRequestMetric
	if err != nil {
		brt.logger.Errorf("BRT: Error uploading to object storage: %v %v", err, batchJobs.BatchDestination.Source.ID)
		batchJobState = jobsdb.Failed.State
		errorResp, _ = json.Marshal(ErrorResponseT{Error: err.Error()})
		batchReqMetric.batchRequestFailed = 1
		// We keep track of number of failed attempts in case of failure and number of events uploaded in case of success in stats
	} else {
		brt.logger.Debugf("BRT: Uploaded to object storage : %v at %v", batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006"))
		batchJobState = jobsdb.Succeeded.State
		errorResp = []byte(`{"success":"OK"}`)
		batchReqMetric.batchRequestSuccess = 1
	}
	brt.trackRequestMetrics(batchReqMetric)
	var statusList []*jobsdb.JobStatusT

	if isWarehouse && postToWarehouseErr {
		warehouseServiceFailedTimeLock.Lock()
		if warehouseServiceFailedTime.IsZero() {
			warehouseServiceFailedTime = time.Now()
		}
		warehouseServiceFailedTimeLock.Unlock()
	} else if isWarehouse {
		warehouseServiceFailedTimeLock.Lock()
		warehouseServiceFailedTime = time.Time{}
		warehouseServiceFailedTimeLock.Unlock()
	}

	jobStateCounts := make(map[string]map[string]int)
	for _, job := range batchJobs.Jobs {
		jobState := batchJobState
		var firstAttemptedAt time.Time
		firstAttemptedAtString := gjson.GetBytes(job.LastJobStatus.ErrorResponse, "firstAttemptedAt").Str
		if firstAttemptedAtString != "" {
			firstAttemptedAt, err = time.Parse(misc.RFC3339Milli, firstAttemptedAtString)
			if err != nil {
				firstAttemptedAt = time.Now()
				firstAttemptedAtString = firstAttemptedAt.Format(misc.RFC3339Milli)
			}
		} else {
			firstAttemptedAt = time.Now()
			firstAttemptedAtString = firstAttemptedAt.Format(misc.RFC3339Milli)
		}
		errorRespString, err := sjson.Set(string(errorResp), "firstAttemptedAt", firstAttemptedAtString)
		if err == nil {
			errorResp = []byte(errorRespString)
		}

		timeElapsed := time.Since(firstAttemptedAt)
		if jobState == jobsdb.Failed.State && timeElapsed > brt.retryTimeWindow && job.LastJobStatus.AttemptNum >= brt.maxFailedCountForJob && !postToWarehouseErr {
			job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "stage", "batch_router")
			abortedEvents = append(abortedEvents, job)
			jobState = jobsdb.Aborted.State
		} else {
			// change job state to abort state after warehouse service is continuously failing more than warehouseServiceMaxRetryTimeinHr time
			if jobState == jobsdb.Failed.State && isWarehouse && postToWarehouseErr {
				warehouseServiceFailedTimeLock.RLock()
				if time.Since(warehouseServiceFailedTime) > warehouseServiceMaxRetryTimeinHr {
					job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "stage", "batch_router")
					abortedEvents = append(abortedEvents, job)
					jobState = jobsdb.Aborted.State
				}
				warehouseServiceFailedTimeLock.RUnlock()
			}
		}
		attemptNum := job.LastJobStatus.AttemptNum + 1
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    attemptNum,
			JobState:      jobState,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "",
			ErrorResponse: errorResp,
		}
		statusList = append(statusList, &status)
		if jobStateCounts[jobState] == nil {
			jobStateCounts[jobState] = make(map[string]int)
		}
		jobStateCounts[jobState][strconv.Itoa(attemptNum)] = jobStateCounts[jobState][strconv.Itoa(attemptNum)] + 1
	}

	//tracking batch router errors
	if diagnostics.EnableDestinationFailuresMetric {
		if batchJobState == jobsdb.Failed.State {
			Diagnostics.Track(diagnostics.BatchRouterFailed, map[string]interface{}{
				diagnostics.BatchRouterDestination: brt.destType,
				diagnostics.ErrorResponse:          string(errorResp),
			})
		}
	}

	parameterFilters := []jobsdb.ParameterFilterT{
		{
			Name:     "destination_id",
			Value:    batchJobs.BatchDestination.Destination.ID,
			Optional: false,
		},
	}

	//Store the aborted jobs to errorDB
	if abortedEvents != nil {
		brt.errorDB.Store(abortedEvents)
	}
	//Mark the status of the jobs
	err = brt.jobsDB.UpdateJobStatus(statusList, []string{brt.destType}, parameterFilters)
	if err != nil {
		brt.logger.Errorf("Error occurred while updating %s jobs statuses. Panicking. Err: %v", brt.destType, err)
		panic(err)
	}

	sendDestStatusStats(batchJobs.BatchDestination, jobStateCounts, brt.destType, isWarehouse)
}

func (brt *HandleT) trackRequestMetrics(batchReqDiagnostics batchRequestMetric) {
	if diagnostics.EnableBatchRouterMetric {
		brt.batchRequestsMetricLock.Lock()
		if brt.batchRequestsMetric == nil {
			var batchRequestsMetric []batchRequestMetric
			brt.batchRequestsMetric = append(batchRequestsMetric, batchReqDiagnostics)
		} else {
			brt.batchRequestsMetric = append(brt.batchRequestsMetric, batchReqDiagnostics)
		}
		brt.batchRequestsMetricLock.Unlock()
	}
}

func (brt *HandleT) recordDeliveryStatus(batchDestination DestinationT, err error, isWarehouse bool) {
	if !destinationdebugger.HasUploadEnabled(batchDestination.Destination.ID) {
		return
	}
	var (
		jobState  string
		errorResp []byte
	)

	if err != nil {
		jobState = jobsdb.Failed.State
		if isWarehouse {
			jobState = warehouse.GeneratingStagingFileFailedState
		}
		errorResp, _ = json.Marshal(ErrorResponseT{Error: err.Error()})
	} else {
		jobState = jobsdb.Succeeded.State
		if isWarehouse {
			jobState = warehouse.GeneratedStagingFileState
		}
		errorResp = []byte(`{"success":"OK"}`)
	}

	//Payload and AttemptNum don't make sense in recording batch router delivery status,
	//So they are set to default values.
	deliveryStatus := destinationdebugger.DeliveryStatusT{
		DestinationID: batchDestination.Destination.ID,
		SourceID:      batchDestination.Source.ID,
		Payload:       []byte(`{}`),
		AttemptNum:    1,
		JobState:      jobState,
		ErrorCode:     "",
		ErrorResponse: errorResp,
	}
	destinationdebugger.RecordEventDeliveryStatus(batchDestination.Destination.ID, &deliveryStatus)
}

func (brt *HandleT) recordUploadStats(destination DestinationT, output StorageUploadOutput) {
	destinationTag := misc.GetTagName(destination.Destination.ID, destination.Destination.Name)
	eventDeliveryStat := stats.NewTaggedStat("event_delivery", stats.CountType, map[string]string{
		"module":      "batch_router",
		"destType":    brt.destType,
		"destination": destinationTag,
	})
	eventDeliveryStat.Count(output.TotalEvents)

	receivedTime, err := time.Parse(misc.RFC3339Milli, output.FirstEventAt)
	if err != nil {
		eventDeliveryTimeStat := stats.NewTaggedStat("event_delivery_time", stats.TimerType, map[string]string{
			"module":      "batch_router",
			"destType":    brt.destType,
			"destination": destinationTag,
		})
		eventDeliveryTimeStat.SendTiming(time.Since(receivedTime))
	}
}

func (brt *HandleT) initWorkers() {
	for i := 0; i < brt.noOfWorkers; i++ {
		rruntime.Go(func() {
			func() {
				for batchDest := range brt.processQ {
					toQuery := jobQueryBatchSize
					parameterFilters := []jobsdb.ParameterFilterT{
						{
							Name:     "destination_id",
							Value:    batchDest.Destination.ID,
							Optional: false,
						},
					}
					brtQueryStat := stats.NewStat("batch_router.jobsdb_query_time", stats.TimerType)
					brtQueryStat.Start()
					brt.logger.Debugf("BRT: %s: DB about to read for parameter Filters: %v ", brt.destType, parameterFilters)

					retryList := brt.jobsDB.GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{brt.destType}, Count: toQuery, ParameterFilters: parameterFilters, IgnoreCustomValFiltersInQuery: true})
					toQuery -= len(retryList)
					waitList := brt.jobsDB.GetWaiting(jobsdb.GetQueryParamsT{CustomValFilters: []string{brt.destType}, Count: toQuery, ParameterFilters: parameterFilters, IgnoreCustomValFiltersInQuery: true}) //Jobs send to waiting state
					toQuery -= len(waitList)
					unprocessedList := brt.jobsDB.GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{brt.destType}, Count: toQuery, ParameterFilters: parameterFilters, IgnoreCustomValFiltersInQuery: true})
					brtQueryStat.End()

					combinedList := append(retryList, append(waitList, unprocessedList...)...)
					if len(combinedList) == 0 {
						brt.logger.Debugf("BRT: DB Read Complete. No BRT Jobs to process for parameter Filters: %v", parameterFilters)
						setDestInProgress(batchDest.Destination.ID, false)
						continue
					}
					brt.logger.Debugf("BRT: %s: DB Read Complete for parameter Filters: %v retryList: %v, waitList: %v unprocessedList: %v, total: %v", brt.destType, parameterFilters, len(retryList), len(waitList), len(unprocessedList), len(combinedList))

					var statusList []*jobsdb.JobStatusT

					jobsBySource := make(map[string][]*jobsdb.JobT)
					for _, job := range combinedList {
						sourceID := gjson.GetBytes(job.Parameters, "source_id").String()
						if _, ok := jobsBySource[sourceID]; !ok {
							jobsBySource[sourceID] = []*jobsdb.JobT{}
						}
						jobsBySource[sourceID] = append(jobsBySource[sourceID], job)

						status := jobsdb.JobStatusT{
							JobID:         job.JobID,
							AttemptNum:    job.LastJobStatus.AttemptNum + 1,
							JobState:      jobsdb.Executing.State,
							ExecTime:      time.Now(),
							RetryTime:     time.Now(),
							ErrorCode:     "",
							ErrorResponse: []byte(`{}`), // check
						}
						statusList = append(statusList, &status)
					}

					//Mark the jobs as executing
					err := brt.jobsDB.UpdateJobStatus(statusList, []string{brt.destType}, parameterFilters)
					if err != nil {
						brt.logger.Errorf("Error occurred while marking %s jobs statuses as executing. Panicking. Err: %v", brt.destType, err)
						panic(err)
					}
					brt.logger.Debugf("BRT: %s: DB Status update complete for parameter Filters: %v", brt.destType, parameterFilters)

					var wg sync.WaitGroup
					wg.Add(len(jobsBySource))

					for sourceID, jobs := range jobsBySource {
						source, ok := funk.Find(batchDest.Sources, func(s backendconfig.SourceT) bool {
							return s.ID == sourceID
						}).(backendconfig.SourceT)
						batchJobs := BatchJobsT{
							Jobs: jobs,
							BatchDestination: &DestinationT{
								Destination: batchDest.Destination,
								Source:      source,
							},
						}
						if !ok {
							// TODO: Should not happen. Handle this
							err := fmt.Errorf("BRT: Batch destination source not found in config for sourceID: %s", sourceID)
							brt.setJobStatus(batchJobs, false, err, false)
							wg.Done()
							continue
						}
						rruntime.Go(func() {
							switch {
							case misc.ContainsString(objectStorageDestinations, brt.destType):
								destUploadStat := stats.NewStat(fmt.Sprintf(`batch_router.%s_dest_upload_time`, brt.destType), stats.TimerType)
								destUploadStat.Start()
								output := brt.copyJobsToStorage(brt.destType, batchJobs, true, false)
								brt.recordDeliveryStatus(*batchJobs.BatchDestination, output.Error, false)
								brt.setJobStatus(batchJobs, false, output.Error, false)
								misc.RemoveFilePaths(output.LocalFilePaths...)
								if output.JournalOpID > 0 {
									brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
								}
								if output.Error == nil {
									brt.recordUploadStats(*batchJobs.BatchDestination, output)
								}

								destUploadStat.End()
							case misc.ContainsString(warehouseDestinations, brt.destType):
								objectStorageType := warehouseutils.ObjectStorageType(brt.destType, batchJobs.BatchDestination.Destination.Config)
								destUploadStat := stats.NewStat(fmt.Sprintf(`batch_router.%s_%s_dest_upload_time`, brt.destType, objectStorageType), stats.TimerType)
								destUploadStat.Start()
								output := brt.copyJobsToStorage(objectStorageType, batchJobs, true, true)
								postToWarehouseErr := false
								if output.Error == nil && output.Key != "" {
									output.Error = brt.postToWarehouse(batchJobs, output)
									if output.Error != nil {
										postToWarehouseErr = true
									}
									warehouseutils.DestStat(stats.CountType, "generate_staging_files", batchJobs.BatchDestination.Destination.ID).Count(1)
									warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", batchJobs.BatchDestination.Destination.ID).Count(len(batchJobs.Jobs))
								}
								brt.recordDeliveryStatus(*batchJobs.BatchDestination, output.Error, true)
								brt.setJobStatus(batchJobs, true, output.Error, postToWarehouseErr)
								misc.RemoveFilePaths(output.LocalFilePaths...)
								destUploadStat.End()
							}
							wg.Done()
						})
					}

					wg.Wait()
					setDestInProgress(batchDest.Destination.ID, false)
				}
			}()
		})
	}
}

type DestinationT struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type BatchJobsT struct {
	Jobs             []*jobsdb.JobT
	BatchDestination *DestinationT
}

func connectionString(batchDestination DestinationT) string {
	return fmt.Sprintf(`source:%s::destination:%s`, batchDestination.Source.ID, batchDestination.Destination.ID)
}

func isDestInProgress(destID string) bool {
	inProgressMapLock.RLock()
	if inProgressMap[destID] {
		inProgressMapLock.RUnlock()
		return true
	}
	inProgressMapLock.RUnlock()
	return false
}

func setDestInProgress(destID string, starting bool) {
	inProgressMapLock.Lock()
	if starting {
		inProgressMap[destID] = true
	} else {
		delete(inProgressMap, destID)
	}
	inProgressMapLock.Unlock()
}

func uploadFrequencyExceeded(destID string) bool {
	lastExecMapLock.Lock()
	defer lastExecMapLock.Unlock()
	if lastExecTime, ok := lastExecMap[destID]; ok && time.Now().Unix()-lastExecTime < uploadFreqInS {
		return true
	}
	lastExecMap[destID] = time.Now().Unix()
	return false
}

func (brt *HandleT) mainLoop() {
	for {
		time.Sleep(mainLoopSleep)
		configSubscriberLock.RLock()
		destinationsMap := brt.destinationsMap
		configSubscriberLock.RUnlock()
		for destID, batchDest := range destinationsMap {
			if isDestInProgress(destID) {
				brt.logger.Debugf("BRT: Skipping batch router upload loop since destination %s:%s is in progress", batchDest.Destination.DestinationDefinition.Name, destID)
				continue
			}
			if uploadFrequencyExceeded(destID) {
				brt.logger.Debugf("BRT: Skipping batch router upload loop since %s:%s upload freq not exceeded", batchDest.Destination.DestinationDefinition.Name, destID)
				continue
			}
			setDestInProgress(destID, true)

			brt.processQ <- *batchDest
		}
	}
}

//Enable enables a router :)
func (brt *HandleT) Enable() {
	brt.isEnabled = true
}

//Disable disables a router:)
func (brt *HandleT) Disable() {
	brt.isEnabled = false
}

func (brt *HandleT) dedupRawDataDestJobsOnCrash() {
	brt.logger.Debug("BRT: Checking for incomplete journal entries to recover from...")
	entries := brt.jobsDB.GetJournalEntries(jobsdb.RawDataDestUploadOperation)
	for _, entry := range entries {
		var object ObjectStorageT
		err := json.Unmarshal(entry.OpPayload, &object)
		if err != nil {
			panic(err)
		}
		if len(object.Config) == 0 {
			//Backward compatibility. If old entries dont have config, just delete journal entry
			brt.jobsDB.JournalDeleteEntry(entry.OpID)
			continue
		}
		downloader, err := filemanager.New(&filemanager.SettingsT{
			Provider: object.Provider,
			Config:   object.Config,
		})
		if err != nil {
			panic(err)
		}

		localTmpDirName := "/rudder-raw-data-dest-upload-crash-recovery/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			panic(err)
		}
		jsonPath := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v", time.Now().Unix(), uuid.NewV4().String()))

		err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
		if err != nil {
			panic(err)
		}
		jsonFile, err := os.Create(jsonPath)
		if err != nil {
			panic(err)
		}

		brt.logger.Debugf("BRT: Downloading data for incomplete journal entry to recover from %s at key: %s\n", object.Provider, object.Key)

		var objKey string
		if prefix, ok := object.Config["prefix"]; ok && prefix != "" {
			objKey += fmt.Sprintf("/%s", strings.TrimSpace(prefix.(string)))
		}
		objKey += object.Key

		err = downloader.Download(jsonFile, objKey)
		if err != nil {
			brt.logger.Errorf("BRT: Failed to download data for incomplete journal entry to recover from %s at key: %s with error: %v\n", object.Provider, object.Key, err)
			continue
		}

		jsonFile.Close()
		defer os.Remove(jsonPath)
		rawf, err := os.Open(jsonPath)
		if err != nil {
			panic(err)
		}
		reader, err := gzip.NewReader(rawf)
		if err != nil {
			panic(err)
		}

		sc := bufio.NewScanner(reader)

		brt.logger.Debug("BRT: Setting go map cache for incomplete journal entry to recover from...")
		for sc.Scan() {
			lineBytes := sc.Bytes()
			eventID := gjson.GetBytes(lineBytes, "messageId").String()
			if _, ok := uploadedRawDataJobsCache[object.DestinationID]; !ok {
				uploadedRawDataJobsCache[object.DestinationID] = make(map[string]bool)
			}
			uploadedRawDataJobsCache[object.DestinationID][eventID] = true
		}
		reader.Close()
		brt.jobsDB.JournalDeleteEntry(entry.OpID)
	}
}

func (brt *HandleT) crashRecover() {

	for {
		execList := brt.jobsDB.GetExecuting(jobsdb.GetQueryParamsT{CustomValFilters: []string{brt.destType}, Count: jobQueryBatchSize})

		if len(execList) == 0 {
			break
		}
		brt.logger.Debug("BRT: Batch Router crash recovering", len(execList))

		var statusList []*jobsdb.JobStatusT

		for _, job := range execList {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				JobState:      jobsdb.Failed.State,
				ErrorCode:     "",
				ErrorResponse: []byte(`{"Error": "Rudder server crashed while copying jobs to storage"}`), // check
			}
			statusList = append(statusList, &status)
		}
		err := brt.jobsDB.UpdateJobStatus(statusList, []string{}, nil)
		if err != nil {
			brt.logger.Errorf("Error occurred while marking %s jobs statuses as failed. Panicking. Err: %v", brt.destType, err)
			panic(err)
		}
	}
	if misc.Contains(objectStorageDestinations, brt.destType) {
		brt.dedupRawDataDestJobsOnCrash()
	}
}

func IsObjectStorageDestination(destType string) bool {
	return misc.Contains(objectStorageDestinations, destType)
}

func IsWarehouseDestination(destType string) bool {
	return misc.Contains(warehouseDestinations, destType)
}

func isWarehouseMasterEnabled() bool {
	return warehouseMode == config.EmbeddedMode ||
		warehouseMode == config.PooledWHSlaveMode
}

func getWarehouseURL() (url string) {
	if isWarehouseMasterEnabled() {
		url = fmt.Sprintf(`http://localhost:%d`, config.GetInt("Warehouse.webPort", 8082))
	} else {
		url = config.GetEnv("WAREHOUSE_URL", "http://localhost:8082")
	}
	return
}

func (brt *HandleT) collectMetrics() {
	if diagnostics.EnableBatchRouterMetric {
		for range brt.diagnosisTicker.C {
			brt.batchRequestsMetricLock.RLock()
			var diagnosisProperties map[string]interface{}
			success := 0
			failed := 0
			for _, batchReqMetric := range brt.batchRequestsMetric {
				success = success + batchReqMetric.batchRequestSuccess
				failed = failed + batchReqMetric.batchRequestFailed
			}
			if len(brt.batchRequestsMetric) > 0 {
				diagnosisProperties = map[string]interface{}{
					brt.destType: map[string]interface{}{
						diagnostics.BatchRouterSuccess: success,
						diagnostics.BatchRouterFailed:  failed,
					},
				}

				Diagnostics.Track(diagnostics.BatchRouterEvents, diagnosisProperties)
			}

			brt.batchRequestsMetric = nil
			brt.batchRequestsMetricLock.RUnlock()
		}
	}
}

func loadConfig() {
	jobQueryBatchSize = config.GetInt("BatchRouter.jobQueryBatchSize", 100000)
	mainLoopSleep = config.GetDuration("BatchRouter.mainLoopSleepInS", 2) * time.Second
	uploadFreqInS = config.GetInt64("BatchRouter.uploadFreqInS", 30)
	objectStorageDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO", "DIGITAL_OCEAN_SPACES"}
	warehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE"}
	inProgressMap = map[string]bool{}
	lastExecMap = map[string]int64{}
	warehouseMode = config.GetString("Warehouse.mode", "embedded")
	warehouseURL = getWarehouseURL()
	// Time period for diagnosis ticker
	diagnosisTickerTime = config.GetDuration("Diagnostics.batchRouterTimePeriodInS", 600) * time.Second
	warehouseServiceMaxRetryTimeinHr = config.GetDuration("batchRouter.warehouseServiceMaxRetryTimeinHr", 3) * time.Hour
	encounteredMergeRuleMap = map[string]map[string]bool{}
}

func init() {
	loadConfig()
	uploadedRawDataJobsCache = make(map[string]map[string]bool)
	pkgLogger = logger.NewLogger().Child("batchrouter")
}

//Setup initializes this module
func (brt *HandleT) Setup(jobsDB *jobsdb.HandleT, errorDB jobsdb.JobsDB, destType string) {
	brt.logger = pkgLogger.Child(destType)
	brt.logger.Infof("BRT: Batch Router started: %s", destType)

	brt.diagnosisTicker = time.NewTicker(diagnosisTickerTime)
	brt.destType = destType
	brt.jobsDB = jobsDB
	brt.errorDB = errorDB
	brt.isEnabled = true
	brt.noOfWorkers = getBatchRouterConfigInt("noOfWorkers", destType, 8)
	brt.maxFailedCountForJob = getBatchRouterConfigInt("maxFailedCountForJob", destType, 128)
	brt.retryTimeWindow = getBatchRouterConfigDuration("retryTimeWindowInMins", destType, time.Duration(180)) * time.Minute

	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	brt.netHandle = client

	brt.processQ = make(chan BatchDestinationT)
	brt.crashRecover()

	rruntime.Go(func() {
		brt.collectMetrics()
	})
	rruntime.Go(func() {
		brt.initWorkers()
	})
	rruntime.Go(func() {
		brt.backendConfigSubscriber()
	})
	rruntime.Go(func() {
		brt.mainLoop()
	})
	adminInstance.registerBatchRouter(destType, brt)
}
