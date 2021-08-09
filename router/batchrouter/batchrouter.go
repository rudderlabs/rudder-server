package batchrouter

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
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

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	jobQueryBatchSize                  int
	mainLoopSleep, diagnosisTickerTime time.Duration
	uploadFreqInS                      int64
	objectStorageDestinations          []string
	warehouseDestinations              []string
	warehouseURL                       string
	warehouseServiceFailedTime         time.Time
	warehouseServiceFailedTimeLock     sync.RWMutex
	warehouseServiceMaxRetryTime       time.Duration
	pkgLogger                          logger.LoggerI
	Diagnostics                        diagnostics.DiagnosticsI = diagnostics.Diagnostics
	QueryFilters                       jobsdb.QueryFiltersT
	readPerDestination                 bool
	disableEgress                      bool
	toAbortDestinationIDs              string
)

const DISABLED_EGRESS = "200: outgoing disabled"

type HandleT struct {
	paused                      bool
	pauseLock                   sync.Mutex
	destType                    string
	destinationsMap             map[string]*router_utils.BatchDestinationT // destinationID -> destination
	connectionWHNamespaceMap    map[string]string                          // connectionIdentifier -> warehouseConnectionIdentifier(+namepsace)
	netHandle                   *http.Client
	processQ                    chan *BatchDestinationDataT
	jobsDB                      jobsdb.JobsDB
	errorDB                     jobsdb.JobsDB
	isEnabled                   bool
	batchRequestsMetricLock     sync.RWMutex
	diagnosisTicker             *time.Ticker
	batchRequestsMetric         []batchRequestMetric
	logger                      logger.LoggerI
	noOfWorkers                 int
	maxFailedCountForJob        int
	retryTimeWindow             time.Duration
	reporting                   types.ReportingI
	reportingEnabled            bool
	workers                     []*workerT
	drainedJobsStat             stats.RudderStats
	backendConfig               backendconfig.BackendConfig
	fileManagerFactory          filemanager.FileManagerFactory
	inProgressMap               map[string]bool
	inProgressMapLock           sync.RWMutex
	lastExecMap                 map[string]int64
	lastExecMapLock             sync.RWMutex
	configSubscriberLock        sync.RWMutex
	encounteredMergeRuleMap     map[string]map[string]bool
	uploadedRawDataJobsCache    map[string]map[string]bool
	encounteredMergeRuleMapLock sync.RWMutex
	isBackendConfigInitialized  bool
	backendConfigInitialized    chan bool
}

type BatchDestinationDataT struct {
	batchDestination router_utils.BatchDestinationT
	jobs             []*jobsdb.JobT
	parentWG         *sync.WaitGroup
}

type ObjectStorageT struct {
	Config          map[string]interface{}
	Key             string
	Provider        string
	DestinationID   string
	DestinationType string
}

//JobParametersT struct holds source id and destination id of a job
type JobParametersT struct {
	SourceID        string `json:"source_id"`
	DestinationID   string `json:"destination_id"`
	ReceivedAt      string `json:"received_at"`
	TransformAt     string `json:"transform_at"`
	SourceBatchID   string `json:"source_batch_id"`
	SourceTaskID    string `json:"source_task_id"`
	SourceTaskRunID string `json:"source_task_run_id"`
	SourceJobID     string `json:"source_job_id"`
	SourceJobRunID  string `json:"source_job_run_id"`
}

func (brt *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	brt.backendConfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		brt.configSubscriberLock.Lock()
		brt.destinationsMap = map[string]*router_utils.BatchDestinationT{}
		brt.connectionWHNamespaceMap = map[string]string{}
		allSources := config.Data.(backendconfig.ConfigT)
		for _, source := range allSources.Sources {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.DestinationDefinition.Name == brt.destType {
						if _, ok := brt.destinationsMap[destination.ID]; !ok {
							brt.destinationsMap[destination.ID] = &router_utils.BatchDestinationT{Destination: destination, Sources: []backendconfig.SourceT{}}
						}
						brt.destinationsMap[destination.ID].Sources = append(brt.destinationsMap[destination.ID].Sources, source)

						// initialize map to track encountered anonymousIds for a warehouse destination
						if warehouseutils.IDResolutionEnabled() && misc.ContainsString(warehouseutils.IdentityEnabledWarehouses, brt.destType) {
							connIdentifier := connectionIdentifier(DestinationT{Destination: destination, Source: source})
							warehouseConnIdentifier := brt.warehouseConnectionIdentifier(connIdentifier, source, destination)
							brt.connectionWHNamespaceMap[connIdentifier] = warehouseConnIdentifier

							brt.encounteredMergeRuleMapLock.Lock()
							if _, ok := brt.encounteredMergeRuleMap[warehouseConnIdentifier]; !ok {
								brt.encounteredMergeRuleMap[warehouseConnIdentifier] = make(map[string]bool)
							}
							brt.encounteredMergeRuleMapLock.Unlock()
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

		if !brt.isBackendConfigInitialized {
			brt.isBackendConfigInitialized = true
			brt.backendConfigInitialized <- true
		}
		brt.configSubscriberLock.Unlock()
	}
}

type batchRequestMetric struct {
	batchRequestSuccess int
	batchRequestFailed  int
}

type StorageUploadOutput struct {
	Config           map[string]interface{}
	Key              string
	LocalFilePaths   []string
	JournalOpID      int64
	Error            error
	FirstEventAt     string
	LastEventAt      string
	TotalEvents      int
	UseRudderStorage bool
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
	if disableEgress {
		return StorageUploadOutput{Error: errors.New(DISABLED_EGRESS)}
	}

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
	connIdentifier := connectionIdentifier(*batchJobs.BatchDestination)
	warehouseConnIdentifier := brt.connectionWHNamespaceMap[connIdentifier]
	for _, job := range batchJobs.Jobs {
		// do not add to staging file if the event is a rudder_identity_merge_rules record
		// and has been previously added to it
		if isWarehouse && warehouseutils.IDResolutionEnabled() && gjson.GetBytes(job.EventPayload, "metadata.isMergeRule").Bool() {
			mergeProp1 := gjson.GetBytes(job.EventPayload, "metadata.mergePropOne").String()
			mergeProp2 := gjson.GetBytes(job.EventPayload, "metadata.mergePropTwo").String()
			ruleIdentifier := fmt.Sprintf(`%s::%s`, mergeProp1, mergeProp2)
			brt.configSubscriberLock.Lock()
			brt.encounteredMergeRuleMapLock.Lock()
			if _, ok := brt.encounteredMergeRuleMap[warehouseConnIdentifier][ruleIdentifier]; ok {
				brt.encounteredMergeRuleMapLock.Unlock()
				brt.configSubscriberLock.Unlock()
				dedupedIDMergeRuleJobs++
				continue
			} else {
				brt.encounteredMergeRuleMap[warehouseConnIdentifier][ruleIdentifier] = true
				brt.encounteredMergeRuleMapLock.Unlock()
				brt.configSubscriberLock.Unlock()
			}
		}

		eventID := gjson.GetBytes(job.EventPayload, "messageId").String()
		var ok bool
		interruptedEventsMap, isDestInterrupted := brt.uploadedRawDataJobsCache[batchJobs.BatchDestination.Destination.ID]
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
	useRudderStorage := isWarehouse && misc.IsConfiguredToUseRudderObjectStorage(batchJobs.BatchDestination.Destination.Config)
	uploader, err := brt.fileManagerFactory.New(&filemanager.SettingsT{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           batchJobs.BatchDestination.Destination.Config,
			UseRudderStorage: useRudderStorage}),
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
		Config:           batchJobs.BatchDestination.Destination.Config,
		Key:              uploadOutput.ObjectName,
		LocalFilePaths:   []string{gzipFilePath},
		JournalOpID:      opID,
		FirstEventAt:     firstEventAt,
		LastEventAt:      lastEventAt,
		TotalEvents:      len(batchJobs.Jobs) - dedupedIDMergeRuleJobs,
		UseRudderStorage: useRudderStorage,
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
	var sampleParameters JobParametersT
	err = json.Unmarshal(batchJobs.Jobs[0].Parameters, &sampleParameters)
	if err != nil {
		brt.logger.Error("Unmarshal of job parameters failed in postToWarehouse function. ", string(batchJobs.Jobs[0].Parameters))
	}
	payload := warehouseutils.StagingFileT{
		Schema: schemaMap,
		BatchDestination: warehouseutils.DestinationT{
			Source:      batchJobs.BatchDestination.Source,
			Destination: batchJobs.BatchDestination.Destination,
		},
		Location:         output.Key,
		FirstEventAt:     output.FirstEventAt,
		LastEventAt:      output.LastEventAt,
		TotalEvents:      output.TotalEvents,
		UseRudderStorage: output.UseRudderStorage,
		SourceBatchID:    sampleParameters.SourceBatchID,
		SourceTaskID:     sampleParameters.SourceTaskID,
		SourceTaskRunID:  sampleParameters.SourceTaskRunID,
		SourceJobID:      sampleParameters.SourceJobID,
		SourceJobRunID:   sampleParameters.SourceJobRunID,
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
	if err != nil && err.Error() == DISABLED_EGRESS {
		brt.logger.Debugf("BRT: Outgoing traffic disabled : %v at %v", batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006"))
		batchJobState = jobsdb.Succeeded.State
		errorResp = []byte(fmt.Sprintf(`{"success":"%s"}`, DISABLED_EGRESS))
	} else if err != nil {
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

	reportMetrics := make([]*types.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*types.ConnectionDetails)
	statusDetailsMap := make(map[string]*types.StatusDetail)
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
				if time.Since(warehouseServiceFailedTime) > warehouseServiceMaxRetryTime {
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

		//REPORTING - START
		if brt.reporting != nil && brt.reportingEnabled {
			//Update metrics maps
			errorCode := getBRTErrorCode(jobState)
			var parameters JobParametersT
			err = json.Unmarshal(job.Parameters, &parameters)
			if err != nil {
				brt.logger.Error("Unmarshal of job parameters failed. ", string(job.Parameters))
			}
			key := fmt.Sprintf("%s:%s:%s:%s:%s", parameters.SourceID, parameters.DestinationID, parameters.SourceBatchID, jobState, strconv.Itoa(errorCode))
			cd, ok := connectionDetailsMap[key]
			if !ok {
				cd = types.CreateConnectionDetail(parameters.SourceID, parameters.DestinationID, parameters.SourceBatchID, parameters.SourceTaskID, parameters.SourceTaskRunID, parameters.SourceJobID, parameters.SourceJobRunID)
				connectionDetailsMap[key] = cd
			}
			sd, ok := statusDetailsMap[key]
			if !ok {
				sd = types.CreateStatusDetail(jobState, 0, errorCode, string(errorResp), job.EventPayload)
				statusDetailsMap[key] = sd
			}
			if status.JobState == jobsdb.Failed.State && status.AttemptNum == 1 {
				sd.Count++
			}
			if status.JobState != jobsdb.Failed.State {
				sd.Count++
			}
		}
		//REPORTING - END
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

	var parameterFilters []jobsdb.ParameterFilterT
	if readPerDestination {
		parameterFilters = []jobsdb.ParameterFilterT{
			{
				Name:     "destination_id",
				Value:    batchJobs.BatchDestination.Destination.ID,
				Optional: false,
			},
		}
	}

	//Store the aborted jobs to errorDB
	if abortedEvents != nil {
		err := brt.errorDB.Store(abortedEvents)
		if err != nil {
			brt.logger.Errorf("[Batch Router] Store into proc error table failed with error: %v", err)
			brt.logger.Errorf("abortedEvents: %v", abortedEvents)
			panic(err)
		}
	}

	//REPORTING - START
	if brt.reporting != nil && brt.reportingEnabled {
		types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)
		terminalPU := true
		if isWarehouse {
			terminalPU = false
		}
		for k, cd := range connectionDetailsMap {
			m := &types.PUReportedMetric{
				ConnectionDetails: *cd,
				PUDetails:         *types.CreatePUDetails(types.DEST_TRANSFORMER, types.BATCH_ROUTER, terminalPU, false),
				StatusDetail:      statusDetailsMap[k],
			}
			if m.StatusDetail.Count != 0 {
				reportMetrics = append(reportMetrics, m)
			}
		}
	}
	//REPORTING - END

	//Mark the status of the jobs
	txn := brt.jobsDB.BeginGlobalTransaction()
	brt.jobsDB.AcquireUpdateJobStatusLocks()
	err = brt.jobsDB.UpdateJobStatusInTxn(txn, statusList, []string{brt.destType}, parameterFilters)
	if err != nil {
		brt.logger.Errorf("[Batch Router] Error occurred while updating %s jobs statuses. Panicking. Err: %v", brt.destType, err)
		panic(err)
	}

	if brt.reporting != nil && brt.reportingEnabled {
		brt.reporting.Report(reportMetrics, txn)
	}
	brt.jobsDB.CommitTransaction(txn)
	brt.jobsDB.ReleaseUpdateJobStatusLocks()

	sendDestStatusStats(batchJobs.BatchDestination, jobStateCounts, brt.destType, isWarehouse)
}

func getBRTErrorCode(state string) int {
	if state == jobsdb.Succeeded.State {
		return 200
	}

	return 500
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

func (brt *HandleT) recordDeliveryStatus(batchDestination DestinationT, output StorageUploadOutput, isWarehouse bool) {
	if !destinationdebugger.HasUploadEnabled(batchDestination.Destination.ID) {
		return
	}
	var (
		errorCode string
		jobState  string
		errorResp []byte
	)

	err := output.Error
	if err != nil {
		jobState = jobsdb.Failed.State
		errorCode = "500"
		if isWarehouse {
			jobState = warehouse.GeneratingStagingFileFailedState
		}
		errorResp, _ = json.Marshal(ErrorResponseT{Error: err.Error()})
	} else {
		jobState = jobsdb.Succeeded.State
		errorCode = "200"
		if isWarehouse {
			jobState = warehouse.GeneratedStagingFileState
		}
		errorResp = []byte(`{"success":"OK"}`)
	}

	//Payload and AttemptNum don't make sense in recording batch router delivery status,
	//So they are set to default values.
	deliveryStatus := destinationdebugger.DeliveryStatusT{
		EventName:     fmt.Sprint(output.TotalEvents) + " events",
		EventType:     "",
		SentAt:        time.Now().Format(misc.RFC3339Milli),
		DestinationID: batchDestination.Destination.ID,
		SourceID:      batchDestination.Source.ID,
		Payload:       []byte(`{}`),
		AttemptNum:    1,
		JobState:      jobState,
		ErrorCode:     errorCode,
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

func (worker *workerT) getValueForParameter(batchDest router_utils.BatchDestinationT, parameter string) string {
	switch {
	case parameter == "destination_id":
		return batchDest.Destination.ID
	default:
		panic(fmt.Errorf("BRT: %s: Unknown parameter(%s) to find value from batchDest %+v", worker.brt.destType, parameter, batchDest))
	}
}

func (worker *workerT) constructParameterFilters(batchDest router_utils.BatchDestinationT) []jobsdb.ParameterFilterT {
	parameterFilters := make([]jobsdb.ParameterFilterT, 0)
	for _, key := range QueryFilters.ParameterFilters {
		parameterFilter := jobsdb.ParameterFilterT{
			Name:     key,
			Value:    worker.getValueForParameter(batchDest, key),
			Optional: false,
		}
		parameterFilters = append(parameterFilters, parameterFilter)
	}

	return parameterFilters
}

func (worker *workerT) workerProcess() {
	brt := worker.brt
	for {
		select {
		case pause := <-worker.pauseChannel:
			pkgLogger.Infof("Batch Router worker %d is paused. Dest type: %s", worker.workerID, worker.brt.destType)
			pause.wg.Done()
			pause.respChannel <- true
			<-worker.resumeChannel
			pkgLogger.Infof("Batch Router worker %d is resumed. Dest type: %s", worker.workerID, worker.brt.destType)

		case batchDestData := <-brt.processQ:
			batchDest := batchDestData.batchDestination
			parameterFilters := worker.constructParameterFilters(batchDest)
			var combinedList []*jobsdb.JobT
			if readPerDestination {
				toQuery := jobQueryBatchSize
				brtQueryStat := stats.NewTaggedStat("batch_router.jobsdb_query_time", stats.TimerType, map[string]string{"function": "workerProcess"})
				brtQueryStat.Start()
				brt.logger.Debugf("BRT: %s: DB about to read for parameter Filters: %v ", brt.destType, parameterFilters)

				retryList := brt.jobsDB.GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{brt.destType}, Count: toQuery, ParameterFilters: parameterFilters, IgnoreCustomValFiltersInQuery: true})
				toQuery -= len(retryList)
				unprocessedList := brt.jobsDB.GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{brt.destType}, Count: toQuery, ParameterFilters: parameterFilters, IgnoreCustomValFiltersInQuery: true})
				brtQueryStat.End()

				combinedList = append(retryList, unprocessedList...)

				brt.logger.Debugf("BRT: %s: DB Read Complete for parameter Filters: %v retryList: %v, unprocessedList: %v, total: %v", brt.destType, parameterFilters, len(retryList), len(unprocessedList), len(combinedList))
			} else {
				for _, job := range batchDestData.jobs {
					var parameters JobParametersT
					err := json.Unmarshal(job.Parameters, &parameters)
					if err != nil {
						worker.brt.logger.Error("BRT: %s: Unmarshal of job parameters failed. ", worker.brt.destType, string(job.Parameters))
					}

					if parameters.DestinationID == batchDest.Destination.ID {
						combinedList = append(combinedList, job)
					}
				}

				brt.logger.Debugf("BRT: %s: length of jobs for destination id: %s is %d", worker.brt.destType, batchDest.Destination.ID, len(combinedList))
			}
			if len(combinedList) == 0 {
				brt.logger.Debugf("BRT: DB Read Complete. No BRT Jobs to process for parameter Filters: %v", parameterFilters)
				brt.setDestInProgress(batchDest.Destination.ID, false)
				//NOTE: Calling Done on parentWG is important before listening on channel again.
				if batchDestData.parentWG != nil {
					batchDestData.parentWG.Done()
				}
				continue
			}

			var statusList []*jobsdb.JobStatusT
			var drainList []*jobsdb.JobStatusT
			drainCountByDest := make(map[string]int)

			jobsBySource := make(map[string][]*jobsdb.JobT)
			for _, job := range combinedList {
				if router_utils.ToBeDrained(job, batchDest.Destination.ID, toAbortDestinationIDs, brt.destinationsMap) {
					status := jobsdb.JobStatusT{
						JobID:         job.JobID,
						AttemptNum:    job.LastJobStatus.AttemptNum + 1,
						JobState:      jobsdb.Aborted.State,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "",
						ErrorResponse: []byte(`{"reason": "Job aborted since destination was disabled or configured to be aborted via ENV" }`),
					}
					drainList = append(drainList, &status)
					if _, ok := drainCountByDest[batchDest.Destination.ID]; !ok {
						drainCountByDest[batchDest.Destination.ID] = 0
					}
					drainCountByDest[batchDest.Destination.ID] = drainCountByDest[batchDest.Destination.ID] + 1
				} else {
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
			}

			//Mark the drainList jobs as Aborted
			if len(drainList) > 0 {
				err := brt.jobsDB.UpdateJobStatus(drainList, []string{brt.destType}, parameterFilters)
				if err != nil {
					brt.logger.Errorf("Error occurred while marking %s jobs statuses as aborted. Panicking. Err: %v", brt.destType, parameterFilters)
					panic(err)
				}
				for destID, count := range drainCountByDest {
					brt.drainedJobsStat = stats.NewTaggedStat("drained_events", stats.CountType, stats.Tags{
						"destType": brt.destType,
						"destId":   destID,
						"module":   "batchrouter",
					})
					brt.drainedJobsStat.Count(count)
				}
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
						brt.recordDeliveryStatus(*batchJobs.BatchDestination, output, false)
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
						useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchJobs.BatchDestination.Destination.Config)
						objectStorageType := warehouseutils.ObjectStorageType(brt.destType, batchJobs.BatchDestination.Destination.Config, useRudderStorage)
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
						brt.recordDeliveryStatus(*batchJobs.BatchDestination, output, true)
						brt.setJobStatus(batchJobs, true, output.Error, postToWarehouseErr)
						misc.RemoveFilePaths(output.LocalFilePaths...)
						destUploadStat.End()
					}
					wg.Done()
				})
			}

			wg.Wait()
			brt.setDestInProgress(batchDest.Destination.ID, false)
			//NOTE: Calling Done on parentWG is important before listening on channel again.
			if batchDestData.parentWG != nil {
				batchDestData.parentWG.Done()
			}
		}
	}
}

func (brt *HandleT) initWorkers() {
	brt.workers = make([]*workerT, brt.noOfWorkers)
	for i := 0; i < brt.noOfWorkers; i++ {
		worker := &workerT{
			pauseChannel:  make(chan *PauseT),
			resumeChannel: make(chan bool),
			workerID:      i,
			brt:           brt,
		}
		brt.workers[i] = worker
		rruntime.Go(func() {
			worker.workerProcess()
		})
	}
}

type PauseT struct {
	respChannel   chan bool
	wg            *sync.WaitGroup
	waitForResume bool
}

type workerT struct {
	pauseChannel  chan *PauseT
	resumeChannel chan bool
	workerID      int // identifies the worker
	brt           *HandleT
}

type DestinationT struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type BatchJobsT struct {
	Jobs             []*jobsdb.JobT
	BatchDestination *DestinationT
}

func connectionIdentifier(batchDestination DestinationT) string {
	return fmt.Sprintf(`source:%s::destination:%s`, batchDestination.Source.ID, batchDestination.Destination.ID)
}

func (brt *HandleT) warehouseConnectionIdentifier(connIdentifier string, source backendconfig.SourceT, destination backendconfig.DestinationT) string {
	namespace := brt.getNamespace(destination.Config, source, destination, brt.destType)
	return fmt.Sprintf(`namespace:%s::%s`, namespace, connIdentifier)
}

func (brt *HandleT) getNamespace(config interface{}, source backendconfig.SourceT, destination backendconfig.DestinationT, destType string) string {
	configMap := config.(map[string]interface{})
	var namespace string
	if destType == "CLICKHOUSE" {
		//TODO: Handle if configMap["database"] is nil
		return configMap["database"].(string)
	}
	if configMap["namespace"] != nil {
		namespace = configMap["namespace"].(string)
		if len(strings.TrimSpace(namespace)) > 0 {
			return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, namespace))
		}
	}
	return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, source.Name))
}

func (brt *HandleT) isDestInProgress(destID string) bool {
	brt.inProgressMapLock.RLock()
	if brt.inProgressMap[destID] {
		brt.inProgressMapLock.RUnlock()
		return true
	}
	brt.inProgressMapLock.RUnlock()
	return false
}

func (brt *HandleT) setDestInProgress(destID string, starting bool) {
	brt.inProgressMapLock.Lock()
	if starting {
		brt.inProgressMap[destID] = true
	} else {
		delete(brt.inProgressMap, destID)
	}
	brt.inProgressMapLock.Unlock()
}

func (brt *HandleT) uploadFrequencyExceeded(destID string) bool {
	brt.lastExecMapLock.Lock()
	defer brt.lastExecMapLock.Unlock()
	if lastExecTime, ok := brt.lastExecMap[destID]; ok && time.Now().Unix()-lastExecTime < uploadFreqInS {
		return true
	}
	brt.lastExecMap[destID] = time.Now().Unix()
	return false
}

func (brt *HandleT) readAndProcess() {
	brt.configSubscriberLock.RLock()
	destinationsMap := brt.destinationsMap
	brt.configSubscriberLock.RUnlock()

	var jobs []*jobsdb.JobT
	if readPerDestination {
		for destID, batchDest := range destinationsMap {
			if brt.isDestInProgress(destID) {
				brt.logger.Debugf("BRT: Skipping batch router upload loop since destination %s:%s is in progress", batchDest.Destination.DestinationDefinition.Name, destID)
				continue
			}
			if brt.uploadFrequencyExceeded(destID) {
				brt.logger.Debugf("BRT: Skipping batch router upload loop since %s:%s upload freq not exceeded", batchDest.Destination.DestinationDefinition.Name, destID)
				continue
			}
			brt.setDestInProgress(destID, true)

			brt.processQ <- &BatchDestinationDataT{batchDestination: *batchDest, jobs: jobs, parentWG: nil}
		}
	} else {
		if brt.uploadFrequencyExceeded(brt.destType) {
			brt.logger.Debugf("BRT: %s: Skipping batch router read since upload freq not exceeded", brt.destType)
			return
		}

		brt.logger.Debugf("BRT: %s: Reading in mainLoop", brt.destType)
		brtQueryStat := stats.NewTaggedStat("batch_router.jobsdb_query_time", stats.TimerType, map[string]string{"function": "mainLoop"})
		brtQueryStat.Start()
		toQuery := jobQueryBatchSize
		retryList := brt.jobsDB.GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{brt.destType}, Count: toQuery})
		toQuery -= len(retryList)
		unprocessedList := brt.jobsDB.GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{brt.destType}, Count: toQuery})
		brtQueryStat.End()

		jobs = append(retryList, unprocessedList...)
		brt.logger.Debugf("BRT: %s: Length of jobs received: %d", brt.destType, len(jobs))

		var wg sync.WaitGroup
		for destID, batchDest := range destinationsMap {
			brt.setDestInProgress(destID, true)

			wg.Add(1)
			brt.processQ <- &BatchDestinationDataT{batchDestination: *batchDest, jobs: jobs, parentWG: &wg}
		}

		wg.Wait()
	}
}

func (brt *HandleT) mainLoop() {
	for {
		time.Sleep(mainLoopSleep)
		brt.readAndProcess()
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
		downloader, err := brt.fileManagerFactory.New(&filemanager.SettingsT{
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
			if _, ok := brt.uploadedRawDataJobsCache[object.DestinationID]; !ok {
				brt.uploadedRawDataJobsCache[object.DestinationID] = make(map[string]bool)
			}
			brt.uploadedRawDataJobsCache[object.DestinationID][eventID] = true
		}
		reader.Close()
		brt.jobsDB.JournalDeleteEntry(entry.OpID)
	}
}

func (brt *HandleT) crashRecover() {

	brt.jobsDB.DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: []string{brt.destType}, Count: -1})

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
	config.RegisterIntConfigVariable(100000, &jobQueryBatchSize, true, 1, "BatchRouter.jobQueryBatchSize")
	config.RegisterDurationConfigVariable(time.Duration(2), &mainLoopSleep, true, time.Second, []string{"BatchRouter.mainLoopSleep", "BatchRouter.mainLoopSleepInS"}...)
	config.RegisterInt64ConfigVariable(30, &uploadFreqInS, true, 1, "BatchRouter.uploadFreqInS")
	objectStorageDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO", "DIGITAL_OCEAN_SPACES"}
	warehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE", "MSSQL", "AZURE_SYNAPSE"}
	warehouseURL = misc.GetWarehouseURL()
	// Time period for diagnosis ticker
	config.RegisterDurationConfigVariable(time.Duration(600), &diagnosisTickerTime, false, time.Second, []string{"Diagnostics.batchRouterTimePeriod", "Diagnostics.batchRouterTimePeriodInS"}...)
	config.RegisterDurationConfigVariable(time.Duration(3), &warehouseServiceMaxRetryTime, true, time.Hour, []string{"BatchRouter.warehouseServiceMaxRetryTime", "BatchRouter.warehouseServiceMaxRetryTimeinHr"}...)
	config.RegisterBoolConfigVariable(false, &disableEgress, false, "disableEgress")
	config.RegisterBoolConfigVariable(true, &readPerDestination, false, "BatchRouter.readPerDestination")
	config.RegisterStringConfigVariable("", &toAbortDestinationIDs, true, "BatchRouter.toAbortDestinationIDs")
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("batchrouter")

	setQueryFilters()
}

func setQueryFilters() {
	if readPerDestination {
		QueryFilters = jobsdb.QueryFiltersT{CustomVal: true, ParameterFilters: []string{"destination_id"}}
	} else {
		QueryFilters = jobsdb.QueryFiltersT{CustomVal: true}
	}
}

//Setup initializes this module
func (brt *HandleT) Setup(backendConfig backendconfig.BackendConfig, jobsDB, errorDB jobsdb.JobsDB, destType string, reporting types.ReportingI) {

	brt.isBackendConfigInitialized = false
	brt.backendConfigInitialized = make(chan bool)
	brt.fileManagerFactory = filemanager.DefaultFileManagerFactory
	brt.backendConfig = backendConfig
	brt.reporting = reporting
	config.RegisterBoolConfigVariable(true, &brt.reportingEnabled, false, "Reporting.enabled")
	brt.logger = pkgLogger.Child(destType)
	brt.logger.Infof("BRT: Batch Router started: %s", destType)

	//waiting for reporting client setup
	if brt.reporting != nil {
		brt.reporting.WaitForSetup(types.CORE_REPORTING_CLIENT)
	}

	brt.inProgressMap = map[string]bool{}
	brt.lastExecMap = map[string]int64{}
	brt.encounteredMergeRuleMap = map[string]map[string]bool{}
	brt.uploadedRawDataJobsCache = make(map[string]map[string]bool)

	brt.diagnosisTicker = time.NewTicker(diagnosisTickerTime)
	brt.destType = destType
	brt.jobsDB = jobsDB
	brt.errorDB = errorDB
	brt.isEnabled = true
	brt.noOfWorkers = getBatchRouterConfigInt("noOfWorkers", destType, 8)
	config.RegisterIntConfigVariable(128, &brt.maxFailedCountForJob, true, 1, []string{"BatchRouter." + brt.destType + "." + "maxFailedCountForJob", "BatchRouter." + "maxFailedCountForJob"}...)
	config.RegisterDurationConfigVariable(180, &brt.retryTimeWindow, true, time.Minute, []string{"BatchRouter." + brt.destType + "." + "retryTimeWindow", "BatchRouter." + brt.destType + "." + "retryTimeWindowInMins", "BatchRouter." + "retryTimeWindow", "BatchRouter." + "retryTimeWindowInMins"}...)
	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	brt.netHandle = client

	brt.processQ = make(chan *BatchDestinationDataT)
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
	adminInstance.registerBatchRouter(destType, brt)
}

func (brt *HandleT) Start() {
	rruntime.Go(func() {
		<-brt.backendConfigInitialized
		brt.mainLoop()
	})

	brm, err := GetBatchRoutersManager()
	if err != nil {
		panic("Batch Routers manager is nil. Shouldn't happen. Go Debug")
	}
	brm.AddBatchRouter(brt)
}

//
//Pause will pause the batch router
//To completely pause the router, we should pause all the workers
func (brt *HandleT) Pause() {
	brt.pauseLock.Lock()
	defer brt.pauseLock.Unlock()

	if brt.paused {
		return
	}

	//Pause workers
	var wg sync.WaitGroup
	for _, worker := range brt.workers {
		_worker := worker
		wg.Add(1)
		rruntime.Go(func() {
			respChannel := make(chan bool)
			_worker.pauseChannel <- &PauseT{respChannel: respChannel, wg: &wg, waitForResume: true}
			<-respChannel
		})
	}
	wg.Wait()

	brt.paused = true
}

//Resume will resume the batch router
//Resuming all the workers
func (brt *HandleT) Resume() {
	brt.pauseLock.Lock()
	defer brt.pauseLock.Unlock()

	if !brt.paused {
		return
	}

	//Resume workers
	for _, worker := range brt.workers {
		worker.resumeChannel <- true
	}

	brt.paused = false
}
