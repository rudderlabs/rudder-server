package batchrouter

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/rterror"
	destinationConnectionTester "github.com/rudderlabs/rudder-server/services/destination-connection-tester"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/warehouse"
	"github.com/thoas/go-funk"
	"golang.org/x/sync/errgroup"

	uuid "github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	mainLoopSleep, diagnosisTickerTime time.Duration
	uploadFreqInS                      int64
	objectStorageDestinations          []string
	warehouseURL                       string
	warehouseServiceFailedTime         time.Time
	warehouseServiceFailedTimeLock     sync.RWMutex
	warehouseServiceMaxRetryTime       time.Duration
	asyncDestinations                  []string
	pkgLogger                          logger.LoggerI
	Diagnostics                        diagnostics.DiagnosticsI
	QueryFilters                       jobsdb.QueryFiltersT
	readPerDestination                 bool
	disableEgress                      bool
	toAbortDestinationIDs              string
	netClientTimeout                   time.Duration
	transformerURL                     string
	datePrefixOverride                 string
	dateFormatLayouts                  map[string]string // string -> string
	dateFormatMap                      map[string]string // (sourceId:destinationId) -> dateFormat
	dateFormatMapLock                  sync.RWMutex
)

type HandleT struct {
	destType                       string
	destinationsMap                map[string]*router_utils.BatchDestinationT // destinationID -> destination
	connectionWHNamespaceMap       map[string]string                          // connectionIdentifier -> warehouseConnectionIdentifier(+namepsace)
	netHandle                      *http.Client
	processQ                       chan *BatchDestinationDataT
	jobsDB                         jobsdb.JobsDB
	errorDB                        jobsdb.JobsDB
	isEnabled                      bool
	batchRequestsMetricLock        sync.RWMutex
	multitenantI                   multitenant.MultiTenantI
	diagnosisTicker                *time.Ticker
	batchRequestsMetric            []batchRequestMetric
	logger                         logger.LoggerI
	noOfWorkers                    int
	maxEventsInABatch              int
	maxFailedCountForJob           int
	asyncUploadTimeout             time.Duration
	uploadIntervalMap              map[string]time.Duration
	retryTimeWindow                time.Duration
	reporting                      types.ReportingI
	reportingEnabled               bool
	workers                        []*workerT
	drainedJobsStat                stats.RudderStats
	backendConfig                  backendconfig.BackendConfig
	fileManagerFactory             filemanager.FileManagerFactory
	inProgressMap                  map[string]bool
	inProgressMapLock              sync.RWMutex
	lastExecMap                    map[string]int64
	lastExecMapLock                sync.RWMutex
	configSubscriberLock           sync.RWMutex
	encounteredMergeRuleMap        map[string]map[string]bool
	uploadedRawDataJobsCache       map[string]map[string]bool
	encounteredMergeRuleMapLock    sync.RWMutex
	isBackendConfigInitialized     bool
	backendConfigInitialized       chan bool
	asyncDestinationStruct         map[string]*asyncdestinationmanager.AsyncDestinationStruct
	jobQueryBatchSize              int
	pollStatusLoopSleep            time.Duration
	asyncUploadWorkerPauseChannel  chan *PauseT
	asyncUploadWorkerResumeChannel chan bool
	pollAsyncStatusPauseChannel    chan *PauseT
	pollAsyncStatusResumeChannel   chan bool
	pollTimeStat                   stats.RudderStats
	failedJobsTimeStat             stats.RudderStats
	successfulJobCount             stats.RudderStats
	failedJobCount                 stats.RudderStats
	abortedJobCount                stats.RudderStats

	backgroundGroup  *errgroup.Group
	backgroundCtx    context.Context
	backgroundCancel context.CancelFunc
	backgroundWait   func() error

	payloadLimit     int64
	transientSources transientsource.Service
}

type BatchDestinationDataT struct {
	batchDestination router_utils.BatchDestinationT
	jobs             []*jobsdb.JobT
	parentWG         *sync.WaitGroup
}
type AsyncPollT struct {
	Config   map[string]interface{} `json:"config"`
	ImportId string                 `json:"importId"`
	DestType string                 `json:"destType"`
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
	SourceID                string `json:"source_id"`
	DestinationID           string `json:"destination_id"`
	ReceivedAt              string `json:"received_at"`
	TransformAt             string `json:"transform_at"`
	SourceBatchID           string `json:"source_batch_id"`
	SourceTaskID            string `json:"source_task_id"`
	SourceTaskRunID         string `json:"source_task_run_id"`
	SourceJobID             string `json:"source_job_id"`
	SourceJobRunID          string `json:"source_job_run_id"`
	SourceDefinitionID      string `json:"source_definition_id"`
	DestinationDefinitionID string `json:"destination_definition_id"`
	SourceCategory          string `json:"source_category"`
	EventName               string `json:"event_name"`
	EventType               string `json:"event_type"`
	MessageID               string `json:"message_id"`
}

func (brt *HandleT) backendConfigSubscriber() {
	ch := make(chan pubsub.DataEvent)
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
							brt.uploadIntervalMap[destination.ID] = brt.parseUploadIntervalFromConfig(destination.Config)
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
	FileLocation     string
	LocalFilePaths   []string
	JournalOpID      int64
	Error            error
	FirstEventAt     string
	LastEventAt      string
	TotalEvents      int
	UseRudderStorage bool
}

type AsyncStatusResponse struct {
	Success        bool
	StatusCode     int
	HasFailed      bool
	HasWarning     bool
	FailedJobsURL  string
	WarningJobsURL string
}
type ErrorResponseT struct {
	Error string
}

func isJobTerminated(status int) bool {
	if status == 429 {
		return false
	}

	return status >= 200 && status < 500
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

func (brt *HandleT) pollAsyncStatus(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case pause := <-brt.pollAsyncStatusPauseChannel:
			pkgLogger.Infof("pollAsyncStatus is paused. Dest type: %s", brt.destType)
			pause.respChannel <- true
			select {
			case <-ctx.Done():
				return
			case <-brt.pollAsyncStatusResumeChannel:
				pkgLogger.Infof("pollAsyncStatus is resumed. Dest type: %s", brt.destType)
			}
		case <-time.After(brt.pollStatusLoopSleep):
			brt.configSubscriberLock.RLock()
			destinationsMap := brt.destinationsMap
			brt.configSubscriberLock.RUnlock()

			for key := range destinationsMap {
				if IsAsyncDestination(brt.destType) {
					pkgLogger.Debugf("pollAsyncStatus Started for Dest type: %s", brt.destType)
					parameterFilters := make([]jobsdb.ParameterFilterT, 0)
					for _, param := range QueryFilters.ParameterFilters {
						parameterFilter := jobsdb.ParameterFilterT{
							Name:     param,
							Value:    key,
							Optional: false,
						}
						parameterFilters = append(parameterFilters, parameterFilter)
					}
					importingJob := brt.jobsDB.GetImportingList(
						jobsdb.GetQueryParamsT{
							CustomValFilters: []string{brt.destType},
							JobsLimit:        1,
							ParameterFilters: parameterFilters,
							PayloadSizeLimit: brt.payloadLimit,
						},
					)
					if len(importingJob) != 0 {
						importingJob := importingJob[0]
						parameters := importingJob.LastJobStatus.Parameters
						pollUrl := gjson.GetBytes(parameters, "pollURL").String()
						importId := gjson.GetBytes(parameters, "importId").String()
						csvHeaders := gjson.GetBytes(parameters, "metadata.csvHeader").String()
						var pollStruct AsyncPollT
						pollStruct.ImportId = importId
						pollStruct.Config = brt.destinationsMap[key].Destination.Config
						pollStruct.DestType = strings.ToLower(brt.destType)
						payload, err := json.Marshal(pollStruct)
						if err != nil {
							panic("JSON Marshal Failed" + err.Error())
						}

						startPollTime := time.Now()
						pkgLogger.Debugf("[Batch Router] Poll Status Started for Dest Type %v", brt.destType)
						bodyBytes, statusCode := misc.HTTPCallWithRetryWithTimeout(transformerURL+pollUrl, payload, asyncdestinationmanager.HTTPTimeout)
						pkgLogger.Debugf("[Batch Router] Poll Status Finished for Dest Type %v", brt.destType)
						brt.pollTimeStat.Since(startPollTime)

						if err != nil {
							panic("HTTP Request Failed" + err.Error())
						}
						if statusCode == 200 {
							var asyncResponse AsyncStatusResponse
							if err != nil {
								panic("Read Body Failed" + err.Error())
							}
							err = json.Unmarshal(bodyBytes, &asyncResponse)
							if err != nil {
								panic("JSON Unmarshal Failed" + err.Error())
							}
							uploadStatus := asyncResponse.Success
							statusCode := asyncResponse.StatusCode
							abortedJobs := make([]*jobsdb.JobT, 0)
							if uploadStatus {
								var statusList []*jobsdb.JobStatusT
								importingList := brt.jobsDB.GetImportingList(
									jobsdb.GetQueryParamsT{
										CustomValFilters: []string{brt.destType},
										JobsLimit:        brt.maxEventsInABatch,
										ParameterFilters: parameterFilters,
										PayloadSizeLimit: brt.payloadLimit,
									},
								)
								if !asyncResponse.HasFailed {
									for _, job := range importingList {
										status := jobsdb.JobStatusT{
											JobID:         job.JobID,
											JobState:      jobsdb.Succeeded.State,
											ExecTime:      time.Now(),
											RetryTime:     time.Now(),
											ErrorCode:     "",
											ErrorResponse: []byte(`{}`),
											Parameters:    []byte(`{}`),
											WorkspaceId:   job.WorkspaceId,
										}
										statusList = append(statusList, &status)
									}
									brt.successfulJobCount.Count(len(statusList))
								} else {
									failedJobUrl := asyncResponse.FailedJobsURL
									payload = asyncdestinationmanager.GenerateFailedPayload(brt.destinationsMap[key].Destination.Config, importingList, importId, brt.destType, csvHeaders)
									startFailedJobsPollTime := time.Now()
									pkgLogger.Debugf("[Batch Router] Fetching Failed Jobs Started for Dest Type %v", brt.destType)
									failedBodyBytes, statusCode := misc.HTTPCallWithRetryWithTimeout(transformerURL+failedJobUrl, payload, asyncdestinationmanager.HTTPTimeout)
									pkgLogger.Debugf("[Batch Router] Fetching Failed Jobs for Dest Type %v", brt.destType)
									brt.failedJobsTimeStat.Since(startFailedJobsPollTime)

									if statusCode != 200 {
										continue
									}
									var failedJobsResponse map[string]interface{}
									err = json.Unmarshal(failedBodyBytes, &failedJobsResponse)
									if err != nil {
										panic("JSON Unmarshal Failed" + err.Error())
									}
									internalStatusCode, ok := failedJobsResponse["status"].(string)
									if internalStatusCode != "200" || !ok {
										pkgLogger.Errorf("[Batch Router] Failed to fetch failed jobs for Dest Type %v with statusCode %v and body %v", brt.destType, internalStatusCode, string(failedBodyBytes))
										continue
									}
									metadata, ok := failedJobsResponse["metadata"].(map[string]interface{})
									if !ok {
										pkgLogger.Errorf("[Batch Router] Failed to typecast failed jobs response for Dest Type %v with statusCode %v and body %v with error %v", brt.destType, internalStatusCode, string(failedBodyBytes), err)
										continue
									}
									failedKeys, errFailed := misc.ConvertStringInterfaceToIntArray(metadata["failedKeys"])
									warningKeys, errWarning := misc.ConvertStringInterfaceToIntArray(metadata["warningKeys"])
									succeededKeys, errSuccess := misc.ConvertStringInterfaceToIntArray(metadata["succeededKeys"])
									var status *jobsdb.JobStatusT
									if errFailed != nil || errWarning != nil || errSuccess != nil || statusCode != 200 {
										for _, job := range importingList {
											jobID := job.JobID
											status = &jobsdb.JobStatusT{
												JobID:         jobID,
												JobState:      jobsdb.Failed.State,
												ExecTime:      time.Now(),
												RetryTime:     time.Now(),
												ErrorCode:     strconv.Itoa(statusCode),
												ErrorResponse: []byte(`{}`),
												Parameters:    []byte(`{}`),
												WorkspaceId:   job.WorkspaceId,
											}
											statusList = append(statusList, status)
										}
										brt.failedJobCount.Count(len(statusList))
										txn := brt.jobsDB.BeginGlobalTransaction()
										brt.jobsDB.AcquireUpdateJobStatusLocks()
										err = brt.jobsDB.UpdateJobStatusInTxn(txn, statusList, []string{brt.destType}, parameterFilters)
										if err != nil {
											brt.logger.Errorf("[Batch Router] Error occurred while updating %s jobs statuses. Panicking. Err: %v", brt.destType, err)
											panic(err)
										}
										brt.jobsDB.CommitTransaction(txn)
										brt.jobsDB.ReleaseUpdateJobStatusLocks()
										continue
									}
									for _, job := range importingList {
										jobID := job.JobID
										if misc.ContainsInt64(append(succeededKeys, warningKeys...), jobID) {
											status = &jobsdb.JobStatusT{
												JobID:         jobID,
												JobState:      jobsdb.Succeeded.State,
												ExecTime:      time.Now(),
												RetryTime:     time.Now(),
												ErrorCode:     "200",
												ErrorResponse: []byte(`{}`),
												Parameters:    []byte(`{}`),
												WorkspaceId:   job.WorkspaceId,
											}
										} else if misc.ContainsInt64(failedKeys, job.JobID) {
											errorResp, _ := json.Marshal(ErrorResponseT{Error: gjson.GetBytes(failedBodyBytes, fmt.Sprintf("metadata.failedReasons.%v", job.JobID)).String()})
											status = &jobsdb.JobStatusT{
												JobID:         jobID,
												JobState:      jobsdb.Aborted.State,
												ExecTime:      time.Now(),
												RetryTime:     time.Now(),
												ErrorCode:     "",
												ErrorResponse: errorResp,
												Parameters:    []byte(`{}`),
												WorkspaceId:   job.WorkspaceId,
											}
											abortedJobs = append(abortedJobs, job)
										}
										statusList = append(statusList, status)
									}
								}
								brt.successfulJobCount.Count(len(statusList) - len(abortedJobs))
								brt.abortedJobCount.Count(len(abortedJobs))
								if len(abortedJobs) > 0 {
									err := brt.errorDB.Store(abortedJobs)
									if err != nil {
										brt.logger.Errorf("Error occurred while storing %s jobs into ErrorDB. Panicking. Err: %v", brt.destType, err)
										panic(err)
									}
								}
								txn := brt.jobsDB.BeginGlobalTransaction()
								brt.jobsDB.AcquireUpdateJobStatusLocks()
								err = brt.jobsDB.UpdateJobStatusInTxn(txn, statusList, []string{brt.destType}, parameterFilters)
								if err != nil {
									brt.logger.Errorf("[Batch Router] Error occurred while updating %s jobs statuses. Panicking. Err: %v", brt.destType, err)
									panic(err)
								}
								brt.jobsDB.CommitTransaction(txn)
								brt.jobsDB.ReleaseUpdateJobStatusLocks()
							} else if statusCode != 0 {
								var statusList []*jobsdb.JobStatusT
								importingList := brt.jobsDB.GetImportingList(
									jobsdb.GetQueryParamsT{
										CustomValFilters: []string{brt.destType},
										JobsLimit:        brt.maxEventsInABatch,
										ParameterFilters: parameterFilters,
										PayloadSizeLimit: brt.payloadLimit,
									},
								)
								if isJobTerminated(statusCode) {
									for _, job := range importingList {
										status := jobsdb.JobStatusT{
											JobID:         job.JobID,
											JobState:      jobsdb.Aborted.State,
											ExecTime:      time.Now(),
											RetryTime:     time.Now(),
											ErrorCode:     "",
											ErrorResponse: []byte(`{}`),
											Parameters:    []byte(`{}`),
											WorkspaceId:   job.WorkspaceId,
										}
										statusList = append(statusList, &status)
										abortedJobs = append(abortedJobs, job)
									}
									brt.abortedJobCount.Count(len(importingList))
								} else {
									for _, job := range importingList {
										status := jobsdb.JobStatusT{
											JobID:         job.JobID,
											JobState:      jobsdb.Failed.State,
											ExecTime:      time.Now(),
											RetryTime:     time.Now(),
											ErrorCode:     "",
											ErrorResponse: []byte(`{}`),
											Parameters:    []byte(`{}`),
											WorkspaceId:   job.WorkspaceId,
										}
										statusList = append(statusList, &status)
									}
									brt.failedJobCount.Count(len(importingList))
								}
								if len(abortedJobs) > 0 {
									err := brt.errorDB.Store(abortedJobs)
									if err != nil {
										brt.logger.Errorf("Error occurred while storing %s jobs into ErrorDB. Panicking. Err: %v", brt.destType, err)
										panic(err)
									}
								}
								txn := brt.jobsDB.BeginGlobalTransaction()
								brt.jobsDB.AcquireUpdateJobStatusLocks()
								err = brt.jobsDB.UpdateJobStatusInTxn(txn, statusList, []string{brt.destType}, parameterFilters)
								if err != nil {
									brt.logger.Errorf("[Batch Router] Error occurred while updating %s jobs statuses. Panicking. Err: %v", brt.destType, err)
									panic(err)
								}
								brt.jobsDB.CommitTransaction(txn)
								brt.jobsDB.ReleaseUpdateJobStatusLocks()
							} else {
								continue
							}
						} else {
							continue
						}

					}
				}
			}
		}
	}
}

func (brt *HandleT) copyJobsToStorage(provider string, batchJobs *BatchJobsT, makeJournalEntry bool, isWarehouse bool) StorageUploadOutput {
	if disableEgress {
		return StorageUploadOutput{Error: rterror.DisabledEgress}
	}

	var localTmpDirName string
	if isWarehouse {
		localTmpDirName = fmt.Sprintf(`/%s/`, misc.RudderWarehouseStagingUploads)
	} else {
		localTmpDirName = fmt.Sprintf(`/%s/`, misc.RudderRawDataDestinationLogs)
	}

	uuid := uuid.Must(uuid.NewV4())
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
			}
			brt.encounteredMergeRuleMap[warehouseConnIdentifier][ruleIdentifier] = true
			brt.encounteredMergeRuleMapLock.Unlock()
			brt.configSubscriberLock.Unlock()
		}

		eventID := gjson.GetBytes(job.EventPayload, "messageId").String()
		var ok bool
		interruptedEventsMap, isDestInterrupted := brt.uploadedRawDataJobsCache[batchJobs.BatchDestination.Destination.ID]
		if isDestInterrupted {
			if _, ok = interruptedEventsMap[eventID]; !ok {
				eventsFound = true
				_ = gzWriter.WriteGZ(string(job.EventPayload) + "\n")
			}
		} else {
			eventsFound = true
			_ = gzWriter.WriteGZ(string(job.EventPayload) + "\n")
		}
	}
	_ = gzWriter.CloseGZ()
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
		return StorageUploadOutput{
			Error:          err,
			LocalFilePaths: []string{gzipFilePath},
		}
	}

	outputFile, err := os.Open(gzipFilePath)
	if err != nil {
		panic(err)
	}

	brt.logger.Debugf("BRT: Starting upload to %s", provider)
	folderName := ""
	if isWarehouse {
		folderName = config.GetEnv("WAREHOUSE_STAGING_BUCKET_FOLDER_NAME", "rudder-warehouse-staging-logs")
	} else {
		folderName = config.GetEnv("DESTINATION_BUCKET_FOLDER_NAME", "rudder-logs")
	}

	var datePrefixLayout string
	if datePrefixOverride != "" {
		datePrefixLayout = datePrefixOverride
	} else {
		dateFormat, _ := GetStorageDateFormat(uploader, batchJobs.BatchDestination, folderName)
		datePrefixLayout = dateFormat
	}

	brt.logger.Debugf("BRT: Date prefix layout is %s", datePrefixLayout)
	switch datePrefixLayout {
	case "MM-DD-YYYY": //used to be earlier default
		datePrefixLayout = time.Now().Format("01-02-2006")
	default:
		datePrefixLayout = time.Now().Format("2006-01-02")
	}
	keyPrefixes := []string{folderName, batchJobs.BatchDestination.Source.ID, datePrefixLayout}

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

	startTime := time.Now()
	uploadOutput, err := uploader.Upload(context.TODO(), outputFile, keyPrefixes...)
	uploadSuccess := err == nil
	brtUploadTimeStat := stats.NewTaggedStat("brt_upload_time", stats.TimerType, map[string]string{
		"success":     strconv.FormatBool(uploadSuccess),
		"destType":    brt.destType,
		"destination": batchJobs.BatchDestination.Destination.ID,
	})
	brtUploadTimeStat.Since(startTime)

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
		FileLocation:     uploadOutput.Location,
		LocalFilePaths:   []string{gzipFilePath},
		JournalOpID:      opID,
		FirstEventAt:     firstEventAt,
		LastEventAt:      lastEventAt,
		TotalEvents:      len(batchJobs.Jobs) - dedupedIDMergeRuleJobs,
		UseRudderStorage: useRudderStorage,
	}
}

func (brt *HandleT) sendJobsToStorage(provider string, batchJobs BatchJobsT, config map[string]interface{}, makeJournalEntry bool, isAsync bool) {
	if disableEgress {
		out := asyncdestinationmanager.AsyncUploadOutput{}
		for _, job := range batchJobs.Jobs {
			out.SucceededJobIDs = append(out.SucceededJobIDs, job.JobID)
			out.SuccessResponse = fmt.Sprintf(`{"error":"%s"`, rterror.DisabledEgress.Error())
		}
		brt.setMultipleJobStatus(out)
		return
	}

	destinationID := batchJobs.BatchDestination.Destination.ID
	brt.logger.Debugf("BRT: Starting logging to %s", provider)
	_, ok := brt.asyncDestinationStruct[destinationID]
	if ok {
		brt.asyncDestinationStruct[destinationID].UploadMutex.Lock()
		defer brt.asyncDestinationStruct[destinationID].UploadMutex.Unlock()
		if brt.asyncDestinationStruct[destinationID].CanUpload {
			out := asyncdestinationmanager.AsyncUploadOutput{}
			for _, job := range batchJobs.Jobs {
				out.FailedJobIDs = append(out.FailedJobIDs, job.JobID)
				out.FailedReason = `{"error":"Jobs flowed over the prescribed limit"}`
			}
			brt.setMultipleJobStatus(out)
			return
		}
	}

	if !ok || !brt.asyncDestinationStruct[destinationID].Exists {
		if !ok {
			asyncStruct := &asyncdestinationmanager.AsyncDestinationStruct{}
			asyncStruct.UploadMutex.Lock()
			defer asyncStruct.UploadMutex.Unlock()
			brt.asyncDestinationStruct[destinationID] = asyncStruct
		}
		brt.asyncStructSetup(batchJobs.BatchDestination.Source.ID, destinationID)
	}

	file, err := os.OpenFile(brt.asyncDestinationStruct[destinationID].FileName, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(fmt.Errorf("BRT: %s: file open failed : %s", brt.destType, err.Error()))
	}
	defer file.Close()
	var jobString string
	writeAtBytes := brt.asyncDestinationStruct[destinationID].Size
	for _, job := range batchJobs.Jobs {
		transformedData := asyncdestinationmanager.GetTransformedData(job.EventPayload)
		if brt.asyncDestinationStruct[destinationID].Count < brt.maxEventsInABatch {
			fileData := asyncdestinationmanager.GetMarshalledData(transformedData, job.JobID)
			brt.asyncDestinationStruct[destinationID].Size = brt.asyncDestinationStruct[destinationID].Size + len([]byte(fileData+"\n"))
			jobString = jobString + fileData + "\n"
			brt.asyncDestinationStruct[destinationID].ImportingJobIDs = append(brt.asyncDestinationStruct[destinationID].ImportingJobIDs, job.JobID)
			brt.asyncDestinationStruct[destinationID].Count = brt.asyncDestinationStruct[destinationID].Count + 1
			brt.asyncDestinationStruct[destinationID].URL = gjson.Get(string(job.EventPayload), "endpoint").String()
		} else {
			brt.asyncDestinationStruct[destinationID].CanUpload = true
			brt.logger.Debugf("BRT: Max Event Limit Reached.Stopped writing to File  %s", brt.asyncDestinationStruct[destinationID].FileName)
			brt.asyncDestinationStruct[destinationID].URL = gjson.Get(string(job.EventPayload), "endpoint").String()
			brt.asyncDestinationStruct[destinationID].FailedJobIDs = append(brt.asyncDestinationStruct[destinationID].FailedJobIDs, job.JobID)
		}
	}

	_, err = file.WriteAt([]byte(jobString), int64(writeAtBytes))
	if err != nil {
		panic(fmt.Errorf("BRT: %s: file write failed : %s", brt.destType, err.Error()))
	}
}

func (brt *HandleT) asyncUploadWorker(ctx context.Context) {
	if !IsAsyncDestination(brt.destType) {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case pause := <-brt.asyncUploadWorkerPauseChannel:
			pkgLogger.Infof("asyncUploadWorker is paused. Dest type: %s", brt.destType)
			pause.respChannel <- true
			select {
			case <-ctx.Done():
				return
			case <-brt.asyncUploadWorkerResumeChannel:
				pkgLogger.Infof("asyncUploadWorker is resumed. Dest type: %s", brt.destType)
			}
		case <-time.After(10 * time.Millisecond):
			brt.configSubscriberLock.RLock()
			destinationsMap := brt.destinationsMap
			uploadIntervalMap := brt.uploadIntervalMap
			brt.configSubscriberLock.RUnlock()

			for destinationID := range destinationsMap {
				_, ok := brt.asyncDestinationStruct[destinationID]
				if !ok {
					continue
				}

				timeElapsed := time.Since(brt.asyncDestinationStruct[destinationID].CreatedAt)
				brt.asyncDestinationStruct[destinationID].UploadMutex.Lock()

				timeout := uploadIntervalMap[destinationID]
				if brt.asyncDestinationStruct[destinationID].Exists && (brt.asyncDestinationStruct[destinationID].CanUpload || timeElapsed > timeout) {
					brt.asyncDestinationStruct[destinationID].CanUpload = true
					uploadResponse := asyncdestinationmanager.Upload(resolveURL(transformerURL, brt.asyncDestinationStruct[destinationID].URL), brt.asyncDestinationStruct[destinationID].FileName, destinationsMap[destinationID].Destination.Config, brt.destType, brt.asyncDestinationStruct[destinationID].FailedJobIDs, brt.asyncDestinationStruct[destinationID].ImportingJobIDs, destinationID)
					brt.asyncStructCleanUp(destinationID)
					brt.setMultipleJobStatus(uploadResponse)
				}
				brt.asyncDestinationStruct[destinationID].UploadMutex.Unlock()
			}
		}
	}
}

func resolveURL(base, relative string) string {
	baseURL, err := url.Parse(base)
	if err != nil {
		pkgLogger.Fatal(err)
	}
	relURL, err := url.Parse(relative)
	if err != nil {
		pkgLogger.Fatal(err)
	}
	destURL := baseURL.ResolveReference(relURL).String()
	return destURL
}

func (brt *HandleT) parseUploadIntervalFromConfig(destinationConfig map[string]interface{}) time.Duration {
	uploadInterval, ok := destinationConfig["uploadInterval"]
	if !ok {
		brt.logger.Debugf("BRT: uploadInterval not found in destination config, falling back to default: %s", brt.asyncUploadTimeout)
		return brt.asyncUploadTimeout
	}
	dur, ok := uploadInterval.(string)
	if !ok {
		brt.logger.Warnf("BRT: not found string type uploadInterval, falling back to default: %s", brt.asyncUploadTimeout)
		return brt.asyncUploadTimeout
	}
	parsedTime, err := strconv.ParseInt(dur, 10, 64)
	if err != nil {
		brt.logger.Warnf("BRT: Couldn't parseint uploadInterval, falling back to default: %s", brt.asyncUploadTimeout)
		return brt.asyncUploadTimeout
	}
	return time.Duration(parsedTime * int64(time.Minute))
}

func (brt *HandleT) asyncStructSetup(sourceID, destinationID string) {
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	uuid := uuid.Must(uuid.NewV4())

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	path := fmt.Sprintf("%v%v", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v", sourceID, uuid.String()))
	jsonPath := fmt.Sprintf(`%v.txt`, path)
	err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	brt.asyncDestinationStruct[destinationID].Exists = true
	brt.asyncDestinationStruct[destinationID].FileName = jsonPath
	brt.asyncDestinationStruct[destinationID].CreatedAt = time.Now()
}

func (brt *HandleT) asyncStructCleanUp(destinationID string) {
	misc.RemoveFilePaths(brt.asyncDestinationStruct[destinationID].FileName)
	brt.asyncDestinationStruct[destinationID].ImportingJobIDs = []int64{}
	brt.asyncDestinationStruct[destinationID].FailedJobIDs = []int64{}
	brt.asyncDestinationStruct[destinationID].Size = 0
	brt.asyncDestinationStruct[destinationID].Exists = false
	brt.asyncDestinationStruct[destinationID].Count = 0
	brt.asyncDestinationStruct[destinationID].CanUpload = false
	brt.asyncDestinationStruct[destinationID].URL = ""
}

func GetFullPrefix(manager filemanager.FileManager, prefix string) (fullPrefix string) {
	fullPrefix = prefix
	configPrefix := manager.GetConfiguredPrefix()

	if configPrefix != "" {
		if configPrefix[len(configPrefix)-1:] == "/" {
			fullPrefix = configPrefix + prefix
		} else {
			fullPrefix = configPrefix + "/" + prefix
		}
	}
	return
}

func isDateFormatExists(connIdentifier string) bool {
	dateFormatMapLock.RLock()
	defer dateFormatMapLock.RUnlock()
	_, ok := dateFormatMap[connIdentifier]
	return ok
}

func GetStorageDateFormat(manager filemanager.FileManager, destination *DestinationT, folderName string) (dateFormat string, err error) {
	connIdentifier := connectionIdentifier(DestinationT{Destination: destination.Destination, Source: destination.Source})
	if isDateFormatExists(connIdentifier) {
		return dateFormatMap[connIdentifier], err
	}

	defer func() {
		if err == nil {
			dateFormatMapLock.RLock()
			defer dateFormatMapLock.RUnlock()
			dateFormatMap[connIdentifier] = dateFormat
		}
	}()

	dateFormat = "YYYY-MM-DD"
	prefixes := []string{folderName, destination.Source.ID}
	prefix := strings.Join(prefixes[0:2], "/")
	fullPrefix := GetFullPrefix(manager, prefix)
	fileObjects, err := manager.ListFilesWithPrefix(context.TODO(), fullPrefix, 5)
	if err != nil {
		pkgLogger.Errorf("[BRT]: Failed to fetch fileObjects with connIdentifier: %s, prefix: %s, Err: %v", connIdentifier, fullPrefix, err)
		// Returning the earlier default as we might not able to fetch the list.
		// because "*:GetObject" and "*:ListBucket" permissions are not available.
		dateFormat = "MM-DD-YYYY"
		return
	}
	if len(fileObjects) == 0 {
		return
	}

	for idx := range fileObjects {
		if fileObjects[idx] == nil {
			pkgLogger.Errorf("[BRT]: nil occurred in file objects for '%T' filemanager of destination ID : %s", manager, destination.Destination.ID)
			continue
		}
		key := fileObjects[idx].Key
		replacedKey := strings.Replace(key, fullPrefix, "", 1)
		splittedKeys := strings.Split(replacedKey, "/")
		if len(splittedKeys) > 1 {
			date := splittedKeys[1]
			for layout, format := range dateFormatLayouts {
				_, err = time.Parse(layout, date)
				if err == nil {
					dateFormat = format
					return
				}
			}
		}
	}
	return
}

func (brt *HandleT) postToWarehouse(batchJobs *BatchJobsT, output StorageUploadOutput) (err error) {
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
			} else if columnType == "text" && schemaMap[tableName][columnName] == "string" {
				// this condition is required for altering string to text. if schemaMap[tableName][columnName] has string and in the next job if it has text type then we change schemaMap[tableName][columnName] to text
				schemaMap[tableName][columnName] = columnType
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

	if misc.ContainsString(warehouseutils.TimeWindowDestinations, brt.destType) {
		payload.TimeWindow = batchJobs.TimeWindow
	}

	jsonPayload, err := json.Marshal(&payload)
	if err != nil {
		brt.logger.Errorf("BRT: Failed to marshal WH staging file payload error:%v", err)
	}
	uri := fmt.Sprintf(`%s/v1/process`, warehouseURL)
	resp, err := brt.netHandle.Post(uri, "application/json; charset=utf-8",
		bytes.NewBuffer(jsonPayload))
	if err != nil {
		brt.logger.Errorf("BRT: Failed to route staging file URL to warehouse service@%v, error:%v", uri, err)
	} else {
		brt.logger.Infof("BRT: Routed successfully staging file URL to warehouse service@%v", uri)
		defer resp.Body.Close()
	}

	return
}

func (brt *HandleT) setJobStatus(batchJobs *BatchJobsT, isWarehouse bool, errOccurred error, postToWarehouseErr bool) {
	var (
		batchJobState string
		errorResp     []byte
	)
	batchRouterWorkspaceJobStatusCount := make(map[string]map[string]int)
	jobRunIDAbortedEventsMap := make(map[string][]*router.FailedEventRowT)
	var abortedEvents []*jobsdb.JobT
	var batchReqMetric batchRequestMetric
	if errOccurred != nil {
		switch {
		case errors.Is(errOccurred, rterror.DisabledEgress):
			brt.logger.Debugf("BRT: Outgoing traffic disabled : %v at %v", batchJobs.BatchDestination.Source.ID,
				time.Now().Format("01-02-2006"))
			batchJobState = jobsdb.Succeeded.State
			errorResp = []byte(fmt.Sprintf(`{"success":"%s"}`, errOccurred.Error()))
		case errors.Is(errOccurred, rterror.InvalidServiceProvider):
			brt.logger.Warnf("BRT: Destination %s : %s for destination ID : %v at %v",
				batchJobs.BatchDestination.Destination.DestinationDefinition.DisplayName, errOccurred.Error(),
				batchJobs.BatchDestination.Destination.ID, time.Now().Format("01-02-2006"))
			batchJobState = jobsdb.Aborted.State
			errorResp = []byte(fmt.Sprintf(`{"reason":"%s"}`, errOccurred.Error()))
		default:
			brt.logger.Errorf("BRT: Error uploading to object storage: %v %v", errOccurred, batchJobs.BatchDestination.Source.ID)
			batchJobState = jobsdb.Failed.State
			errorResp, _ = json.Marshal(ErrorResponseT{Error: errOccurred.Error()})
			batchReqMetric.batchRequestFailed = 1
			// We keep track of number of failed attempts in case of failure and number of events uploaded in case of success in stats
		}
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

	var err error
	reportMetrics := make([]*types.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*types.ConnectionDetails)
	transformedAtMap := make(map[string]string)
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

		var parameters JobParametersT
		err = json.Unmarshal(job.Parameters, &parameters)
		if err != nil {
			brt.logger.Error("Unmarshal of job parameters failed. ", string(job.Parameters))
		}

		timeElapsed := time.Since(firstAttemptedAt)
		switch jobState {
		case jobsdb.Failed.State:
			if !postToWarehouseErr && timeElapsed > brt.retryTimeWindow && job.LastJobStatus.AttemptNum >= brt.
				maxFailedCountForJob {
				job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "stage", "batch_router")
				job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "reason", errOccurred.Error())
				abortedEvents = append(abortedEvents, job)
				router.PrepareJobRunIdAbortedEventsMap(job.Parameters, jobRunIDAbortedEventsMap)
				jobState = jobsdb.Aborted.State
			}
			if postToWarehouseErr && isWarehouse {
				// change job state to abort state after warehouse service is continuously failing more than warehouseServiceMaxRetryTimeinHr time
				warehouseServiceFailedTimeLock.RLock()
				if time.Since(warehouseServiceFailedTime) > warehouseServiceMaxRetryTime {
					job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "stage", "batch_router")
					job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "reason", errOccurred.Error())
					abortedEvents = append(abortedEvents, job)
					router.PrepareJobRunIdAbortedEventsMap(job.Parameters, jobRunIDAbortedEventsMap)
					jobState = jobsdb.Aborted.State
				}
				warehouseServiceFailedTimeLock.RUnlock()
			}
		case jobsdb.Aborted.State:
			job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "stage", "batch_router")
			job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "reason", errOccurred.Error())
			abortedEvents = append(abortedEvents, job)
			router.PrepareJobRunIdAbortedEventsMap(job.Parameters, jobRunIDAbortedEventsMap)
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
			Parameters:    []byte(`{}`),
			WorkspaceId:   job.WorkspaceId,
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
			var cd *types.ConnectionDetails
			workspaceID := brt.backendConfig.GetWorkspaceIDForSourceID((parameters.SourceID))
			_, ok := batchRouterWorkspaceJobStatusCount[workspaceID]
			if !ok {
				batchRouterWorkspaceJobStatusCount[workspaceID] = make(map[string]int)
			}
			key := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s", parameters.SourceID, parameters.DestinationID, parameters.SourceBatchID, jobState, strconv.Itoa(errorCode), parameters.EventName, parameters.EventType)
			_, ok = connectionDetailsMap[key]
			if !ok {
				cd = types.CreateConnectionDetail(parameters.SourceID, parameters.DestinationID, parameters.SourceBatchID, parameters.SourceTaskID, parameters.SourceTaskRunID, parameters.SourceJobID, parameters.SourceJobRunID, parameters.SourceDefinitionID, parameters.DestinationDefinitionID, parameters.SourceCategory)
				connectionDetailsMap[key] = cd
				transformedAtMap[key] = parameters.TransformAt
			}
			sd, ok := statusDetailsMap[key]
			if !ok {
				sampleEvent := job.EventPayload
				if brt.transientSources.Apply(parameters.SourceID) {
					sampleEvent = []byte(`{}`)
				}
				sd = types.CreateStatusDetail(jobState, 0, errorCode, string(errorResp), sampleEvent, parameters.EventName, parameters.EventType)
				statusDetailsMap[key] = sd
			}
			if status.JobState == jobsdb.Failed.State && status.AttemptNum == 1 {
				sd.Count++
			}
			if status.JobState != jobsdb.Failed.State {
				if status.JobState == jobsdb.Succeeded.State || status.JobState == jobsdb.Aborted.State {
					batchRouterWorkspaceJobStatusCount[workspaceID][parameters.DestinationID] += 1
				}
				sd.Count++
			}
		}
		//REPORTING - END
	}

	for workspace := range batchRouterWorkspaceJobStatusCount {
		for destID := range batchRouterWorkspaceJobStatusCount[workspace] {
			metric.DecreasePendingEvents("batch_rt", workspace, brt.destType, float64(batchRouterWorkspaceJobStatusCount[workspace][destID]))
		}
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
			var inPu string
			if transformedAtMap[k] == "processor" {
				inPu = types.DEST_TRANSFORMER
			} else {
				inPu = types.EVENT_FILTER
			}
			m := &types.PUReportedMetric{
				ConnectionDetails: *cd,
				PUDetails:         *types.CreatePUDetails(inPu, types.BATCH_ROUTER, terminalPU, false),
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

	//Save msgids of aborted jobs
	if len(jobRunIDAbortedEventsMap) > 0 {
		router.GetFailedEventsManager().SaveFailedRecordIDs(jobRunIDAbortedEventsMap, txn)
	}
	if brt.reporting != nil && brt.reportingEnabled {
		brt.reporting.Report(reportMetrics, txn)
	}
	brt.jobsDB.CommitTransaction(txn)
	brt.jobsDB.ReleaseUpdateJobStatusLocks()

	sendDestStatusStats(batchJobs.BatchDestination, jobStateCounts, brt.destType, isWarehouse)
}

func (brt *HandleT) GetWorkspaceIDForDestID(destID string) string {
	var workspaceID string

	brt.configSubscriberLock.RLock()
	defer brt.configSubscriberLock.RUnlock()
	workspaceID = brt.destinationsMap[destID].Sources[0].WorkspaceID

	return workspaceID
}

func (brt *HandleT) setMultipleJobStatus(asyncOutput asyncdestinationmanager.AsyncUploadOutput) {
	workspace := brt.GetWorkspaceIDForDestID(asyncOutput.DestinationID)
	var statusList []*jobsdb.JobStatusT
	if len(asyncOutput.ImportingJobIDs) > 0 {
		for _, jobId := range asyncOutput.ImportingJobIDs {
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Importing.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: []byte(`{}`),
				Parameters:    asyncOutput.ImportingParameters,
				WorkspaceId:   workspace,
			}
			statusList = append(statusList, &status)
		}
	}
	if len(asyncOutput.SucceededJobIDs) > 0 {
		for _, jobId := range asyncOutput.FailedJobIDs {
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Succeeded.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: json.RawMessage(asyncOutput.SuccessResponse),
				Parameters:    []byte(`{}`),
				WorkspaceId:   workspace,
			}
			statusList = append(statusList, &status)
		}
	}
	if len(asyncOutput.FailedJobIDs) > 0 {
		for _, jobId := range asyncOutput.FailedJobIDs {
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Failed.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "500",
				ErrorResponse: json.RawMessage(asyncOutput.FailedReason),
				Parameters:    []byte(`{}`),
				WorkspaceId:   workspace,
			}
			statusList = append(statusList, &status)
		}
	}
	if len(asyncOutput.AbortJobIDs) > 0 {
		for _, jobId := range asyncOutput.AbortJobIDs {
			status := jobsdb.JobStatusT{
				JobID:         jobId,
				JobState:      jobsdb.Aborted.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "400",
				ErrorResponse: json.RawMessage(asyncOutput.AbortReason),
				Parameters:    []byte(`{}`),
				WorkspaceId:   workspace,
			}
			statusList = append(statusList, &status)
		}
	}

	if len(statusList) == 0 {
		return
	}

	parameterFilters := []jobsdb.ParameterFilterT{
		{
			Name:     "destination_id",
			Value:    asyncOutput.DestinationID,
			Optional: false,
		},
	}

	//Mark the status of the jobs
	txn := brt.jobsDB.BeginGlobalTransaction()
	brt.jobsDB.AcquireUpdateJobStatusLocks()
	err := brt.jobsDB.UpdateJobStatusInTxn(txn, statusList, []string{brt.destType}, parameterFilters)
	if err != nil {
		brt.logger.Errorf("[Batch Router] Error occurred while updating %s jobs statuses. Panicking. Err: %v", brt.destType, err)
		panic(err)
	}

	brt.jobsDB.CommitTransaction(txn)
	brt.jobsDB.ReleaseUpdateJobStatusLocks()
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

func (*HandleT) recordDeliveryStatus(batchDestination DestinationT, output StorageUploadOutput, isWarehouse bool) {
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
	payload, err := sjson.SetBytes([]byte(`{}`), "location", output.FileLocation)
	if err != nil {
		payload = []byte(`{}`)
	}
	deliveryStatus := destinationdebugger.DeliveryStatusT{
		EventName:     fmt.Sprint(output.TotalEvents) + " events",
		EventType:     "",
		SentAt:        time.Now().Format(misc.RFC3339Milli),
		DestinationID: batchDestination.Destination.ID,
		SourceID:      batchDestination.Source.ID,
		Payload:       payload,
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
			_, hasMore := <-worker.resumeChannel
			if hasMore {
				pkgLogger.Infof("Batch Router worker %d is resumed. Dest type: %s", worker.workerID, worker.brt.destType)
			} else {
				return
			}
		case batchDestData, hasMore := <-brt.processQ:
			if !hasMore {
				return
			}

			batchDest := batchDestData.batchDestination
			parameterFilters := worker.constructParameterFilters(batchDest)
			var combinedList []*jobsdb.JobT
			if readPerDestination {
				toQuery := worker.brt.jobQueryBatchSize
				if !brt.holdFetchingJobs(parameterFilters) {
					brtQueryStat := stats.NewTaggedStat("batch_router.jobsdb_query_time", stats.TimerType, map[string]string{"function": "workerProcess"})
					brtQueryStat.Start()
					brt.logger.Debugf("BRT: %s: DB about to read for parameter Filters: %v ", brt.destType, parameterFilters)
					retryList := brt.jobsDB.GetToRetry(
						jobsdb.GetQueryParamsT{
							CustomValFilters:              []string{brt.destType},
							JobsLimit:                     toQuery,
							ParameterFilters:              parameterFilters,
							IgnoreCustomValFiltersInQuery: true,
							PayloadSizeLimit:              brt.payloadLimit,
						},
					)
					unprocessedList := brt.jobsDB.GetUnprocessed(
						jobsdb.GetQueryParamsT{
							CustomValFilters:              []string{brt.destType},
							JobsLimit:                     toQuery,
							ParameterFilters:              parameterFilters,
							IgnoreCustomValFiltersInQuery: true,
							PayloadSizeLimit:              brt.payloadLimit,
						},
					)
					brtQueryStat.End()

					combinedList = append(retryList, unprocessedList...)

					brt.logger.Debugf("BRT: %s: DB Read Complete for parameter Filters: %v retryList: %v, unprocessedList: %v, total: %v", brt.destType, parameterFilters, len(retryList), len(unprocessedList), len(combinedList))
				}
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
			var drainJobList []*jobsdb.JobT
			drainStatsbyDest := make(map[string]*router_utils.DrainStats)

			jobsBySource := make(map[string][]*jobsdb.JobT)
			for _, job := range combinedList {
				brt.configSubscriberLock.RLock()
				drain, reason := router_utils.ToBeDrained(job, batchDest.Destination.ID, toAbortDestinationIDs, brt.destinationsMap)
				brt.configSubscriberLock.RUnlock()
				if drain {
					status := jobsdb.JobStatusT{
						JobID:         job.JobID,
						AttemptNum:    job.LastJobStatus.AttemptNum + 1,
						JobState:      jobsdb.Aborted.State,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "",
						ErrorResponse: router_utils.EnhanceJSON([]byte(`{}`), "reason", reason),
						Parameters:    []byte(`{}`), // check
						WorkspaceId:   job.WorkspaceId,
					}
					//Enhancing job parameter with the drain reason.
					job.Parameters = router_utils.EnhanceJSON(job.Parameters, "stage", "batch_router")
					job.Parameters = router_utils.EnhanceJSON(job.Parameters, "reason", reason)
					drainList = append(drainList, &status)
					drainJobList = append(drainJobList, job)
					if _, ok := drainStatsbyDest[batchDest.Destination.ID]; !ok {
						drainStatsbyDest[batchDest.Destination.ID] = &router_utils.DrainStats{
							Count:     0,
							Reasons:   []string{},
							Workspace: job.WorkspaceId,
						}
					}
					drainStatsbyDest[batchDest.Destination.ID].Count = drainStatsbyDest[batchDest.Destination.ID].Count + 1
					if !misc.ContainsString(drainStatsbyDest[batchDest.Destination.ID].Reasons, reason) {
						drainStatsbyDest[batchDest.Destination.ID].Reasons = append(drainStatsbyDest[batchDest.Destination.ID].Reasons, reason)
					}
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
						Parameters:    []byte(`{}`), // check
						WorkspaceId:   job.WorkspaceId,
					}
					statusList = append(statusList, &status)
				}
			}

			//Mark the drainList jobs as Aborted
			if len(drainList) > 0 {
				err := brt.errorDB.Store(drainJobList)
				if err != nil {
					brt.logger.Errorf("Error occurred while storing %s jobs into ErrorDB. Panicking. Err: %v", brt.destType, err)
					panic(err)
				}
				err = brt.jobsDB.UpdateJobStatus(drainList, []string{brt.destType}, parameterFilters)
				if err != nil {
					brt.logger.Errorf("Error occurred while marking %s jobs statuses as aborted. Panicking. Err: %v", brt.destType, parameterFilters)
					panic(err)
				}
				for destID, destDrainStat := range drainStatsbyDest {
					brt.drainedJobsStat = stats.NewTaggedStat("drained_events", stats.CountType, stats.Tags{
						"destType":  brt.destType,
						"destId":    destID,
						"module":    "batchrouter",
						"reasons":   strings.Join(destDrainStat.Reasons, ", "),
						"workspace": destDrainStat.Workspace,
					})
					brt.drainedJobsStat.Count(destDrainStat.Count)
					metric.DecreasePendingEvents("batch_rt", destDrainStat.Workspace, brt.destType, float64(drainStatsbyDest[destID].Count))
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
					brt.setJobStatus(&batchJobs, false, err, false)
					wg.Done()
					continue
				}
				rruntime.Go(func() {
					switch {
					case misc.ContainsString(objectStorageDestinations, brt.destType):
						destUploadStat := stats.NewStat(fmt.Sprintf(`batch_router.%s_dest_upload_time`, brt.destType), stats.TimerType)
						destUploadStat.Start()
						output := brt.copyJobsToStorage(brt.destType, &batchJobs, true, false)
						brt.recordDeliveryStatus(*batchJobs.BatchDestination, output, false)
						brt.setJobStatus(&batchJobs, false, output.Error, false)
						misc.RemoveFilePaths(output.LocalFilePaths...)
						if output.JournalOpID > 0 {
							brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
						}
						if output.Error == nil {
							brt.recordUploadStats(*batchJobs.BatchDestination, output)
						}

						destUploadStat.End()
					case misc.ContainsString(warehouseutils.WarehouseDestinations, brt.destType):
						useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchJobs.BatchDestination.Destination.Config)
						objectStorageType := warehouseutils.ObjectStorageType(brt.destType, batchJobs.BatchDestination.Destination.Config, useRudderStorage)
						destUploadStat := stats.NewStat(fmt.Sprintf(`batch_router.%s_%s_dest_upload_time`, brt.destType, objectStorageType), stats.TimerType)
						destUploadStat.Start()
						splitBatchJobs := brt.splitBatchJobsOnTimeWindow(batchJobs)
						for _, batchJob := range splitBatchJobs {
							output := brt.copyJobsToStorage(objectStorageType, batchJob, true, true)
							postToWarehouseErr := false
							if output.Error == nil && output.Key != "" {
								output.Error = brt.postToWarehouse(batchJob, output)
								if output.Error != nil {
									postToWarehouseErr = true
								}
								warehouseutils.DestStat(stats.CountType, "generate_staging_files", batchJob.BatchDestination.Destination.ID).Count(1)
								warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", batchJob.BatchDestination.Destination.ID).Count(len(batchJob.Jobs))
							}
							brt.recordDeliveryStatus(*batchJob.BatchDestination, output, true)
							brt.setJobStatus(batchJob, true, output.Error, postToWarehouseErr)
							misc.RemoveFilePaths(output.LocalFilePaths...)
						}
						destUploadStat.End()
					case misc.ContainsString(asyncDestinations, brt.destType):
						destUploadStat := stats.NewStat(fmt.Sprintf(`batch_router.%s_dest_upload_time`, brt.destType), stats.TimerType)
						destUploadStat.Start()
						brt.sendJobsToStorage(brt.destType, batchJobs, batchJobs.BatchDestination.Destination.Config, true, true)
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

	g, _ := errgroup.WithContext(brt.backgroundCtx)

	for i := 0; i < brt.noOfWorkers; i++ {
		worker := &workerT{
			pauseChannel:  make(chan *PauseT),
			resumeChannel: make(chan bool),
			workerID:      i,
			brt:           brt,
		}
		brt.workers[i] = worker
		g.Go(misc.WithBugsnag(func() error {
			worker.workerProcess()
			return nil
		}))
	}

	_ = g.Wait()
}

type PauseT struct {
	respChannel chan bool
	wg          *sync.WaitGroup
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
	TimeWindow       time.Time
}

func connectionIdentifier(batchDestination DestinationT) string {
	return fmt.Sprintf(`source:%s::destination:%s`, batchDestination.Source.ID, batchDestination.Destination.ID)
}

func (brt *HandleT) warehouseConnectionIdentifier(connIdentifier string, source backendconfig.SourceT, destination backendconfig.DestinationT) string {
	namespace := brt.getNamespace(destination.Config, source, brt.destType)
	return fmt.Sprintf(`namespace:%s::%s`, namespace, connIdentifier)
}

func (*HandleT) getNamespace(config interface{}, source backendconfig.SourceT, destType string) string {
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
		toQuery := brt.jobQueryBatchSize
		if !brt.holdFetchingJobs([]jobsdb.ParameterFilterT{}) {
			retryList := brt.jobsDB.GetToRetry(
				jobsdb.GetQueryParamsT{
					CustomValFilters: []string{brt.destType},
					JobsLimit:        toQuery,
					PayloadSizeLimit: brt.payloadLimit,
				},
			)
			toQuery -= len(retryList)
			unprocessedList := brt.jobsDB.GetUnprocessed(
				jobsdb.GetQueryParamsT{
					CustomValFilters: []string{brt.destType},
					JobsLimit:        toQuery,
					PayloadSizeLimit: brt.payloadLimit,
				},
			)
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
}

func (brt *HandleT) mainLoop(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-time.After(mainLoopSleep):
			brt.readAndProcess()
		}
	}

	// Closing channels mainLoop was publishing to:
	close(brt.processQ)

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
		jsonPath := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v", time.Now().Unix(), uuid.Must(uuid.NewV4()).String()))

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

		err = downloader.Download(context.TODO(), jsonFile, objKey)
		if err != nil {
			brt.logger.Errorf("BRT: Failed to download data for incomplete journal entry to recover from %s at key: %s with error: %v\n", object.Provider, object.Key, err)
			brt.jobsDB.JournalDeleteEntry(entry.OpID)
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

func (brt *HandleT) holdFetchingJobs(parameterFilters []jobsdb.ParameterFilterT) bool {
	var importingList []*jobsdb.JobT
	if IsAsyncDestination(brt.destType) {
		importingList = brt.jobsDB.GetImportingList(
			jobsdb.GetQueryParamsT{
				CustomValFilters: []string{brt.destType},
				JobsLimit:        1,
				ParameterFilters: parameterFilters,
				PayloadSizeLimit: brt.payloadLimit,
			},
		)
		return len(importingList) != 0
	}
	return false
}

func IsAsyncDestination(destType string) bool {
	return misc.ContainsString(asyncDestinations, destType)
}

func (brt *HandleT) crashRecover() {
	if misc.ContainsString(objectStorageDestinations, brt.destType) {
		brt.dedupRawDataDestJobsOnCrash()
	}
}

func IsObjectStorageDestination(destType string) bool {
	return misc.ContainsString(objectStorageDestinations, destType)
}

func IsWarehouseDestination(destType string) bool {
	return misc.ContainsString(warehouseutils.WarehouseDestinations, destType)
}

func (brt *HandleT) splitBatchJobsOnTimeWindow(batchJobs BatchJobsT) map[time.Time]*BatchJobsT {
	var splitBatches = map[time.Time]*BatchJobsT{}
	if !misc.ContainsString(warehouseutils.TimeWindowDestinations, brt.destType) {
		// return only one batchJob if the destination type is not time window destinations
		splitBatches[time.Time{}] = &batchJobs
		return splitBatches
	}

	// split batchJobs based on timeWindow
	for _, job := range batchJobs.Jobs {
		// ignore error as receivedAt will always be in the expected format
		receivedAtStr := gjson.Get(string(job.EventPayload), "metadata.receivedAt").String()
		receivedAt, err := time.Parse(time.RFC3339, receivedAtStr)
		if err != nil {
			pkgLogger.Errorf("Invalid value '%s' for receivedAt : %v ", receivedAtStr, err)
			panic(err)
		}
		timeWindow := warehouseutils.GetTimeWindow(receivedAt)

		// create batchJob for timeWindow if it does not exist
		if _, ok := splitBatches[timeWindow]; !ok {
			splitBatches[timeWindow] = &BatchJobsT{
				Jobs:             make([]*jobsdb.JobT, 0),
				BatchDestination: batchJobs.BatchDestination,
				TimeWindow:       timeWindow,
			}
		}

		splitBatches[timeWindow].Jobs = append(splitBatches[timeWindow].Jobs, job)
	}
	return splitBatches
}

func (brt *HandleT) collectMetrics(ctx context.Context) {
	if !diagnostics.EnableBatchRouterMetric {
		return
	}

	for {
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

		select {
		case <-ctx.Done():
			return
		case <-brt.diagnosisTicker.C:
		}
	}
}

func loadConfig() {
	config.RegisterDurationConfigVariable(2, &mainLoopSleep, true, time.Second, []string{"BatchRouter.mainLoopSleep", "BatchRouter.mainLoopSleepInS"}...)
	config.RegisterInt64ConfigVariable(30, &uploadFreqInS, true, 1, "BatchRouter.uploadFreqInS")
	objectStorageDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO", "DIGITAL_OCEAN_SPACES"}
	asyncDestinations = []string{"MARKETO_BULK_UPLOAD"}
	warehouseURL = misc.GetWarehouseURL()
	// Time period for diagnosis ticker
	config.RegisterDurationConfigVariable(600, &diagnosisTickerTime, false, time.Second, []string{"Diagnostics.batchRouterTimePeriod", "Diagnostics.batchRouterTimePeriodInS"}...)
	config.RegisterDurationConfigVariable(3, &warehouseServiceMaxRetryTime, true, time.Hour, []string{"BatchRouter.warehouseServiceMaxRetryTime", "BatchRouter.warehouseServiceMaxRetryTimeinHr"}...)
	config.RegisterBoolConfigVariable(false, &disableEgress, false, "disableEgress")
	config.RegisterBoolConfigVariable(true, &readPerDestination, false, "BatchRouter.readPerDestination")
	config.RegisterStringConfigVariable("", &toAbortDestinationIDs, true, "BatchRouter.toAbortDestinationIDs")
	config.RegisterDurationConfigVariable(10, &netClientTimeout, false, time.Second, "BatchRouter.httpTimeout")
	transformerURL = config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090")
	config.RegisterStringConfigVariable("", &datePrefixOverride, true, "BatchRouter.datePrefixOverride")
	dateFormatLayouts = map[string]string{
		"01-02-2006": "MM-DD-YYYY",
		"2006-01-02": "YYYY-MM-DD",
		//"02-01-2006" : "DD-MM-YYYY", //adding this might match with that of MM-DD-YYYY too
	}
	dateFormatMap = make(map[string]string)
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("batchrouter")

	setQueryFilters()
	Diagnostics = diagnostics.Diagnostics
}

func setQueryFilters() {
	if readPerDestination {
		QueryFilters = jobsdb.QueryFiltersT{CustomVal: true, ParameterFilters: []string{"destination_id"}}
	} else {
		QueryFilters = jobsdb.QueryFiltersT{CustomVal: true}
	}
}

//Setup initializes this module
func (brt *HandleT) Setup(backendConfig backendconfig.BackendConfig, jobsDB jobsdb.JobsDB, errorDB jobsdb.JobsDB, destType string, reporting types.ReportingI, multitenantStat multitenant.MultiTenantI, transientSources transientsource.Service) {
	brt.isBackendConfigInitialized = false
	brt.backendConfigInitialized = make(chan bool)
	brt.fileManagerFactory = filemanager.DefaultFileManagerFactory
	brt.backendConfig = backendConfig
	brt.reporting = reporting
	config.RegisterBoolConfigVariable(types.DEFAULT_REPORTING_ENABLED, &brt.reportingEnabled, false, "Reporting.enabled")
	brt.logger = pkgLogger.Child(destType)
	brt.logger.Infof("BRT: Batch Router started: %s", destType)

	brt.asyncUploadWorkerPauseChannel = make(chan *PauseT)
	brt.asyncUploadWorkerResumeChannel = make(chan bool)
	brt.pollAsyncStatusPauseChannel = make(chan *PauseT)
	brt.pollAsyncStatusResumeChannel = make(chan bool)
	brt.multitenantI = multitenantStat
	brt.transientSources = transientSources
	//waiting for reporting client setup
	if brt.reporting != nil && brt.reportingEnabled {
		brt.reporting.WaitForSetup(context.TODO(), types.CORE_REPORTING_CLIENT)
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
	brt.asyncDestinationStruct = make(map[string]*asyncdestinationmanager.AsyncDestinationStruct)
	brt.noOfWorkers = getBatchRouterConfigInt("noOfWorkers", destType, 8)
	config.RegisterDurationConfigVariable(10, &brt.pollStatusLoopSleep, true, time.Second, []string{"BatchRouter." + brt.destType + "." + "pollStatusLoopSleep", "BatchRouter.pollStatusLoopSleep"}...)
	config.RegisterIntConfigVariable(100000, &brt.jobQueryBatchSize, true, 1, []string{"BatchRouter." + brt.destType + "." + "jobQueryBatchSize", "BatchRouter.jobQueryBatchSize"}...)
	config.RegisterIntConfigVariable(10000, &brt.maxEventsInABatch, false, 1, []string{"BatchRouter." + brt.destType + "." + "maxEventsInABatch", "BatchRouter.maxEventsInABatch"}...)
	config.RegisterIntConfigVariable(128, &brt.maxFailedCountForJob, true, 1, []string{"BatchRouter." + brt.destType + "." + "maxFailedCountForJob", "BatchRouter." + "maxFailedCountForJob"}...)
	config.RegisterDurationConfigVariable(180, &brt.retryTimeWindow, true, time.Minute, []string{"BatchRouter." + brt.destType + "." + "retryTimeWindow", "BatchRouter." + brt.destType + "." + "retryTimeWindowInMins", "BatchRouter." + "retryTimeWindow", "BatchRouter." + "retryTimeWindowInMins"}...)
	config.RegisterDurationConfigVariable(30, &brt.asyncUploadTimeout, true, time.Minute, []string{"BatchRouter." + brt.destType + "." + "asyncUploadTimeout", "BatchRouter." + "asyncUploadTimeout"}...)
	config.RegisterInt64ConfigVariable(1*bytesize.GB, &brt.payloadLimit, true, 1, []string{"BatchRouter." + brt.destType + "." + "PayloadLimit", "BatchRouter.PayloadLimit"}...)
	brt.uploadIntervalMap = map[string]time.Duration{}

	tr := &http.Transport{}
	client := &http.Client{Transport: tr, Timeout: netClientTimeout}
	brt.netHandle = client
	brt.pollTimeStat = stats.NewTaggedStat("async_poll_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})
	brt.failedJobsTimeStat = stats.NewTaggedStat("async_failed_job_poll_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})
	brt.successfulJobCount = stats.NewTaggedStat("async_successful_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	brt.failedJobCount = stats.NewTaggedStat("async_failed_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	brt.abortedJobCount = stats.NewTaggedStat("async_aborted_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	brt.processQ = make(chan *BatchDestinationDataT)
	brt.crashRecover()

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	brt.backgroundCtx = ctx
	brt.backgroundGroup = g
	brt.backgroundCancel = cancel
	brt.backgroundWait = g.Wait

	g.Go(misc.WithBugsnag(func() error {
		brt.collectMetrics(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		brt.initWorkers()
		return nil
	}))

	g.Go(misc.WithBugsnag(func() error {
		brt.pollAsyncStatus(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnag(func() error {
		brt.asyncUploadWorker(ctx)
		return nil
	}))

	rruntime.Go(func() {
		brt.backendConfigSubscriber()
	})
	adminInstance.registerBatchRouter(destType, brt)
}

func (brt *HandleT) Start() {
	ctx := brt.backgroundCtx
	brt.backgroundGroup.Go(misc.WithBugsnag(func() error {
		<-brt.backendConfigInitialized
		brt.mainLoop(ctx)

		return nil
	}))
}

func (brt *HandleT) Shutdown() {
	brt.backgroundCancel()
	// close paused workers
	for _, worker := range brt.workers {
		close(worker.resumeChannel)
	}
	_ = brt.backgroundWait()
}
