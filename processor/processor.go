package processor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/dedup"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	event_schema "github.com/rudderlabs/rudder-server/event-schema"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
)

func RegisterAdminHandlers(readonlyProcErrorDB jobsdb.ReadonlyJobsDB) {
	admin.RegisterAdminHandler("ProcErrors", &stash.StashRpcHandler{ReadOnlyJobsDB: readonlyProcErrorDB})
}

type PauseT struct {
	respChannel chan bool
}

//HandleT is an handle to this object used in main.go
type HandleT struct {
	paused                         bool
	pauseLock                      sync.Mutex
	pauseChannel                   chan *PauseT
	resumeChannel                  chan bool
	backendConfig                  backendconfig.BackendConfig
	stats                          stats.Stats
	gatewayDB                      jobsdb.JobsDB
	routerDB                       jobsdb.JobsDB
	batchRouterDB                  jobsdb.JobsDB
	errorDB                        jobsdb.JobsDB
	transformer                    transformer.Transformer
	pStatsJobs                     *misc.PerfStats
	pStatsDBR                      *misc.PerfStats
	statGatewayDBR                 stats.RudderStats
	pStatsDBW                      *misc.PerfStats
	statGatewayDBW                 stats.RudderStats
	statRouterDBW                  stats.RudderStats
	statBatchRouterDBW             stats.RudderStats
	statProcErrDBW                 stats.RudderStats
	transformEventsByTimeMutex     sync.RWMutex
	destTransformEventsByTimeTaken transformRequestPQ
	userTransformEventsByTimeTaken transformRequestPQ
	statDBR                        stats.RudderStats
	statDBW                        stats.RudderStats
	statLoopTime                   stats.RudderStats
	eventSchemasTime               stats.RudderStats
	validateEventsTime             stats.RudderStats
	processJobsTime                stats.RudderStats
	statSessionTransform           stats.RudderStats
	statUserTransform              stats.RudderStats
	statDestTransform              stats.RudderStats
	statListSort                   stats.RudderStats
	marshalSingularEvents          stats.RudderStats
	destProcessing                 stats.RudderStats
	statNumRequests                stats.RudderStats
	statNumEvents                  stats.RudderStats
	statDestNumOutputEvents        stats.RudderStats
	statBatchDestNumOutputEvents   stats.RudderStats
	logger                         logger.LoggerI
	eventSchemaHandler             types.EventSchemasI
	dedupHandler                   dedup.DedupI
	reporting                      types.ReportingI
	reportingEnabled               bool
	transformerFeatures            json.RawMessage
}

var defaultTransformerFeatures = `{
	"routerTransform": {
	  "MARKETO": true,
	  "HS": true
	}
  }`

var mainLoopTimeout = 200 * time.Millisecond
var featuresRetryMaxAttempts = 10

type DestStatT struct {
	numEvents              stats.RudderStats
	numOutputSuccessEvents stats.RudderStats
	numOutputFailedEvents  stats.RudderStats
	transformTime          stats.RudderStats
}

type ParametersT struct {
	SourceID                string `json:"source_id"`
	DestinationID           string `json:"destination_id"`
	ReceivedAt              string `json:"received_at"`
	TransformAt             string `json:"transform_at"`
	MessageID               string `json:"message_id"`
	GatewayJobID            int64  `json:"gateway_job_id"`
	SourceBatchID           string `json:"source_batch_id"`
	SourceTaskID            string `json:"source_task_id"`
	SourceTaskRunID         string `json:"source_task_run_id"`
	SourceJobID             string `json:"source_job_id"`
	SourceJobRunID          string `json:"source_job_run_id"`
	EventName               string `json:"event_name"`
	EventType               string `json:"event_type"`
	SourceDefinitionID      string `json:"source_definition_id"`
	DestinationDefinitionID string `json:"destination_definition_id"`
	SourceCategory          string `json:"source_category"`
}

type MetricMetadata struct {
	sourceID                string
	destinationID           string
	sourceBatchID           string
	sourceTaskID            string
	sourceTaskRunID         string
	sourceJobID             string
	sourceJobRunID          string
	sourceDefinitionID      string
	destinationDefinitionID string
	sourceCategory          string
}

type WriteKeyT string
type SourceIDT string

const USER_TRANSFORMATION = "USER_TRANSFORMATION"
const DEST_TRANSFORMATION = "DEST_TRANSFORMATION"

func (proc *HandleT) buildStatTags(sourceID, workspaceID string, destination backendconfig.DestinationT, transformationType string) map[string]string {
	var module = "router"
	if batchrouter.IsObjectStorageDestination(destination.DestinationDefinition.Name) {
		module = "batch_router"
	}
	if batchrouter.IsWarehouseDestination(destination.DestinationDefinition.Name) {
		module = "warehouse"
	}

	return map[string]string{
		"module":         module,
		"destination":    destination.ID,
		"destType":       destination.DestinationDefinition.Name,
		"source":         sourceID,
		"workspaceId":    workspaceID,
		"transformation": transformationType,
	}
}

func (proc *HandleT) newUserTransformationStat(sourceID, workspaceID string, destination backendconfig.DestinationT) *DestStatT {
	tags := proc.buildStatTags(sourceID, workspaceID, destination, USER_TRANSFORMATION)

	tags["transformation_id"] = destination.Transformations[0].ID
	tags["transformation_version_id"] = destination.Transformations[0].VersionID

	numEvents := proc.stats.NewTaggedStat("proc_num_ut_input_events", stats.CountType, tags)
	numOutputSuccessEvents := proc.stats.NewTaggedStat("proc_num_ut_output_success_events", stats.CountType, tags)
	numOutputFailedEvents := proc.stats.NewTaggedStat("proc_num_ut_output_failed_events", stats.CountType, tags)
	transformTime := proc.stats.NewTaggedStat("proc_user_transform", stats.TimerType, tags)

	return &DestStatT{
		numEvents:              numEvents,
		numOutputSuccessEvents: numOutputSuccessEvents,
		numOutputFailedEvents:  numOutputFailedEvents,
		transformTime:          transformTime,
	}
}

func (proc *HandleT) newDestinationTransformationStat(sourceID, workspaceID, transformAt string, destination backendconfig.DestinationT) *DestStatT {
	tags := proc.buildStatTags(sourceID, workspaceID, destination, DEST_TRANSFORMATION)

	tags["transform_at"] = transformAt

	numEvents := proc.stats.NewTaggedStat("proc_num_dt_input_events", stats.CountType, tags)
	numOutputSuccessEvents := proc.stats.NewTaggedStat("proc_num_dt_output_success_events", stats.CountType, tags)
	numOutputFailedEvents := proc.stats.NewTaggedStat("proc_num_dt_output_failed_events", stats.CountType, tags)
	destTransform := proc.stats.NewTaggedStat("proc_dest_transform", stats.TimerType, tags)

	return &DestStatT{
		numEvents:              numEvents,
		numOutputSuccessEvents: numOutputSuccessEvents,
		numOutputFailedEvents:  numOutputFailedEvents,
		transformTime:          destTransform,
	}
}

//Print the internal structure
func (proc *HandleT) Print() {
	if !proc.logger.IsDebugLevel() {
		return
	}
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("processor")
}

// NewProcessor creates a new Processor intanstace
func NewProcessor() *HandleT {
	return &HandleT{
		transformer: transformer.NewTransformer(),
	}
}

func (proc *HandleT) Status() interface{} {
	proc.transformEventsByTimeMutex.RLock()
	defer proc.transformEventsByTimeMutex.RUnlock()
	statusRes := make(map[string][]TransformRequestT)
	for _, pqDestEvent := range proc.destTransformEventsByTimeTaken {
		statusRes["dest-transformer"] = append(statusRes["dest-transformer"], *pqDestEvent)
	}
	for _, pqUserEvent := range proc.userTransformEventsByTimeTaken {
		statusRes["user-transformer"] = append(statusRes["user-transformer"], *pqUserEvent)
	}

	if enableDedup {
		proc.dedupHandler.PrintHistogram()
	}

	return statusRes
}

//Setup initializes the module
func (proc *HandleT) Setup(backendConfig backendconfig.BackendConfig, gatewayDB jobsdb.JobsDB, routerDB jobsdb.JobsDB, batchRouterDB jobsdb.JobsDB, errorDB jobsdb.JobsDB, clearDB *bool, reporting types.ReportingI) {
	proc.pauseChannel = make(chan *PauseT)
	proc.resumeChannel = make(chan bool)
	proc.reporting = reporting
	config.RegisterBoolConfigVariable(types.DEFAULT_REPORTING_ENABLED, &proc.reportingEnabled, false, "Reporting.enabled")
	proc.logger = pkgLogger
	proc.backendConfig = backendConfig
	proc.stats = stats.DefaultStats

	proc.gatewayDB = gatewayDB
	proc.routerDB = routerDB
	proc.batchRouterDB = batchRouterDB
	proc.errorDB = errorDB
	proc.pStatsJobs = &misc.PerfStats{}
	proc.pStatsDBR = &misc.PerfStats{}
	proc.pStatsDBW = &misc.PerfStats{}
	proc.userTransformEventsByTimeTaken = make([]*TransformRequestT, 0, transformTimesPQLength)
	proc.destTransformEventsByTimeTaken = make([]*TransformRequestT, 0, transformTimesPQLength)
	proc.pStatsJobs.Setup("ProcessorJobs")
	proc.pStatsDBR.Setup("ProcessorDBRead")
	proc.pStatsDBW.Setup("ProcessorDBWrite")

	proc.statGatewayDBR = proc.stats.NewStat("processor.gateway_db_read", stats.CountType)
	proc.statGatewayDBW = proc.stats.NewStat("processor.gateway_db_write", stats.CountType)
	proc.statRouterDBW = proc.stats.NewStat("processor.router_db_write", stats.CountType)
	proc.statBatchRouterDBW = proc.stats.NewStat("processor.batch_router_db_write", stats.CountType)
	proc.statDBR = proc.stats.NewStat("processor.gateway_db_read_time", stats.TimerType)
	proc.statDBW = proc.stats.NewStat("processor.gateway_db_write_time", stats.TimerType)
	proc.statProcErrDBW = proc.stats.NewStat("processor.proc_err_db_write", stats.CountType)
	proc.statLoopTime = proc.stats.NewStat("processor.loop_time", stats.TimerType)
	proc.eventSchemasTime = proc.stats.NewStat("processor.event_schemas_time", stats.TimerType)
	proc.validateEventsTime = proc.stats.NewStat("processor.validate_events_time", stats.TimerType)
	proc.processJobsTime = proc.stats.NewStat("processor.process_jobs_time", stats.TimerType)
	proc.statSessionTransform = proc.stats.NewStat("processor.session_transform_time", stats.TimerType)
	proc.statUserTransform = proc.stats.NewStat("processor.user_transform_time", stats.TimerType)
	proc.statDestTransform = proc.stats.NewStat("processor.dest_transform_time", stats.TimerType)
	proc.statListSort = proc.stats.NewStat("processor.job_list_sort", stats.TimerType)
	proc.marshalSingularEvents = proc.stats.NewStat("processor.marshal_singular_events", stats.TimerType)
	proc.destProcessing = proc.stats.NewStat("processor.dest_processing", stats.TimerType)
	proc.statNumRequests = proc.stats.NewStat("processor.num_requests", stats.CountType)
	proc.statNumEvents = proc.stats.NewStat("processor.num_events", stats.CountType)
	// Add a separate tag for batch router
	proc.statDestNumOutputEvents = proc.stats.NewTaggedStat("processor.num_output_events", stats.CountType, stats.Tags{
		"module": "router",
	})
	proc.statBatchDestNumOutputEvents = proc.stats.NewTaggedStat("processor.num_output_events", stats.CountType, stats.Tags{
		"module": "batch_router",
	})
	admin.RegisterStatusHandler("processor", proc)
	if enableEventSchemasFeature {
		proc.eventSchemaHandler = event_schema.GetInstance()
	}
	if enableDedup {
		proc.dedupHandler = dedup.GetInstance(clearDB)
	}
	rruntime.Go(func() {
		proc.backendConfigSubscriber()
	})

	rruntime.Go(func() {
		proc.getTransformerFeatureJson()
	})

	proc.transformer.Setup()

	proc.crashRecover()
}

// Start starts this processor's main loops.
func (proc *HandleT) Start() {
	rruntime.Go(func() {
		//waiting till the backend config is received
		proc.backendConfig.WaitForConfig()
		proc.mainLoop()
	})
	rruntime.Go(func() {
		st := stash.New()
		st.Setup(proc.errorDB)
		st.Start()
	})
}

var (
	loopSleep                 time.Duration
	maxLoopSleep              time.Duration
	fixedLoopSleep            time.Duration
	maxEventsToProcess        int
	avgEventsInRequest        int
	dbReadBatchSize           int
	transformBatchSize        int
	userTransformBatchSize    int
	writeKeyDestinationMap    map[string][]backendconfig.DestinationT
	writeKeySourceMap         map[string]backendconfig.SourceT
	destinationIDtoTypeMap    map[string]string
	batchDestinations         []string
	configSubscriberLock      sync.RWMutex
	customDestinations        []string
	pkgLogger                 logger.LoggerI
	enableEventSchemasFeature bool
	enableEventSchemasAPIOnly bool
	enableDedup               bool
	transformTimesPQLength    int
	captureEventNameStats     bool
	transformerURL            string
	pollInterval              time.Duration
	isUnLocked                bool
	GWCustomVal               string
)

func loadConfig() {
	config.RegisterDurationConfigVariable(time.Duration(5000), &maxLoopSleep, true, time.Millisecond, []string{"Processor.maxLoopSleep", "Processor.maxLoopSleepInMS"}...)
	config.RegisterDurationConfigVariable(time.Duration(10), &loopSleep, true, time.Millisecond, []string{"Processor.loopSleep", "Processor.loopSleepInMS"}...)
	config.RegisterDurationConfigVariable(time.Duration(0), &fixedLoopSleep, true, time.Millisecond, []string{"Processor.fixedLoopSleep", "Processor.fixedLoopSleepInMS"}...)
	config.RegisterIntConfigVariable(100, &transformBatchSize, true, 1, "Processor.transformBatchSize")
	config.RegisterIntConfigVariable(200, &userTransformBatchSize, true, 1, "Processor.userTransformBatchSize")
	// Enable dedup of incoming events by default
	config.RegisterBoolConfigVariable(false, &enableDedup, false, "Dedup.enableDedup")
	batchDestinations = []string{"S3", "GCS", "MINIO", "RS", "BQ", "AZURE_BLOB", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE", "DIGITAL_OCEAN_SPACES", "MSSQL", "AZURE_SYNAPSE", "S3_DATALAKE", "MARKETO_BULK_UPLOAD"}
	customDestinations = []string{"KAFKA", "KINESIS", "AZURE_EVENT_HUB", "CONFLUENT_CLOUD"}
	// EventSchemas feature. false by default
	config.RegisterBoolConfigVariable(false, &enableEventSchemasFeature, false, "EventSchemas.enableEventSchemasFeature")
	config.RegisterBoolConfigVariable(false, &enableEventSchemasAPIOnly, false, "EventSchemas.enableEventSchemasAPIOnly")
	config.RegisterIntConfigVariable(10000, &maxEventsToProcess, true, 1, "Processor.maxLoopProcessEvents")
	config.RegisterIntConfigVariable(1, &avgEventsInRequest, true, 1, "Processor.avgEventsInRequest")
	// assuming every job in gw_jobs has atleast one event, max value for dbReadBatchSize can be maxEventsToProcess
	dbReadBatchSize = int(math.Ceil(float64(maxEventsToProcess) / float64(avgEventsInRequest)))
	config.RegisterIntConfigVariable(5, &transformTimesPQLength, false, 1, "Processor.transformTimesPQLength")
	// Capture event name as a tag in event level stats
	config.RegisterBoolConfigVariable(false, &captureEventNameStats, true, "Processor.Stats.captureEventName")
	transformerURL = config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090")
	config.RegisterDurationConfigVariable(time.Duration(5), &pollInterval, false, time.Second, []string{"Processor.pollInterval", "Processor.pollIntervalInS"}...)
	// GWCustomVal is used as a key in the jobsDB customval column
	config.RegisterStringConfigVariable("GW", &GWCustomVal, false, "Gateway.CustomVal")
}

func (proc *HandleT) getTransformerFeatureJson() {
	for {
		for i := 0; i < featuresRetryMaxAttempts; i++ {
			url := transformerURL + "/features"
			req, err := http.NewRequest("GET", url, bytes.NewReader([]byte{}))
			if err != nil {
				proc.logger.Error("error creating request - %s", err)
				time.Sleep(200 * time.Millisecond)
				continue
			}
			tr := &http.Transport{}
			client := &http.Client{Transport: tr}
			res, err := client.Do(req)

			if err != nil {
				proc.logger.Error("error sending request - %s", err)
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if res.StatusCode == 200 {
				body, err := io.ReadAll(res.Body)
				if err == nil {
					proc.transformerFeatures = json.RawMessage(body)
					res.Body.Close()
					break
				} else {
					res.Body.Close()
					time.Sleep(200 * time.Millisecond)
					continue
				}
			} else if res.StatusCode == 404 {
				proc.transformerFeatures = json.RawMessage(defaultTransformerFeatures)
				break
			}
		}
		if proc.transformerFeatures != nil && !isUnLocked {
			isUnLocked = true
		}
		time.Sleep(pollInterval)
	}
}

//SetDisableDedupFeature overrides SetDisableDedupFeature configuration and returns previous value
func SetDisableDedupFeature(b bool) bool {
	prev := enableDedup
	enableDedup = b
	return prev
}

func SetMainLoopTimeout(timeout time.Duration) {
	mainLoopTimeout = timeout
}

func SetFeaturesRetryAttempts(overrideAttempts int) {
	featuresRetryMaxAttempts = overrideAttempts
}

func SetIsUnlocked(unlockVar bool) {
	isUnLocked = unlockVar
}

func (proc *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	proc.backendConfig.Subscribe(ch, backendconfig.TopicProcessConfig)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		writeKeyDestinationMap = make(map[string][]backendconfig.DestinationT)
		writeKeySourceMap = map[string]backendconfig.SourceT{}
		destinationIDtoTypeMap = make(map[string]string)
		sources := config.Data.(backendconfig.ConfigT)
		for _, source := range sources.Sources {
			writeKeySourceMap[source.WriteKey] = source
			if source.Enabled {
				writeKeyDestinationMap[source.WriteKey] = source.Destinations
				for _, destination := range source.Destinations {
					destinationIDtoTypeMap[destination.ID] = destination.DestinationDefinition.Name
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

//We create sessions (of individul events) from set of input jobs  from a user
//Those sesssion events are transformed and we have a transformed set of
//events that must be processed further via destination specific transformations
//(in processJobsForDest). This function creates jobs from eventList
func createUserTransformedJobsFromEvents(transformUserEventList [][]types.SingularEventT,
	userIDList []string, userJobs map[string][]*jobsdb.JobT) ([]*jobsdb.JobT, [][]types.SingularEventT) {

	transJobList := make([]*jobsdb.JobT, 0)
	transEventList := make([][]types.SingularEventT, 0)
	if len(transformUserEventList) != len(userIDList) {
		panic(fmt.Errorf("len(transformUserEventList):%d != len(userIDList):%d", len(transformUserEventList), len(userIDList)))
	}
	for idx, userID := range userIDList {
		userEvents := transformUserEventList[idx]

		for idx, job := range userJobs[userID] {
			//We put all the transformed event on the first job
			//and empty out the remaining payloads
			transJobList = append(transJobList, job)
			if idx == 0 {
				transEventList = append(transEventList, userEvents)
			} else {
				transEventList = append(transEventList, nil)
			}
		}
	}
	return transJobList, transEventList
}

func getSourceByWriteKey(writeKey string) (backendconfig.SourceT, error) {
	var err error
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	source, ok := writeKeySourceMap[writeKey]
	if !ok {
		err = errors.New("source not found for writeKey")
		pkgLogger.Errorf(`Processor : source not found for writeKey: %s`, writeKey)
	}
	return source, err
}

func getEnabledDestinations(writeKey string, destinationName string) []backendconfig.DestinationT {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	var enabledDests []backendconfig.DestinationT
	for _, dest := range writeKeyDestinationMap[writeKey] {
		if destinationName == dest.DestinationDefinition.Name && dest.Enabled {
			enabledDests = append(enabledDests, dest)
		}
	}
	return enabledDests
}

func getBackendEnabledDestinationTypes(writeKey string) map[string]backendconfig.DestinationDefinitionT {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	var enabledDestinationTypes = make(map[string]backendconfig.DestinationDefinitionT)
	for _, destination := range writeKeyDestinationMap[writeKey] {
		if destination.Enabled {
			enabledDestinationTypes[destination.DestinationDefinition.DisplayName] = destination.DestinationDefinition
		}
	}
	return enabledDestinationTypes
}

func getTimestampFromEvent(event types.SingularEventT, field string) time.Time {
	var timestamp time.Time
	var ok bool
	if timestamp, ok = misc.GetParsedTimestamp(event[field]); !ok {
		timestamp = time.Now()
	}
	return timestamp
}

func enhanceWithTimeFields(event *transformer.TransformerEventT, singularEventMap types.SingularEventT, receivedAt time.Time) {
	// set timestamp skew based on timestamp fields from SDKs
	originalTimestamp := getTimestampFromEvent(singularEventMap, "originalTimestamp")
	sentAt := getTimestampFromEvent(singularEventMap, "sentAt")
	var timestamp time.Time
	var ok bool

	// use existing timestamp if it exists in the event, add new timestamp otherwise
	if timestamp, ok = misc.GetParsedTimestamp(event.Message["timestamp"]); !ok {
		// calculate new timestamp using using the formula
		// timestamp = receivedAt - (sentAt - originalTimestamp)
		timestamp = misc.GetChronologicalTimeStamp(receivedAt, sentAt, originalTimestamp)
	}

	// set all timestamps in RFC3339 format
	event.Message["receivedAt"] = receivedAt.Format(misc.RFC3339Milli)
	event.Message["originalTimestamp"] = originalTimestamp.Format(misc.RFC3339Milli)
	event.Message["sentAt"] = sentAt.Format(misc.RFC3339Milli)
	event.Message["timestamp"] = timestamp.Format(misc.RFC3339Milli)
}

func makeCommonMetadataFromSingularEvent(singularEvent types.SingularEventT, batchEvent *jobsdb.JobT, receivedAt time.Time, source backendconfig.SourceT) *transformer.MetadataT {
	commonMetadata := transformer.MetadataT{}

	eventBytes, err := json.Marshal(singularEvent)
	if err != nil {
		//Marshalling should never fail. But still panicking.
		panic(fmt.Errorf("[Processor] couldn't marshal singularEvent. singularEvent: %v", singularEvent))
	}
	commonMetadata.SourceID = gjson.GetBytes(batchEvent.Parameters, "source_id").Str
	commonMetadata.WorkspaceID = source.WorkspaceID
	commonMetadata.Namespace = config.GetKubeNamespace()
	commonMetadata.InstanceID = config.GetInstanceID()
	commonMetadata.RudderID = batchEvent.UserID
	commonMetadata.JobID = batchEvent.JobID
	commonMetadata.MessageID = misc.GetStringifiedData(singularEvent["messageId"])
	commonMetadata.ReceivedAt = receivedAt.Format(misc.RFC3339Milli)
	commonMetadata.SourceBatchID = gjson.GetBytes(eventBytes, "context.sources.batch_id").String()
	commonMetadata.SourceTaskID = gjson.GetBytes(eventBytes, "context.sources.task_id").String()
	commonMetadata.SourceTaskRunID = gjson.GetBytes(eventBytes, "context.sources.task_run_id").String()
	commonMetadata.SourceJobID = gjson.GetBytes(eventBytes, "context.sources.job_id").String()
	commonMetadata.SourceJobRunID = gjson.GetBytes(eventBytes, "context.sources.job_run_id").String()
	commonMetadata.SourceType = source.SourceDefinition.Name
	commonMetadata.SourceCategory = source.SourceDefinition.Category
	commonMetadata.EventName = misc.GetStringifiedData(singularEvent["event"])
	commonMetadata.EventType = misc.GetStringifiedData(singularEvent["type"])
	commonMetadata.SourceDefinitionID = source.SourceDefinition.ID
	return &commonMetadata
}

// add metadata to each singularEvent which will be returned by transformer in response
func enhanceWithMetadata(commonMetadata *transformer.MetadataT, event *transformer.TransformerEventT, destination backendconfig.DestinationT) {
	metadata := transformer.MetadataT{}
	metadata.SourceType = commonMetadata.SourceType
	metadata.SourceCategory = commonMetadata.SourceCategory
	metadata.SourceID = commonMetadata.SourceID
	metadata.WorkspaceID = commonMetadata.WorkspaceID
	metadata.Namespace = commonMetadata.Namespace
	metadata.InstanceID = commonMetadata.InstanceID
	metadata.RudderID = commonMetadata.RudderID
	metadata.JobID = commonMetadata.JobID
	metadata.MessageID = commonMetadata.MessageID
	metadata.ReceivedAt = commonMetadata.ReceivedAt
	metadata.SourceBatchID = commonMetadata.SourceBatchID
	metadata.SourceTaskID = commonMetadata.SourceTaskID
	metadata.SourceTaskRunID = commonMetadata.SourceTaskRunID
	metadata.SourceJobID = commonMetadata.SourceJobID
	metadata.SourceJobRunID = commonMetadata.SourceJobRunID
	metadata.EventName = commonMetadata.EventName
	metadata.EventType = commonMetadata.EventType
	metadata.SourceDefinitionID = commonMetadata.SourceDefinitionID
	metadata.DestinationID = destination.ID
	metadata.DestinationDefinitionID = destination.DestinationDefinition.ID
	metadata.DestinationType = destination.DestinationDefinition.Name
	if event.SessionID != "" {
		metadata.SessionID = event.SessionID
	}
	event.Metadata = metadata
}

func getKeyFromSourceAndDest(srcID string, destID string) string {
	return srcID + "::" + destID
}

func getSourceAndDestIDsFromKey(key string) (sourceID string, destID string) {
	fields := strings.Split(key, "::")
	return fields[0], fields[1]
}

func recordEventDeliveryStatus(jobsByDestID map[string][]*jobsdb.JobT) {
	for destID, jobs := range jobsByDestID {
		if !destinationdebugger.HasUploadEnabled(destID) {
			continue
		}
		for _, job := range jobs {
			var params map[string]interface{}
			err := json.Unmarshal(job.Parameters, &params)
			if err != nil {
				pkgLogger.Errorf("Error while UnMarshaling live event parameters: %w", err)
				continue
			}

			sourceID, _ := params["source_id"].(string)
			destID, _ := params["destination_id"].(string)
			procErr, _ := params["error"].(string)
			procErr = strconv.Quote(procErr)
			statusCode := fmt.Sprint(params["status_code"])
			sentAt := time.Now().Format(misc.RFC3339Milli)
			events := make([]map[string]interface{}, 0)
			err = json.Unmarshal(job.EventPayload, &events)
			if err != nil {
				pkgLogger.Errorf("Error while UnMarshaling live event payload: %w", err)
				continue
			}
			for i := range events {
				event := &events[i]
				eventPayload, err := json.Marshal(*event)
				if err != nil {
					pkgLogger.Errorf("Error while Marshaling live event payload: %w", err)
					continue
				}

				eventName := misc.GetStringifiedData(gjson.GetBytes(eventPayload, "event").String())
				eventType := misc.GetStringifiedData(gjson.GetBytes(eventPayload, "type").String())
				deliveryStatus := destinationdebugger.DeliveryStatusT{
					EventName:     eventName,
					EventType:     eventType,
					SentAt:        sentAt,
					DestinationID: destID,
					SourceID:      sourceID,
					Payload:       eventPayload,
					AttemptNum:    1,
					JobState:      jobsdb.Aborted.State,
					ErrorCode:     statusCode,
					ErrorResponse: []byte(fmt.Sprintf(`{"error": %s}`, procErr)),
				}
				destinationdebugger.RecordEventDeliveryStatus(destID, &deliveryStatus)
			}

		}
	}
}

func (proc *HandleT) getDestTransformerEvents(response transformer.ResponseT, commonMetaData transformer.MetadataT, destination backendconfig.DestinationT, stage string, trackingPlanEnabled bool) ([]transformer.TransformerEventT, []*types.PUReportedMetric, map[string]int64, map[string]MetricMetadata) {
	successMetrics := make([]*types.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*types.ConnectionDetails)
	statusDetailsMap := make(map[string]*types.StatusDetail)
	successCountMap := make(map[string]int64)
	successCountMetadataMap := make(map[string]MetricMetadata)
	var eventsToTransform []transformer.TransformerEventT
	for _, userTransformedEvent := range response.Events {
		//Update metrics maps
		proc.updateMetricMaps(successCountMetadataMap, successCountMap, connectionDetailsMap, statusDetailsMap, userTransformedEvent, jobsdb.Succeeded.State, []byte(`{}`))

		eventMetadata := commonMetaData
		eventMetadata.MessageIDs = userTransformedEvent.Metadata.MessageIDs
		eventMetadata.MessageID = userTransformedEvent.Metadata.MessageID
		eventMetadata.JobID = userTransformedEvent.Metadata.JobID
		eventMetadata.SourceBatchID = userTransformedEvent.Metadata.SourceBatchID
		eventMetadata.SourceTaskID = userTransformedEvent.Metadata.SourceTaskID
		eventMetadata.SourceTaskRunID = userTransformedEvent.Metadata.SourceTaskRunID
		eventMetadata.SourceJobID = userTransformedEvent.Metadata.SourceJobID
		eventMetadata.SourceJobRunID = userTransformedEvent.Metadata.SourceJobRunID
		eventMetadata.RudderID = userTransformedEvent.Metadata.RudderID
		eventMetadata.ReceivedAt = userTransformedEvent.Metadata.ReceivedAt
		eventMetadata.SessionID = userTransformedEvent.Metadata.SessionID
		eventMetadata.EventName = userTransformedEvent.Metadata.EventName
		eventMetadata.EventType = userTransformedEvent.Metadata.EventType
		eventMetadata.SourceDefinitionID = userTransformedEvent.Metadata.SourceDefinitionID
		eventMetadata.DestinationDefinitionID = userTransformedEvent.Metadata.DestinationDefinitionID
		eventMetadata.SourceCategory = userTransformedEvent.Metadata.SourceCategory
		updatedEvent := transformer.TransformerEventT{
			Message:     userTransformedEvent.Output,
			Metadata:    eventMetadata,
			Destination: destination,
		}
		eventsToTransform = append(eventsToTransform, updatedEvent)
	}

	//REPORTING - START
	if proc.isReportingEnabled() {
		types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)

		var inPU, pu string
		if stage == transformer.UserTransformerStage {
			if trackingPlanEnabled {
				inPU = types.TRACKINGPLAN_VALIDATOR
			} else {
				inPU = types.GATEWAY
			}
			pu = types.USER_TRANSFORMER
		} else if stage == transformer.TrackingPlanValidationStage {
			inPU = types.GATEWAY
			pu = types.TRACKINGPLAN_VALIDATOR
		}

		for k, cd := range connectionDetailsMap {
			m := &types.PUReportedMetric{
				ConnectionDetails: *cd,
				PUDetails:         *types.CreatePUDetails(inPU, pu, false, false),
				StatusDetail:      statusDetailsMap[k],
			}
			successMetrics = append(successMetrics, m)
		}
	}
	//REPORTING - END

	return eventsToTransform, successMetrics, successCountMap, successCountMetadataMap
}

func (proc *HandleT) updateMetricMaps(countMetadataMap map[string]MetricMetadata, countMap map[string]int64, connectionDetailsMap map[string]*types.ConnectionDetails, statusDetailsMap map[string]*types.StatusDetail, event transformer.TransformerResponseT, status string, payload json.RawMessage) {
	if proc.isReportingEnabled() {
		countKey := fmt.Sprintf("%s:%s:%s", event.Metadata.SourceID, event.Metadata.DestinationID, event.Metadata.SourceBatchID)
		if _, ok := countMap[countKey]; !ok {
			countMap[countKey] = 0
		}
		countMap[countKey] = countMap[countKey] + 1
		if countMetadataMap != nil {
			if _, ok := countMetadataMap[countKey]; !ok {
				countMetadataMap[countKey] = MetricMetadata{sourceID: event.Metadata.SourceID, destinationID: event.Metadata.DestinationID, sourceBatchID: event.Metadata.SourceBatchID, sourceTaskID: event.Metadata.SourceTaskID, sourceTaskRunID: event.Metadata.SourceTaskRunID, sourceJobID: event.Metadata.SourceJobID, sourceJobRunID: event.Metadata.SourceJobRunID, sourceDefinitionID: event.Metadata.SourceDefinitionID, destinationDefinitionID: event.Metadata.DestinationDefinitionID, sourceCategory: event.Metadata.SourceCategory}
			}
		}

		key := fmt.Sprintf("%s:%s:%s:%s:%d", event.Metadata.SourceID, event.Metadata.DestinationID, event.Metadata.SourceBatchID, status, event.StatusCode)
		cd, ok := connectionDetailsMap[key]
		if !ok {
			cd = types.CreateConnectionDetail(event.Metadata.SourceID, event.Metadata.DestinationID, event.Metadata.SourceBatchID, event.Metadata.SourceTaskID, event.Metadata.SourceTaskRunID, event.Metadata.SourceJobID, event.Metadata.SourceJobRunID, event.Metadata.SourceDefinitionID, event.Metadata.DestinationDefinitionID, event.Metadata.SourceCategory)
			connectionDetailsMap[key] = cd
		}
		sd, ok := statusDetailsMap[key]
		if !ok {
			var eventName string
			var eventType string
			if string(payload) != `{}` {
				eventName = event.Metadata.EventName
				eventType = event.Metadata.EventType
			}
			sd = types.CreateStatusDetail(status, 0, event.StatusCode, event.Error, payload, eventName, eventType)
			statusDetailsMap[key] = sd
		}
		sd.Count++
	}
}

func (proc *HandleT) getFailedEventJobs(response transformer.ResponseT, commonMetaData transformer.MetadataT, eventsByMessageID map[string]types.SingularEventWithReceivedAt, stage string, transformationEnabled bool, trackingPlanEnabled bool) ([]*jobsdb.JobT, []*types.PUReportedMetric, map[string]int64) {
	failedMetrics := make([]*types.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*types.ConnectionDetails)
	statusDetailsMap := make(map[string]*types.StatusDetail)
	failedCountMap := make(map[string]int64)
	var failedEventsToStore []*jobsdb.JobT
	for _, failedEvent := range response.FailedEvents {
		var messages []types.SingularEventT
		if len(failedEvent.Metadata.MessageIDs) > 0 {
			messageIds := failedEvent.Metadata.MessageIDs
			for _, msgID := range messageIds {
				messages = append(messages, eventsByMessageID[msgID].SingularEvent)
			}
		} else {
			messages = append(messages, eventsByMessageID[failedEvent.Metadata.MessageID].SingularEvent)
		}
		payload, err := json.Marshal(messages)
		if err != nil {
			proc.logger.Errorf(`[Processor: getFailedEventJobs] Failed to unmarshal list of failed events: %v`, err)
			continue
		}

		//Using first element in messages array for sample event
		sampleEvent, err := json.Marshal(messages[0])
		if err != nil {
			proc.logger.Errorf(`[Processor: getFailedEventJobs] Failed to unmarshal first element in failed events: %v`, err)
			sampleEvent = []byte(`{}`)
		}
		//Update metrics maps
		proc.updateMetricMaps(nil, failedCountMap, connectionDetailsMap, statusDetailsMap, failedEvent, jobsdb.Aborted.State, sampleEvent)

		id := uuid.NewV4()

		params := map[string]interface{}{
			"source_id":         commonMetaData.SourceID,
			"destination_id":    commonMetaData.DestinationID,
			"source_job_run_id": failedEvent.Metadata.JobRunID,
			"error":             failedEvent.Error,
			"status_code":       failedEvent.StatusCode,
			"stage":             stage,
		}
		marshalledParams, err := json.Marshal(params)
		if err != nil {
			proc.logger.Errorf("[Processor] Failed to marshal parameters. Parameters: %v", params)
			marshalledParams = []byte(`{"error": "Processor failed to marshal params"}`)
		}

		newFailedJob := jobsdb.JobT{
			UUID:         id,
			EventPayload: payload,
			Parameters:   marshalledParams,
			CreatedAt:    time.Now(),
			ExpireAt:     time.Now(),
			CustomVal:    commonMetaData.DestinationType,
			UserID:       failedEvent.Metadata.RudderID,
		}
		failedEventsToStore = append(failedEventsToStore, &newFailedJob)

		procErrorStat := stats.NewTaggedStat("proc_error_counts", stats.CountType, stats.Tags{
			"destName":   commonMetaData.DestinationType,
			"statusCode": strconv.Itoa(failedEvent.StatusCode),
			"stage":      stage,
		})

		procErrorStat.Increment()
	}

	//REPORTING - START
	if proc.isReportingEnabled() {
		types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)

		var inPU, pu string
		if stage == transformer.DestTransformerStage {
			if transformationEnabled {
				inPU = types.USER_TRANSFORMER
			} else {
				if trackingPlanEnabled {
					inPU = types.TRACKINGPLAN_VALIDATOR
				} else {
					inPU = types.GATEWAY
				}
			}
			pu = types.DEST_TRANSFORMER
		} else if stage == transformer.UserTransformerStage {
			if trackingPlanEnabled {
				inPU = types.TRACKINGPLAN_VALIDATOR
			} else {
				inPU = types.GATEWAY
			}
			pu = types.USER_TRANSFORMER
		} else if stage == transformer.TrackingPlanValidationStage {
			inPU = types.GATEWAY
			pu = types.TRACKINGPLAN_VALIDATOR
		}
		for k, cd := range connectionDetailsMap {
			m := &types.PUReportedMetric{
				ConnectionDetails: *cd,
				PUDetails:         *types.CreatePUDetails(inPU, pu, false, false),
				StatusDetail:      statusDetailsMap[k],
			}
			failedMetrics = append(failedMetrics, m)
		}
	}
	//REPORTING - END

	return failedEventsToStore, failedMetrics, failedCountMap
}

func (proc *HandleT) updateSourceEventStatsDetailed(event types.SingularEventT, writeKey string) {
	//Any panics in this function are captured and ignore sending the stat
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
		}
	}()
	var eventType string
	var eventName string
	if val, ok := event["type"]; ok {
		eventType = val.(string)
		tags := map[string]string{
			"writeKey":   writeKey,
			"event_type": eventType,
		}
		statEventType := proc.stats.NewSampledTaggedStat("processor.event_type", stats.CountType, tags)
		statEventType.Count(1)
		if captureEventNameStats {
			if eventType != "track" {
				eventName = eventType
			} else {
				if val, ok := event["event"]; ok {
					eventName = val.(string)
				} else {
					eventName = eventType
				}
			}
			tags_detailed := map[string]string{
				"writeKey":   writeKey,
				"event_type": eventType,
				"event_name": eventName,
			}
			statEventTypeDetailed := proc.stats.NewSampledTaggedStat("processor.event_type_detailed", stats.CountType, tags_detailed)
			statEventTypeDetailed.Count(1)
		}
	}
}

func getDiffMetrics(inPU, pu string, inCountMetadataMap map[string]MetricMetadata, inCountMap, successCountMap, failedCountMap map[string]int64) []*types.PUReportedMetric {
	//Calculate diff and append to reportMetrics
	//diff = succesCount + abortCount - inCount
	diffMetrics := make([]*types.PUReportedMetric, 0)
	for key, inCount := range inCountMap {
		successCount, _ := successCountMap[key]
		failedCount, _ := failedCountMap[key]
		diff := successCount + failedCount - inCount
		if diff != 0 {
			metricMetadata := inCountMetadataMap[key]
			metric := &types.PUReportedMetric{
				ConnectionDetails: *types.CreateConnectionDetail(metricMetadata.sourceID, metricMetadata.destinationID, metricMetadata.sourceBatchID, metricMetadata.sourceTaskID, metricMetadata.sourceTaskRunID, metricMetadata.sourceJobID, metricMetadata.sourceJobRunID, metricMetadata.sourceDefinitionID, metricMetadata.destinationDefinitionID, metricMetadata.sourceCategory),
				PUDetails:         *types.CreatePUDetails(inPU, pu, false, false),
				StatusDetail:      types.CreateStatusDetail(types.DiffStatus, diff, 0, "", []byte(`{}`), "", ""),
			}
			diffMetrics = append(diffMetrics, metric)
		}
	}

	return diffMetrics
}

func (proc *HandleT) processJobsForDest(jobList []*jobsdb.JobT, parsedEventList [][]types.SingularEventT) {
	proc.processJobsTime.Start()
	proc.pStatsJobs.Start()

	proc.statNumRequests.Count(len(jobList))

	var destJobs []*jobsdb.JobT
	var batchDestJobs []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	var groupedEvents = make(map[string][]transformer.TransformerEventT)
	var groupedEventsByWriteKey = make(map[WriteKeyT][]transformer.TransformerEventT)
	var eventsByMessageID = make(map[string]types.SingularEventWithReceivedAt)
	var procErrorJobsByDestID = make(map[string][]*jobsdb.JobT)
	var procErrorJobs []*jobsdb.JobT

	if !(parsedEventList == nil || len(jobList) == len(parsedEventList)) {
		panic(fmt.Errorf("parsedEventList != nil and len(jobList):%d != len(parsedEventList):%d", len(jobList), len(parsedEventList)))
	}
	//Each block we receive from a client has a bunch of
	//requests. We parse the block and take out individual
	//requests, call the destination specific transformation
	//function and create jobs for them.
	//Transformation is called for a batch of jobs at a time
	//to speed-up execution.

	//Event count for performance stat monitoring
	totalEvents := 0

	proc.logger.Debug("[Processor] Total jobs picked up : ", len(jobList))

	proc.marshalSingularEvents.Start()
	uniqueMessageIds := make(map[string]struct{})
	uniqueMessageIdsBySrcDestKey := make(map[string]map[string]struct{})
	var sourceDupStats = make(map[string]int)

	reportMetrics := make([]*types.PUReportedMetric, 0)
	inCountMap := make(map[string]int64)
	inCountMetadataMap := make(map[string]MetricMetadata)
	connectionDetailsMap := make(map[string]*types.ConnectionDetails)
	statusDetailsMap := make(map[string]*types.StatusDetail)

	for idx, batchEvent := range jobList {

		var singularEvents []types.SingularEventT
		var ok bool
		if parsedEventList == nil {
			singularEvents, ok = misc.ParseRudderEventBatch(batchEvent.EventPayload)
		} else {
			singularEvents = parsedEventList[idx]
			ok = (singularEvents != nil)
		}
		writeKey := gjson.Get(string(batchEvent.EventPayload), "writeKey").Str
		requestIP := gjson.Get(string(batchEvent.EventPayload), "requestIP").Str
		receivedAt := gjson.Get(string(batchEvent.EventPayload), "receivedAt").Time()

		if ok {
			var duplicateIndexes []int
			if enableDedup {
				var allMessageIdsInBatch []string
				for _, singularEvent := range singularEvents {
					allMessageIdsInBatch = append(allMessageIdsInBatch, misc.GetStringifiedData(singularEvent["messageId"]))
				}
				duplicateIndexes = proc.dedupHandler.FindDuplicates(allMessageIdsInBatch, uniqueMessageIds)
			}

			//Iterate through all the events in the batch
			for eventIndex, singularEvent := range singularEvents {
				messageId := misc.GetStringifiedData(singularEvent["messageId"])
				if enableDedup && misc.Contains(duplicateIndexes, eventIndex) {
					proc.logger.Debugf("Dropping event with duplicate messageId: %s", messageId)
					misc.IncrementMapByKey(sourceDupStats, writeKey, 1)
					continue
				}

				proc.updateSourceEventStatsDetailed(singularEvent, writeKey)

				uniqueMessageIds[messageId] = struct{}{}
				//We count this as one, not destination specific ones
				totalEvents++
				eventsByMessageID[messageId] = types.SingularEventWithReceivedAt{SingularEvent: singularEvent, ReceivedAt: receivedAt}

				//Getting all the destinations which are enabled for this
				//event
				backendEnabledDestTypes := getBackendEnabledDestinationTypes(writeKey)
				enabledDestTypes := integrations.FilterClientIntegrations(singularEvent, backendEnabledDestTypes)
				sourceForSingularEvent, sourceIdError := getSourceByWriteKey(writeKey)
				if sourceIdError != nil {
					proc.logger.Error("Dropping Job since Source not found for writeKey : ", writeKey)
					continue
				}

				// proc.logger.Debug("=== enabledDestTypes ===", enabledDestTypes)
				if len(enabledDestTypes) == 0 {
					proc.logger.Debug("No enabled destinations")
					continue
				}

				commonMetadataFromSingularEvent := makeCommonMetadataFromSingularEvent(singularEvent, batchEvent, receivedAt, sourceForSingularEvent)

				_, ok = groupedEventsByWriteKey[WriteKeyT(writeKey)]
				if !ok {
					groupedEventsByWriteKey[WriteKeyT(writeKey)] = make([]transformer.TransformerEventT, 0)
				}
				shallowEventCopy := transformer.TransformerEventT{}
				shallowEventCopy.Message = singularEvent
				shallowEventCopy.Message["request_ip"] = requestIP
				enhanceWithTimeFields(&shallowEventCopy, singularEvent, receivedAt)
				enhanceWithMetadata(commonMetadataFromSingularEvent, &shallowEventCopy, backendconfig.DestinationT{})

				eventType, ok := singularEvent["type"].(string)
				if !ok {
					proc.logger.Error("singular event type is unknown")
				}

				source, sourceError := getSourceByWriteKey(writeKey)
				if sourceError != nil {
					proc.logger.Error("Source not found for writeKey : ", writeKey)
				} else {
					// TODO: TP ID preference 1.event.context set by rudderTyper   2.From WorkSpaceConfig (currently being used)
					shallowEventCopy.Metadata.TrackingPlanId = source.DgSourceTrackingPlanConfig.TrackingPlan.Id
					shallowEventCopy.Metadata.TrackingPlanVersion = source.DgSourceTrackingPlanConfig.TrackingPlan.Version
					shallowEventCopy.Metadata.SourceTpConfig = source.DgSourceTrackingPlanConfig.Config
					shallowEventCopy.Metadata.MergedTpConfig = source.DgSourceTrackingPlanConfig.GetMergedConfig(eventType)
				}

				groupedEventsByWriteKey[WriteKeyT(writeKey)] = append(groupedEventsByWriteKey[WriteKeyT(writeKey)], shallowEventCopy)

				//REPORTING - GATEWAY metrics - START
				if proc.isReportingEnabled() {
					//Grouping events by sourceid + destinationid + source batch id to find the count
					key := fmt.Sprintf("%s:%s", commonMetadataFromSingularEvent.SourceID, commonMetadataFromSingularEvent.SourceBatchID)
					if _, ok := inCountMap[key]; !ok {
						inCountMap[key] = 0
					}
					inCountMap[key] = inCountMap[key] + 1
					if _, ok := inCountMetadataMap[key]; !ok {
						inCountMetadataMap[key] = MetricMetadata{sourceID: commonMetadataFromSingularEvent.SourceID, sourceBatchID: commonMetadataFromSingularEvent.SourceBatchID, sourceTaskID: commonMetadataFromSingularEvent.SourceTaskID, sourceTaskRunID: commonMetadataFromSingularEvent.SourceTaskRunID, sourceJobID: commonMetadataFromSingularEvent.SourceJobID, sourceJobRunID: commonMetadataFromSingularEvent.SourceJobRunID}
					}

					cd, ok := connectionDetailsMap[key]
					if !ok {
						cd = types.CreateConnectionDetail(commonMetadataFromSingularEvent.SourceID, "", commonMetadataFromSingularEvent.SourceBatchID, commonMetadataFromSingularEvent.SourceTaskID, commonMetadataFromSingularEvent.SourceTaskRunID, commonMetadataFromSingularEvent.SourceJobID, commonMetadataFromSingularEvent.SourceJobRunID, commonMetadataFromSingularEvent.SourceDefinitionID, commonMetadataFromSingularEvent.DestinationDefinitionID, commonMetadataFromSingularEvent.SourceCategory)
						connectionDetailsMap[key] = cd
					}
					sd, ok := statusDetailsMap[key]
					if !ok {
						sd = types.CreateStatusDetail(jobsdb.Succeeded.State, 0, 200, "", []byte(`{}`), "", "")
						statusDetailsMap[key] = sd
					}
					sd.Count++
				}
				//REPORTING - GATEWAY metrics - END
			}
		}

		//Mark the batch event as processed
		newStatus := jobsdb.JobStatusT{
			JobID:         batchEvent.JobID,
			JobState:      jobsdb.Succeeded.State,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{"success":"OK"}`),
			Parameters:    []byte(`{}`),
		}
		statusList = append(statusList, &newStatus)
	}

	//REPORTING - GATEWAY metrics - START
	if proc.isReportingEnabled() {
		types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)
		for k, cd := range connectionDetailsMap {
			m := &types.PUReportedMetric{
				ConnectionDetails: *cd,
				PUDetails:         *types.CreatePUDetails("", types.GATEWAY, false, true),
				StatusDetail:      statusDetailsMap[k],
			}
			reportMetrics = append(reportMetrics, m)
		}
	}
	//REPORTING - GATEWAY metrics - END

	proc.statNumEvents.Count(totalEvents)

	proc.marshalSingularEvents.End()

	//TRACKING PLAN - START
	//Placing the trackingPlan validation filters here.
	//Else further down events are duplicated by destId, so multiple validation takes places for same event
	proc.validateEventsTime.Start()
	validatedEventsByWriteKey, validatedReportMetrics, validatedErrorJobs, trackingPlanEnabledMap := proc.validateEvents(groupedEventsByWriteKey, eventsByMessageID)
	proc.validateEventsTime.End()

	// Appending validatedErrorJobs to procErrorJobs
	procErrorJobs = append(procErrorJobs, validatedErrorJobs...)

	// Appending validatedReportMetrics to reportMetrics
	reportMetrics = append(reportMetrics, validatedReportMetrics...)
	//TRACKING PLAN - END

	// The below part further segregates events by sourceID and DestinationID.
	for writeKeyT, eventList := range validatedEventsByWriteKey {
		for _, event := range eventList {
			writeKey := string(writeKeyT)
			singularEvent := event.Message

			backendEnabledDestTypes := getBackendEnabledDestinationTypes(writeKey)
			enabledDestTypes := integrations.FilterClientIntegrations(singularEvent, backendEnabledDestTypes)
			workspaceID := proc.backendConfig.GetWorkspaceIDForWriteKey(writeKey)
			workspaceLibraries := proc.backendConfig.GetWorkspaceLibrariesForWorkspaceID(workspaceID)

			enabledDestinationsMap := map[string][]backendconfig.DestinationT{}
			for _, destType := range enabledDestTypes {
				enabledDestinationsList := getEnabledDestinations(writeKey, destType)
				enabledDestinationsMap[destType] = enabledDestinationsList
				// Adding a singular event multiple times if there are multiple destinations of same type
				for _, destination := range enabledDestinationsList {
					shallowEventCopy := transformer.TransformerEventT{}
					shallowEventCopy.Message = singularEvent
					shallowEventCopy.Destination = reflect.ValueOf(destination).Interface().(backendconfig.DestinationT)
					shallowEventCopy.Libraries = workspaceLibraries
					shallowEventCopy.Metadata = event.Metadata

					// At the TP flow we are not having destination information, so adding it here.
					shallowEventCopy.Metadata.DestinationID = destination.ID
					shallowEventCopy.Metadata.DestinationType = destination.DestinationDefinition.Name

					//TODO: Test for multiple workspaces ex: hosted data plane
					/* Stream destinations does not need config in transformer. As the Kafka destination config
					holds the ca-certificate and it depends on user input, it may happen that they provide entire
					certificate chain. So, that will make the payload huge while sending a batch of events to transformer,
					it may result into payload larger than accepted by transformer. So, discarding destination config from being
					sent to transformer for such destination. */
					if misc.ContainsString(customDestinations, destType) {
						shallowEventCopy.Destination.Config = nil
					}

					metadata := shallowEventCopy.Metadata
					srcAndDestKey := getKeyFromSourceAndDest(metadata.SourceID, metadata.DestinationID)
					//We have at-least one event so marking it good
					_, ok := groupedEvents[srcAndDestKey]
					if !ok {
						groupedEvents[srcAndDestKey] = make([]transformer.TransformerEventT, 0)
					}
					groupedEvents[srcAndDestKey] = append(groupedEvents[srcAndDestKey],
						shallowEventCopy)
					if _, ok := uniqueMessageIdsBySrcDestKey[srcAndDestKey]; !ok {
						uniqueMessageIdsBySrcDestKey[srcAndDestKey] = make(map[string]struct{})
					}
					uniqueMessageIdsBySrcDestKey[srcAndDestKey][metadata.MessageID] = struct{}{}
				}
			}
		}
	}

	//Now do the actual transformation. We call it in batches, once
	//for each destination ID

	proc.destProcessing.Start()
	proc.logger.Debug("[Processor: processJobsForDest] calling transformations")
	var startedAt, endedAt time.Time
	var timeTaken float64
	for srcAndDestKey, eventList := range groupedEvents {
		sourceID, destID := getSourceAndDestIDsFromKey(srcAndDestKey)
		destination := eventList[0].Destination
		workspaceID := eventList[0].Metadata.WorkspaceID
		commonMetaData := transformer.MetadataT{
			SourceID:        sourceID,
			SourceType:      eventList[0].Metadata.SourceType,
			SourceCategory:  eventList[0].Metadata.SourceCategory,
			WorkspaceID:     workspaceID,
			Namespace:       config.GetKubeNamespace(),
			InstanceID:      config.GetInstanceID(),
			DestinationID:   destID,
			DestinationType: destination.DestinationDefinition.Name,
		}

		configSubscriberLock.RLock()
		destType := destinationIDtoTypeMap[destID]
		transformationEnabled := len(destination.Transformations) > 0
		configSubscriberLock.RUnlock()

		trackingPlanEnabled := trackingPlanEnabledMap[SourceIDT(sourceID)]

		//REPORTING - START
		if proc.isReportingEnabled() {
			//Grouping events by sourceid + destinationid + source batch id to find the count
			inCountMap = make(map[string]int64)
			inCountMetadataMap = make(map[string]MetricMetadata)
			for i := range eventList {
				event := &eventList[i]
				key := fmt.Sprintf("%s:%s:%s", event.Metadata.SourceID, event.Metadata.DestinationID, event.Metadata.SourceBatchID)
				if _, ok := inCountMap[key]; !ok {
					inCountMap[key] = 0
				}
				if _, ok := inCountMetadataMap[key]; !ok {
					inCountMetadataMap[key] = MetricMetadata{sourceID: event.Metadata.SourceID, destinationID: event.Metadata.DestinationID, sourceBatchID: event.Metadata.SourceBatchID, sourceTaskID: event.Metadata.SourceTaskID, sourceTaskRunID: event.Metadata.SourceTaskRunID, sourceJobID: event.Metadata.SourceJobID, sourceJobRunID: event.Metadata.SourceJobRunID, sourceDefinitionID: event.Metadata.SourceDefinitionID, destinationDefinitionID: event.Metadata.DestinationDefinitionID, sourceCategory: event.Metadata.SourceCategory}
				}
				inCountMap[key] = inCountMap[key] + 1
			}
		}
		//REPORTING - END

		url := integrations.GetDestinationURL(destType)
		var response transformer.ResponseT
		var eventsToTransform []transformer.TransformerEventT
		// Send to custom transformer only if the destination has a transformer enabled
		if transformationEnabled {
			userTransformationStat := proc.newUserTransformationStat(sourceID, workspaceID, destination)
			userTransformationStat.numEvents.Count(len(eventList))
			proc.logger.Debug("Custom Transform input size", len(eventList))

			userTransformationStat.transformTime.Start()
			startedAt = time.Now()
			response = proc.transformer.Transform(eventList, integrations.GetUserTransformURL(), userTransformBatchSize)
			endedAt = time.Now()
			timeTaken = endedAt.Sub(startedAt).Seconds()
			userTransformationStat.transformTime.End()
			proc.addToTransformEventByTimePQ(&TransformRequestT{Event: eventList, Stage: transformer.UserTransformerStage, ProcessingTime: timeTaken, Index: -1}, &proc.userTransformEventsByTimeTaken)

			var successMetrics []*types.PUReportedMetric
			var successCountMap map[string]int64
			var successCountMetadataMap map[string]MetricMetadata
			eventsToTransform, successMetrics, successCountMap, successCountMetadataMap = proc.getDestTransformerEvents(response, commonMetaData, destination, transformer.UserTransformerStage, trackingPlanEnabled)
			failedJobs, failedMetrics, failedCountMap := proc.getFailedEventJobs(response, commonMetaData, eventsByMessageID, transformer.UserTransformerStage, transformationEnabled, trackingPlanEnabled)
			if _, ok := procErrorJobsByDestID[destID]; !ok {
				procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
			}
			procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], failedJobs...)
			userTransformationStat.numOutputSuccessEvents.Count(len(eventsToTransform))
			userTransformationStat.numOutputFailedEvents.Count(len(failedJobs))
			proc.logger.Debug("Custom Transform output size", len(eventsToTransform))

			transformationdebugger.UploadTransformationStatus(&transformationdebugger.TransformationStatusT{SourceID: sourceID, DestID: destID, Destination: &destination, UserTransformedEvents: eventsToTransform, EventsByMessageID: eventsByMessageID, FailedEvents: response.FailedEvents, UniqueMessageIds: uniqueMessageIdsBySrcDestKey[srcAndDestKey]})

			//REPORTING - START
			if proc.isReportingEnabled() {
				diffMetrics := getDiffMetrics(types.GATEWAY, types.USER_TRANSFORMER, inCountMetadataMap, inCountMap, successCountMap, failedCountMap)
				reportMetrics = append(reportMetrics, successMetrics...)
				reportMetrics = append(reportMetrics, failedMetrics...)
				reportMetrics = append(reportMetrics, diffMetrics...)

				//successCountMap will be inCountMap for destination transform
				inCountMap = successCountMap
				inCountMetadataMap = successCountMetadataMap
			}
			//REPORTING - END
		} else {
			proc.logger.Debug("No custom transformation")
			eventsToTransform = eventList
		}

		if len(eventsToTransform) == 0 {
			continue
		}

		proc.logger.Debug("Dest Transform input size", len(eventsToTransform))
		startedAt = time.Now()

		transformAt := "processor"
		if val, ok := destination.DestinationDefinition.Config["transformAtV1"].(string); ok {
			transformAt = val
		}
		//Check for overrides through env
		transformAtOverrideFound := config.IsSet("Processor." + destination.DestinationDefinition.Name + ".transformAt")
		if transformAtOverrideFound {
			transformAt = config.GetString("Processor."+destination.DestinationDefinition.Name+".transformAt", "processor")
		}
		transformAtFromFeaturesFile := gjson.Get(string(proc.transformerFeatures), fmt.Sprintf("routerTransform.%s", destination.DestinationDefinition.Name)).String()

		destTransformationStat := proc.newDestinationTransformationStat(sourceID, workspaceID, transformAt, destination)
		//If transformAt is none
		// OR
		//router and transformer supports router transform, then no destination transformation happens.
		if transformAt == "none" || (transformAt == "router" && transformAtFromFeaturesFile != "") {
			response = ConvertToFilteredTransformerResponse(eventsToTransform, transformAt != "none")
		} else {
			destTransformationStat.transformTime.Start()
			response = proc.transformer.Transform(eventsToTransform, url, transformBatchSize)
			destTransformationStat.transformTime.End()
			transformAt = "processor"
		}

		endedAt = time.Now()
		timeTaken = endedAt.Sub(startedAt).Seconds()
		proc.addToTransformEventByTimePQ(&TransformRequestT{Event: eventsToTransform, Stage: "destination-transformer", ProcessingTime: timeTaken, Index: -1}, &proc.destTransformEventsByTimeTaken)

		destTransformEventList := response.Events
		proc.logger.Debug("Dest Transform output size", len(destTransformEventList))

		failedJobs, failedMetrics, failedCountMap := proc.getFailedEventJobs(response, commonMetaData, eventsByMessageID, transformer.DestTransformerStage, transformationEnabled, trackingPlanEnabled)
		destTransformationStat.numEvents.Count(len(eventsToTransform))
		destTransformationStat.numOutputSuccessEvents.Count(len(destTransformEventList))
		destTransformationStat.numOutputFailedEvents.Count(len(failedJobs))

		if _, ok := procErrorJobsByDestID[destID]; !ok {
			procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
		}
		procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], failedJobs...)

		successMetrics := make([]*types.PUReportedMetric, 0)
		connectionDetailsMap := make(map[string]*types.ConnectionDetails)
		statusDetailsMap := make(map[string]*types.StatusDetail)
		successCountMap := make(map[string]int64)
		//Save the JSON in DB. This is what the router uses
		for _, destEvent := range destTransformEventList {
			//Update metrics maps
			proc.updateMetricMaps(nil, successCountMap, connectionDetailsMap, statusDetailsMap, destEvent, jobsdb.Succeeded.State, []byte(`{}`))

			destEventJSON, err := json.Marshal(destEvent.Output)
			//Should be a valid JSON since its our transformation
			//but we handle anyway
			if err != nil {
				continue
			}

			//Need to replace UUID his with messageID from client
			id := uuid.NewV4()
			// read source_id from metadata that is replayed back from transformer
			// in case of custom transformations metadata of first event is returned along with all events in session
			// source_id will be same for all events belong to same user in a session
			sourceID := destEvent.Metadata.SourceID
			destID := destEvent.Metadata.DestinationID
			rudderID := destEvent.Metadata.RudderID
			receivedAt := destEvent.Metadata.ReceivedAt
			messageId := destEvent.Metadata.MessageID
			jobId := destEvent.Metadata.JobID
			sourceBatchId := destEvent.Metadata.SourceBatchID
			sourceTaskId := destEvent.Metadata.SourceTaskID
			sourceTaskRunId := destEvent.Metadata.SourceTaskRunID
			sourceJobId := destEvent.Metadata.SourceJobID
			sourceJobRunId := destEvent.Metadata.SourceJobRunID
			eventName := destEvent.Metadata.EventName
			eventType := destEvent.Metadata.EventType
			sourceDefID := destEvent.Metadata.SourceDefinitionID
			destDefID := destEvent.Metadata.DestinationDefinitionID
			sourceCategory := destEvent.Metadata.SourceCategory
			//If the response from the transformer does not have userID in metadata, setting userID to random-uuid.
			//This is done to respect findWorker logic in router.
			if rudderID == "" {
				rudderID = "random-" + id.String()
			}

			params := ParametersT{
				SourceID:                sourceID,
				DestinationID:           destID,
				ReceivedAt:              receivedAt,
				TransformAt:             transformAt,
				MessageID:               messageId,
				GatewayJobID:            jobId,
				SourceBatchID:           sourceBatchId,
				SourceTaskID:            sourceTaskId,
				SourceTaskRunID:         sourceTaskRunId,
				SourceJobID:             sourceJobId,
				SourceJobRunID:          sourceJobRunId,
				EventName:               eventName,
				EventType:               eventType,
				SourceCategory:          sourceCategory,
				SourceDefinitionID:      sourceDefID,
				DestinationDefinitionID: destDefID,
			}
			marshalledParams, err := json.Marshal(params)
			if err != nil {
				proc.logger.Errorf("[Processor] Failed to marshal parameters object. Parameters: %v", params)
				panic(err)
			}

			newJob := jobsdb.JobT{
				UUID:         id,
				UserID:       rudderID,
				Parameters:   marshalledParams,
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
				CustomVal:    destType,
				EventPayload: destEventJSON,
			}
			if misc.Contains(batchDestinations, newJob.CustomVal) {
				batchDestJobs = append(batchDestJobs, &newJob)
			} else {
				destJobs = append(destJobs, &newJob)
			}
		}

		//REPORTING - PROCESSOR metrics - START
		if proc.isReportingEnabled() {
			types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)

			var inPU string
			if transformationEnabled {
				inPU = types.USER_TRANSFORMER
			} else {
				if trackingPlanEnabled {
					inPU = types.TRACKINGPLAN_VALIDATOR
				} else {
					inPU = types.GATEWAY
				}
			}

			for k, cd := range connectionDetailsMap {
				m := &types.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *types.CreatePUDetails(inPU, types.DEST_TRANSFORMER, false, false),
					StatusDetail:      statusDetailsMap[k],
				}
				successMetrics = append(successMetrics, m)
			}

			diffMetrics := getDiffMetrics(inPU, types.DEST_TRANSFORMER, inCountMetadataMap, inCountMap, successCountMap, failedCountMap)

			reportMetrics = append(reportMetrics, failedMetrics...)
			reportMetrics = append(reportMetrics, successMetrics...)
			reportMetrics = append(reportMetrics, diffMetrics...)
		}
		//REPORTING - PROCESSOR metrics - END
	}

	proc.destProcessing.End()
	if len(statusList) != len(jobList) {
		panic(fmt.Errorf("len(statusList):%d != len(jobList):%d", len(statusList), len(jobList)))
	}

	proc.statDBW.Start()
	proc.pStatsDBW.Start()
	//XX: Need to do this in a transaction
	if len(destJobs) > 0 {
		proc.logger.Debug("[Processor] Total jobs written to router : ", len(destJobs))
		err := proc.routerDB.Store(destJobs)
		if err != nil {
			proc.logger.Errorf("Store into router table failed with error: %v", err)
			proc.logger.Errorf("destJobs: %v", destJobs)
			panic(err)
		}
		proc.statDestNumOutputEvents.Count(len(destJobs))
	}
	if len(batchDestJobs) > 0 {
		proc.logger.Debug("[Processor] Total jobs written to batch router : ", len(batchDestJobs))
		err := proc.batchRouterDB.Store(batchDestJobs)
		if err != nil {
			proc.logger.Errorf("Store into batch router table failed with error: %v", err)
			proc.logger.Errorf("batchDestJobs: %v", batchDestJobs)
			panic(err)
		}
		proc.statBatchDestNumOutputEvents.Count(len(batchDestJobs))
	}

	for _, jobs := range procErrorJobsByDestID {
		procErrorJobs = append(procErrorJobs, jobs...)
	}
	if len(procErrorJobs) > 0 {
		proc.logger.Info("[Processor] Total jobs written to proc_error: ", len(procErrorJobs))
		err := proc.errorDB.Store(procErrorJobs)
		if err != nil {
			proc.logger.Errorf("Store into proc error table failed with error: %v", err)
			proc.logger.Errorf("procErrorJobs: %v", procErrorJobs)
			panic(err)
		}
		recordEventDeliveryStatus(procErrorJobsByDestID)
	}

	txn := proc.gatewayDB.BeginGlobalTransaction()
	proc.gatewayDB.AcquireUpdateJobStatusLocks()
	err := proc.gatewayDB.UpdateJobStatusInTxn(txn, statusList, []string{GWCustomVal}, nil)
	if err != nil {
		pkgLogger.Errorf("Error occurred while updating gateway jobs statuses. Panicking. Err: %v", err)
		panic(err)
	}
	if proc.isReportingEnabled() {
		proc.reporting.Report(reportMetrics, txn)
	}

	if enableDedup {
		proc.updateSourceStats(sourceDupStats, "processor.write_key_duplicate_events")
		if len(uniqueMessageIds) > 0 {
			var dedupedMessageIdsAcrossJobs []string
			for k := range uniqueMessageIds {
				dedupedMessageIdsAcrossJobs = append(dedupedMessageIdsAcrossJobs, k)
			}
			proc.dedupHandler.MarkProcessed(dedupedMessageIdsAcrossJobs)
		}
	}
	proc.gatewayDB.CommitTransaction(txn)
	proc.gatewayDB.ReleaseUpdateJobStatusLocks()
	proc.statDBW.End()

	proc.logger.Debugf("Processor GW DB Write Complete. Total Processed: %v", len(statusList))
	//XX: End of transaction

	proc.pStatsDBW.End(len(statusList))
	proc.pStatsJobs.End(totalEvents)

	proc.statGatewayDBW.Count(len(statusList))
	proc.statRouterDBW.Count(len(destJobs))
	proc.statBatchRouterDBW.Count(len(batchDestJobs))
	proc.statProcErrDBW.Count(len(procErrorJobs))

	proc.pStatsJobs.Print()
	proc.pStatsDBW.Print()
	proc.processJobsTime.End()
}

func ConvertToFilteredTransformerResponse(events []transformer.TransformerEventT, filterUnsupportedMessageTypes bool) transformer.ResponseT {
	var responses []transformer.TransformerResponseT
	var failedEvents []transformer.TransformerResponseT

	// filter unsupported message types
	var resp transformer.TransformerResponseT
	var errMessage string
	for _, event := range events {
		destinationDef := event.Destination.DestinationDefinition
		supportedTypes, ok := destinationDef.Config["supportedMessageTypes"]
		if ok && filterUnsupportedMessageTypes {
			messageType, typOk := event.Message["type"].(string)
			if !typOk {
				// add to FailedEvents
				errMessage = "Invalid message type. Type assertion failed"
				resp = transformer.TransformerResponseT{Output: event.Message, StatusCode: 400, Metadata: event.Metadata, Error: errMessage}
				failedEvents = append(failedEvents, resp)
				continue
			}

			messageType = strings.TrimSpace(strings.ToLower(messageType))
			if misc.Contains(supportedTypes, messageType) {
				resp = transformer.TransformerResponseT{Output: event.Message, StatusCode: 200, Metadata: event.Metadata}
				responses = append(responses, resp)
			} else {
				// add to FailedEvents
				errMessage = "Message type " + messageType + " not supported"
				resp = transformer.TransformerResponseT{Output: event.Message, StatusCode: 400, Metadata: event.Metadata, Error: errMessage}
				failedEvents = append(failedEvents, resp)
			}
		} else {
			// allow event
			resp = transformer.TransformerResponseT{Output: event.Message, StatusCode: 200, Metadata: event.Metadata}
			responses = append(responses, resp)
		}
	}

	return transformer.ResponseT{Events: responses, FailedEvents: failedEvents}
}

func getTruncatedEventList(jobList []*jobsdb.JobT, maxEvents int) (truncatedList []*jobsdb.JobT, totalEvents int) {
	for idx, job := range jobList {
		eventsInJob := len(gjson.GetBytes(job.EventPayload, "batch").Array())
		totalEvents += eventsInJob
		if totalEvents >= maxEvents {
			return jobList[:idx+1], totalEvents
		}
	}
	return jobList, totalEvents
}

func (proc *HandleT) addToTransformEventByTimePQ(event *TransformRequestT, pq *transformRequestPQ) {
	proc.transformEventsByTimeMutex.Lock()
	defer proc.transformEventsByTimeMutex.Unlock()
	if pq.Len() < transformTimesPQLength {
		pq.Add(event)
		return
	}
	if pq.Top().ProcessingTime < event.ProcessingTime {
		pq.RemoveTop()
		pq.Add(event)

	}
}

// handlePendingGatewayJobs is checking for any pending gateway jobs (failed and unprocessed), and routes them appropriately
// Returns true if any job is handled, otherwise returns false.
func (proc *HandleT) handlePendingGatewayJobs() bool {
	proc.statLoopTime.Start()
	proc.pStatsDBR.Start()
	proc.statDBR.Start()

	toQuery := dbReadBatchSize
	proc.logger.Debugf("Processor DB Read size: %v", toQuery)
	//Should not have any failure while processing (in v0) so
	//retryList should be empty. Remove the assert

	var retryList, unprocessedList []*jobsdb.JobT
	var totalRetryEvents, totalUnprocessedEvents int

	unTruncatedRetryList := proc.gatewayDB.GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{GWCustomVal}, Count: toQuery})
	retryList, totalRetryEvents = getTruncatedEventList(unTruncatedRetryList, maxEventsToProcess)

	if len(unTruncatedRetryList) >= dbReadBatchSize || totalRetryEvents >= maxEventsToProcess {
		// skip querying for unprocessed jobs if either retreived dbReadBatchSize or retreived maxEventToProcess
	} else {
		eventsLeftToProcess := maxEventsToProcess - totalRetryEvents
		toQuery = misc.MinInt(eventsLeftToProcess, dbReadBatchSize)
		unTruncatedUnProcessedList := proc.gatewayDB.GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{GWCustomVal}, Count: toQuery})
		unprocessedList, totalUnprocessedEvents = getTruncatedEventList(unTruncatedUnProcessedList, eventsLeftToProcess)
	}

	proc.statDBR.End()

	// check if there is work to be done
	if len(unprocessedList)+len(retryList) == 0 {
		proc.logger.Debugf("Processor DB Read Complete. No GW Jobs to process.")
		proc.pStatsDBR.End(0)
		return false
	}
	proc.eventSchemasTime.Start()
	if enableEventSchemasFeature && !enableEventSchemasAPIOnly {
		for _, unprocessedJob := range unprocessedList {
			writeKey := gjson.GetBytes(unprocessedJob.EventPayload, "writeKey").Str
			proc.eventSchemaHandler.RecordEventSchema(writeKey, string(unprocessedJob.EventPayload))
		}
	}
	proc.eventSchemasTime.End()
	// handle pending jobs
	proc.statListSort.Start()
	combinedList := append(unprocessedList, retryList...)
	proc.logger.Debugf("Processor DB Read Complete. retryList: %v, unprocessedList: %v, total_requests: %v, total_events: %d", len(retryList), len(unprocessedList), len(combinedList), totalRetryEvents+totalUnprocessedEvents)
	proc.pStatsDBR.End(len(combinedList))
	proc.statGatewayDBR.Count(len(combinedList))

	proc.pStatsDBR.Print()

	//Sort by JOBID
	sort.Slice(combinedList, func(i, j int) bool {
		return combinedList[i].JobID < combinedList[j].JobID
	})

	proc.statListSort.End()

	proc.processJobsForDest(combinedList, nil)

	proc.statLoopTime.End()

	return true
}

func (proc *HandleT) mainLoop() {
	//waiting for reporting client setup
	if proc.reporting != nil && proc.reportingEnabled {
		proc.reporting.WaitForSetup(types.CORE_REPORTING_CLIENT)
	}

	proc.logger.Info("Processor loop started")
	currLoopSleep := time.Duration(0)
	timeout := time.After(mainLoopTimeout)
	for {
		select {
		case pause := <-proc.pauseChannel:
			pkgLogger.Info("Processor is paused.")
			proc.paused = true
			pause.respChannel <- true
			<-proc.resumeChannel
		case <-timeout:
			proc.paused = false
			timeout = time.After(mainLoopTimeout)
			if isUnLocked {
				if proc.handlePendingGatewayJobs() {
					currLoopSleep = time.Duration(0)
				} else {
					currLoopSleep = 2*currLoopSleep + loopSleep
					if currLoopSleep > maxLoopSleep {
						currLoopSleep = maxLoopSleep
					}
					time.Sleep(currLoopSleep)
				}
				time.Sleep(fixedLoopSleep) // adding sleep here to reduce cpu load on postgres when we have less rps
			} else {
				time.Sleep(fixedLoopSleep)
			}
		}
	}
}

func (proc *HandleT) crashRecover() {
	proc.gatewayDB.DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: []string{GWCustomVal}, Count: -1})
}

func (proc *HandleT) updateSourceStats(sourceStats map[string]int, bucket string) {
	for sourceTag, count := range sourceStats {
		tags := map[string]string{
			"source": sourceTag,
		}
		sourceStatsD := proc.stats.NewTaggedStat(bucket, stats.CountType, tags)
		sourceStatsD.Count(count)
	}
}

//Pause is a blocking call.
//Pause returns after the processor is paused.
func (proc *HandleT) Pause() {
	proc.pauseLock.Lock()
	defer proc.pauseLock.Unlock()

	if proc.paused {
		return
	}

	respChannel := make(chan bool)
	proc.pauseChannel <- &PauseT{respChannel: respChannel}
	<-respChannel
}

func (proc *HandleT) Resume() {
	proc.pauseLock.Lock()
	defer proc.pauseLock.Unlock()

	if !proc.paused {
		return
	}

	proc.resumeChannel <- true
}

func (proc *HandleT) isReportingEnabled() bool {
	return proc.reporting != nil && proc.reportingEnabled
}
