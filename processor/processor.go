package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	event_schema "github.com/rudderlabs/rudder-server/event-schema"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

func RegisterAdminHandlers(readonlyProcErrorDB jobsdb.ReadonlyJobsDB) {
	admin.RegisterAdminHandler("ProcErrors", &stash.StashRpcHandler{ReadOnlyJobsDB: readonlyProcErrorDB})
}

//HandleT is a handle to this object used in main.go
type HandleT struct {
	backendConfig       backendconfig.BackendConfig
	transformer         transformer.Transformer
	lastJobID           int64
	gatewayDB           jobsdb.JobsDB
	routerDB            jobsdb.JobsDB
	batchRouterDB       jobsdb.JobsDB
	errorDB             jobsdb.JobsDB
	logger              logger.LoggerI
	eventSchemaHandler  types.EventSchemasI
	dedupHandler        dedup.DedupI
	reporting           types.ReportingI
	reportingEnabled    bool
	multitenantI        multitenant.MultiTenantI
	backgroundWait      func() error
	backgroundCancel    context.CancelFunc
	transformerFeatures json.RawMessage
	readLoopSleep       time.Duration
	maxLoopSleep        time.Duration
	storeTimeout        time.Duration
	statsFactory        stats.Stats
	stats               processorStats
	payloadLimit        int64
	transientSources    transientsource.Service
}

type processorStats struct {
	transformEventsByTimeMutex     sync.RWMutex
	destTransformEventsByTimeTaken transformRequestPQ
	userTransformEventsByTimeTaken transformRequestPQ
	pStatsJobs                     *misc.PerfStats
	pStatsDBR                      *misc.PerfStats
	statGatewayDBR                 stats.RudderStats
	pStatsDBW                      *misc.PerfStats
	statGatewayDBW                 stats.RudderStats
	statRouterDBW                  stats.RudderStats
	statBatchRouterDBW             stats.RudderStats
	statProcErrDBW                 stats.RudderStats
	statDBR                        stats.RudderStats
	statDBW                        stats.RudderStats
	statLoopTime                   stats.RudderStats
	eventSchemasTime               stats.RudderStats
	validateEventsTime             stats.RudderStats
	processJobsTime                stats.RudderStats
	statSessionTransform           stats.RudderStats
	statUserTransform              stats.RudderStats
	statDestTransform              stats.RudderStats
	marshalSingularEvents          stats.RudderStats
	destProcessing                 stats.RudderStats
	pipeProcessing                 stats.RudderStats
	statNumRequests                stats.RudderStats
	statNumEvents                  stats.RudderStats
	statDBReadRequests             stats.RudderStats
	statDBReadEvents               stats.RudderStats
	statDBReadPayloadBytes         stats.RudderStats
	statDBReadOutOfOrder           stats.RudderStats
	statDBReadOutOfSequence        stats.RudderStats
	statMarkExecuting              stats.RudderStats
	statDBWriteStatusTime          stats.RudderStats
	statDBWriteJobsTime            stats.RudderStats
	statDBWriteRouterPayloadBytes  stats.RudderStats
	statDBWriteBatchPayloadBytes   stats.RudderStats
	statDBWriteRouterEvents        stats.RudderStats
	statDBWriteBatchEvents         stats.RudderStats
	statDestNumOutputEvents        stats.RudderStats
	statBatchDestNumOutputEvents   stats.RudderStats
	DBReadThroughput               stats.RudderStats
	processJobThroughput           stats.RudderStats
	transformationsThroughput      stats.RudderStats
	DBWriteThroughput              stats.RudderStats
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
	SourceID                string      `json:"source_id"`
	DestinationID           string      `json:"destination_id"`
	ReceivedAt              string      `json:"received_at"`
	TransformAt             string      `json:"transform_at"`
	MessageID               string      `json:"message_id"`
	GatewayJobID            int64       `json:"gateway_job_id"`
	SourceBatchID           string      `json:"source_batch_id"`
	SourceTaskID            string      `json:"source_task_id"`
	SourceTaskRunID         string      `json:"source_task_run_id"`
	SourceJobID             string      `json:"source_job_id"`
	SourceJobRunID          string      `json:"source_job_run_id"`
	EventName               string      `json:"event_name"`
	EventType               string      `json:"event_type"`
	SourceDefinitionID      string      `json:"source_definition_id"`
	DestinationDefinitionID string      `json:"destination_definition_id"`
	SourceCategory          string      `json:"source_category"`
	RecordID                interface{} `json:"record_id"`
	WorkspaceId             string      `json:"workspaceId"`
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

const METRICKEYDELIMITER = "!<<#>>!"
const USER_TRANSFORMATION = "USER_TRANSFORMATION"
const DEST_TRANSFORMATION = "DEST_TRANSFORMATION"
const EVENT_FILTER = "EVENT_FILTER"

func buildStatTags(sourceID, workspaceID string, destination backendconfig.DestinationT, transformationType string) map[string]string {
	var module = "router"
	if batchrouter.IsObjectStorageDestination(destination.DestinationDefinition.Name) {
		module = "batch_router"
	}
	if batchrouter.IsWarehouseDestination(destination.DestinationDefinition.Name) {
		module = "warehouse"
	}

	return map[string]string{
		"module":             module,
		"destination":        destination.ID,
		"destType":           destination.DestinationDefinition.Name,
		"source":             sourceID,
		"workspace":          workspaceID,
		"transformationType": transformationType,
	}
}

func (proc *HandleT) newUserTransformationStat(sourceID, workspaceID string, destination backendconfig.DestinationT) *DestStatT {
	tags := buildStatTags(sourceID, workspaceID, destination, USER_TRANSFORMATION)

	tags["transformation_id"] = destination.Transformations[0].ID
	tags["transformation_version_id"] = destination.Transformations[0].VersionID
	tags["error"] = "false"

	numEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_in_count", stats.CountType, tags)
	numOutputSuccessEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_out_count", stats.CountType, tags)

	errTags := misc.CopyStringMap(tags)
	errTags["error"] = "true"
	numOutputFailedEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_out_count", stats.CountType, errTags)
	transformTime := proc.statsFactory.NewTaggedStat("proc_transform_stage_duration", stats.TimerType, tags)

	return &DestStatT{
		numEvents:              numEvents,
		numOutputSuccessEvents: numOutputSuccessEvents,
		numOutputFailedEvents:  numOutputFailedEvents,
		transformTime:          transformTime,
	}
}

func (proc *HandleT) newDestinationTransformationStat(sourceID, workspaceID, transformAt string, destination backendconfig.DestinationT) *DestStatT {
	tags := buildStatTags(sourceID, workspaceID, destination, DEST_TRANSFORMATION)

	tags["transform_at"] = transformAt
	tags["error"] = "false"

	numEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_in_count", stats.CountType, tags)
	numOutputSuccessEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_out_count", stats.CountType, tags)

	errTags := misc.CopyStringMap(tags)
	errTags["error"] = "true"
	numOutputFailedEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_out_count", stats.CountType, errTags)
	destTransform := proc.statsFactory.NewTaggedStat("proc_transform_stage_duration", stats.TimerType, tags)

	return &DestStatT{
		numEvents:              numEvents,
		numOutputSuccessEvents: numOutputSuccessEvents,
		numOutputFailedEvents:  numOutputFailedEvents,
		transformTime:          destTransform,
	}
}

func (proc *HandleT) newEventFilterStat(sourceID, workspaceID string, destination backendconfig.DestinationT) *DestStatT {
	tags := buildStatTags(sourceID, workspaceID, destination, EVENT_FILTER)
	tags["error"] = "false"

	numEvents := proc.statsFactory.NewTaggedStat("proc_event_filter_in_count", stats.CountType, tags)
	numOutputSuccessEvents := proc.statsFactory.NewTaggedStat("proc_event_filter_out_count", stats.CountType, tags)

	errTags := misc.CopyStringMap(tags)
	errTags["error"] = "true"
	numOutputFailedEvents := proc.statsFactory.NewTaggedStat("proc_event_filter_out_count", stats.CountType, errTags)
	eventFilterTime := proc.statsFactory.NewTaggedStat("proc_event_filter_time", stats.TimerType, tags)

	return &DestStatT{
		numEvents:              numEvents,
		numOutputSuccessEvents: numOutputSuccessEvents,
		numOutputFailedEvents:  numOutputFailedEvents,
		transformTime:          eventFilterTime,
	}
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("processor")
}

// NewProcessor creates a new Processor instance
func NewProcessor() *HandleT {
	return &HandleT{
		transformer: transformer.NewTransformer(),
	}
}

func (proc *HandleT) Status() interface{} {
	proc.stats.transformEventsByTimeMutex.RLock()
	defer proc.stats.transformEventsByTimeMutex.RUnlock()
	statusRes := make(map[string][]interface{})
	for _, pqDestEvent := range proc.stats.destTransformEventsByTimeTaken {
		statusRes["dest-transformer"] = append(statusRes["dest-transformer"], *pqDestEvent)
	}
	for _, pqUserEvent := range proc.stats.userTransformEventsByTimeTaken {
		statusRes["user-transformer"] = append(statusRes["user-transformer"], *pqUserEvent)
	}

	if enableDedup {
		proc.dedupHandler.PrintHistogram()
	}

	return statusRes
}

//Setup initializes the module
func (proc *HandleT) Setup(
	backendConfig backendconfig.BackendConfig, gatewayDB jobsdb.JobsDB, routerDB jobsdb.JobsDB,
	batchRouterDB jobsdb.JobsDB, errorDB jobsdb.JobsDB, clearDB *bool, reporting types.ReportingI,
	multiTenantStat multitenant.MultiTenantI, transientSources transientsource.Service,
) {
	proc.reporting = reporting
	config.RegisterBoolConfigVariable(types.DEFAULT_REPORTING_ENABLED, &proc.reportingEnabled, false, "Reporting.enabled")
	config.RegisterInt64ConfigVariable(100*bytesize.MB, &proc.payloadLimit, true, 1, "Processor.payloadLimit")
	proc.logger = pkgLogger
	proc.backendConfig = backendConfig

	proc.readLoopSleep = readLoopSleep
	proc.maxLoopSleep = maxLoopSleep
	proc.storeTimeout = storeTimeout

	proc.multitenantI = multiTenantStat
	proc.gatewayDB = gatewayDB
	proc.routerDB = routerDB
	proc.batchRouterDB = batchRouterDB
	proc.errorDB = errorDB

	proc.transientSources = transientSources

	// Stats
	proc.statsFactory = stats.DefaultStats
	proc.stats.pStatsJobs = &misc.PerfStats{}
	proc.stats.pStatsDBR = &misc.PerfStats{}
	proc.stats.pStatsDBW = &misc.PerfStats{}
	proc.stats.userTransformEventsByTimeTaken = make([]*TransformRequestT, 0, transformTimesPQLength)
	proc.stats.destTransformEventsByTimeTaken = make([]*TransformRequestT, 0, transformTimesPQLength)
	proc.stats.pStatsJobs.Setup("ProcessorJobs")
	proc.stats.pStatsDBR.Setup("ProcessorDBRead")
	proc.stats.pStatsDBW.Setup("ProcessorDBWrite")
	proc.stats.statGatewayDBR = proc.statsFactory.NewStat("processor.gateway_db_read", stats.CountType)
	proc.stats.statGatewayDBW = proc.statsFactory.NewStat("processor.gateway_db_write", stats.CountType)
	proc.stats.statRouterDBW = proc.statsFactory.NewStat("processor.router_db_write", stats.CountType)
	proc.stats.statBatchRouterDBW = proc.statsFactory.NewStat("processor.batch_router_db_write", stats.CountType)
	proc.stats.statDBR = proc.statsFactory.NewStat("processor.gateway_db_read_time", stats.TimerType)
	proc.stats.statDBW = proc.statsFactory.NewStat("processor.gateway_db_write_time", stats.TimerType)
	proc.stats.statProcErrDBW = proc.statsFactory.NewStat("processor.proc_err_db_write", stats.CountType)
	proc.stats.statLoopTime = proc.statsFactory.NewStat("processor.loop_time", stats.TimerType)
	proc.stats.statMarkExecuting = proc.statsFactory.NewStat("processor.mark_executing", stats.TimerType)
	proc.stats.eventSchemasTime = proc.statsFactory.NewStat("processor.event_schemas_time", stats.TimerType)
	proc.stats.validateEventsTime = proc.statsFactory.NewStat("processor.validate_events_time", stats.TimerType)
	proc.stats.processJobsTime = proc.statsFactory.NewStat("processor.process_jobs_time", stats.TimerType)
	proc.stats.statSessionTransform = proc.statsFactory.NewStat("processor.session_transform_time", stats.TimerType)
	proc.stats.statUserTransform = proc.statsFactory.NewStat("processor.user_transform_time", stats.TimerType)
	proc.stats.statDestTransform = proc.statsFactory.NewStat("processor.dest_transform_time", stats.TimerType)
	proc.stats.marshalSingularEvents = proc.statsFactory.NewStat("processor.marshal_singular_events", stats.TimerType)
	proc.stats.destProcessing = proc.statsFactory.NewStat("processor.dest_processing", stats.TimerType)
	proc.stats.pipeProcessing = proc.statsFactory.NewStat("processor.pipe_processing", stats.TimerType)
	proc.stats.statNumRequests = proc.statsFactory.NewStat("processor.num_requests", stats.CountType)
	proc.stats.statNumEvents = proc.statsFactory.NewStat("processor.num_events", stats.CountType)

	proc.stats.statDBReadRequests = proc.statsFactory.NewStat("processor.db_read_requests", stats.HistogramType)
	proc.stats.statDBReadEvents = proc.statsFactory.NewStat("processor.db_read_events", stats.HistogramType)
	proc.stats.statDBReadPayloadBytes = proc.statsFactory.NewStat("processor.db_read_payload_bytes", stats.HistogramType)

	proc.stats.statDBReadOutOfOrder = proc.statsFactory.NewStat("processor.db_read_out_of_order", stats.CountType)
	proc.stats.statDBReadOutOfSequence = proc.statsFactory.NewStat("processor.db_read_out_of_sequence", stats.CountType)

	proc.stats.statDBWriteJobsTime = proc.statsFactory.NewStat("processor.db_write_jobs_time", stats.TimerType)
	proc.stats.statDBWriteStatusTime = proc.statsFactory.NewStat("processor.db_write_status_time", stats.TimerType)
	proc.stats.statDBWriteRouterPayloadBytes = proc.statsFactory.NewTaggedStat("processor.db_write_payload_bytes", stats.HistogramType, stats.Tags{
		"module": "router",
	})
	proc.stats.statDBWriteBatchPayloadBytes = proc.statsFactory.NewTaggedStat("processor.db_write_payload_bytes", stats.HistogramType, stats.Tags{
		"module": "batch_router",
	})
	proc.stats.statDBWriteRouterEvents = proc.statsFactory.NewTaggedStat("processor.db_write_events", stats.HistogramType, stats.Tags{
		"module": "router",
	})
	proc.stats.statDBWriteBatchEvents = proc.statsFactory.NewTaggedStat("processor.db_write_events", stats.HistogramType, stats.Tags{
		"module": "batch_router",
	})

	// Add a separate tag for batch router
	proc.stats.statDestNumOutputEvents = proc.statsFactory.NewTaggedStat("processor.num_output_events", stats.CountType, stats.Tags{
		"module": "router",
	})
	proc.stats.statBatchDestNumOutputEvents = proc.statsFactory.NewTaggedStat("processor.num_output_events", stats.CountType, stats.Tags{
		"module": "batch_router",
	})
	proc.stats.DBReadThroughput = proc.statsFactory.NewStat("processor.db_read_throughput", stats.CountType)
	proc.stats.processJobThroughput = proc.statsFactory.NewStat("processor.processJob_thoughput", stats.CountType)
	proc.stats.transformationsThroughput = proc.statsFactory.NewStat("processor.transformations_throughput", stats.CountType)
	proc.stats.DBWriteThroughput = proc.statsFactory.NewStat("processor.db_write_throughput", stats.CountType)

	admin.RegisterStatusHandler("processor", proc)
	if enableEventSchemasFeature {
		proc.eventSchemaHandler = event_schema.GetInstance()
	}
	if enableDedup {
		proc.dedupHandler = dedup.GetInstance(clearDB)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	proc.backgroundWait = g.Wait
	proc.backgroundCancel = cancel

	rruntime.Go(func() {
		proc.backendConfigSubscriber()
	})

	g.Go(misc.WithBugsnag(func() error {
		proc.syncTransformerFeatureJson(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnag(func() error {
		router.CleanFailedRecordsTableProcess(ctx)
		return nil
	}))

	proc.transformer.Setup()

	proc.crashRecover()
}

// Start starts this processor's main loops.
func (proc *HandleT) Start(ctx context.Context) {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(misc.WithBugsnag(func() error {
		if err := proc.backendConfig.WaitForConfig(ctx); err != nil {
			return err
		}
		if enablePipelining {
			proc.mainPipeline(ctx)
		} else {
			proc.mainLoop(ctx)
		}
		return nil
	}))

	g.Go(misc.WithBugsnag(func() error {
		st := stash.New()
		st.Setup(proc.errorDB, proc.transientSources)
		st.Start(ctx)
		return nil
	}))

	_ = g.Wait()
}

func (proc *HandleT) Shutdown() {
	proc.backgroundCancel()
	_ = proc.backgroundWait()
}

var (
	enablePipelining          bool
	pipelineBufferedItems     int
	subJobSize                int
	readLoopSleep             time.Duration
	maxLoopSleep              time.Duration
	storeTimeout              time.Duration
	loopSleep                 time.Duration // DEPRECATED: used only on the old mainLoop
	fixedLoopSleep            time.Duration // DEPRECATED: used only on the old mainLoop
	maxEventsToProcess        int
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
	enableEventCount          bool
	transformTimesPQLength    int
	captureEventNameStats     bool
	transformerURL            string
	pollInterval              time.Duration
	isUnLocked                bool
	GWCustomVal               string
)

func loadConfig() {
	config.RegisterBoolConfigVariable(true, &enablePipelining, false, "Processor.enablePipelining")
	config.RegisterIntConfigVariable(0, &pipelineBufferedItems, false, 1, "Processor.pipelineBufferedItems")
	config.RegisterIntConfigVariable(2000, &subJobSize, false, 1, "Processor.subJobSize")
	config.RegisterDurationConfigVariable(5000, &maxLoopSleep, true, time.Millisecond, []string{"Processor.maxLoopSleep", "Processor.maxLoopSleepInMS"}...)
	config.RegisterDurationConfigVariable(5, &storeTimeout, true, time.Minute, "Processor.storeTimeout")

	config.RegisterDurationConfigVariable(200, &readLoopSleep, true, time.Millisecond, "Processor.readLoopSleep")
	//DEPRECATED: used only on the old mainLoop:
	config.RegisterDurationConfigVariable(10, &loopSleep, true, time.Millisecond, []string{"Processor.loopSleep", "Processor.loopSleepInMS"}...)
	//DEPRECATED: used only on the old mainLoop:
	config.RegisterDurationConfigVariable(0, &fixedLoopSleep, true, time.Millisecond, []string{"Processor.fixedLoopSleep", "Processor.fixedLoopSleepInMS"}...)
	config.RegisterIntConfigVariable(100, &transformBatchSize, true, 1, "Processor.transformBatchSize")
	config.RegisterIntConfigVariable(200, &userTransformBatchSize, true, 1, "Processor.userTransformBatchSize")
	// Enable dedup of incoming events by default
	config.RegisterBoolConfigVariable(false, &enableDedup, false, "Dedup.enableDedup")
	config.RegisterBoolConfigVariable(true, &enableEventCount, true, "Processor.enableEventCount")
	// EventSchemas feature. false by default
	config.RegisterBoolConfigVariable(false, &enableEventSchemasFeature, false, "EventSchemas.enableEventSchemasFeature")
	config.RegisterBoolConfigVariable(false, &enableEventSchemasAPIOnly, true, "EventSchemas.enableEventSchemasAPIOnly")
	config.RegisterIntConfigVariable(10000, &maxEventsToProcess, true, 1, "Processor.maxLoopProcessEvents")

	batchDestinations, customDestinations = misc.LoadDestinations()
	config.RegisterIntConfigVariable(5, &transformTimesPQLength, false, 1, "Processor.transformTimesPQLength")
	// Capture event name as a tag in event level stats
	config.RegisterBoolConfigVariable(false, &captureEventNameStats, true, "Processor.Stats.captureEventName")
	transformerURL = config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090")
	config.RegisterDurationConfigVariable(5, &pollInterval, false, time.Second, []string{"Processor.pollInterval", "Processor.pollIntervalInS"}...)
	// GWCustomVal is used as a key in the jobsDB customval column
	config.RegisterStringConfigVariable("GW", &GWCustomVal, false, "Gateway.CustomVal")
}

// syncTransformerFeatureJson polls the transformer feature json endpoint,
//	updates the transformer feature map.
// It will set isUnLocked to true if it successfully fetches the transformer feature json at least once.
func (proc *HandleT) syncTransformerFeatureJson(ctx context.Context) {
	for {
		for i := 0; i < featuresRetryMaxAttempts; i++ {
			if ctx.Err() != nil {
				return
			}

			retry := proc.makeFeaturesFetchCall()
			if retry {
				select {
				case <-ctx.Done():
					return
				case <-time.After(200 * time.Millisecond):
					continue
				}
			}
		}

		if proc.transformerFeatures != nil && !isUnLocked {
			isUnLocked = true
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

func (proc *HandleT) makeFeaturesFetchCall() bool {
	url := transformerURL + "/features"
	req, err := http.NewRequest("GET", url, bytes.NewReader([]byte{}))
	if err != nil {
		proc.logger.Error("error creating request - %s", err)
		return true
	}
	tr := &http.Transport{}
	client := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)}
	res, err := client.Do(req)
	if err != nil {
		proc.logger.Error("error sending request - %s", err)
		return true
	}

	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return true
	}

	if res.StatusCode == 200 {
		proc.transformerFeatures = body
	} else if res.StatusCode == 404 {
		proc.transformerFeatures = json.RawMessage(defaultTransformerFeatures)
	}

	return false
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
	ch := make(chan pubsub.DataEvent)
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
		// calculate new timestamp using the formula
		// timestamp = receivedAt - (sentAt - originalTimestamp)
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
	commonMetadata.SourceID = gjson.GetBytes(batchEvent.Parameters, "source_id").Str
	commonMetadata.WorkspaceID = source.WorkspaceID
	commonMetadata.Namespace = config.GetKubeNamespace()
	commonMetadata.InstanceID = config.GetInstanceID()
	commonMetadata.RudderID = batchEvent.UserID
	commonMetadata.JobID = batchEvent.JobID
	commonMetadata.MessageID = misc.GetStringifiedData(singularEvent["messageId"])
	commonMetadata.ReceivedAt = receivedAt.Format(misc.RFC3339Milli)
	commonMetadata.SourceType = source.SourceDefinition.Name
	commonMetadata.SourceCategory = source.SourceDefinition.Category

	commonMetadata.SourceBatchID, _ = misc.MapLookup(singularEvent, "context", "sources", "batch_id").(string)
	commonMetadata.SourceTaskID, _ = misc.MapLookup(singularEvent, "context", "sources", "task_id").(string)
	commonMetadata.SourceJobRunID, _ = misc.MapLookup(singularEvent, "context", "sources", "job_run_id").(string)
	commonMetadata.SourceJobID, _ = misc.MapLookup(singularEvent, "context", "sources", "job_id").(string)
	commonMetadata.SourceTaskRunID, _ = misc.MapLookup(singularEvent, "context", "sources", "task_run_id").(string)
	commonMetadata.RecordID = misc.MapLookup(singularEvent, "context", "record_id")

	commonMetadata.EventName, _ = misc.MapLookup(singularEvent, "event").(string)
	commonMetadata.EventType, _ = misc.MapLookup(singularEvent, "type").(string)
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
	metadata.RecordID = commonMetadata.RecordID
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
			err := jsonfast.Unmarshal(job.Parameters, &params)
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
			err = jsonfast.Unmarshal(job.EventPayload, &events)
			if err != nil {
				pkgLogger.Errorf("Error while UnMarshaling live event payload: %w", err)
				continue
			}
			for i := range events {
				event := &events[i]
				eventPayload, err := jsonfast.Marshal(*event)
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

func (proc *HandleT) getDestTransformerEvents(response transformer.ResponseT, commonMetaData transformer.MetadataT, destination backendconfig.DestinationT, stage string, trackingPlanEnabled, userTransformationEnabled bool) ([]transformer.TransformerEventT, []*types.PUReportedMetric, map[string]int64, map[string]MetricMetadata) {
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
		eventMetadata.RecordID = userTransformedEvent.Metadata.RecordID
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
				inPU = types.DESTINATION_FILTER
			}
			pu = types.USER_TRANSFORMER
		} else if stage == transformer.TrackingPlanValidationStage {
			inPU = types.DESTINATION_FILTER
			pu = types.TRACKINGPLAN_VALIDATOR
		} else if stage == transformer.EventFilterStage {
			if userTransformationEnabled {
				inPU = types.USER_TRANSFORMER
			} else {
				if trackingPlanEnabled {
					inPU = types.TRACKINGPLAN_VALIDATOR
				} else {
					inPU = types.DESTINATION_FILTER
				}
			}
			pu = types.EVENT_FILTER
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
		var eventName string
		var eventType string
		eventName = event.Metadata.EventName
		eventType = event.Metadata.EventType

		countKey := strings.Join([]string{
			event.Metadata.SourceID,
			event.Metadata.DestinationID,
			event.Metadata.SourceBatchID,
			eventName,
			eventType,
		}, METRICKEYDELIMITER)

		if _, ok := countMap[countKey]; !ok {
			countMap[countKey] = 0
		}
		countMap[countKey] = countMap[countKey] + 1

		if countMetadataMap != nil {
			if _, ok := countMetadataMap[countKey]; !ok {
				countMetadataMap[countKey] = MetricMetadata{
					sourceID:                event.Metadata.SourceID,
					destinationID:           event.Metadata.DestinationID,
					sourceBatchID:           event.Metadata.SourceBatchID,
					sourceTaskID:            event.Metadata.SourceTaskID,
					sourceTaskRunID:         event.Metadata.SourceTaskRunID,
					sourceJobID:             event.Metadata.SourceJobID,
					sourceJobRunID:          event.Metadata.SourceJobRunID,
					sourceDefinitionID:      event.Metadata.SourceDefinitionID,
					destinationDefinitionID: event.Metadata.DestinationDefinitionID,
					sourceCategory:          event.Metadata.SourceCategory,
				}
			}
		}

		key := fmt.Sprintf("%s:%s:%s:%s:%d:%s:%s",
			event.Metadata.SourceID,
			event.Metadata.DestinationID,
			event.Metadata.SourceBatchID,
			status, event.StatusCode,
			eventName, eventType,
		)

		_, ok := connectionDetailsMap[key]
		if !ok {
			cd := types.CreateConnectionDetail(
				event.Metadata.SourceID,
				event.Metadata.DestinationID,
				event.Metadata.SourceBatchID,
				event.Metadata.SourceTaskID,
				event.Metadata.SourceTaskRunID,
				event.Metadata.SourceJobID,
				event.Metadata.SourceJobRunID,
				event.Metadata.SourceDefinitionID,
				event.Metadata.DestinationDefinitionID,
				event.Metadata.SourceCategory,
			)
			connectionDetailsMap[key] = cd
		}
		sd, ok := statusDetailsMap[key]
		if !ok {
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
		payload, err := jsonfast.Marshal(messages)
		if err != nil {
			proc.logger.Errorf(`[Processor: getFailedEventJobs] Failed to unmarshal list of failed events: %v`, err)
			continue
		}

		for _, message := range messages {
			sampleEvent, err := jsonfast.Marshal(message)
			if err != nil {
				proc.logger.Errorf(`[Processor: getFailedEventJobs] Failed to unmarshal first element in failed events: %v`, err)
			}
			if err != nil || proc.transientSources.Apply(commonMetaData.SourceID) {
				sampleEvent = []byte(`{}`)
			}
			proc.updateMetricMaps(nil, failedCountMap, connectionDetailsMap, statusDetailsMap, failedEvent, jobsdb.Aborted.State, sampleEvent)
		}

		id := misc.FastUUID()

		params := map[string]interface{}{
			"source_id":          commonMetaData.SourceID,
			"destination_id":     commonMetaData.DestinationID,
			"source_job_run_id":  failedEvent.Metadata.SourceJobRunID,
			"error":              failedEvent.Error,
			"status_code":        failedEvent.StatusCode,
			"stage":              stage,
			"record_id":          failedEvent.Metadata.RecordID,
			"source_task_run_id": failedEvent.Metadata.SourceTaskRunID,
		}
		eventContext, castOk := failedEvent.Output["context"].(map[string]interface{})
		if castOk {
			params["violationErrors"] = eventContext["violationErrors"]
		}
		marshalledParams, err := jsonfast.Marshal(params)
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
			WorkspaceId:  failedEvent.Metadata.WorkspaceID,
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
		if stage == transformer.EventFilterStage {
			if transformationEnabled {
				inPU = types.USER_TRANSFORMER
			} else {
				if trackingPlanEnabled {
					inPU = types.TRACKINGPLAN_VALIDATOR
				} else {
					inPU = types.DESTINATION_FILTER
				}
			}
			pu = types.EVENT_FILTER
		} else if stage == transformer.DestTransformerStage {
			inPU = types.EVENT_FILTER
			pu = types.DEST_TRANSFORMER
		} else if stage == transformer.UserTransformerStage {
			if trackingPlanEnabled {
				inPU = types.TRACKINGPLAN_VALIDATOR
			} else {
				inPU = types.DESTINATION_FILTER
			}
			pu = types.USER_TRANSFORMER
		} else if stage == transformer.TrackingPlanValidationStage {
			inPU = types.DESTINATION_FILTER
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
		statEventType := proc.statsFactory.NewSampledTaggedStat("processor.event_type", stats.CountType, tags)
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
			tagsDetailed := map[string]string{
				"writeKey":   writeKey,
				"event_type": eventType,
				"event_name": eventName,
			}
			statEventTypeDetailed := proc.statsFactory.NewSampledTaggedStat("processor.event_type_detailed", stats.CountType, tagsDetailed)
			statEventTypeDetailed.Count(1)
		}
	}
}

func getDiffMetrics(inPU, pu string, inCountMetadataMap map[string]MetricMetadata, inCountMap, successCountMap, failedCountMap map[string]int64) []*types.PUReportedMetric {
	//Calculate diff and append to reportMetrics
	//diff = successCount + abortCount - inCount
	diffMetrics := make([]*types.PUReportedMetric, 0)
	for key, inCount := range inCountMap {
		var eventName, eventType string
		splitKey := strings.Split(key, METRICKEYDELIMITER)
		if len(splitKey) < 5 {
			eventName = ""
			eventType = ""
		} else {
			eventName = splitKey[3]
			eventType = splitKey[4]
		}
		successCount := successCountMap[key]
		failedCount := failedCountMap[key]
		diff := successCount + failedCount - inCount
		if diff != 0 {
			metricMetadata := inCountMetadataMap[key]
			metric := &types.PUReportedMetric{
				ConnectionDetails: *types.CreateConnectionDetail(metricMetadata.sourceID, metricMetadata.destinationID, metricMetadata.sourceBatchID, metricMetadata.sourceTaskID, metricMetadata.sourceTaskRunID, metricMetadata.sourceJobID, metricMetadata.sourceJobRunID, metricMetadata.sourceDefinitionID, metricMetadata.destinationDefinitionID, metricMetadata.sourceCategory),
				PUDetails:         *types.CreatePUDetails(inPU, pu, false, false),
				StatusDetail:      types.CreateStatusDetail(types.DiffStatus, diff, 0, "", []byte(`{}`), eventName, eventType),
			}
			diffMetrics = append(diffMetrics, metric)
		}
	}

	return diffMetrics
}

func (proc *HandleT) processJobsForDest(subJobs subJob, parsedEventList [][]types.SingularEventT) transformationMessage {
	jobList := subJobs.subJobs
	start := time.Now()

	proc.stats.statNumRequests.Count(len(jobList))

	var statusList []*jobsdb.JobStatusT
	var groupedEvents = make(map[string][]transformer.TransformerEventT)
	var groupedEventsByWriteKey = make(map[WriteKeyT][]transformer.TransformerEventT)
	var eventsByMessageID = make(map[string]types.SingularEventWithReceivedAt)
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

	marshalStart := time.Now()
	uniqueMessageIds := make(map[string]struct{})
	uniqueMessageIdsBySrcDestKey := make(map[string]map[string]struct{})
	var sourceDupStats = make(map[string]int)

	reportMetrics := make([]*types.PUReportedMetric, 0)
	inCountMap := make(map[string]int64)
	inCountMetadataMap := make(map[string]MetricMetadata)
	connectionDetailsMap := make(map[string]*types.ConnectionDetails)
	statusDetailsMap := make(map[string]*types.StatusDetail)

	outCountMap := make(map[string]int64) // destinations enabled
	destFilterStatusDetailMap := make(map[string]*types.StatusDetail)

	for idx, batchEvent := range jobList {

		var singularEvents []types.SingularEventT
		var ok bool
		if parsedEventList == nil {
			singularEvents, ok = misc.ParseRudderEventBatch(batchEvent.EventPayload)
		} else {
			singularEvents = parsedEventList[idx]
			ok = singularEvents != nil
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
				if enableDedup && misc.ContainsInt(duplicateIndexes, eventIndex) {
					proc.logger.Debugf("Dropping event with duplicate messageId: %s", messageId)
					misc.IncrementMapByKey(sourceDupStats, writeKey, 1)
					continue
				}

				proc.updateSourceEventStatsDetailed(singularEvent, writeKey)

				uniqueMessageIds[messageId] = struct{}{}
				//We count this as one, not destination specific ones
				totalEvents++
				eventsByMessageID[messageId] = types.SingularEventWithReceivedAt{
					SingularEvent: singularEvent,
					ReceivedAt:    receivedAt,
				}

				sourceForSingularEvent, sourceIdError := getSourceByWriteKey(writeKey)
				if sourceIdError != nil {
					proc.logger.Error("Dropping Job since Source not found for writeKey : ", writeKey)
					continue
				}

				commonMetadataFromSingularEvent := makeCommonMetadataFromSingularEvent(
					singularEvent,
					batchEvent,
					receivedAt,
					sourceForSingularEvent,
				)

				//REPORTING - GATEWAY metrics - START
				// dummy event for metrics purposes only
				event := transformer.TransformerResponseT{}
				if proc.isReportingEnabled() {
					event.Metadata = *commonMetadataFromSingularEvent
					proc.updateMetricMaps(inCountMetadataMap, inCountMap, connectionDetailsMap, statusDetailsMap, event, jobsdb.Succeeded.State, []byte(`{}`))
				}
				//REPORTING - GATEWAY metrics - END

				//Getting all the destinations which are enabled for this
				//event
				backendEnabledDestTypes := getBackendEnabledDestinationTypes(writeKey)
				enabledDestTypes := integrations.FilterClientIntegrations(singularEvent, backendEnabledDestTypes)
				if len(enabledDestTypes) == 0 {
					proc.logger.Debug("No enabled destinations")
					continue
				}

				_, ok = groupedEventsByWriteKey[WriteKeyT(writeKey)]
				if !ok {
					groupedEventsByWriteKey[WriteKeyT(writeKey)] = make([]transformer.TransformerEventT, 0)
				}
				shallowEventCopy := transformer.TransformerEventT{}
				shallowEventCopy.Message = singularEvent
				shallowEventCopy.Message["request_ip"] = requestIP
				enhanceWithTimeFields(&shallowEventCopy, singularEvent, receivedAt)
				enhanceWithMetadata(commonMetadataFromSingularEvent, &shallowEventCopy, backendconfig.DestinationT{})

				source, sourceError := getSourceByWriteKey(writeKey)
				if sourceError != nil {
					proc.logger.Error("Source not found for writeKey : ", writeKey)
				} else {
					// TODO: TP ID preference 1.event.context set by rudderTyper   2.From WorkSpaceConfig (currently being used)
					shallowEventCopy.Metadata.TrackingPlanId = source.DgSourceTrackingPlanConfig.TrackingPlan.Id
					shallowEventCopy.Metadata.TrackingPlanVersion = source.DgSourceTrackingPlanConfig.TrackingPlan.Version
					shallowEventCopy.Metadata.SourceTpConfig = source.DgSourceTrackingPlanConfig.Config
					shallowEventCopy.Metadata.MergedTpConfig = source.DgSourceTrackingPlanConfig.GetMergedConfig(commonMetadataFromSingularEvent.EventType)
				}

				groupedEventsByWriteKey[WriteKeyT(writeKey)] = append(groupedEventsByWriteKey[WriteKeyT(writeKey)], shallowEventCopy)

				if proc.isReportingEnabled() {
					proc.updateMetricMaps(inCountMetadataMap, outCountMap, connectionDetailsMap, destFilterStatusDetailMap, event, jobsdb.Succeeded.State, []byte(`{}`))
				}
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
			WorkspaceId:   batchEvent.WorkspaceId,
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

			if _, ok := destFilterStatusDetailMap[k]; ok {
				destFilterMetric := &types.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *types.CreatePUDetails(types.GATEWAY, types.DESTINATION_FILTER, false, false),
					StatusDetail:      destFilterStatusDetailMap[k],
				}
				reportMetrics = append(reportMetrics, destFilterMetric)
			}
		}
		// empty failedCountMap because no failures,
		// events are just dropped at this point if no destination is found to route the events
		diffMetrics := getDiffMetrics(
			types.GATEWAY,
			types.DESTINATION_FILTER,
			inCountMetadataMap,
			inCountMap,
			outCountMap,
			map[string]int64{},
		)
		reportMetrics = append(reportMetrics, diffMetrics...)
	}
	//REPORTING - GATEWAY metrics - END

	proc.stats.statNumEvents.Count(totalEvents)

	marshalTime := time.Since(marshalStart)
	defer proc.stats.marshalSingularEvents.SendTiming(marshalTime)

	//TRACKING PLAN - START
	//Placing the trackingPlan validation filters here.
	//Else further down events are duplicated by destId, so multiple validation takes places for same event
	validateEventsStart := time.Now()
	validatedEventsByWriteKey, validatedReportMetrics, validatedErrorJobs, trackingPlanEnabledMap := proc.validateEvents(groupedEventsByWriteKey, eventsByMessageID)
	validateEventsTime := time.Since(validateEventsStart)
	defer proc.stats.validateEventsTime.SendTiming(validateEventsTime)

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

	if len(statusList) != len(jobList) {
		panic(fmt.Errorf("len(statusList):%d != len(jobList):%d", len(statusList), len(jobList)))
	}
	processTime := time.Since(start)
	proc.stats.processJobsTime.SendTiming(processTime)
	processJobThroughput := throughputPerSecond(totalEvents, processTime)
	//processJob throughput per second.
	proc.stats.processJobThroughput.Count(processJobThroughput)
	return transformationMessage{
		groupedEvents,
		trackingPlanEnabledMap,
		eventsByMessageID,
		uniqueMessageIdsBySrcDestKey,
		reportMetrics,
		statusList,
		procErrorJobs,
		sourceDupStats,
		uniqueMessageIds,

		totalEvents,
		start,

		subJobs.hasMore,
	}
}

type transformationMessage struct {
	groupedEvents map[string][]transformer.TransformerEventT

	trackingPlanEnabledMap       map[SourceIDT]bool
	eventsByMessageID            map[string]types.SingularEventWithReceivedAt
	uniqueMessageIdsBySrcDestKey map[string]map[string]struct{}
	reportMetrics                []*types.PUReportedMetric
	statusList                   []*jobsdb.JobStatusT
	procErrorJobs                []*jobsdb.JobT
	sourceDupStats               map[string]int
	uniqueMessageIds             map[string]struct{}

	totalEvents int
	start       time.Time

	hasMore bool
}

func (proc *HandleT) transformations(in transformationMessage) storeMessage {
	//Now do the actual transformation. We call it in batches, once
	//for each destination ID

	ctx, task := trace.NewTask(context.Background(), "transformations")
	defer task.End()

	var procErrorJobsByDestID = make(map[string][]*jobsdb.JobT)
	var batchDestJobs []*jobsdb.JobT
	var destJobs []*jobsdb.JobT

	destProcStart := time.Now()

	chOut := make(chan transformSrcDestOutput, 1)
	wg := sync.WaitGroup{}
	wg.Add(len(in.groupedEvents))

	for srcAndDestKey, eventList := range in.groupedEvents {
		srcAndDestKey, eventList := srcAndDestKey, eventList
		go func() {
			defer wg.Done()
			chOut <- proc.transformSrcDest(
				ctx,

				srcAndDestKey, eventList,

				in.trackingPlanEnabledMap,
				in.eventsByMessageID,
				in.uniqueMessageIdsBySrcDestKey,
			)
		}()
	}
	go func() {
		wg.Wait()
		close(chOut)
	}()

	for o := range chOut {
		destJobs = append(destJobs, o.destJobs...)
		batchDestJobs = append(batchDestJobs, o.batchDestJobs...)

		in.reportMetrics = append(in.reportMetrics, o.reportMetrics...)
		for k, v := range o.errorsPerDestID {
			procErrorJobsByDestID[k] = append(procErrorJobsByDestID[k], v...)
		}
	}

	destProcTime := time.Since(destProcStart)
	defer proc.stats.destProcessing.SendTiming(destProcTime)

	//this tells us how many transformations we are doing per second.
	transformationsThroughput := throughputPerSecond(in.totalEvents, destProcTime)
	proc.stats.transformationsThroughput.Count(transformationsThroughput)
	return storeMessage{
		in.statusList,
		destJobs,
		batchDestJobs,

		procErrorJobsByDestID,
		in.procErrorJobs,

		in.reportMetrics,
		in.sourceDupStats,
		in.uniqueMessageIds,
		in.totalEvents,
		in.start,
		in.hasMore,
	}
}

type storeMessage struct {
	statusList    []*jobsdb.JobStatusT
	destJobs      []*jobsdb.JobT
	batchDestJobs []*jobsdb.JobT

	procErrorJobsByDestID map[string][]*jobsdb.JobT
	procErrorJobs         []*jobsdb.JobT

	reportMetrics    []*types.PUReportedMetric
	sourceDupStats   map[string]int
	uniqueMessageIds map[string]struct{}

	totalEvents int
	start       time.Time

	hasMore bool
}

func (proc *HandleT) Store(in storeMessage) {
	// FIXME: This is a hack to get around the fact that,
	// 	processor will stuck in case write query takes for ever.
	// SHOULD BE REMOVED AFTER PROPER TIMEOUTS ARE IMPLEMENTED.
	ctx, cancel := context.WithTimeout(context.TODO(), proc.storeTimeout)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			panic(fmt.Sprintf("processor .Store() timed out after %s", proc.storeTimeout))
		}
	}()

	statusList, destJobs, batchDestJobs := in.statusList, in.destJobs, in.batchDestJobs
	processorLoopStats := make(map[string]map[string]map[string]int)
	beforeStoreStatus := time.Now()
	//XX: Need to do this in a transaction
	if len(batchDestJobs) > 0 {
		proc.logger.Debug("[Processor] Total jobs written to batch router : ", len(batchDestJobs))
		err := proc.batchRouterDB.Store(batchDestJobs)
		if err != nil {
			proc.logger.Errorf("Store into batch router table failed with error: %v", err)
			proc.logger.Errorf("batchDestJobs: %v", batchDestJobs)
			panic(err)
		}
		totalPayloadBatchBytes := 0
		processorLoopStats["batch_router"] = make(map[string]map[string]int)
		for i := range batchDestJobs {
			_, ok := processorLoopStats["batch_router"][batchDestJobs[i].WorkspaceId]
			if !ok {
				processorLoopStats["batch_router"][batchDestJobs[i].WorkspaceId] = make(map[string]int)
			}
			processorLoopStats["batch_router"][batchDestJobs[i].WorkspaceId][batchDestJobs[i].CustomVal] += 1
			totalPayloadBatchBytes += len(batchDestJobs[i].EventPayload)
		}
		proc.multitenantI.ReportProcLoopAddStats(processorLoopStats["batch_router"], "batch_rt")

		proc.stats.statBatchDestNumOutputEvents.Count(len(batchDestJobs))
		proc.stats.statDBWriteBatchEvents.Observe(float64(len(batchDestJobs)))
		proc.stats.statDBWriteBatchPayloadBytes.Observe(float64(totalPayloadBatchBytes))
	}

	if len(destJobs) > 0 {
		proc.logger.Debug("[Processor] Total jobs written to router : ", len(destJobs))

		err := proc.routerDB.Store(destJobs)
		if err != nil {
			proc.logger.Errorf("Store into router table failed with error: %v", err)
			proc.logger.Errorf("destJobs: %v", destJobs)
			panic(err)
		}
		totalPayloadRouterBytes := 0
		processorLoopStats["router"] = make(map[string]map[string]int)
		for i := range destJobs {
			_, ok := processorLoopStats["router"][destJobs[i].WorkspaceId]
			if !ok {
				processorLoopStats["router"][destJobs[i].WorkspaceId] = make(map[string]int)
			}
			processorLoopStats["router"][destJobs[i].WorkspaceId][destJobs[i].CustomVal] += 1
			totalPayloadRouterBytes += len(destJobs[i].EventPayload)
		}
		proc.multitenantI.ReportProcLoopAddStats(processorLoopStats["router"], "rt")

		proc.stats.statDestNumOutputEvents.Count(len(destJobs))
		proc.stats.statDBWriteRouterEvents.Observe(float64(len(destJobs)))
		proc.stats.statDBWriteRouterPayloadBytes.Observe(float64(totalPayloadRouterBytes))
	}

	for _, jobs := range in.procErrorJobsByDestID {
		in.procErrorJobs = append(in.procErrorJobs, jobs...)
	}
	if len(in.procErrorJobs) > 0 {
		proc.logger.Debug("[Processor] Total jobs written to proc_error: ", len(in.procErrorJobs))
		err := proc.errorDB.Store(in.procErrorJobs)
		if err != nil {
			proc.logger.Errorf("Store into proc error table failed with error: %v", err)
			proc.logger.Errorf("procErrorJobs: %v", in.procErrorJobs)
			panic(err)
		}
		recordEventDeliveryStatus(in.procErrorJobsByDestID)
	}
	writeJobsTime := time.Since(beforeStoreStatus)

	txnStart := time.Now()
	txn := proc.gatewayDB.BeginGlobalTransaction()
	proc.gatewayDB.AcquireUpdateJobStatusLocks()
	err := proc.gatewayDB.UpdateJobStatusInTxn(txn, statusList, []string{GWCustomVal}, nil)
	if err != nil {
		pkgLogger.Errorf("Error occurred while updating gateway jobs statuses. Panicking. Err: %v", err)
		panic(err)
	}
	if proc.isReportingEnabled() {
		proc.reporting.Report(in.reportMetrics, txn)
	}

	if enableDedup {
		proc.updateSourceStats(in.sourceDupStats, "processor.write_key_duplicate_events")
		if len(in.uniqueMessageIds) > 0 {
			var dedupedMessageIdsAcrossJobs []string
			for k := range in.uniqueMessageIds {
				dedupedMessageIdsAcrossJobs = append(dedupedMessageIdsAcrossJobs, k)
			}
			proc.dedupHandler.MarkProcessed(dedupedMessageIdsAcrossJobs)
		}
	}

	proc.gatewayDB.CommitTransaction(txn)
	proc.gatewayDB.ReleaseUpdateJobStatusLocks()
	proc.stats.statDBW.Since(beforeStoreStatus)
	dbWriteTime := time.Since(beforeStoreStatus)
	//DB write throughput per second.
	dbWriteThroughput := throughputPerSecond(len(destJobs), dbWriteTime)
	proc.stats.DBWriteThroughput.Count(dbWriteThroughput)
	proc.stats.statDBWriteJobsTime.SendTiming(writeJobsTime)
	proc.stats.statDBWriteStatusTime.Since(txnStart)
	proc.logger.Debugf("Processor GW DB Write Complete. Total Processed: %v", len(statusList))
	//XX: End of transaction

	proc.stats.pStatsDBW.Rate(len(statusList), time.Since(beforeStoreStatus))
	proc.stats.pStatsJobs.Rate(in.totalEvents, time.Since(in.start))

	proc.stats.statGatewayDBW.Count(len(statusList))
	proc.stats.statRouterDBW.Count(len(destJobs))
	proc.stats.statBatchRouterDBW.Count(len(batchDestJobs))
	proc.stats.statProcErrDBW.Count(len(in.procErrorJobs))
}

type transformSrcDestOutput struct {
	reportMetrics   []*types.PUReportedMetric
	destJobs        []*jobsdb.JobT
	batchDestJobs   []*jobsdb.JobT
	errorsPerDestID map[string][]*jobsdb.JobT
}

func (proc *HandleT) transformSrcDest(
	ctx context.Context,
	// main inputs
	srcAndDestKey string, eventList []transformer.TransformerEventT,

	// helpers
	trackingPlanEnabledMap map[SourceIDT]bool,
	eventsByMessageID map[string]types.SingularEventWithReceivedAt,
	uniqueMessageIdsBySrcDestKey map[string]map[string]struct{},
) transformSrcDestOutput {
	defer proc.stats.pipeProcessing.Since(time.Now())

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

	reportMetrics := make([]*types.PUReportedMetric, 0)
	batchDestJobs := make([]*jobsdb.JobT, 0)
	destJobs := make([]*jobsdb.JobT, 0)
	procErrorJobsByDestID := make(map[string][]*jobsdb.JobT)

	configSubscriberLock.RLock()
	destType := destinationIDtoTypeMap[destID]
	transformationEnabled := len(destination.Transformations) > 0
	configSubscriberLock.RUnlock()

	trackingPlanEnabled := trackingPlanEnabledMap[SourceIDT(sourceID)]

	var inCountMap map[string]int64
	var inCountMetadataMap map[string]MetricMetadata

	//REPORTING - START
	if proc.isReportingEnabled() {
		//Grouping events by sourceid + destinationid + sourcebatchid + eventName + eventType to find the count
		inCountMap = make(map[string]int64)
		inCountMetadataMap = make(map[string]MetricMetadata)
		for i := range eventList {
			event := &eventList[i]
			key := strings.Join([]string{
				event.Metadata.SourceID,
				event.Metadata.DestinationID,
				event.Metadata.SourceBatchID,
				event.Metadata.EventName,
				event.Metadata.EventType,
			}, METRICKEYDELIMITER)
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

		trace.WithRegion(ctx, "UserTransform", func() {
			startedAt := time.Now()
			response = proc.transformer.Transform(ctx, eventList, integrations.GetUserTransformURL(), userTransformBatchSize)
			d := time.Since(startedAt)
			userTransformationStat.transformTime.SendTiming(d)
			proc.addToTransformEventByTimePQ(&TransformRequestT{
				Event:          eventList,
				Stage:          transformer.UserTransformerStage,
				ProcessingTime: d.Seconds(),
				Index:          -1,
			}, &proc.stats.userTransformEventsByTimeTaken)

			var successMetrics []*types.PUReportedMetric
			var successCountMap map[string]int64
			var successCountMetadataMap map[string]MetricMetadata
			eventsToTransform, successMetrics, successCountMap, successCountMetadataMap = proc.getDestTransformerEvents(response, commonMetaData, destination, transformer.UserTransformerStage, trackingPlanEnabled, transformationEnabled)
			failedJobs, failedMetrics, failedCountMap := proc.getFailedEventJobs(response, commonMetaData, eventsByMessageID, transformer.UserTransformerStage, transformationEnabled, trackingPlanEnabled)
			proc.saveFailedJobs(failedJobs)
			if _, ok := procErrorJobsByDestID[destID]; !ok {
				procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
			}
			procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], failedJobs...)
			userTransformationStat.numOutputSuccessEvents.Count(len(eventsToTransform))
			userTransformationStat.numOutputFailedEvents.Count(len(failedJobs))
			proc.logger.Debug("Custom Transform output size", len(eventsToTransform))
			trace.Logf(ctx, "UserTransform", "User Transform output size: %d", len(eventsToTransform))

			transformationdebugger.UploadTransformationStatus(&transformationdebugger.TransformationStatusT{SourceID: sourceID, DestID: destID, Destination: &destination, UserTransformedEvents: eventsToTransform, EventsByMessageID: eventsByMessageID, FailedEvents: response.FailedEvents, UniqueMessageIds: uniqueMessageIdsBySrcDestKey[srcAndDestKey]})

			//REPORTING - START
			if proc.isReportingEnabled() {
				diffMetrics := getDiffMetrics(
					types.DESTINATION_FILTER,
					types.USER_TRANSFORMER,
					inCountMetadataMap,
					inCountMap,
					successCountMap,
					failedCountMap,
				)
				reportMetrics = append(reportMetrics, successMetrics...)
				reportMetrics = append(reportMetrics, failedMetrics...)
				reportMetrics = append(reportMetrics, diffMetrics...)

				//successCountMap will be inCountMap for filtering events based on supported event types
				inCountMap = successCountMap
				inCountMetadataMap = successCountMetadataMap
			}
			//REPORTING - END
		})
	} else {
		proc.logger.Debug("No custom transformation")
		eventsToTransform = eventList
	}

	if len(eventsToTransform) == 0 {
		return transformSrcDestOutput{
			destJobs:        destJobs,
			batchDestJobs:   batchDestJobs,
			errorsPerDestID: procErrorJobsByDestID,
			reportMetrics:   reportMetrics,
		}
	}

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

	//Filtering events based on the supported message types - START
	s := time.Now()
	proc.logger.Debug("Supported messages filtering input size", len(eventsToTransform))
	response = ConvertToFilteredTransformerResponse(eventsToTransform, transformAt != "none")
	var successMetrics []*types.PUReportedMetric
	var successCountMap map[string]int64
	var successCountMetadataMap map[string]MetricMetadata
	eventsToTransform, successMetrics, successCountMap, successCountMetadataMap = proc.getDestTransformerEvents(response, commonMetaData, destination, transformer.EventFilterStage, trackingPlanEnabled, transformationEnabled)
	failedJobs, failedMetrics, failedCountMap := proc.getFailedEventJobs(response, commonMetaData, eventsByMessageID, transformer.EventFilterStage, transformationEnabled, trackingPlanEnabled)
	proc.saveFailedJobs(failedJobs)
	proc.logger.Debug("Supported messages filtering output size", len(eventsToTransform))

	//REPORTING - START
	if proc.isReportingEnabled() {
		var inPU string
		if transformationEnabled {
			inPU = types.USER_TRANSFORMER
		} else {
			if trackingPlanEnabled {
				inPU = types.TRACKINGPLAN_VALIDATOR
			} else {
				inPU = types.DESTINATION_FILTER
			}
		}

		diffMetrics := getDiffMetrics(inPU, types.EVENT_FILTER, inCountMetadataMap, inCountMap, successCountMap, failedCountMap)
		reportMetrics = append(reportMetrics, successMetrics...)
		reportMetrics = append(reportMetrics, failedMetrics...)
		reportMetrics = append(reportMetrics, diffMetrics...)

		//successCountMap will be inCountMap for destination transform
		inCountMap = successCountMap
		inCountMetadataMap = successCountMetadataMap
	}
	//REPORTING - END
	eventFilterStat := proc.newEventFilterStat(sourceID, workspaceID, destination)
	eventFilterStat.numEvents.Count(len(eventsToTransform))
	eventFilterStat.numOutputSuccessEvents.Count(len(response.Events))
	eventFilterStat.numOutputFailedEvents.Count(len(failedJobs))
	eventFilterStat.transformTime.Since(s)

	//Filtering events based on the supported message types - END

	if len(eventsToTransform) == 0 {
		return transformSrcDestOutput{
			destJobs:        destJobs,
			batchDestJobs:   batchDestJobs,
			errorsPerDestID: procErrorJobsByDestID,
			reportMetrics:   reportMetrics,
		}
	}

	//Destination transformation - START
	//Send to transformer only if is
	// a. transformAt is processor
	// OR
	// b. transformAt is router and transformer doesn't support router transform
	if transformAt == "processor" || (transformAt == "router" && transformAtFromFeaturesFile == "") {
		trace.WithRegion(ctx, "Dest Transform", func() {
			trace.Logf(ctx, "Dest Transform", "input size %d", len(eventsToTransform))
			proc.logger.Debug("Dest Transform input size", len(eventsToTransform))
			s := time.Now()
			response = proc.transformer.Transform(ctx, eventsToTransform, url, transformBatchSize)

			destTransformationStat := proc.newDestinationTransformationStat(sourceID, workspaceID, transformAt, destination)
			destTransformationStat.transformTime.Since(s)
			transformAt = "processor"

			timeTaken := time.Since(s).Seconds()
			proc.addToTransformEventByTimePQ(
				&TransformRequestT{
					Event:          eventsToTransform,
					Stage:          "destination-transformer",
					ProcessingTime: timeTaken, Index: -1,
				},
				&proc.stats.destTransformEventsByTimeTaken,
			)

			proc.logger.Debug("Dest Transform output size", len(response.Events))
			trace.Logf(ctx, "DestTransform", "output size %d", len(response.Events))

			failedJobs, failedMetrics, failedCountMap := proc.getFailedEventJobs(
				response, commonMetaData, eventsByMessageID,
				transformer.DestTransformerStage, transformationEnabled, trackingPlanEnabled,
			)
			destTransformationStat.numEvents.Count(len(eventsToTransform))
			destTransformationStat.numOutputSuccessEvents.Count(len(response.Events))
			destTransformationStat.numOutputFailedEvents.Count(len(failedJobs))

			proc.saveFailedJobs(failedJobs)

			if _, ok := procErrorJobsByDestID[destID]; !ok {
				procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
			}
			procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], failedJobs...)

			//REPORTING - PROCESSOR metrics - START
			if proc.isReportingEnabled() {
				successMetrics := make([]*types.PUReportedMetric, 0)
				connectionDetailsMap := make(map[string]*types.ConnectionDetails)
				statusDetailsMap := make(map[string]*types.StatusDetail)
				successCountMap := make(map[string]int64)
				for i := range response.Events {
					//Update metrics maps
					proc.updateMetricMaps(nil, successCountMap, connectionDetailsMap, statusDetailsMap, response.Events[i], jobsdb.Succeeded.State, []byte(`{}`))
				}
				types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)

				for k, cd := range connectionDetailsMap {
					m := &types.PUReportedMetric{
						ConnectionDetails: *cd,
						PUDetails:         *types.CreatePUDetails(types.EVENT_FILTER, types.DEST_TRANSFORMER, false, false),
						StatusDetail:      statusDetailsMap[k],
					}
					successMetrics = append(successMetrics, m)
				}

				diffMetrics := getDiffMetrics(types.EVENT_FILTER, types.DEST_TRANSFORMER, inCountMetadataMap, inCountMap, successCountMap, failedCountMap)

				reportMetrics = append(reportMetrics, failedMetrics...)
				reportMetrics = append(reportMetrics, successMetrics...)
				reportMetrics = append(reportMetrics, diffMetrics...)
			}
			//REPORTING - PROCESSOR metrics - END
		})
	}

	trace.WithRegion(ctx, "MarshalForDB", func() {
		//Save the JSON in DB. This is what the router uses
		for i := range response.Events {
			destEventJSON, err := jsonfast.Marshal(response.Events[i].Output)
			// Should be a valid JSON since it's our transformation, but we handle it anyway
			if err != nil {
				continue
			}

			//Need to replace UUID his with messageID from client
			id := misc.FastUUID()
			// read source_id from metadata that is replayed back from transformer
			// in case of custom transformations metadata of first event is returned along with all events in session
			// source_id will be same for all events belong to same user in a session
			metadata := response.Events[i].Metadata

			sourceID := metadata.SourceID
			destID := metadata.DestinationID
			rudderID := metadata.RudderID
			receivedAt := metadata.ReceivedAt
			messageId := metadata.MessageID
			jobId := metadata.JobID
			sourceBatchId := metadata.SourceBatchID
			sourceTaskId := metadata.SourceTaskID
			sourceTaskRunId := metadata.SourceTaskRunID
			recordId := metadata.RecordID
			sourceJobId := metadata.SourceJobID
			sourceJobRunId := metadata.SourceJobRunID
			eventName := metadata.EventName
			eventType := metadata.EventType
			sourceDefID := metadata.SourceDefinitionID
			destDefID := metadata.DestinationDefinitionID
			sourceCategory := metadata.SourceCategory
			workspaceId := metadata.WorkspaceID
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
				RecordID:                recordId,
				WorkspaceId:             workspaceId,
			}
			marshalledParams, err := jsonfast.Marshal(params)
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
				WorkspaceId:  workspaceId,
			}
			if misc.ContainsString(batchDestinations, newJob.CustomVal) {
				batchDestJobs = append(batchDestJobs, &newJob)
			} else {
				destJobs = append(destJobs, &newJob)
			}
		}
	})
	return transformSrcDestOutput{
		destJobs:        destJobs,
		batchDestJobs:   batchDestJobs,
		errorsPerDestID: procErrorJobsByDestID,
		reportMetrics:   reportMetrics,
	}
}

func (proc *HandleT) saveFailedJobs(failedJobs []*jobsdb.JobT) {
	if len(failedJobs) > 0 {
		jobRunIDAbortedEventsMap := make(map[string][]*router.FailedEventRowT)
		for _, failedJob := range failedJobs {
			router.PrepareJobRunIdAbortedEventsMap(failedJob.Parameters, jobRunIDAbortedEventsMap)
		}
		txn := proc.errorDB.BeginGlobalTransaction()
		router.GetFailedEventsManager().SaveFailedRecordIDs(jobRunIDAbortedEventsMap, txn)
		proc.errorDB.CommitTransaction(txn)
	}
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
		if ok {
			supportedTypeInterface, ok := supportedTypes.([]interface{})
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
				supportedTypesArr := misc.ConvertInterfaceToStringArray(supportedTypeInterface)
				if misc.ContainsString(supportedTypesArr, messageType) {
					resp = transformer.TransformerResponseT{Output: event.Message, StatusCode: 200, Metadata: event.Metadata}
					responses = append(responses, resp)
				}
			} else {
				// allow event
				resp = transformer.TransformerResponseT{Output: event.Message, StatusCode: 200, Metadata: event.Metadata}
				responses = append(responses, resp)
			}
		} else {
			// allow event
			resp = transformer.TransformerResponseT{Output: event.Message, StatusCode: 200, Metadata: event.Metadata}
			responses = append(responses, resp)
		}
	}

	return transformer.ResponseT{Events: responses, FailedEvents: failedEvents}
}

func (proc *HandleT) addToTransformEventByTimePQ(event *TransformRequestT, pq *transformRequestPQ) {
	proc.stats.transformEventsByTimeMutex.Lock()
	defer proc.stats.transformEventsByTimeMutex.Unlock()
	if pq.Len() < transformTimesPQLength {
		pq.Add(event)
		return
	}
	if pq.Top().ProcessingTime < event.ProcessingTime {
		pq.RemoveTop()
		pq.Add(event)

	}
}

func (proc *HandleT) getJobs() []*jobsdb.JobT {
	s := time.Now()

	proc.logger.Debugf("Processor DB Read size: %d", maxEventsToProcess)

	eventCount := maxEventsToProcess
	if !enableEventCount {
		eventCount = 0
	}

	unprocessedList := proc.gatewayDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{GWCustomVal},
		JobsLimit:        maxEventsToProcess,
		EventsLimit:      eventCount,
		PayloadSizeLimit: proc.payloadLimit,
	})
	totalEvents := 0
	totalPayloadBytes := 0
	for i, job := range unprocessedList {
		totalEvents += job.EventCount
		totalPayloadBytes += len(job.EventPayload)

		if !enableEventCount && totalEvents > maxEventsToProcess {
			unprocessedList = unprocessedList[:i]
			break
		}

		if job.JobID <= proc.lastJobID {
			proc.logger.Debugf("Out of order job_id: prev: %d cur: %d", proc.lastJobID, job.JobID)
			proc.stats.statDBReadOutOfOrder.Count(1)
		} else if proc.lastJobID != 0 && job.JobID != proc.lastJobID+1 {
			proc.logger.Debugf("Out of sequence job_id: prev: %d cur: %d", proc.lastJobID, job.JobID)
			proc.stats.statDBReadOutOfSequence.Count(1)
		}
		proc.lastJobID = job.JobID
	}
	dbReadTime := time.Since(s)
	defer proc.stats.statDBR.SendTiming(dbReadTime)

	// check if there is work to be done
	if len(unprocessedList) == 0 {
		proc.logger.Debugf("Processor DB Read Complete. No GW Jobs to process.")
		proc.stats.pStatsDBR.Rate(0, time.Since(s))
		return unprocessedList
	}

	eventSchemasStart := time.Now()
	if enableEventSchemasFeature && !enableEventSchemasAPIOnly {
		for _, unprocessedJob := range unprocessedList {
			writeKey := gjson.GetBytes(unprocessedJob.EventPayload, "writeKey").Str
			proc.eventSchemaHandler.RecordEventSchema(writeKey, string(unprocessedJob.EventPayload))
		}
	}
	eventSchemasTime := time.Since(eventSchemasStart)
	defer proc.stats.eventSchemasTime.SendTiming(eventSchemasTime)

	proc.logger.Debugf("Processor DB Read Complete. unprocessedList: %v total_events: %d", len(unprocessedList), totalEvents)
	proc.stats.pStatsDBR.Rate(len(unprocessedList), time.Since(s))
	proc.stats.statGatewayDBR.Count(len(unprocessedList))

	proc.stats.statDBReadRequests.Observe(float64(len(unprocessedList)))
	proc.stats.statDBReadEvents.Observe(float64(totalEvents))
	proc.stats.statDBReadPayloadBytes.Observe(float64(totalPayloadBytes))

	return unprocessedList
}

func (proc *HandleT) markExecuting(jobs []*jobsdb.JobT) error {
	start := time.Now()
	defer proc.stats.statMarkExecuting.Since(start)

	statusList := make([]*jobsdb.JobStatusT, len(jobs))
	for i, job := range jobs {
		statusList[i] = &jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			JobState:      jobsdb.Executing.State,
			ExecTime:      start,
			RetryTime:     start,
			ErrorCode:     "",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			WorkspaceId:   job.WorkspaceId,
		}
	}
	//Mark the jobs as executing
	err := proc.gatewayDB.UpdateJobStatus(statusList, []string{GWCustomVal}, nil)
	if err != nil {
		return fmt.Errorf("marking jobs as executing: %w", err)
	}

	return nil
}

// handlePendingGatewayJobs is checking for any pending gateway jobs (failed and unprocessed), and routes them appropriately
// Returns true if any job is handled, otherwise returns false.
func (proc *HandleT) handlePendingGatewayJobs() bool {
	s := time.Now()

	unprocessedList := proc.getJobs()

	if len(unprocessedList) == 0 {
		return false
	}

	proc.Store(
		proc.transformations(
			proc.processJobsForDest(subJob{
				subJobs: unprocessedList,
				hasMore: false,
			}, nil),
		),
	)
	proc.stats.statLoopTime.Since(s)

	return true
}

// mainLoop: legacy way of handling jobs
func (proc *HandleT) mainLoop(ctx context.Context) {
	//waiting for reporting client setup
	if proc.reporting != nil && proc.reportingEnabled {
		proc.reporting.WaitForSetup(ctx, types.CORE_REPORTING_CLIENT)
	}

	proc.logger.Info("Processor loop started")
	currLoopSleep := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(mainLoopTimeout):
			if isUnLocked {
				found := proc.handlePendingGatewayJobs()
				if found {
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

//`jobSplitter` func Splits the read Jobs into sub-batches after reading from DB to process.
//`subJobMerger` func merges the split jobs into a single batch before writing to DB.
//So, to keep track of sub-batch we have `hasMore` variable.
//each sub-batch has `hasMore`. If, a sub-batch is the last one from the batch it's marked as `false`, else `true`.
type subJob struct {
	subJobs []*jobsdb.JobT
	hasMore bool
}

func jobSplitter(jobs []*jobsdb.JobT) []subJob {
	subJobCount := 1
	if len(jobs)/subJobSize > 1 {
		subJobCount = len(jobs) / subJobSize
		if len(jobs)%subJobSize != 0 {
			subJobCount++
		}
	}
	var subJobs []subJob
	for i := 0; i < subJobCount; i++ {
		if i == subJobCount-1 {
			//all the remaining jobs are sent in last sub-job batch.
			subJobs = append(subJobs, subJob{
				subJobs: jobs,
				hasMore: false,
			})
			continue
		}
		subJobs = append(subJobs, subJob{
			subJobs: jobs[:subJobSize],
			hasMore: true,
		})
		jobs = jobs[subJobSize:]
	}

	return subJobs
}

// mainPipeline: new way of handling jobs
//
// [getJobs] -chProc-> [processJobsForDest] -chTrans-> [transformations] -chStore-> [Store]
func (proc *HandleT) mainPipeline(ctx context.Context) {
	//waiting for reporting client setup
	proc.logger.Infof("Processor mainPipeline started, subJobSize=%d pipelineBufferedItems=%d", subJobSize, pipelineBufferedItems)

	if proc.reporting != nil && proc.reportingEnabled {
		proc.reporting.WaitForSetup(ctx, types.CORE_REPORTING_CLIENT)
	}
	wg := sync.WaitGroup{}
	bufferSize := pipelineBufferedItems

	chProc := make(chan subJob, bufferSize)
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(chProc)
		nextSleepTime := time.Duration(0)

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(nextSleepTime):
				if !isUnLocked {
					nextSleepTime = proc.maxLoopSleep
					continue
				}
				dbReadStart := time.Now()
				jobs := proc.getJobs()
				if len(jobs) == 0 {
					// no jobs found, double sleep time until maxLoopSleep
					nextSleepTime = 2 * nextSleepTime
					if nextSleepTime > proc.maxLoopSleep {
						nextSleepTime = proc.maxLoopSleep
					} else if nextSleepTime == 0 {
						nextSleepTime = proc.readLoopSleep
					}
					continue
				}

				err := proc.markExecuting(jobs)
				if err != nil {
					pkgLogger.Error(err)
					panic(err)
				}
				dbReadTime := time.Since(dbReadStart)
				events := 0
				for i := range jobs {
					events += jobs[i].EventCount
				}
				dbReadThroughput := throughputPerSecond(events, dbReadTime)
				//DB read throughput per second.
				proc.stats.DBReadThroughput.Count(dbReadThroughput)

				// nextSleepTime is dependent on the number of events read in this loop
				emptyRatio := 1.0 - math.Min(1, float64(events)/float64(maxEventsToProcess))
				nextSleepTime = time.Duration(emptyRatio * float64(proc.readLoopSleep))

				subJobs := jobSplitter(jobs)
				for _, subJob := range subJobs {
					chProc <- subJob
				}
			}
		}
	}()

	chTrans := make(chan transformationMessage, bufferSize)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(chTrans)
		for jobs := range chProc {
			chTrans <- proc.processJobsForDest(jobs, nil)
		}
	}()

	//we need the below buffer size to ensure that `proc.Store(*mergedJob)` is not blocking rest of the Go routines.
	chStore := make(chan storeMessage, (bufferSize+1)*(maxEventsToProcess/subJobSize+1))
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(chStore)
		for msg := range chTrans {
			chStore <- proc.transformations(msg)
		}
	}()

	wg.Add(1)
	go func() {
		var mergedJob storeMessage
		firstSubJob := true
		defer wg.Done()
		for subJob := range chStore {

			if firstSubJob && !subJob.hasMore {
				proc.Store(subJob)
				continue
			}

			if firstSubJob {
				mergedJob = storeMessage{}
				mergedJob.uniqueMessageIds = make(map[string]struct{})
				mergedJob.procErrorJobsByDestID = make(map[string][]*jobsdb.JobT)
				mergedJob.sourceDupStats = make(map[string]int)

				mergedJob.start = subJob.start
				firstSubJob = false
			}
			mergedJob := subJobMerger(&mergedJob, &subJob)

			if !subJob.hasMore {
				proc.Store(*mergedJob)
				firstSubJob = true
			}
		}
	}()

	wg.Wait()
}

func subJobMerger(mergedJob *storeMessage, subJob *storeMessage) *storeMessage {

	mergedJob.statusList = append(mergedJob.statusList, subJob.statusList...)
	mergedJob.destJobs = append(mergedJob.destJobs, subJob.destJobs...)
	mergedJob.batchDestJobs = append(mergedJob.batchDestJobs, subJob.batchDestJobs...)

	mergedJob.procErrorJobs = append(mergedJob.procErrorJobs, subJob.procErrorJobs...)
	for id, job := range subJob.procErrorJobsByDestID {
		mergedJob.procErrorJobsByDestID[id] = append(mergedJob.procErrorJobsByDestID[id], job...)
	}

	mergedJob.reportMetrics = append(mergedJob.reportMetrics, subJob.reportMetrics...)
	for tag, count := range subJob.sourceDupStats {
		mergedJob.sourceDupStats[tag] += count
	}
	for id := range subJob.uniqueMessageIds {
		mergedJob.uniqueMessageIds[id] = struct{}{}
	}
	mergedJob.totalEvents += subJob.totalEvents

	return mergedJob
}

func throughputPerSecond(processedJob int, timeTaken time.Duration) int {
	normalizedTime := float64(timeTaken) / float64(time.Second)
	return int(float64(processedJob) / normalizedTime)
}

func (proc *HandleT) crashRecover() {
	proc.gatewayDB.DeleteExecuting()
}

func (proc *HandleT) updateSourceStats(sourceStats map[string]int, bucket string) {
	for sourceTag, count := range sourceStats {
		tags := map[string]string{
			"source": sourceTag,
		}
		sourceStatsD := proc.statsFactory.NewTaggedStat(bucket, stats.CountType, tags)
		sourceStatsD.Count(count)
	}
}

func (proc *HandleT) isReportingEnabled() bool {
	return proc.reporting != nil && proc.reportingEnabled
}
