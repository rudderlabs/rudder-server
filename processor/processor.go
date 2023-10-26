package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/ro"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	eventschema "github.com/rudderlabs/rudder-server/event-schema"
	"github.com/rudderlabs/rudder-server/internal/enricher"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/eventfilter"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/isolation"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

const (
	MetricKeyDelimiter = "!<<#>>!"
	UserTransformation = "USER_TRANSFORMATION"
	DestTransformation = "DEST_TRANSFORMATION"
	EventFilter        = "EVENT_FILTER"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

func NewHandle(transformer transformer.Transformer) *Handle {
	h := &Handle{transformer: transformer}
	h.loadConfig()
	return h
}

// Handle is a handle to the processor module
type Handle struct {
	backendConfig backendconfig.BackendConfig
	transformer   transformer.Transformer
	lastJobID     int64

	gatewayDB     jobsdb.JobsDB
	routerDB      jobsdb.JobsDB
	batchRouterDB jobsdb.JobsDB
	readErrorDB   jobsdb.JobsDB
	writeErrorDB  jobsdb.JobsDB
	eventSchemaDB jobsdb.JobsDB
	archivalDB    jobsdb.JobsDB

	logger                    logger.Logger
	eventSchemaHandler        types.EventSchemasI
	enrichers                 []enricher.PipelineEnricher
	dedup                     dedup.Dedup
	reporting                 types.Reporting
	reportingEnabled          bool
	backgroundWait            func() error
	backgroundCancel          context.CancelFunc
	transformerFeatures       json.RawMessage
	statsFactory              stats.Stats
	stats                     processorStats
	payloadLimit              misc.ValueLoader[int64]
	jobsDBCommandTimeout      misc.ValueLoader[time.Duration]
	jobdDBQueryRequestTimeout misc.ValueLoader[time.Duration]
	jobdDBMaxRetries          misc.ValueLoader[int]
	transientSources          transientsource.Service
	fileuploader              fileuploader.Provider
	rsourcesService           rsources.JobService
	destDebugger              destinationdebugger.DestinationDebugger
	transDebugger             transformationdebugger.TransformationDebugger
	isolationStrategy         isolation.Strategy
	limiter                   struct {
		read       kitsync.Limiter
		preprocess kitsync.Limiter
		transform  kitsync.Limiter
		store      kitsync.Limiter
	}
	config struct {
		isolationMode             isolation.Mode
		mainLoopTimeout           time.Duration
		featuresRetryMaxAttempts  int
		enablePipelining          bool
		pipelineBufferedItems     int
		subJobSize                int
		pingerSleep               misc.ValueLoader[time.Duration]
		readLoopSleep             misc.ValueLoader[time.Duration]
		maxLoopSleep              misc.ValueLoader[time.Duration]
		storeTimeout              misc.ValueLoader[time.Duration]
		maxEventsToProcess        misc.ValueLoader[int]
		transformBatchSize        misc.ValueLoader[int]
		userTransformBatchSize    misc.ValueLoader[int]
		sourceIdDestinationMap    map[string][]backendconfig.DestinationT
		sourceIdSourceMap         map[string]backendconfig.SourceT
		workspaceLibrariesMap     map[string]backendconfig.LibrariesT
		destinationIDtoTypeMap    map[string]string
		destConsentCategories     map[string][]string
		batchDestinations         []string
		configSubscriberLock      sync.RWMutex
		enableEventSchemasFeature bool
		enableEventSchemasAPIOnly misc.ValueLoader[bool]
		enableDedup               bool
		enableEventCount          misc.ValueLoader[bool]
		transformTimesPQLength    int
		captureEventNameStats     misc.ValueLoader[bool]
		transformerURL            string
		pollInterval              time.Duration
		GWCustomVal               string
		asyncInit                 *misc.AsyncInit
		eventSchemaV2Enabled      bool
		archivalEnabled           misc.ValueLoader[bool]
		eventAuditEnabled         map[string]bool
	}

	adaptiveLimit func(int64) int64
	storePlocker  kitsync.PartitionLocker
}
type processorStats struct {
	statGatewayDBR                stats.Measurement
	statGatewayDBW                stats.Measurement
	statRouterDBW                 stats.Measurement
	statBatchRouterDBW            stats.Measurement
	statProcErrDBW                stats.Measurement
	statDBR                       stats.Measurement
	statDBW                       stats.Measurement
	statLoopTime                  stats.Measurement
	eventSchemasTime              stats.Measurement
	validateEventsTime            stats.Measurement
	processJobsTime               stats.Measurement
	statSessionTransform          stats.Measurement
	statUserTransform             stats.Measurement
	statDestTransform             stats.Measurement
	marshalSingularEvents         stats.Measurement
	destProcessing                stats.Measurement
	pipeProcessing                stats.Measurement
	statNumRequests               stats.Measurement
	statNumEvents                 stats.Measurement
	statDBReadRequests            stats.Measurement
	statDBReadEvents              stats.Measurement
	statDBReadPayloadBytes        stats.Measurement
	statDBReadOutOfOrder          stats.Measurement
	statDBReadOutOfSequence       stats.Measurement
	statMarkExecuting             stats.Measurement
	statDBWriteStatusTime         stats.Measurement
	statDBWriteJobsTime           stats.Measurement
	statDBWriteRouterPayloadBytes stats.Measurement
	statDBWriteBatchPayloadBytes  stats.Measurement
	statDBWriteRouterEvents       stats.Measurement
	statDBWriteBatchEvents        stats.Measurement
	statDestNumOutputEvents       stats.Measurement
	statBatchDestNumOutputEvents  stats.Measurement
	DBReadThroughput              stats.Measurement
	processJobThroughput          stats.Measurement
	transformationsThroughput     stats.Measurement
	DBWriteThroughput             stats.Measurement
}

var defaultTransformerFeatures = `{
	"routerTransform": {
	  "MARKETO": true,
	  "HS": true
	}
  }`

type DestStatT struct {
	numEvents               stats.Measurement
	numOutputSuccessEvents  stats.Measurement
	numOutputFailedEvents   stats.Measurement
	numOutputFilteredEvents stats.Measurement
	transformTime           stats.Measurement
}

type ParametersT struct {
	SourceID                string      `json:"source_id"`
	DestinationID           string      `json:"destination_id"`
	ReceivedAt              string      `json:"received_at"`
	TransformAt             string      `json:"transform_at"`
	MessageID               string      `json:"message_id"`
	GatewayJobID            int64       `json:"gateway_job_id"`
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
	sourceTaskRunID         string
	sourceJobID             string
	sourceJobRunID          string
	sourceDefinitionID      string
	destinationDefinitionID string
	sourceCategory          string
	transformationID        string
	transformationVersionID string
	trackingPlanID          string
	trackingPlanVersion     int
}

type NonSuccessfulTransformationMetrics struct {
	failedJobs       []*jobsdb.JobT
	failedMetrics    []*types.PUReportedMetric
	failedCountMap   map[string]int64
	filteredJobs     []*jobsdb.JobT
	filteredMetrics  []*types.PUReportedMetric
	filteredCountMap map[string]int64
}

type (
	SourceIDT string
)

func buildStatTags(sourceID, workspaceID string, destination *backendconfig.DestinationT, transformationType string) map[string]string {
	module := "router"
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
		"workspaceId":        workspaceID,
		"transformationType": transformationType,
	}
}

func (proc *Handle) newUserTransformationStat(sourceID, workspaceID string, destination *backendconfig.DestinationT) *DestStatT {
	tags := buildStatTags(sourceID, workspaceID, destination, UserTransformation)

	tags["transformation_id"] = destination.Transformations[0].ID
	tags["transformation_version_id"] = destination.Transformations[0].VersionID
	tags["error"] = "false"

	numEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_in_count", stats.CountType, tags)
	numOutputSuccessEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_out_count", stats.CountType, tags)

	errTags := misc.CopyStringMap(tags)
	errTags["error"] = "true"
	numOutputFailedEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_out_count", stats.CountType, errTags)

	filterTags := misc.CopyStringMap(tags)
	filterTags["error"] = "filtered"
	numOutputFilteredEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_out_count", stats.CountType, filterTags)
	transformTime := proc.statsFactory.NewTaggedStat("proc_transform_stage_duration", stats.TimerType, tags)

	return &DestStatT{
		numEvents:               numEvents,
		numOutputSuccessEvents:  numOutputSuccessEvents,
		numOutputFailedEvents:   numOutputFailedEvents,
		numOutputFilteredEvents: numOutputFilteredEvents,
		transformTime:           transformTime,
	}
}

func (proc *Handle) newDestinationTransformationStat(sourceID, workspaceID, transformAt string, destination *backendconfig.DestinationT) *DestStatT {
	tags := buildStatTags(sourceID, workspaceID, destination, DestTransformation)

	tags["transform_at"] = transformAt
	tags["error"] = "false"

	numEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_in_count", stats.CountType, tags)
	numOutputSuccessEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_out_count", stats.CountType, tags)

	errTags := misc.CopyStringMap(tags)
	errTags["error"] = "true"
	numOutputFailedEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_out_count", stats.CountType, errTags)

	filterTags := misc.CopyStringMap(tags)
	filterTags["error"] = "filtered"
	numOutputFilteredEvents := proc.statsFactory.NewTaggedStat("proc_transform_stage_out_count", stats.CountType, filterTags)
	destTransform := proc.statsFactory.NewTaggedStat("proc_transform_stage_duration", stats.TimerType, tags)

	return &DestStatT{
		numEvents:               numEvents,
		numOutputSuccessEvents:  numOutputSuccessEvents,
		numOutputFailedEvents:   numOutputFailedEvents,
		numOutputFilteredEvents: numOutputFilteredEvents,
		transformTime:           destTransform,
	}
}

func (proc *Handle) newEventFilterStat(sourceID, workspaceID string, destination *backendconfig.DestinationT) *DestStatT {
	tags := buildStatTags(sourceID, workspaceID, destination, EventFilter)
	tags["error"] = "false"

	numEvents := proc.statsFactory.NewTaggedStat("proc_event_filter_in_count", stats.CountType, tags)
	numOutputSuccessEvents := proc.statsFactory.NewTaggedStat("proc_event_filter_out_count", stats.CountType, tags)

	errTags := misc.CopyStringMap(tags)
	errTags["error"] = "true"
	numOutputFailedEvents := proc.statsFactory.NewTaggedStat("proc_event_filter_out_count", stats.CountType, errTags)

	filterTags := misc.CopyStringMap(tags)
	filterTags["error"] = "filtered"
	numOutputFilteredEvents := proc.statsFactory.NewTaggedStat("proc_event_filter_out_count", stats.CountType, filterTags)
	eventFilterTime := proc.statsFactory.NewTaggedStat("proc_event_filter_time", stats.TimerType, tags)

	return &DestStatT{
		numEvents:               numEvents,
		numOutputSuccessEvents:  numOutputSuccessEvents,
		numOutputFailedEvents:   numOutputFailedEvents,
		numOutputFilteredEvents: numOutputFilteredEvents,
		transformTime:           eventFilterTime,
	}
}

// Setup initializes the module
func (proc *Handle) Setup(
	backendConfig backendconfig.BackendConfig, gatewayDB, routerDB,
	batchRouterDB, readErrorDB, writeErrorDB, eventSchemaDB, archivalDB jobsdb.JobsDB, reporting types.Reporting,
	transientSources transientsource.Service,
	fileuploader fileuploader.Provider,
	rsourcesService rsources.JobService,
	destDebugger destinationdebugger.DestinationDebugger,
	transDebugger transformationdebugger.TransformationDebugger,
	enrichers []enricher.PipelineEnricher,
) {
	proc.reporting = reporting
	proc.destDebugger = destDebugger
	proc.transDebugger = transDebugger
	proc.reportingEnabled = config.GetBoolVar(types.DefaultReportingEnabled, "Reporting.enabled")
	proc.setupReloadableVars()
	proc.logger = logger.NewLogger().Child("processor")
	proc.backendConfig = backendConfig

	proc.gatewayDB = gatewayDB
	proc.routerDB = routerDB
	proc.batchRouterDB = batchRouterDB
	proc.readErrorDB = readErrorDB
	proc.writeErrorDB = writeErrorDB
	proc.eventSchemaDB = eventSchemaDB
	proc.archivalDB = archivalDB

	proc.transientSources = transientSources
	proc.fileuploader = fileuploader
	proc.rsourcesService = rsourcesService
	proc.enrichers = enrichers

	if proc.adaptiveLimit == nil {
		proc.adaptiveLimit = func(limit int64) int64 { return limit }
	}
	proc.storePlocker = *kitsync.NewPartitionLocker()

	// Stats
	proc.statsFactory = stats.Default
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
	if proc.config.enableEventSchemasFeature {
		proc.eventSchemaHandler = eventschema.GetInstance()
	}
	if proc.config.enableDedup {
		proc.dedup = dedup.New(dedup.DefaultPath())
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	proc.backgroundWait = g.Wait
	proc.backgroundCancel = cancel

	proc.config.asyncInit = misc.NewAsyncInit(2)
	g.Go(misc.WithBugsnag(func() error {
		proc.backendConfigSubscriber(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnag(func() error {
		proc.syncTransformerFeatureJson(ctx)
		return nil
	}))

	// periodically publish a zero counter for ensuring that stuck processing pipeline alert
	// can always detect a stuck processor
	g.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(15 * time.Second):
				proc.stats.statGatewayDBW.Count(0)
			}
		}
	}))

	proc.crashRecover()
}

func (proc *Handle) setupReloadableVars() {
	proc.jobdDBQueryRequestTimeout = config.GetReloadableDurationVar(600, time.Second, "JobsDB.Processor.QueryRequestTimeout", "JobsDB.QueryRequestTimeout")
	proc.jobsDBCommandTimeout = config.GetReloadableDurationVar(600, time.Second, "JobsDB.Processor.CommandRequestTimeout", "JobsDB.CommandRequestTimeout")
	proc.jobdDBMaxRetries = config.GetReloadableIntVar(2, 1, "JobsDB.Processor.MaxRetries", "JobsDB.MaxRetries")
}

// Start starts this processor's main loops.
func (proc *Handle) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	var err error
	proc.logger.Infof("Starting processor in isolation mode: %s", proc.config.isolationMode)
	if proc.isolationStrategy, err = isolation.GetStrategy(proc.config.isolationMode); err != nil {
		return fmt.Errorf("resolving isolation strategy for mode %q: %w", proc.config.isolationMode, err)
	}

	// limiters
	s := proc.statsFactory
	var limiterGroup sync.WaitGroup
	proc.limiter.read = kitsync.NewLimiter(ctx, &limiterGroup, "proc_read",
		config.GetInt("Processor.Limiter.read.limit", 50),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.read.dynamicPeriod", 1, time.Second)))
	proc.limiter.preprocess = kitsync.NewLimiter(ctx, &limiterGroup, "proc_preprocess",
		config.GetInt("Processor.Limiter.preprocess.limit", 50),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.preprocess.dynamicPeriod", 1, time.Second)))
	proc.limiter.transform = kitsync.NewLimiter(ctx, &limiterGroup, "proc_transform",
		config.GetInt("Processor.Limiter.transform.limit", 50),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.transform.dynamicPeriod", 1, time.Second)))
	proc.limiter.store = kitsync.NewLimiter(ctx, &limiterGroup, "proc_store",
		config.GetInt("Processor.Limiter.store.limit", 50),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.store.dynamicPeriod", 1, time.Second)))
	g.Go(func() error {
		limiterGroup.Wait()
		return nil
	})

	// pinger loop
	g.Go(misc.WithBugsnag(func() error {
		proc.logger.Info("Starting pinger loop")
		proc.backendConfig.WaitForConfig(ctx)
		proc.logger.Info("Backend config received")

		// waiting for init group
		proc.logger.Info("Waiting for async init group")
		select {
		case <-ctx.Done():
			return nil
		case <-proc.config.asyncInit.Wait():
			// proceed
		}
		proc.logger.Info("Async init group done")

		h := &workerHandleAdapter{proc}
		pool := workerpool.New(ctx, func(partition string) workerpool.Worker { return newProcessorWorker(partition, h) }, proc.logger)
		defer pool.Shutdown()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(proc.config.pingerSleep.Load()):
			}
			for _, partition := range proc.activePartitions(ctx) {
				pool.PingWorker(partition)
			}
		}
	}))

	// stash loop
	g.Go(misc.WithBugsnag(func() error {
		st := stash.New()
		st.Setup(
			proc.readErrorDB,
			proc.transientSources,
			proc.fileuploader,
			proc.adaptiveLimit,
		)
		st.Start(ctx)
		return nil
	}))

	return g.Wait()
}

func (proc *Handle) activePartitions(ctx context.Context) []string {
	defer proc.statsFactory.NewStat("proc_active_partitions_time", stats.TimerType).RecordDuration()()
	keys, err := proc.isolationStrategy.ActivePartitions(ctx, proc.gatewayDB)
	if err != nil && ctx.Err() == nil {
		// TODO: retry?
		panic(err)
	}
	proc.statsFactory.NewStat("proc_active_partitions", stats.GaugeType).Gauge(len(keys))
	return keys
}

func (proc *Handle) Shutdown() {
	proc.backgroundCancel()
	_ = proc.backgroundWait()
	if proc.dedup != nil {
		proc.dedup.Close()
	}
	metric.Instance.Reset()
}

func (proc *Handle) loadConfig() {
	proc.config.mainLoopTimeout = 200 * time.Millisecond
	proc.config.featuresRetryMaxAttempts = 10

	defaultSubJobSize := 2000
	defaultMaxEventsToProcess := 10000
	defaultPayloadLimit := 100 * bytesize.MB

	defaultIsolationMode := isolation.ModeSource
	if config.IsSet("WORKSPACE_NAMESPACE") {
		defaultIsolationMode = isolation.ModeWorkspace
	}
	proc.config.isolationMode = isolation.Mode(config.GetString("Processor.isolationMode", string(defaultIsolationMode)))
	// If isolation mode is not none, we need to reduce the values for some of the config variables to more sensible defaults
	if proc.config.isolationMode != isolation.ModeNone {
		defaultSubJobSize = 400
		defaultMaxEventsToProcess = 2000
		defaultPayloadLimit = 20 * bytesize.MB
	}

	proc.config.enablePipelining = config.GetBoolVar(true, "Processor.enablePipelining")
	proc.config.pipelineBufferedItems = config.GetIntVar(0, 1, "Processor.pipelineBufferedItems")
	proc.config.subJobSize = config.GetIntVar(defaultSubJobSize, 1, "Processor.subJobSize")
	// Enable dedup of incoming events by default
	proc.config.enableDedup = config.GetBoolVar(false, "Dedup.enableDedup")
	// EventSchemas feature. false by default
	proc.config.enableEventSchemasFeature = config.GetBoolVar(false, "EventSchemas.enableEventSchemasFeature")
	proc.config.eventSchemaV2Enabled = config.GetBoolVar(false, "EventSchemas2.enabled")
	proc.config.batchDestinations = misc.BatchDestinations()
	proc.config.transformTimesPQLength = config.GetIntVar(5, 1, "Processor.transformTimesPQLength")
	proc.config.transformerURL = config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	proc.config.pollInterval = config.GetDurationVar(5, time.Second, "Processor.pollInterval", "Processor.pollIntervalInS")
	// GWCustomVal is used as a key in the jobsDB customval column
	proc.config.GWCustomVal = config.GetStringVar("GW", "Gateway.CustomVal")

	proc.loadReloadableConfig(defaultPayloadLimit, defaultMaxEventsToProcess)
}

func (proc *Handle) loadReloadableConfig(defaultPayloadLimit int64, defaultMaxEventsToProcess int) {
	proc.payloadLimit = config.GetReloadableInt64Var(defaultPayloadLimit, 1, "Processor.payloadLimit")
	proc.config.maxLoopSleep = config.GetReloadableDurationVar(10000, time.Millisecond, "Processor.maxLoopSleep", "Processor.maxLoopSleepInMS")
	proc.config.storeTimeout = config.GetReloadableDurationVar(5, time.Minute, "Processor.storeTimeout")
	proc.config.pingerSleep = config.GetReloadableDurationVar(1000, time.Millisecond, "Processor.pingerSleep")
	proc.config.readLoopSleep = config.GetReloadableDurationVar(1000, time.Millisecond, "Processor.readLoopSleep")
	proc.config.transformBatchSize = config.GetReloadableIntVar(100, 1, "Processor.transformBatchSize")
	proc.config.userTransformBatchSize = config.GetReloadableIntVar(200, 1, "Processor.userTransformBatchSize")
	proc.config.enableEventCount = config.GetReloadableBoolVar(true, "Processor.enableEventCount")
	proc.config.enableEventSchemasAPIOnly = config.GetReloadableBoolVar(false, "EventSchemas.enableEventSchemasAPIOnly")
	proc.config.maxEventsToProcess = config.GetReloadableIntVar(defaultMaxEventsToProcess, 1, "Processor.maxLoopProcessEvents")
	proc.config.archivalEnabled = config.GetReloadableBoolVar(true, "archival.Enabled")
	// Capture event name as a tag in event level stats
	proc.config.captureEventNameStats = config.GetReloadableBoolVar(false, "Processor.Stats.captureEventName")
}

// syncTransformerFeatureJson polls the transformer feature json endpoint,
//
//	updates the transformer feature map.
//
// It will set isUnLocked to true if it successfully fetches the transformer feature json at least once.
func (proc *Handle) syncTransformerFeatureJson(ctx context.Context) {
	var initDone bool
	proc.logger.Infof("Fetching transformer features from %s", proc.config.transformerURL)
	for {
		for i := 0; i < proc.config.featuresRetryMaxAttempts; i++ {

			if ctx.Err() != nil {
				return
			}

			retry := proc.makeFeaturesFetchCall()
			if retry {
				proc.logger.Infof("Fetched transformer features from %s (retry: %v)", proc.config.transformerURL, retry)
			}
			if retry {
				select {
				case <-ctx.Done():
					return
				case <-time.After(200 * time.Millisecond):
					continue
				}
			}
			break
		}

		if proc.transformerFeatures != nil && !initDone {
			initDone = true
			proc.config.asyncInit.Done()
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(proc.config.pollInterval):
		}
	}
}

func (proc *Handle) makeFeaturesFetchCall() bool {
	url := proc.config.transformerURL + "/features"
	req, err := http.NewRequest("GET", url, bytes.NewReader([]byte{}))
	if err != nil {
		proc.logger.Error("error creating request - %s", err)
		return true
	}
	tr := &http.Transport{}
	client := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.processor.timeout", 30, time.Second)}
	res, err := client.Do(req)
	if err != nil {
		proc.logger.Error("error sending request - %s", err)
		return true
	}

	defer func() { httputil.CloseResponse(res) }()
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

func getConsentCategories(dest *backendconfig.DestinationT) []string {
	config := dest.Config
	cookieCategories, _ := misc.MapLookup(
		config,
		"oneTrustCookieCategories",
	).([]interface{})
	if len(cookieCategories) == 0 {
		return nil
	}
	return lo.FilterMap(
		cookieCategories,
		func(cookieCategory interface{}, _ int) (string, bool) {
			switch category := cookieCategory.(type) {
			case map[string]interface{}:
				cCategory, ok := category["oneTrustCookieCategory"].(string)
				return cCategory, ok && cCategory != ""
			default:
				return "", false
			}
		},
	)
}

func (proc *Handle) backendConfigSubscriber(ctx context.Context) {
	var initDone bool
	ch := proc.backendConfig.Subscribe(ctx, backendconfig.TopicProcessConfig)
	for data := range ch {
		config := data.Data.(map[string]backendconfig.ConfigT)
		var (
			destConsentCategories  = make(map[string][]string)
			workspaceLibrariesMap  = make(map[string]backendconfig.LibrariesT, len(config))
			sourceIdDestinationMap = make(map[string][]backendconfig.DestinationT)
			sourceIdSourceMap      = map[string]backendconfig.SourceT{}
			destinationIDtoTypeMap = make(map[string]string)
			eventAuditEnabled      = make(map[string]bool)
		)
		for workspaceID, wConfig := range config {
			for i := range wConfig.Sources {
				source := &wConfig.Sources[i]
				sourceIdSourceMap[source.ID] = *source
				if source.Enabled {
					sourceIdDestinationMap[source.ID] = source.Destinations
					for j := range source.Destinations {
						destination := &source.Destinations[j]
						destinationIDtoTypeMap[destination.ID] = destination.DestinationDefinition.Name
						destConsentCategories[destination.ID] = getConsentCategories(destination)
					}
				}
			}
			workspaceLibrariesMap[workspaceID] = wConfig.Libraries
			eventAuditEnabled[workspaceID] = wConfig.Settings.EventAuditEnabled
		}
		proc.config.configSubscriberLock.Lock()
		proc.config.destConsentCategories = destConsentCategories
		proc.config.workspaceLibrariesMap = workspaceLibrariesMap
		proc.config.sourceIdDestinationMap = sourceIdDestinationMap
		proc.config.sourceIdSourceMap = sourceIdSourceMap
		proc.config.destinationIDtoTypeMap = destinationIDtoTypeMap
		proc.config.eventAuditEnabled = eventAuditEnabled
		proc.config.configSubscriberLock.Unlock()
		if !initDone {
			initDone = true
			proc.config.asyncInit.Done()
		}
	}
}

func (proc *Handle) getConsentCategories(destinationID string) []string {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.destConsentCategories[destinationID]
}

func (proc *Handle) getWorkspaceLibraries(workspaceID string) backendconfig.LibrariesT {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.workspaceLibrariesMap[workspaceID]
}

func (proc *Handle) getSourceBySourceID(sourceId string) (*backendconfig.SourceT, error) {
	var err error
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	source, ok := proc.config.sourceIdSourceMap[sourceId]
	if !ok {
		err = errors.New("source not found for sourceId")
		proc.logger.Errorf(`Processor : source not found for sourceId: %s`, sourceId)
	}
	return &source, err
}

func (proc *Handle) getEnabledDestinations(sourceId, destinationName string) []backendconfig.DestinationT {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	var enabledDests []backendconfig.DestinationT
	for i := range proc.config.sourceIdDestinationMap[sourceId] {
		dest := &proc.config.sourceIdDestinationMap[sourceId][i]
		if destinationName == dest.DestinationDefinition.Name && dest.Enabled {
			enabledDests = append(enabledDests, *dest)
		}
	}
	return enabledDests
}

func (proc *Handle) getBackendEnabledDestinationTypes(sourceId string) map[string]backendconfig.DestinationDefinitionT {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	enabledDestinationTypes := make(map[string]backendconfig.DestinationDefinitionT)
	for i := range proc.config.sourceIdDestinationMap[sourceId] {
		destination := &proc.config.sourceIdDestinationMap[sourceId][i]
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

func enhanceWithTimeFields(event *transformer.TransformerEvent, singularEventMap types.SingularEventT, receivedAt time.Time) {
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

func makeCommonMetadataFromSingularEvent(singularEvent types.SingularEventT, batchEvent *jobsdb.JobT, receivedAt time.Time, source *backendconfig.SourceT, eventParams types.EventParams) *transformer.Metadata {
	commonMetadata := transformer.Metadata{}
	commonMetadata.SourceID = source.ID
	commonMetadata.WorkspaceID = source.WorkspaceID
	commonMetadata.Namespace = config.GetKubeNamespace()
	commonMetadata.InstanceID = misc.GetInstanceID()
	commonMetadata.RudderID = batchEvent.UserID
	commonMetadata.JobID = batchEvent.JobID
	commonMetadata.MessageID = misc.GetStringifiedData(singularEvent["messageId"])
	commonMetadata.ReceivedAt = receivedAt.Format(misc.RFC3339Milli)
	commonMetadata.SourceType = source.SourceDefinition.Name
	commonMetadata.SourceCategory = source.SourceDefinition.Category

	commonMetadata.SourceJobRunID = eventParams.SourceJobRunId
	commonMetadata.SourceJobID, _ = misc.MapLookup(singularEvent, "context", "sources", "job_id").(string)
	commonMetadata.SourceTaskRunID = eventParams.SourceTaskRunId
	commonMetadata.RecordID = misc.MapLookup(singularEvent, "recordId")

	commonMetadata.EventName, _ = misc.MapLookup(singularEvent, "event").(string)
	commonMetadata.EventType, _ = misc.MapLookup(singularEvent, "type").(string)
	commonMetadata.SourceDefinitionID = source.SourceDefinition.ID

	commonMetadata.SourceDefinitionType = source.SourceDefinition.Type

	return &commonMetadata
}

// add metadata to each singularEvent which will be returned by transformer in response
func enhanceWithMetadata(commonMetadata *transformer.Metadata, event *transformer.TransformerEvent, destination *backendconfig.DestinationT) {
	metadata := transformer.Metadata{}
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
	metadata.SourceDefinitionType = commonMetadata.SourceDefinitionType
	event.Metadata = metadata
}

func getKeyFromSourceAndDest(srcID, destID string) string {
	return srcID + "::" + destID
}

func getSourceAndDestIDsFromKey(key string) (sourceID, destID string) {
	fields := strings.Split(key, "::")
	return fields[0], fields[1]
}

func (proc *Handle) recordEventDeliveryStatus(jobsByDestID map[string][]*jobsdb.JobT) {
	for destID, jobs := range jobsByDestID {
		if !proc.destDebugger.HasUploadEnabled(destID) {
			continue
		}
		for _, job := range jobs {
			var params map[string]interface{}
			err := jsonfast.Unmarshal(job.Parameters, &params)
			if err != nil {
				proc.logger.Errorf("Error while UnMarshaling live event parameters: %w", err)
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
				proc.logger.Errorf("Error while UnMarshaling live event payload: %w", err)
				continue
			}
			for i := range events {
				event := &events[i]
				eventPayload, err := jsonfast.Marshal(*event)
				if err != nil {
					proc.logger.Errorf("Error while Marshaling live event payload: %w", err)
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
				proc.destDebugger.RecordEventDeliveryStatus(destID, &deliveryStatus)
			}
		}
	}
}

func (proc *Handle) getDestTransformerEvents(response transformer.Response, commonMetaData *transformer.Metadata, eventsByMessageID map[string]types.SingularEventWithReceivedAt, destination *backendconfig.DestinationT, stage string, trackingPlanEnabled, userTransformationEnabled bool) ([]transformer.TransformerEvent, []*types.PUReportedMetric, map[string]int64, map[string]MetricMetadata) {
	successMetrics := make([]*types.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*types.ConnectionDetails)
	statusDetailsMap := make(map[string]map[string]*types.StatusDetail)
	successCountMap := make(map[string]int64)
	successCountMetadataMap := make(map[string]MetricMetadata)
	var eventsToTransform []transformer.TransformerEvent
	for i := range response.Events {
		// Update metrics maps
		userTransformedEvent := &response.Events[i]
		messages := lo.Map(
			userTransformedEvent.Metadata.GetMessagesIDs(),
			func(msgID string, _ int) types.SingularEventT {
				return eventsByMessageID[msgID].SingularEvent
			},
		)

		for _, message := range messages {
			proc.updateMetricMaps(successCountMetadataMap, successCountMap, connectionDetailsMap, statusDetailsMap, userTransformedEvent, jobsdb.Succeeded.State, stage,
				func() json.RawMessage {
					if stage != transformer.TrackingPlanValidationStage {
						return []byte(`{}`)
					}
					if proc.transientSources.Apply(commonMetaData.SourceID) {
						return []byte(`{}`)
					}

					sampleEvent, err := jsonfast.Marshal(message)
					if err != nil {
						proc.logger.Errorf(`[Processor: getDestTransformerEvents] Failed to unmarshal first element in transformed events: %v`, err)
						sampleEvent = []byte(`{}`)
					}
					return sampleEvent
				},
				nil)
		}

		eventMetadata := commonMetaData
		eventMetadata.MessageIDs = userTransformedEvent.Metadata.MessageIDs
		eventMetadata.MessageID = userTransformedEvent.Metadata.MessageID
		eventMetadata.JobID = userTransformedEvent.Metadata.JobID
		eventMetadata.SourceTaskRunID = userTransformedEvent.Metadata.SourceTaskRunID
		eventMetadata.SourceJobID = userTransformedEvent.Metadata.SourceJobID
		eventMetadata.SourceJobRunID = userTransformedEvent.Metadata.SourceJobRunID
		eventMetadata.RudderID = userTransformedEvent.Metadata.RudderID
		eventMetadata.RecordID = userTransformedEvent.Metadata.RecordID
		eventMetadata.ReceivedAt = userTransformedEvent.Metadata.ReceivedAt
		eventMetadata.EventName = userTransformedEvent.Metadata.EventName
		eventMetadata.EventType = userTransformedEvent.Metadata.EventType
		eventMetadata.SourceDefinitionID = userTransformedEvent.Metadata.SourceDefinitionID
		eventMetadata.DestinationDefinitionID = userTransformedEvent.Metadata.DestinationDefinitionID
		eventMetadata.SourceCategory = userTransformedEvent.Metadata.SourceCategory
		updatedEvent := transformer.TransformerEvent{
			Message:     userTransformedEvent.Output,
			Metadata:    *eventMetadata,
			Destination: *destination,
		}
		eventsToTransform = append(eventsToTransform, updatedEvent)
	}

	// REPORTING - START
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
			for _, sd := range statusDetailsMap[k] {
				m := &types.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *types.CreatePUDetails(inPU, pu, false, false),
					StatusDetail:      sd,
				}
				successMetrics = append(successMetrics, m)
			}
		}
	}
	// REPORTING - END

	return eventsToTransform, successMetrics, successCountMap, successCountMetadataMap
}

func (proc *Handle) updateMetricMaps(
	countMetadataMap map[string]MetricMetadata,
	countMap map[string]int64,
	connectionDetailsMap map[string]*types.ConnectionDetails,
	statusDetailsMap map[string]map[string]*types.StatusDetail,
	event *transformer.TransformerResponse,
	status, stage string,
	payload func() json.RawMessage,
	eventsByMessageID map[string]types.SingularEventWithReceivedAt,
) {
	if !proc.isReportingEnabled() {
		return
	}
	incrementCount := int64(len(event.Metadata.GetMessagesIDs()))
	eventName := event.Metadata.EventName
	eventType := event.Metadata.EventType
	countKey := strings.Join([]string{
		event.Metadata.SourceID,
		event.Metadata.DestinationID,
		event.Metadata.SourceJobRunID,
		eventName,
		eventType,
	}, MetricKeyDelimiter)

	countMap[countKey] = countMap[countKey] + 1

	if countMetadataMap != nil {
		if _, ok := countMetadataMap[countKey]; !ok {
			countMetadataMap[countKey] = MetricMetadata{
				sourceID:                event.Metadata.SourceID,
				destinationID:           event.Metadata.DestinationID,
				sourceTaskRunID:         event.Metadata.SourceTaskRunID,
				sourceJobID:             event.Metadata.SourceJobID,
				sourceJobRunID:          event.Metadata.SourceJobRunID,
				sourceDefinitionID:      event.Metadata.SourceDefinitionID,
				destinationDefinitionID: event.Metadata.DestinationDefinitionID,
				sourceCategory:          event.Metadata.SourceCategory,
				transformationID:        event.Metadata.TransformationID,
				transformationVersionID: event.Metadata.TransformationVersionID,
				trackingPlanID:          event.Metadata.TrackingPlanId,
				trackingPlanVersion:     event.Metadata.TrackingPlanVersion,
			}
		}
	}

	key := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%d:%s:%d:%s:%s",
		event.Metadata.SourceID,
		event.Metadata.DestinationID,
		event.Metadata.SourceJobRunID,
		event.Metadata.TransformationID,
		event.Metadata.TransformationVersionID,
		event.Metadata.TrackingPlanId,
		event.Metadata.TrackingPlanVersion,
		status, event.StatusCode,
		eventName, eventType,
	)

	if _, ok := connectionDetailsMap[key]; !ok {
		connectionDetailsMap[key] = types.CreateConnectionDetail(
			event.Metadata.SourceID,
			event.Metadata.DestinationID,
			event.Metadata.SourceTaskRunID,
			event.Metadata.SourceJobID,
			event.Metadata.SourceJobRunID,
			event.Metadata.SourceDefinitionID,
			event.Metadata.DestinationDefinitionID,
			event.Metadata.SourceCategory,
			event.Metadata.TransformationID,
			event.Metadata.TransformationVersionID,
			event.Metadata.TrackingPlanId,
			event.Metadata.TrackingPlanVersion,
		)
	}

	if _, ok := statusDetailsMap[key]; !ok {
		statusDetailsMap[key] = make(map[string]*types.StatusDetail)
	}
	// create status details for each validation error
	// single event can have multiple validation errors of same type
	veCount := len(event.ValidationErrors)
	if stage == transformer.TrackingPlanValidationStage && status == jobsdb.Succeeded.State {
		if veCount > 0 {
			status = types.SUCCEEDED_WITH_VIOLATIONS
		} else {
			status = types.SUCCEEDED_WITHOUT_VIOLATIONS
		}
	}
	sdkeySet := map[string]struct{}{}
	for _, ve := range event.ValidationErrors {
		sdkey := fmt.Sprintf("%s:%d:%s:%s:%s", status, event.StatusCode, eventName, eventType, ve.Type)
		sdkeySet[sdkey] = struct{}{}

		sd, ok := statusDetailsMap[key][sdkey]
		if !ok {
			sd = types.CreateStatusDetail(status, 0, 0, event.StatusCode, event.Error, payload(), eventName, eventType, ve.Type)
			statusDetailsMap[key][sdkey] = sd
		}
		sd.ViolationCount += incrementCount
	}
	for k := range sdkeySet {
		statusDetailsMap[key][k].Count += incrementCount
	}

	// create status details for a whole event
	sdkey := fmt.Sprintf("%s:%d:%s:%s:%s", status, event.StatusCode, eventName, eventType, "")
	sd, ok := statusDetailsMap[key][sdkey]
	if !ok {
		sd = types.CreateStatusDetail(status, 0, 0, event.StatusCode, event.Error, payload(), eventName, eventType, "")
		statusDetailsMap[key][sdkey] = sd
	}

	sd.Count += incrementCount
	if status == jobsdb.Aborted.State {
		messageIDs := lo.Uniq(event.Metadata.GetMessagesIDs()) // this is called defensive programming... :(
		for _, messageID := range messageIDs {
			receivedAt := eventsByMessageID[messageID].ReceivedAt
			sd.FailedMessages = append(sd.FailedMessages, &types.FailedMessage{MessageID: messageID, ReceivedAt: receivedAt})
		}
	}
	sd.ViolationCount += int64(veCount)
}

func (proc *Handle) getNonSuccessfulMetrics(response transformer.Response, commonMetaData *transformer.Metadata, eventsByMessageID map[string]types.SingularEventWithReceivedAt, stage string, transformationEnabled, trackingPlanEnabled bool) *NonSuccessfulTransformationMetrics {
	m := &NonSuccessfulTransformationMetrics{}

	grouped := lo.GroupBy(response.FailedEvents, func(event transformer.TransformerResponse) bool { return event.StatusCode == types.FilterEventCode })
	filtered, failed := grouped[true], grouped[false]

	m.filteredJobs, m.filteredMetrics, m.filteredCountMap = proc.getTransformationMetrics(filtered, jobsdb.Filtered.State, commonMetaData, eventsByMessageID, stage, transformationEnabled, trackingPlanEnabled)
	m.failedJobs, m.failedMetrics, m.failedCountMap = proc.getTransformationMetrics(failed, jobsdb.Aborted.State, commonMetaData, eventsByMessageID, stage, transformationEnabled, trackingPlanEnabled)

	return m
}

func (proc *Handle) getTransformationMetrics(transformerResponses []transformer.TransformerResponse, state string, commonMetaData *transformer.Metadata, eventsByMessageID map[string]types.SingularEventWithReceivedAt, stage string, transformationEnabled, trackingPlanEnabled bool) ([]*jobsdb.JobT, []*types.PUReportedMetric, map[string]int64) {
	metrics := make([]*types.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*types.ConnectionDetails)
	statusDetailsMap := make(map[string]map[string]*types.StatusDetail)
	countMap := make(map[string]int64)
	var jobs []*jobsdb.JobT
	for i := range transformerResponses {
		failedEvent := &transformerResponses[i]
		messages := lo.Map(
			failedEvent.Metadata.GetMessagesIDs(),
			func(msgID string, _ int) types.SingularEventT {
				return eventsByMessageID[msgID].SingularEvent
			},
		)
		payload, err := jsonfast.Marshal(messages)
		if err != nil {
			proc.logger.Errorf(`[Processor: getTransformationMetrics] Failed to unmarshal list of failed events: %v`, err)
			continue
		}

		for _, message := range messages {
			proc.updateMetricMaps(nil, countMap, connectionDetailsMap, statusDetailsMap, failedEvent, state, stage, func() json.RawMessage {
				if proc.transientSources.Apply(commonMetaData.SourceID) {
					return []byte(`{}`)
				}
				sampleEvent, err := jsonfast.Marshal(message)
				if err != nil {
					proc.logger.Errorf(`[Processor: getTransformationMetrics] Failed to unmarshal first element in failed events: %v`, err)
					sampleEvent = []byte(`{}`)
				}
				return sampleEvent
			},
				eventsByMessageID)
		}

		proc.logger.Debugf(
			"[Processor: getTransformationMetrics] Error [%d] for source %q and destination %q: %s",
			failedEvent.StatusCode, commonMetaData.SourceID, commonMetaData.DestinationID, failedEvent.Error,
		)

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
		if eventContext, castOk := failedEvent.Output["context"].(map[string]interface{}); castOk {
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
		jobs = append(jobs, &newFailedJob)

		procErrorStat := stats.Default.NewTaggedStat("proc_error_counts", stats.CountType, stats.Tags{
			"destName":   commonMetaData.DestinationType,
			"statusCode": strconv.Itoa(failedEvent.StatusCode),
			"stage":      stage,
		})

		procErrorStat.Increment()
	}

	// REPORTING - START
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
			for _, sd := range statusDetailsMap[k] {
				m := &types.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *types.CreatePUDetails(inPU, pu, false, false),
					StatusDetail:      sd,
				}
				metrics = append(metrics, m)
			}
		}
	}
	// REPORTING - END

	return jobs, metrics, countMap
}

func (proc *Handle) updateSourceEventStatsDetailed(event types.SingularEventT, sourceId string) {
	// Any panics in this function are captured and ignore sending the stat
	defer func() {
		if r := recover(); r != nil {
			proc.logger.Error(r)
		}
	}()
	var eventType string
	var eventName string
	source, err := proc.getSourceBySourceID(sourceId)
	if err != nil {
		proc.logger.Errorf("[Processor] Failed to get source by source id: %s", sourceId)
		return
	}
	if val, ok := event["type"]; ok {
		eventType, _ = val.(string)
		tags := map[string]string{
			"writeKey":   source.WriteKey,
			"event_type": eventType,
		}
		statEventType := proc.statsFactory.NewSampledTaggedStat("processor.event_type", stats.CountType, tags)
		statEventType.Count(1)
		if proc.config.captureEventNameStats.Load() {
			if eventType != "track" {
				eventName = eventType
			} else {
				if val, ok := event["event"]; ok {
					eventName, _ = val.(string)
				} else {
					eventName = eventType
				}
			}
			tagsDetailed := map[string]string{
				"writeKey":   source.WriteKey,
				"event_type": eventType,
				"event_name": eventName,
			}
			statEventTypeDetailed := proc.statsFactory.NewSampledTaggedStat("processor.event_type_detailed", stats.CountType, tagsDetailed)
			statEventTypeDetailed.Count(1)
		}
	}
}

func getDiffMetrics(inPU, pu string, inCountMetadataMap map[string]MetricMetadata, inCountMap, successCountMap, failedCountMap, filteredCountMap map[string]int64) []*types.PUReportedMetric {
	// Calculate diff and append to reportMetrics
	// diff = successCount + abortCount - inCount
	diffMetrics := make([]*types.PUReportedMetric, 0)
	for key, inCount := range inCountMap {
		var eventName, eventType string
		splitKey := strings.Split(key, MetricKeyDelimiter)
		if len(splitKey) < 5 {
			eventName = ""
			eventType = ""
		} else {
			eventName = splitKey[3]
			eventType = splitKey[4]
		}
		successCount := successCountMap[key]
		failedCount := failedCountMap[key]
		filteredCount := filteredCountMap[key]
		diff := successCount + failedCount + filteredCount - inCount
		if diff != 0 {
			metricMetadata := inCountMetadataMap[key]
			metric := &types.PUReportedMetric{
				ConnectionDetails: *types.CreateConnectionDetail(metricMetadata.sourceID, metricMetadata.destinationID, metricMetadata.sourceTaskRunID, metricMetadata.sourceJobID, metricMetadata.sourceJobRunID, metricMetadata.sourceDefinitionID, metricMetadata.destinationDefinitionID, metricMetadata.sourceCategory, metricMetadata.transformationID, metricMetadata.transformationVersionID, metricMetadata.trackingPlanID, metricMetadata.trackingPlanVersion),
				PUDetails:         *types.CreatePUDetails(inPU, pu, false, false),
				StatusDetail:      types.CreateStatusDetail(types.DiffStatus, diff, 0, 0, "", []byte(`{}`), eventName, eventType, ""),
			}
			diffMetrics = append(diffMetrics, metric)
		}
	}

	return diffMetrics
}

type dupStatKey struct {
	sourceID  string
	equalSize bool
}

func (proc *Handle) eventAuditEnabled(workspaceID string) bool {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.eventAuditEnabled[workspaceID]
}

func (proc *Handle) processJobsForDest(partition string, subJobs subJob) *transformationMessage {
	if proc.limiter.preprocess != nil {
		defer proc.limiter.preprocess.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
	}

	jobList := subJobs.subJobs
	start := time.Now()

	proc.stats.statNumRequests.Count(len(jobList))

	var statusList []*jobsdb.JobStatusT
	groupedEvents := make(map[string][]transformer.TransformerEvent)
	groupedEventsBySourceId := make(map[SourceIDT][]transformer.TransformerEvent)
	eventsByMessageID := make(map[string]types.SingularEventWithReceivedAt)
	var procErrorJobs []*jobsdb.JobT
	eventSchemaJobs := make([]*jobsdb.JobT, 0)
	archivalJobs := make([]*jobsdb.JobT, 0)

	// Each block we receive from a client has a bunch of
	// requests. We parse the block and take out individual
	// requests, call the destination specific transformation
	// function and create jobs for them.
	// Transformation is called for a batch of jobs at a time
	// to speed-up execution.

	// Event count for performance stat monitoring
	totalEvents := 0

	proc.logger.Debug("[Processor] Total jobs picked up : ", len(jobList))

	marshalStart := time.Now()
	dedupKeys := make(map[string]struct{})
	uniqueMessageIdsBySrcDestKey := make(map[string]map[string]struct{})
	sourceDupStats := make(map[dupStatKey]int)

	reportMetrics := make([]*types.PUReportedMetric, 0)
	inCountMap := make(map[string]int64)
	inCountMetadataMap := make(map[string]MetricMetadata)
	connectionDetailsMap := make(map[string]*types.ConnectionDetails)
	statusDetailsMap := make(map[string]map[string]*types.StatusDetail)

	outCountMap := make(map[string]int64) // destinations enabled
	destFilterStatusDetailMap := make(map[string]map[string]*types.StatusDetail)

	for _, batchEvent := range jobList {

		var gatewayBatchEvent types.GatewayBatchRequest
		err := jsonfast.Unmarshal(batchEvent.EventPayload, &gatewayBatchEvent)
		if err != nil {
			proc.logger.Warnf("json parsing of event payload for %s: %v", batchEvent.JobID, err)
			gatewayBatchEvent.Batch = []types.SingularEventT{}
		}
		var eventParams types.EventParams
		err = jsonfast.Unmarshal(batchEvent.Parameters, &eventParams)
		if err != nil {
			panic(err)
		}
		sourceId := eventParams.SourceId
		requestIP := gatewayBatchEvent.RequestIP
		receivedAt := gatewayBatchEvent.ReceivedAt

		newStatus := jobsdb.JobStatusT{
			JobID:         batchEvent.JobID,
			JobState:      jobsdb.Succeeded.State,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{"success":"OK"}`),
			Parameters:    []byte(`{}`),
			JobParameters: batchEvent.Parameters,
			WorkspaceId:   batchEvent.WorkspaceId,
		}
		statusList = append(statusList, &newStatus)

		source, err := proc.getSourceBySourceID(sourceId)
		if err != nil {
			continue
		}

		for _, enricher := range proc.enrichers {
			if err := enricher.Enrich(source, &gatewayBatchEvent); err != nil {
				proc.logger.Errorf("unable to enrich the gateway batch event: %v", err.Error())
			}
		}

		// Iterate through all the events in the batch
		for _, singularEvent := range gatewayBatchEvent.Batch {
			messageId := misc.GetStringifiedData(singularEvent["messageId"])

			payloadFunc := ro.Memoize(func() json.RawMessage {
				payloadBytes, err := jsonfast.Marshal(singularEvent)
				if err != nil {
					return nil
				}
				return payloadBytes
			})

			if proc.config.enableDedup {
				p := payloadFunc()
				messageSize := int64(len(p))
				dedupKey := fmt.Sprintf("%v%v", messageId, eventParams.SourceJobRunId)
				if ok, previousSize := proc.dedup.Set(dedup.KeyValue{Key: dedupKey, Value: messageSize}); !ok {
					proc.logger.Debugf("Dropping event with duplicate dedupKey: %s", dedupKey)
					sourceDupStats[dupStatKey{sourceID: source.ID, equalSize: messageSize == previousSize}] += 1
					continue
				}
				dedupKeys[dedupKey] = struct{}{}
			}

			proc.updateSourceEventStatsDetailed(singularEvent, sourceId)

			// We count this as one, not destination specific ones
			totalEvents++
			eventsByMessageID[messageId] = types.SingularEventWithReceivedAt{
				SingularEvent: singularEvent,
				ReceivedAt:    receivedAt,
			}

			commonMetadataFromSingularEvent := makeCommonMetadataFromSingularEvent(
				singularEvent,
				batchEvent,
				receivedAt,
				source,
				eventParams,
			)

			sourceIsTransient := proc.transientSources.Apply(source.ID)
			if proc.config.eventSchemaV2Enabled && // schemas enabled
				proc.eventAuditEnabled(batchEvent.WorkspaceId) &&
				// TODO: could use source.SourceDefinition.Category instead?
				commonMetadataFromSingularEvent.SourceJobRunID == "" &&
				!sourceIsTransient {
				if eventPayload := payloadFunc(); eventPayload != nil {
					eventSchemaJobs = append(eventSchemaJobs,
						&jobsdb.JobT{
							UUID:         batchEvent.UUID,
							UserID:       batchEvent.UserID,
							Parameters:   batchEvent.Parameters,
							CustomVal:    batchEvent.CustomVal,
							EventPayload: eventPayload,
							CreatedAt:    time.Now(),
							ExpireAt:     time.Now(),
							WorkspaceId:  batchEvent.WorkspaceId,
						},
					)
				}
			}

			if proc.config.archivalEnabled.Load() &&
				commonMetadataFromSingularEvent.SourceJobRunID == "" && // archival enabled&&
				!sourceIsTransient {
				if eventPayload := payloadFunc(); eventPayload != nil {
					archivalJobs = append(archivalJobs,
						&jobsdb.JobT{
							UUID:         batchEvent.UUID,
							UserID:       batchEvent.UserID,
							Parameters:   batchEvent.Parameters,
							CustomVal:    batchEvent.CustomVal,
							EventPayload: eventPayload,
							CreatedAt:    time.Now(),
							ExpireAt:     time.Now(),
							WorkspaceId:  batchEvent.WorkspaceId,
						},
					)
				}
			}

			// REPORTING - GATEWAY metrics - START
			// dummy event for metrics purposes only
			event := &transformer.TransformerResponse{}
			if proc.isReportingEnabled() {
				event.Metadata = *commonMetadataFromSingularEvent
				proc.updateMetricMaps(
					inCountMetadataMap,
					inCountMap,
					connectionDetailsMap,
					statusDetailsMap,
					event,
					jobsdb.Succeeded.State,
					types.GATEWAY,
					func() json.RawMessage {
						if sourceIsTransient {
							return []byte(`{}`)
						}
						if payload := payloadFunc(); payload != nil {
							return payload
						}
						return []byte("{}")
					},
					nil,
				)
			}
			// REPORTING - GATEWAY metrics - END

			// Getting all the destinations which are enabled for this
			// event
			if !proc.isDestinationAvailable(singularEvent, sourceId) {
				continue
			}

			if _, ok := groupedEventsBySourceId[SourceIDT(sourceId)]; !ok {
				groupedEventsBySourceId[SourceIDT(sourceId)] = make([]transformer.TransformerEvent, 0)
			}
			shallowEventCopy := transformer.TransformerEvent{}
			shallowEventCopy.Message = singularEvent
			shallowEventCopy.Message["request_ip"] = requestIP
			enhanceWithTimeFields(&shallowEventCopy, singularEvent, receivedAt)
			enhanceWithMetadata(
				commonMetadataFromSingularEvent,
				&shallowEventCopy,
				&backendconfig.DestinationT{},
			)

			// TODO: TP ID preference 1.event.context set by rudderTyper   2.From WorkSpaceConfig (currently being used)
			shallowEventCopy.Metadata.TrackingPlanId = source.DgSourceTrackingPlanConfig.TrackingPlan.Id
			shallowEventCopy.Metadata.TrackingPlanVersion = source.DgSourceTrackingPlanConfig.TrackingPlan.Version
			shallowEventCopy.Metadata.SourceTpConfig = source.DgSourceTrackingPlanConfig.Config
			shallowEventCopy.Metadata.MergedTpConfig = source.DgSourceTrackingPlanConfig.GetMergedConfig(commonMetadataFromSingularEvent.EventType)

			groupedEventsBySourceId[SourceIDT(sourceId)] = append(groupedEventsBySourceId[SourceIDT(sourceId)], shallowEventCopy)

			if proc.isReportingEnabled() {
				proc.updateMetricMaps(inCountMetadataMap, outCountMap, connectionDetailsMap, destFilterStatusDetailMap, event, jobsdb.Succeeded.State, types.DESTINATION_FILTER, func() json.RawMessage { return []byte(`{}`) }, nil)
			}
		}

	}

	g, groupCtx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		if len(eventSchemaJobs) == 0 {
			return nil
		}
		err := misc.RetryWithNotify(
			groupCtx,
			proc.jobsDBCommandTimeout.Load(),
			proc.jobdDBMaxRetries.Load(),
			func(ctx context.Context) error {
				return proc.eventSchemaDB.WithStoreSafeTx(
					ctx,
					func(tx jobsdb.StoreSafeTx) error {
						return proc.eventSchemaDB.StoreInTx(ctx, tx, eventSchemaJobs)
					},
				)
			}, proc.sendRetryStoreStats)
		if err != nil {
			return fmt.Errorf("store into event schema table failed with error: %v", err)
		}
		proc.logger.Debug("[Processor] Total jobs written to event_schema: ", len(eventSchemaJobs))
		return nil
	})

	g.Go(func() error {
		if len(archivalJobs) == 0 {
			return nil
		}
		err := misc.RetryWithNotify(
			groupCtx,
			proc.jobsDBCommandTimeout.Load(),
			proc.jobdDBMaxRetries.Load(),
			func(ctx context.Context) error {
				return proc.archivalDB.WithStoreSafeTx(
					ctx,
					func(tx jobsdb.StoreSafeTx) error {
						return proc.archivalDB.StoreInTx(ctx, tx, archivalJobs)
					},
				)
			}, proc.sendRetryStoreStats)
		if err != nil {
			return fmt.Errorf("store into archival table failed with error: %v", err)
		}
		proc.logger.Debug("[Processor] Total jobs written to archiver: ", len(archivalJobs))
		return nil
	})

	if err := g.Wait(); err != nil {
		panic(err)
	}

	// REPORTING - GATEWAY metrics - START
	if proc.isReportingEnabled() {
		types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)
		for k, cd := range connectionDetailsMap {
			for _, sd := range statusDetailsMap[k] {
				m := &types.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *types.CreatePUDetails("", types.GATEWAY, false, true),
					StatusDetail:      sd,
				}
				reportMetrics = append(reportMetrics, m)
			}

			for _, dsd := range destFilterStatusDetailMap[k] {
				destFilterMetric := &types.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *types.CreatePUDetails(types.GATEWAY, types.DESTINATION_FILTER, false, false),
					StatusDetail:      dsd,
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
			map[string]int64{},
		)
		reportMetrics = append(reportMetrics, diffMetrics...)
	}
	// REPORTING - GATEWAY metrics - END

	proc.stats.statNumEvents.Count(totalEvents)

	marshalTime := time.Since(marshalStart)
	defer proc.stats.marshalSingularEvents.SendTiming(marshalTime)

	// TRACKING PLAN - START
	// Placing the trackingPlan validation filters here.
	// Else further down events are duplicated by destId, so multiple validation takes places for same event
	validateEventsStart := time.Now()
	validatedEventsBySourceId, validatedReportMetrics, validatedErrorJobs, trackingPlanEnabledMap := proc.validateEvents(groupedEventsBySourceId, eventsByMessageID)
	validateEventsTime := time.Since(validateEventsStart)
	defer proc.stats.validateEventsTime.SendTiming(validateEventsTime)

	// Appending validatedErrorJobs to procErrorJobs
	procErrorJobs = append(procErrorJobs, validatedErrorJobs...)

	// Appending validatedReportMetrics to reportMetrics
	reportMetrics = append(reportMetrics, validatedReportMetrics...)
	// TRACKING PLAN - END

	// The below part further segregates events by sourceID and DestinationID.
	for sourceIdT, eventList := range validatedEventsBySourceId {
		for idx := range eventList {
			event := &eventList[idx]
			sourceId := string(sourceIdT)
			singularEvent := event.Message

			backendEnabledDestTypes := proc.getBackendEnabledDestinationTypes(sourceId)
			enabledDestTypes := integrations.FilterClientIntegrations(singularEvent, backendEnabledDestTypes)
			workspaceID := eventList[idx].Metadata.WorkspaceID
			workspaceLibraries := proc.getWorkspaceLibraries(workspaceID)
			source, _ := proc.getSourceBySourceID(sourceId)

			for i := range enabledDestTypes {
				destType := &enabledDestTypes[i]
				enabledDestinationsList := proc.filterDestinations(
					singularEvent,
					proc.getEnabledDestinations(sourceId, *destType),
				)

				// Adding a singular event multiple times if there are multiple destinations of same type
				for idx := range enabledDestinationsList {
					destination := &enabledDestinationsList[idx]
					shallowEventCopy := transformer.TransformerEvent{}
					shallowEventCopy.Message = singularEvent
					shallowEventCopy.Destination = *destination
					shallowEventCopy.Libraries = workspaceLibraries
					// just in-case the source-type value is not set
					if event.Metadata.SourceDefinitionType == "" {
						event.Metadata.SourceDefinitionType = source.SourceDefinition.Type
					}
					shallowEventCopy.Metadata = event.Metadata

					// At the TP flow we are not having destination information, so adding it here.
					shallowEventCopy.Metadata.DestinationID = destination.ID
					shallowEventCopy.Metadata.DestinationType = destination.DestinationDefinition.Name
					if len(destination.Transformations) > 0 {
						shallowEventCopy.Metadata.TransformationID = destination.Transformations[0].ID
						shallowEventCopy.Metadata.TransformationVersionID = destination.Transformations[0].VersionID
					}
					filterConfig(&shallowEventCopy)
					metadata := shallowEventCopy.Metadata
					srcAndDestKey := getKeyFromSourceAndDest(metadata.SourceID, metadata.DestinationID)
					// We have at-least one event so marking it good
					_, ok := groupedEvents[srcAndDestKey]
					if !ok {
						groupedEvents[srcAndDestKey] = make([]transformer.TransformerEvent, 0)
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
	// processJob throughput per second.
	proc.stats.processJobThroughput.Count(processJobThroughput)
	return &transformationMessage{
		groupedEvents,
		trackingPlanEnabledMap,
		eventsByMessageID,
		uniqueMessageIdsBySrcDestKey,
		reportMetrics,
		statusList,
		procErrorJobs,
		sourceDupStats,
		dedupKeys,

		totalEvents,
		start,

		subJobs.hasMore,
		subJobs.rsourcesStats,
	}
}

type transformationMessage struct {
	groupedEvents map[string][]transformer.TransformerEvent

	trackingPlanEnabledMap       map[SourceIDT]bool
	eventsByMessageID            map[string]types.SingularEventWithReceivedAt
	uniqueMessageIdsBySrcDestKey map[string]map[string]struct{}
	reportMetrics                []*types.PUReportedMetric
	statusList                   []*jobsdb.JobStatusT
	procErrorJobs                []*jobsdb.JobT
	sourceDupStats               map[dupStatKey]int
	dedupKeys                    map[string]struct{}

	totalEvents int
	start       time.Time

	hasMore       bool
	rsourcesStats rsources.StatsCollector
}

func (proc *Handle) transformations(partition string, in *transformationMessage) *storeMessage {
	if proc.limiter.transform != nil {
		defer proc.limiter.transform.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
	}
	// Now do the actual transformation. We call it in batches, once
	// for each destination ID

	ctx, task := trace.NewTask(context.Background(), "transformations")
	defer task.End()

	procErrorJobsByDestID := make(map[string][]*jobsdb.JobT)
	var batchDestJobs []*jobsdb.JobT
	var destJobs []*jobsdb.JobT
	var droppedJobs []*jobsdb.JobT
	routerDestIDs := make(map[string]struct{})

	destProcStart := time.Now()

	chOut := make(chan transformSrcDestOutput, 1)
	wg := sync.WaitGroup{}
	wg.Add(len(in.groupedEvents))

	for srcAndDestKey, eventList := range in.groupedEvents {
		srcAndDestKey, eventList := srcAndDestKey, eventList
		rruntime.Go(func() {
			defer wg.Done()
			chOut <- proc.transformSrcDest(
				ctx,

				srcAndDestKey, eventList,

				in.trackingPlanEnabledMap,
				in.eventsByMessageID,
				in.uniqueMessageIdsBySrcDestKey,
			)
		})
	}
	rruntime.Go(func() {
		wg.Wait()
		close(chOut)
	})

	for o := range chOut {
		destJobs = append(destJobs, o.destJobs...)
		batchDestJobs = append(batchDestJobs, o.batchDestJobs...)
		droppedJobs = append(droppedJobs, o.droppedJobs...)
		routerDestIDs = lo.Assign(routerDestIDs, o.routerDestIDs)
		in.reportMetrics = append(in.reportMetrics, o.reportMetrics...)
		for k, v := range o.errorsPerDestID {
			procErrorJobsByDestID[k] = append(procErrorJobsByDestID[k], v...)
		}
	}

	destProcTime := time.Since(destProcStart)
	defer proc.stats.destProcessing.SendTiming(destProcTime)

	// this tells us how many transformations we are doing per second.
	transformationsThroughput := throughputPerSecond(in.totalEvents, destProcTime)
	proc.stats.transformationsThroughput.Count(transformationsThroughput)

	return &storeMessage{
		in.statusList,
		destJobs,
		batchDestJobs,
		droppedJobs,

		procErrorJobsByDestID,
		in.procErrorJobs,
		lo.Keys(routerDestIDs),

		in.reportMetrics,
		in.sourceDupStats,
		in.dedupKeys,
		in.totalEvents,
		in.start,
		in.hasMore,
		in.rsourcesStats,
	}
}

type storeMessage struct {
	statusList    []*jobsdb.JobStatusT
	destJobs      []*jobsdb.JobT
	batchDestJobs []*jobsdb.JobT
	droppedJobs   []*jobsdb.JobT

	procErrorJobsByDestID map[string][]*jobsdb.JobT
	procErrorJobs         []*jobsdb.JobT
	routerDestIDs         []string

	reportMetrics  []*types.PUReportedMetric
	sourceDupStats map[dupStatKey]int
	dedupKeys      map[string]struct{}

	totalEvents int
	start       time.Time

	hasMore       bool
	rsourcesStats rsources.StatsCollector
}

func (sm *storeMessage) merge(subJob *storeMessage) {
	sm.statusList = append(sm.statusList, subJob.statusList...)
	sm.destJobs = append(sm.destJobs, subJob.destJobs...)
	sm.batchDestJobs = append(sm.batchDestJobs, subJob.batchDestJobs...)
	sm.droppedJobs = append(sm.droppedJobs, subJob.droppedJobs...)

	sm.procErrorJobs = append(sm.procErrorJobs, subJob.procErrorJobs...)
	for id, job := range subJob.procErrorJobsByDestID {
		sm.procErrorJobsByDestID[id] = append(sm.procErrorJobsByDestID[id], job...)
	}
	sm.routerDestIDs = append(sm.routerDestIDs, subJob.routerDestIDs...)

	sm.reportMetrics = append(sm.reportMetrics, subJob.reportMetrics...)
	for dupStatKey, count := range subJob.sourceDupStats {
		sm.sourceDupStats[dupStatKey] += count
	}
	for id, v := range subJob.dedupKeys {
		sm.dedupKeys[id] = v
	}
	sm.totalEvents += subJob.totalEvents
}

func (proc *Handle) sendRetryStoreStats(attempt int) {
	proc.logger.Warnf("Timeout during store jobs in processor module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_store_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "processor"}).Count(1)
}

func (proc *Handle) sendRetryUpdateStats(attempt int) {
	proc.logger.Warnf("Timeout during update job status in processor module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_update_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "processor"}).Count(1)
}

func (proc *Handle) sendQueryRetryStats(attempt int) {
	proc.logger.Warnf("Timeout during query jobs in processor module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "processor"}).Count(1)
}

func (proc *Handle) Store(partition string, in *storeMessage) {
	if proc.limiter.store != nil {
		defer proc.limiter.store.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
	}

	statusList, destJobs, batchDestJobs := in.statusList, in.destJobs, in.batchDestJobs
	beforeStoreStatus := time.Now()
	// XX: Need to do this in a transaction
	if len(batchDestJobs) > 0 {
		err := misc.RetryWithNotify(
			context.Background(),
			proc.jobsDBCommandTimeout.Load(),
			proc.jobdDBMaxRetries.Load(),
			func(ctx context.Context) error {
				return proc.batchRouterDB.WithStoreSafeTx(
					ctx,
					func(tx jobsdb.StoreSafeTx) error {
						err := proc.batchRouterDB.StoreInTx(ctx, tx, batchDestJobs)
						if err != nil {
							return fmt.Errorf("storing batch router jobs: %w", err)
						}

						// rsources stats
						err = proc.updateRudderSourcesStats(ctx, tx, batchDestJobs)
						if err != nil {
							return fmt.Errorf("publishing rsources stats for batch router: %w", err)
						}
						return nil
					})
			}, proc.sendRetryStoreStats)
		if err != nil {
			panic(err)
		}
		proc.logger.Debug("[Processor] Total jobs written to batch router : ", len(batchDestJobs))

		proc.IncreasePendingEvents("batch_rt", getJobCountsByWorkspaceDestType(batchDestJobs))
		proc.stats.statBatchDestNumOutputEvents.Count(len(batchDestJobs))
		proc.stats.statDBWriteBatchEvents.Observe(float64(len(batchDestJobs)))
		proc.stats.statDBWriteBatchPayloadBytes.Observe(
			float64(lo.SumBy(destJobs, func(j *jobsdb.JobT) int { return len(j.EventPayload) })),
		)
	}

	if len(destJobs) > 0 {
		func() {
			// Only one goroutine can store to a router destination at a time, otherwise we may have different transactions
			// committing at different timestamps which can cause events with lower jobIDs to appear after events with higher ones.
			// For that purpose, before storing, we lock the relevant destination IDs (in sorted order to avoid deadlocks).
			if len(in.routerDestIDs) > 0 {
				destIDs := lo.Uniq(in.routerDestIDs)
				slices.Sort(destIDs)
				for _, destID := range destIDs {
					proc.storePlocker.Lock(destID)
					defer proc.storePlocker.Unlock(destID)
				}
			} else {
				proc.logger.Warnw("empty storeMessage.routerDestIDs",
					"expected",
					lo.Uniq(
						lo.Map(destJobs, func(j *jobsdb.JobT, _ int) string {
							return gjson.GetBytes(j.Parameters, "destination_id").String()
						}),
					))
			}
			err := misc.RetryWithNotify(
				context.Background(),
				proc.jobsDBCommandTimeout.Load(),
				proc.jobdDBMaxRetries.Load(),
				func(ctx context.Context) error {
					return proc.routerDB.WithStoreSafeTx(
						ctx,
						func(tx jobsdb.StoreSafeTx) error {
							err := proc.routerDB.StoreInTx(ctx, tx, destJobs)
							if err != nil {
								return fmt.Errorf("storing router jobs: %w", err)
							}

							// rsources stats
							err = proc.updateRudderSourcesStats(ctx, tx, destJobs)
							if err != nil {
								return fmt.Errorf("publishing rsources stats for router: %w", err)
							}
							return nil
						})
				}, proc.sendRetryStoreStats)
			if err != nil {
				panic(err)
			}
			proc.logger.Debug("[Processor] Total jobs written to router : ", len(destJobs))
			proc.IncreasePendingEvents("rt", getJobCountsByWorkspaceDestType(destJobs))
			proc.stats.statDestNumOutputEvents.Count(len(destJobs))
			proc.stats.statDBWriteRouterEvents.Observe(float64(len(destJobs)))
			proc.stats.statDBWriteRouterPayloadBytes.Observe(
				float64(lo.SumBy(destJobs, func(j *jobsdb.JobT) int { return len(j.EventPayload) })),
			)
		}()
	}

	for _, jobs := range in.procErrorJobsByDestID {
		in.procErrorJobs = append(in.procErrorJobs, jobs...)
	}
	if len(in.procErrorJobs) > 0 {
		err := misc.RetryWithNotify(context.Background(), proc.jobsDBCommandTimeout.Load(), proc.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
			return proc.writeErrorDB.Store(ctx, in.procErrorJobs)
		}, proc.sendRetryStoreStats)
		if err != nil {
			proc.logger.Errorf("Store into proc error table failed with error: %v", err)
			proc.logger.Errorf("procErrorJobs: %v", in.procErrorJobs)
			panic(err)
		}
		proc.logger.Debug("[Processor] Total jobs written to proc_error: ", len(in.procErrorJobs))
		proc.recordEventDeliveryStatus(in.procErrorJobsByDestID)
	}

	writeJobsTime := time.Since(beforeStoreStatus)

	txnStart := time.Now()
	err := misc.RetryWithNotify(context.Background(), proc.jobsDBCommandTimeout.Load(), proc.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return proc.gatewayDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
			err := proc.gatewayDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{proc.config.GWCustomVal}, nil)
			if err != nil {
				return fmt.Errorf("updating gateway jobs statuses: %w", err)
			}

			// rsources stats
			in.rsourcesStats.CollectStats(statusList)
			err = in.rsourcesStats.Publish(ctx, tx.SqlTx())
			if err != nil {
				return fmt.Errorf("publishing rsources stats: %w", err)
			}
			err = proc.saveDroppedJobs(in.droppedJobs, tx.Tx())
			if err != nil {
				return fmt.Errorf("saving dropped jobs: %w", err)
			}

			if proc.isReportingEnabled() {
				if err = proc.reporting.Report(in.reportMetrics, tx.Tx()); err != nil {
					return fmt.Errorf("reporting metrics: %w", err)
				}
			}

			return nil
		})
	}, proc.sendRetryUpdateStats)
	if err != nil {
		panic(err)
	}
	if proc.config.enableDedup {
		proc.updateSourceStats(in.sourceDupStats, "processor.write_key_duplicate_events")
		if len(in.dedupKeys) > 0 {
			if err := proc.dedup.Commit(lo.Keys(in.dedupKeys)); err != nil {
				panic(err)
			}
		}
	}
	proc.stats.statDBW.Since(beforeStoreStatus)
	dbWriteTime := time.Since(beforeStoreStatus)
	// DB write throughput per second.
	dbWriteThroughput := throughputPerSecond(len(destJobs)+len(batchDestJobs), dbWriteTime)
	proc.stats.DBWriteThroughput.Count(dbWriteThroughput)
	proc.stats.statDBWriteJobsTime.SendTiming(writeJobsTime)
	proc.stats.statDBWriteStatusTime.Since(txnStart)
	proc.logger.Debugf("Processor GW DB Write Complete. Total Processed: %v", len(statusList))
	// XX: End of transaction

	proc.stats.statGatewayDBW.Count(len(statusList))
	proc.stats.statRouterDBW.Count(len(destJobs))
	proc.stats.statBatchRouterDBW.Count(len(batchDestJobs))
	proc.stats.statProcErrDBW.Count(len(in.procErrorJobs))
}

// getJobCountsByWorkspaceDestType returns the number of jobs per workspace and destination type
//
// map[workspaceID]map[destType]count
func getJobCountsByWorkspaceDestType(jobs []*jobsdb.JobT) map[string]map[string]int {
	jobCounts := make(map[string]map[string]int)
	for _, job := range jobs {
		workspace := job.WorkspaceId
		destType := job.CustomVal
		if _, ok := jobCounts[workspace]; !ok {
			jobCounts[workspace] = make(map[string]int)
		}
		jobCounts[workspace][destType] += 1
	}
	return jobCounts
}

type transformSrcDestOutput struct {
	reportMetrics   []*types.PUReportedMetric
	destJobs        []*jobsdb.JobT
	batchDestJobs   []*jobsdb.JobT
	errorsPerDestID map[string][]*jobsdb.JobT
	routerDestIDs   map[string]struct{}
	droppedJobs     []*jobsdb.JobT
}

func (proc *Handle) transformSrcDest(
	ctx context.Context,
	// main inputs
	srcAndDestKey string, eventList []transformer.TransformerEvent,

	// helpers
	trackingPlanEnabledMap map[SourceIDT]bool,
	eventsByMessageID map[string]types.SingularEventWithReceivedAt,
	uniqueMessageIdsBySrcDestKey map[string]map[string]struct{},
) transformSrcDestOutput {
	defer proc.stats.pipeProcessing.Since(time.Now())

	sourceID, destID := getSourceAndDestIDsFromKey(srcAndDestKey)
	destination := &eventList[0].Destination
	workspaceID := eventList[0].Metadata.WorkspaceID
	commonMetaData := &transformer.Metadata{
		SourceID:             sourceID,
		SourceType:           eventList[0].Metadata.SourceType,
		SourceCategory:       eventList[0].Metadata.SourceCategory,
		WorkspaceID:          workspaceID,
		Namespace:            config.GetKubeNamespace(),
		InstanceID:           misc.GetInstanceID(),
		DestinationID:        destID,
		DestinationType:      destination.DestinationDefinition.Name,
		SourceDefinitionType: eventList[0].Metadata.SourceDefinitionType,
	}

	reportMetrics := make([]*types.PUReportedMetric, 0)
	batchDestJobs := make([]*jobsdb.JobT, 0)
	destJobs := make([]*jobsdb.JobT, 0)
	routerDestIDs := make(map[string]struct{})
	procErrorJobsByDestID := make(map[string][]*jobsdb.JobT)
	droppedJobs := make([]*jobsdb.JobT, 0)

	proc.config.configSubscriberLock.RLock()
	destType := proc.config.destinationIDtoTypeMap[destID]
	transformationEnabled := len(destination.Transformations) > 0
	proc.config.configSubscriberLock.RUnlock()

	trackingPlanEnabled := trackingPlanEnabledMap[SourceIDT(sourceID)]

	var inCountMap map[string]int64
	var inCountMetadataMap map[string]MetricMetadata

	// REPORTING - START
	if proc.isReportingEnabled() {
		// Grouping events by sourceid + destinationid + jobruniD + eventName + eventType to find the count
		inCountMap = make(map[string]int64)
		inCountMetadataMap = make(map[string]MetricMetadata)
		for i := range eventList {
			event := &eventList[i]
			key := strings.Join([]string{
				event.Metadata.SourceID,
				event.Metadata.DestinationID,
				event.Metadata.SourceJobRunID,
				event.Metadata.EventName,
				event.Metadata.EventType,
			}, MetricKeyDelimiter)
			if _, ok := inCountMap[key]; !ok {
				inCountMap[key] = 0
			}
			if _, ok := inCountMetadataMap[key]; !ok {
				inCountMetadataMap[key] = MetricMetadata{
					sourceID:                event.Metadata.SourceID,
					destinationID:           event.Metadata.DestinationID,
					sourceTaskRunID:         event.Metadata.SourceTaskRunID,
					sourceJobID:             event.Metadata.SourceJobID,
					sourceJobRunID:          event.Metadata.SourceJobRunID,
					sourceDefinitionID:      event.Metadata.SourceDefinitionID,
					destinationDefinitionID: event.Metadata.DestinationDefinitionID,
					sourceCategory:          event.Metadata.SourceCategory,
					transformationID:        event.Metadata.TransformationID,
					transformationVersionID: event.Metadata.TransformationVersionID,
					trackingPlanID:          event.Metadata.TrackingPlanId,
					trackingPlanVersion:     event.Metadata.TrackingPlanVersion,
				}
			}
			inCountMap[key] = inCountMap[key] + 1
		}
	}
	// REPORTING - END

	var response transformer.Response
	var eventsToTransform []transformer.TransformerEvent
	// Send to custom transformer only if the destination has a transformer enabled
	if transformationEnabled {
		userTransformationStat := proc.newUserTransformationStat(sourceID, workspaceID, destination)
		userTransformationStat.numEvents.Count(len(eventList))
		proc.logger.Debug("Custom Transform input size", len(eventList))

		trace.WithRegion(ctx, "UserTransform", func() {
			startedAt := time.Now()
			response = proc.transformer.UserTransform(ctx, eventList, proc.config.userTransformBatchSize.Load())
			d := time.Since(startedAt)
			userTransformationStat.transformTime.SendTiming(d)

			var successMetrics []*types.PUReportedMetric
			var successCountMap map[string]int64
			var successCountMetadataMap map[string]MetricMetadata
			eventsToTransform, successMetrics, successCountMap, successCountMetadataMap = proc.getDestTransformerEvents(response, commonMetaData, eventsByMessageID, destination, transformer.UserTransformerStage, trackingPlanEnabled, transformationEnabled)
			nonSuccessMetrics := proc.getNonSuccessfulMetrics(response, commonMetaData, eventsByMessageID, transformer.UserTransformerStage, transformationEnabled, trackingPlanEnabled)
			droppedJobs = append(droppedJobs, append(proc.getDroppedJobs(response, eventList), append(nonSuccessMetrics.failedJobs, nonSuccessMetrics.filteredJobs...)...)...)
			if _, ok := procErrorJobsByDestID[destID]; !ok {
				procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
			}
			procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], nonSuccessMetrics.failedJobs...)
			userTransformationStat.numOutputSuccessEvents.Count(len(eventsToTransform))
			userTransformationStat.numOutputFailedEvents.Count(len(nonSuccessMetrics.failedJobs))
			userTransformationStat.numOutputFilteredEvents.Count(len(nonSuccessMetrics.filteredJobs))
			proc.logger.Debug("Custom Transform output size", len(eventsToTransform))
			trace.Logf(ctx, "UserTransform", "User Transform output size: %d", len(eventsToTransform))

			proc.transDebugger.UploadTransformationStatus(&transformationdebugger.TransformationStatusT{SourceID: sourceID, DestID: destID, Destination: destination, UserTransformedEvents: eventsToTransform, EventsByMessageID: eventsByMessageID, FailedEvents: response.FailedEvents, UniqueMessageIds: uniqueMessageIdsBySrcDestKey[srcAndDestKey]})

			// REPORTING - START
			if proc.isReportingEnabled() {
				diffMetrics := getDiffMetrics(
					types.DESTINATION_FILTER,
					types.USER_TRANSFORMER,
					inCountMetadataMap,
					inCountMap,
					successCountMap,
					nonSuccessMetrics.failedCountMap,
					nonSuccessMetrics.filteredCountMap,
				)
				reportMetrics = append(reportMetrics, successMetrics...)
				reportMetrics = append(reportMetrics, nonSuccessMetrics.failedMetrics...)
				reportMetrics = append(reportMetrics, nonSuccessMetrics.filteredMetrics...)
				reportMetrics = append(reportMetrics, diffMetrics...)

				// successCountMap will be inCountMap for filtering events based on supported event types
				inCountMap = successCountMap
				inCountMetadataMap = successCountMetadataMap
			}
			// REPORTING - END
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
			routerDestIDs:   routerDestIDs,
			droppedJobs:     droppedJobs,
		}
	}

	transformAt := "processor"
	if val, ok := destination.DestinationDefinition.Config["transformAtV1"].(string); ok {
		transformAt = val
	}
	// Check for overrides through env
	transformAtOverrideFound := config.IsSet("Processor." + destination.DestinationDefinition.Name + ".transformAt")
	if transformAtOverrideFound {
		transformAt = config.GetString("Processor."+destination.DestinationDefinition.Name+".transformAt", "processor")
	}
	transformAtFromFeaturesFile := gjson.Get(string(proc.transformerFeatures), fmt.Sprintf("routerTransform.%s", destination.DestinationDefinition.Name)).String()

	// Filtering events based on the supported message types - START
	s := time.Now()
	eventFilterInCount := len(eventsToTransform)
	proc.logger.Debug("Supported messages filtering input size", eventFilterInCount)
	response = ConvertToFilteredTransformerResponse(eventsToTransform, transformAt != "none")
	var successMetrics []*types.PUReportedMetric
	var successCountMap map[string]int64
	var successCountMetadataMap map[string]MetricMetadata
	nonSuccessMetrics := proc.getNonSuccessfulMetrics(response, commonMetaData, eventsByMessageID, transformer.EventFilterStage, transformationEnabled, trackingPlanEnabled)
	droppedJobs = append(droppedJobs, append(proc.getDroppedJobs(response, eventsToTransform), append(nonSuccessMetrics.failedJobs, nonSuccessMetrics.filteredJobs...)...)...)
	if _, ok := procErrorJobsByDestID[destID]; !ok {
		procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
	}
	procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], nonSuccessMetrics.failedJobs...)
	eventsToTransform, successMetrics, successCountMap, successCountMetadataMap = proc.getDestTransformerEvents(response, commonMetaData, eventsByMessageID, destination, transformer.EventFilterStage, trackingPlanEnabled, transformationEnabled)
	proc.logger.Debug("Supported messages filtering output size", len(eventsToTransform))

	// REPORTING - START
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

		diffMetrics := getDiffMetrics(inPU, types.EVENT_FILTER, inCountMetadataMap, inCountMap, successCountMap, nonSuccessMetrics.failedCountMap, nonSuccessMetrics.filteredCountMap)
		reportMetrics = append(reportMetrics, successMetrics...)
		reportMetrics = append(reportMetrics, nonSuccessMetrics.failedMetrics...)
		reportMetrics = append(reportMetrics, nonSuccessMetrics.filteredMetrics...)
		reportMetrics = append(reportMetrics, diffMetrics...)

		// successCountMap will be inCountMap for destination transform
		inCountMap = successCountMap
		inCountMetadataMap = successCountMetadataMap
	}
	// REPORTING - END
	eventFilterStat := proc.newEventFilterStat(sourceID, workspaceID, destination)
	eventFilterStat.numEvents.Count(eventFilterInCount)
	eventFilterStat.numOutputSuccessEvents.Count(len(response.Events))
	eventFilterStat.numOutputFailedEvents.Count(len(nonSuccessMetrics.failedJobs))
	eventFilterStat.numOutputFilteredEvents.Count(len(nonSuccessMetrics.filteredJobs))
	eventFilterStat.transformTime.Since(s)

	// Filtering events based on the supported message types - END

	if len(eventsToTransform) == 0 {
		return transformSrcDestOutput{
			destJobs:        destJobs,
			batchDestJobs:   batchDestJobs,
			errorsPerDestID: procErrorJobsByDestID,
			reportMetrics:   reportMetrics,
			routerDestIDs:   routerDestIDs,
			droppedJobs:     droppedJobs,
		}
	}

	// Destination transformation - START
	// Send to transformer only if is
	// a. transformAt is processor
	// OR
	// b. transformAt is router and transformer doesn't support router transform
	if transformAt == "processor" || (transformAt == "router" && transformAtFromFeaturesFile == "") {
		trace.WithRegion(ctx, "Dest Transform", func() {
			trace.Logf(ctx, "Dest Transform", "input size %d", len(eventsToTransform))
			proc.logger.Debug("Dest Transform input size", len(eventsToTransform))
			s := time.Now()
			response = proc.transformer.Transform(ctx, eventsToTransform, proc.config.transformBatchSize.Load())

			destTransformationStat := proc.newDestinationTransformationStat(sourceID, workspaceID, transformAt, destination)
			destTransformationStat.transformTime.Since(s)
			transformAt = "processor"

			proc.logger.Debugf("Dest Transform output size %d", len(response.Events))
			trace.Logf(ctx, "DestTransform", "output size %d", len(response.Events))

			nonSuccessMetrics := proc.getNonSuccessfulMetrics(
				response, commonMetaData, eventsByMessageID,
				transformer.DestTransformerStage, transformationEnabled, trackingPlanEnabled,
			)
			destTransformationStat.numEvents.Count(len(eventsToTransform))
			destTransformationStat.numOutputSuccessEvents.Count(len(response.Events))
			destTransformationStat.numOutputFailedEvents.Count(len(nonSuccessMetrics.failedJobs))
			destTransformationStat.numOutputFilteredEvents.Count(len(nonSuccessMetrics.filteredJobs))
			droppedJobs = append(droppedJobs, append(proc.getDroppedJobs(response, eventsToTransform), append(nonSuccessMetrics.failedJobs, nonSuccessMetrics.filteredJobs...)...)...)

			if _, ok := procErrorJobsByDestID[destID]; !ok {
				procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
			}
			procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], nonSuccessMetrics.failedJobs...)

			// REPORTING - PROCESSOR metrics - START
			if proc.isReportingEnabled() {
				successMetrics := make([]*types.PUReportedMetric, 0)
				connectionDetailsMap := make(map[string]*types.ConnectionDetails)
				statusDetailsMap := make(map[string]map[string]*types.StatusDetail)
				successCountMap := make(map[string]int64)
				for i := range response.Events {
					// Update metrics maps
					proc.updateMetricMaps(nil, successCountMap, connectionDetailsMap, statusDetailsMap, &response.Events[i], jobsdb.Succeeded.State, types.DEST_TRANSFORMER, func() json.RawMessage { return []byte(`{}`) }, nil)
				}
				types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)

				for k, cd := range connectionDetailsMap {
					for _, sd := range statusDetailsMap[k] {
						m := &types.PUReportedMetric{
							ConnectionDetails: *cd,
							PUDetails:         *types.CreatePUDetails(types.EVENT_FILTER, types.DEST_TRANSFORMER, false, false),
							StatusDetail:      sd,
						}
						successMetrics = append(successMetrics, m)
					}
				}

				diffMetrics := getDiffMetrics(types.EVENT_FILTER, types.DEST_TRANSFORMER, inCountMetadataMap, inCountMap, successCountMap, nonSuccessMetrics.failedCountMap, nonSuccessMetrics.filteredCountMap)

				reportMetrics = append(reportMetrics, nonSuccessMetrics.failedMetrics...)
				reportMetrics = append(reportMetrics, nonSuccessMetrics.filteredMetrics...)
				reportMetrics = append(reportMetrics, successMetrics...)
				reportMetrics = append(reportMetrics, diffMetrics...)
			}
			// REPORTING - PROCESSOR metrics - END
		})
	}

	trace.WithRegion(ctx, "MarshalForDB", func() {
		// Save the JSON in DB. This is what the router uses
		for i := range response.Events {
			destEventJSON, err := jsonfast.Marshal(response.Events[i].Output)
			// Should be a valid JSON since it's our transformation, but we handle it anyway
			if err != nil {
				continue
			}

			// Need to replace UUID his with messageID from client
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
			// If the response from the transformer does not have userID in metadata, setting userID to random-uuid.
			// This is done to respect findWorker logic in router.
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
			if slices.Contains(proc.config.batchDestinations, newJob.CustomVal) {
				batchDestJobs = append(batchDestJobs, &newJob)
			} else {
				destJobs = append(destJobs, &newJob)
				routerDestIDs[destID] = struct{}{}
			}
		}
	})
	return transformSrcDestOutput{
		destJobs:        destJobs,
		batchDestJobs:   batchDestJobs,
		errorsPerDestID: procErrorJobsByDestID,
		reportMetrics:   reportMetrics,
		routerDestIDs:   routerDestIDs,
		droppedJobs:     droppedJobs,
	}
}

func (proc *Handle) saveDroppedJobs(droppedJobs []*jobsdb.JobT, tx *Tx) error {
	if len(droppedJobs) > 0 {
		for i := range droppedJobs { // each dropped job should have a unique jobID in the scope of the batch
			droppedJobs[i].JobID = int64(i)
		}
		rsourcesStats := rsources.NewDroppedJobsCollector(proc.rsourcesService)
		rsourcesStats.JobsDropped(droppedJobs)
		return rsourcesStats.Publish(context.TODO(), tx.Tx)
	}
	return nil
}

func ConvertToFilteredTransformerResponse(events []transformer.TransformerEvent, filter bool) transformer.Response {
	var responses []transformer.TransformerResponse
	var failedEvents []transformer.TransformerResponse

	type cacheValue struct {
		values []string
		ok     bool
	}
	supportedMessageTypesCache := make(map[string]*cacheValue)
	supportedMessageEventsCache := make(map[string]*cacheValue)

	// filter unsupported message types
	var resp transformer.TransformerResponse
	for i := range events {
		event := &events[i]

		if filter {
			// filter unsupported message types
			supportedTypes, ok := supportedMessageTypesCache[event.Destination.ID]
			if !ok {
				v, o := eventfilter.GetSupportedMessageTypes(&event.Destination)
				supportedTypes = &cacheValue{values: v, ok: o}
				supportedMessageTypesCache[event.Destination.ID] = supportedTypes
			}
			if supportedTypes.ok {
				allow, failedEvent := eventfilter.AllowEventToDestTransformation(event, supportedTypes.values)
				if !allow {
					if failedEvent != nil {
						failedEvents = append(failedEvents, *failedEvent)
					}
					continue
				}
			}

			// filter unsupported message events
			supportedEvents, ok := supportedMessageEventsCache[event.Destination.ID]
			if !ok {
				v, o := eventfilter.GetSupportedMessageEvents(&event.Destination)
				supportedEvents = &cacheValue{values: v, ok: o}
				supportedMessageEventsCache[event.Destination.ID] = supportedEvents
			}
			if supportedEvents.ok {
				messageEvent, typOk := event.Message["event"].(string)
				if !typOk {
					// add to FailedEvents
					resp = transformer.TransformerResponse{Output: event.Message, StatusCode: 400, Metadata: event.Metadata, Error: "Invalid message event. Type assertion failed"}
					failedEvents = append(failedEvents, resp)
					continue
				}
				if !slices.Contains(supportedEvents.values, messageEvent) {
					resp = transformer.TransformerResponse{Output: event.Message, StatusCode: types.FilterEventCode, Metadata: event.Metadata, Error: "Event not supported"}
					failedEvents = append(failedEvents, resp)
					continue
				}
			}

		}
		// allow event
		resp = transformer.TransformerResponse{Output: event.Message, StatusCode: 200, Metadata: event.Metadata}
		responses = append(responses, resp)
	}

	return transformer.Response{Events: responses, FailedEvents: failedEvents}
}

func (proc *Handle) getJobs(partition string) jobsdb.JobsResult {
	if proc.limiter.read != nil {
		defer proc.limiter.read.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
	}

	s := time.Now()

	proc.logger.Debugf("Processor DB Read size: %d", proc.config.maxEventsToProcess)

	eventCount := proc.config.maxEventsToProcess.Load()
	if !proc.config.enableEventCount.Load() {
		eventCount = 0
	}
	queryParams := jobsdb.GetQueryParams{
		CustomValFilters: []string{proc.config.GWCustomVal},
		JobsLimit:        proc.config.maxEventsToProcess.Load(),
		EventsLimit:      eventCount,
		PayloadSizeLimit: proc.adaptiveLimit(proc.payloadLimit.Load()),
	}
	proc.isolationStrategy.AugmentQueryParams(partition, &queryParams)

	unprocessedList, err := misc.QueryWithRetriesAndNotify(context.Background(), proc.jobdDBQueryRequestTimeout.Load(), proc.jobdDBMaxRetries.Load(), func(ctx context.Context) (jobsdb.JobsResult, error) {
		return proc.gatewayDB.GetUnprocessed(ctx, queryParams)
	}, proc.sendQueryRetryStats)
	if err != nil {
		proc.logger.Errorf("Failed to get unprocessed jobs from DB. Error: %v", err)
		panic(err)
	}

	totalPayloadBytes := 0
	for _, job := range unprocessedList.Jobs {
		totalPayloadBytes += len(job.EventPayload)

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

	var firstJob *jobsdb.JobT
	var lastJob *jobsdb.JobT
	if len(unprocessedList.Jobs) > 0 {
		firstJob = unprocessedList.Jobs[0]
		lastJob = unprocessedList.Jobs[len(unprocessedList.Jobs)-1]
	}
	proc.pipelineDelayStats(partition, firstJob, lastJob)

	// check if there is work to be done
	if len(unprocessedList.Jobs) == 0 {
		proc.logger.Debugf("Processor DB Read Complete. No GW Jobs to process.")
		return unprocessedList
	}

	eventSchemasStart := time.Now()
	if proc.config.enableEventSchemasFeature && !proc.config.enableEventSchemasAPIOnly.Load() {
		for _, unprocessedJob := range unprocessedList.Jobs {
			writeKey := gjson.GetBytes(unprocessedJob.EventPayload, "writeKey").Str
			proc.eventSchemaHandler.RecordEventSchema(writeKey, string(unprocessedJob.EventPayload))
		}
	}
	eventSchemasTime := time.Since(eventSchemasStart)
	defer proc.stats.eventSchemasTime.SendTiming(eventSchemasTime)

	proc.logger.Debugf("Processor DB Read Complete. unprocessedList: %v total_events: %d", len(unprocessedList.Jobs), unprocessedList.EventsCount)
	proc.stats.statGatewayDBR.Count(len(unprocessedList.Jobs))

	proc.stats.statDBReadRequests.Observe(float64(len(unprocessedList.Jobs)))
	proc.stats.statDBReadEvents.Observe(float64(unprocessedList.EventsCount))
	proc.stats.statDBReadPayloadBytes.Observe(float64(totalPayloadBytes))

	return unprocessedList
}

func (proc *Handle) markExecuting(jobs []*jobsdb.JobT) error {
	start := time.Now()
	defer proc.stats.statMarkExecuting.Since(start)

	statusList := make([]*jobsdb.JobStatusT, len(jobs))
	for i, job := range jobs {
		statusList[i] = &jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			JobState:      jobsdb.Executing.State,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
		}
	}
	// Mark the jobs as executing
	err := misc.RetryWithNotify(context.Background(), proc.jobsDBCommandTimeout.Load(), proc.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return proc.gatewayDB.UpdateJobStatus(ctx, statusList, []string{proc.config.GWCustomVal}, nil)
	}, proc.sendRetryUpdateStats)
	if err != nil {
		return fmt.Errorf("marking jobs as executing: %w", err)
	}

	return nil
}

// handlePendingGatewayJobs is checking for any pending gateway jobs (failed and unprocessed), and routes them appropriately
// Returns true if any job is handled, otherwise returns false.
func (proc *Handle) handlePendingGatewayJobs(partition string) bool {
	s := time.Now()

	unprocessedList := proc.getJobs(partition)

	if len(unprocessedList.Jobs) == 0 {
		return false
	}

	rsourcesStats := rsources.NewStatsCollector(proc.rsourcesService)
	rsourcesStats.BeginProcessing(unprocessedList.Jobs)

	proc.Store(partition,
		proc.transformations(partition,
			proc.processJobsForDest(partition, subJob{
				subJobs:       unprocessedList.Jobs,
				hasMore:       false,
				rsourcesStats: rsourcesStats,
			},
			),
		),
	)
	proc.stats.statLoopTime.Since(s)

	return true
}

// `jobSplitter` func Splits the read Jobs into sub-batches after reading from DB to process.
// `subJobMerger` func merges the split jobs into a single batch before writing to DB.
// So, to keep track of sub-batch we have `hasMore` variable.
// each sub-batch has `hasMore`. If, a sub-batch is the last one from the batch it's marked as `false`, else `true`.
type subJob struct {
	subJobs       []*jobsdb.JobT
	hasMore       bool
	rsourcesStats rsources.StatsCollector
}

func (proc *Handle) jobSplitter(jobs []*jobsdb.JobT, rsourcesStats rsources.StatsCollector) []subJob { //nolint:unparam
	chunks := lo.Chunk(jobs, proc.config.subJobSize)
	return lo.Map(chunks, func(subJobs []*jobsdb.JobT, index int) subJob {
		return subJob{
			subJobs:       subJobs,
			hasMore:       index+1 < len(chunks),
			rsourcesStats: rsourcesStats,
		}
	})
}

func throughputPerSecond(processedJob int, timeTaken time.Duration) int {
	normalizedTime := float64(timeTaken) / float64(time.Second)
	return int(float64(processedJob) / normalizedTime)
}

func (proc *Handle) crashRecover() {
	proc.gatewayDB.DeleteExecuting()
}

func (proc *Handle) updateSourceStats(sourceStats map[dupStatKey]int, bucket string) {
	for dupStat, count := range sourceStats {
		tags := map[string]string{
			"source":    dupStat.sourceID,
			"equalSize": strconv.FormatBool(dupStat.equalSize),
		}
		sourceStatsD := proc.statsFactory.NewTaggedStat(bucket, stats.CountType, tags)
		sourceStatsD.Count(count)
	}
}

func (proc *Handle) isReportingEnabled() bool {
	return proc.reporting != nil && proc.reportingEnabled
}

func (proc *Handle) updateRudderSourcesStats(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) error {
	rsourcesStats := rsources.NewStatsCollector(proc.rsourcesService)
	rsourcesStats.JobsStored(jobs)
	err := rsourcesStats.Publish(ctx, tx.SqlTx())
	return err
}

func filterConfig(eventCopy *transformer.TransformerEvent) {
	if configsToFilterI, ok := eventCopy.Destination.DestinationDefinition.Config["configFilters"]; ok {
		if configsToFilter, ok := configsToFilterI.([]interface{}); ok {
			omitKeys := lo.FilterMap(configsToFilter, func(configKey interface{}, _ int) (string, bool) {
				configKeyStr, ok := configKey.(string)
				return configKeyStr, ok
			})
			eventCopy.Destination.Config = lo.OmitByKeys(eventCopy.Destination.Config, omitKeys)
		}
	}
}

func (*Handle) getLimiterPriority(partition string) kitsync.LimiterPriorityValue {
	return kitsync.LimiterPriorityValue(config.GetInt(fmt.Sprintf("Processor.Limiter.%s.Priority", partition), 1))
}

func (proc *Handle) filterDestinations(
	event types.SingularEventT,
	dests []backendconfig.DestinationT,
) []backendconfig.DestinationT {
	deniedCategories := deniedConsentCategories(event)
	if len(deniedCategories) == 0 {
		return dests
	}
	return lo.Filter(dests, func(dest backendconfig.DestinationT, _ int) bool {
		if consentCategories := proc.getConsentCategories(dest.ID); len(consentCategories) > 0 {
			return len(lo.Intersect(consentCategories, deniedCategories)) == 0
		}
		return true
	})
}

// check if event has eligible destinations to send to
//
// event will be dropped if no destination is found
func (proc *Handle) isDestinationAvailable(event types.SingularEventT, sourceId string) bool {
	enabledDestTypes := integrations.FilterClientIntegrations(
		event,
		proc.getBackendEnabledDestinationTypes(sourceId),
	)
	if len(enabledDestTypes) == 0 {
		proc.logger.Debug("No enabled destination types")
		return false
	}

	if enabledDestinationsList := proc.filterDestinations(
		event,
		lo.Flatten(
			lo.Map(
				enabledDestTypes,
				func(destType string, _ int) []backendconfig.DestinationT {
					return proc.getEnabledDestinations(sourceId, destType)
				},
			),
		),
	); len(enabledDestinationsList) == 0 {
		proc.logger.Debug("No destination to route this event to")
		return false
	}

	return true
}

func deniedConsentCategories(se types.SingularEventT) []string {
	if deniedConsents, _ := misc.MapLookup(se, "context", "consentManagement", "deniedConsentIds").([]interface{}); len(deniedConsents) > 0 {
		return lo.FilterMap(
			deniedConsents,
			func(consent interface{}, _ int) (string, bool) {
				consentStr, ok := consent.(string)
				return consentStr, ok && consentStr != ""
			},
		)
	}
	return nil
}

// pipelineDelayStats reports the delay of the pipeline as a range:
//
// - max - time elapsed since the first job was created
//
// - min - time elapsed since the last job was created
func (proc *Handle) pipelineDelayStats(partition string, first, last *jobsdb.JobT) {
	var firstJobDelay float64
	var lastJobDelay float64
	if first != nil {
		firstJobDelay = time.Since(first.CreatedAt).Seconds()
	}
	if last != nil {
		lastJobDelay = time.Since(last.CreatedAt).Seconds()
	}
	proc.statsFactory.NewTaggedStat("pipeline_delay_min_seconds", stats.GaugeType, stats.Tags{"partition": partition, "module": "processor"}).Gauge(lastJobDelay)
	proc.statsFactory.NewTaggedStat("pipeline_delay_max_seconds", stats.GaugeType, stats.Tags{"partition": partition, "module": "processor"}).Gauge(firstJobDelay)
}

func (proc *Handle) IncreasePendingEvents(tablePrefix string, stats map[string]map[string]int) {
	for workspace := range stats {
		for destType := range stats[workspace] {
			rmetrics.IncreasePendingEvents(tablePrefix, workspace, destType, float64(stats[workspace][destType]))
		}
	}
}

func (proc *Handle) countPendingEvents(ctx context.Context) error {
	dbs := map[string]jobsdb.JobsDB{"rt": proc.routerDB, "batch_rt": proc.batchRouterDB}
	jobdDBQueryRequestTimeout := config.GetDurationVar(600, time.Second, "JobsDB.GetPileUpCounts.QueryRequestTimeout", "JobsDB.QueryRequestTimeout")
	jobdDBMaxRetries := config.GetReloadableIntVar(2, 1, "JobsDB.Processor.MaxRetries", "JobsDB.MaxRetries")

	for tablePrefix, db := range dbs {
		pileUpStatMap, err := misc.QueryWithRetriesAndNotify(ctx,
			jobdDBQueryRequestTimeout,
			jobdDBMaxRetries.Load(),
			func(ctx context.Context) (map[string]map[string]int, error) {
				return db.GetPileUpCounts(ctx)
			}, func(attempt int) {
				proc.logger.Warnf("Timeout during GetPileUpCounts, attempt %d", attempt)
				stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "pileup"}).Count(1)
			})
		if err != nil {
			return err
		}
		proc.IncreasePendingEvents(tablePrefix, pileUpStatMap)
	}
	return nil
}
