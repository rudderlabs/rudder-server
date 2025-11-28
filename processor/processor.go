package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"runtime/trace"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/ro"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stringify"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/trackedusers"
	"github.com/rudderlabs/rudder-server/internal/enricher"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/delayed"
	"github.com/rudderlabs/rudder-server/processor/eventfilter"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/internal/preprocessdelay"
	"github.com/rudderlabs/rudder-server/processor/isolation"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/dedup"
	deduptypes "github.com/rudderlabs/rudder-server/services/dedup/types"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/tracing"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

// Custom type definitions for deeply nested map
type (
	DestinationID      string
	SourceID           string
	ConsentProviderKey string
)

type (
	ConsentProviderMap map[ConsentProviderKey]GenericConsentManagementProviderData
	DestConsentMap     map[DestinationID]ConsentProviderMap
	SourceConsentMap   map[SourceID]DestConsentMap
)

const (
	MetricKeyDelimiter    = "!<<#>>!"
	UserTransformation    = "USER_TRANSFORMATION"
	DestTransformation    = "DEST_TRANSFORMATION"
	EventFilter           = "EVENT_FILTER"
	sourceCategoryWebhook = "webhook"
)

func NewHandle(c *config.Config, transformerClients transformer.TransformerClients) *Handle {
	h := &Handle{transformerClients: transformerClients, conf: c}
	h.loadConfig()
	return h
}

type sourceObserver interface {
	ObserveSourceEvents(source *backendconfig.SourceT, events []types.TransformerEvent)
}

type trackedUsersReporter interface {
	ReportUsers(ctx context.Context, reports []*trackedusers.UsersReport, tx *Tx) error
	GenerateReportsFromJobs(jobs []*jobsdb.JobT, sourceIdFilter map[string]bool) []*trackedusers.UsersReport
}

// Handle is a handle to the processor module
type Handle struct {
	conf               *config.Config
	tracer             *tracing.Tracer
	backendConfig      backendconfig.BackendConfig
	transformerClients transformer.TransformerClients

	gatewayDB                  jobsdb.JobsDB
	routerDB                   jobsdb.JobsDB
	batchRouterDB              jobsdb.JobsDB
	eventSchemaDB              jobsdb.JobsDB
	archivalDB                 jobsdb.JobsDB
	pendingEventsRegistry      rmetrics.PendingEventsRegistry
	logger                     logger.Logger
	enrichers                  []enricher.PipelineEnricher
	dedup                      deduptypes.Dedup
	reporting                  reportingtypes.Reporting
	reportingEnabled           bool
	backgroundCtx              context.Context
	backgroundWait             func() error
	backgroundCancel           context.CancelFunc
	statsFactory               stats.Stats
	stats                      processorStats
	payloadLimit               config.ValueLoader[int64]
	jobsDBCommandTimeout       config.ValueLoader[time.Duration]
	jobdDBQueryRequestTimeout  config.ValueLoader[time.Duration]
	jobdDBMaxRetries           config.ValueLoader[int]
	transientSources           transientsource.Service
	fileuploader               fileuploader.Provider
	utSamplingFileManager      filemanager.FileManager
	storeSamplingFileManager   filemanager.FileManager
	rsourcesService            rsources.JobService
	transformerFeaturesService transformerFeaturesService.FeaturesService
	destDebugger               destinationdebugger.DestinationDebugger
	transDebugger              transformationdebugger.TransformationDebugger
	isolationStrategy          isolation.Strategy
	limiter                    struct {
		read         kitsync.Limiter
		preprocess   kitsync.Limiter
		srcHydration kitsync.Limiter
		pretransform kitsync.Limiter
		utransform   kitsync.Limiter
		dtransform   kitsync.Limiter
		store        kitsync.Limiter
	}
	config struct {
		isolationMode                             isolation.Mode
		mainLoopTimeout                           time.Duration
		enablePipelining                          bool
		pipelineBufferedItems                     int
		subJobSize                                int
		pipelinesPerPartition                     int
		pingerSleep                               config.ValueLoader[time.Duration]
		readLoopSleep                             config.ValueLoader[time.Duration]
		maxLoopSleep                              config.ValueLoader[time.Duration]
		storeTimeout                              config.ValueLoader[time.Duration]
		maxEventsToProcess                        config.ValueLoader[int]
		sourceIdDestinationMap                    map[string][]backendconfig.DestinationT
		sourceIdSourceMap                         map[string]backendconfig.SourceT
		workspaceLibrariesMap                     map[string]backendconfig.LibrariesT
		oneTrustConsentCategoriesMap              map[string][]string
		connectionConfigMap                       map[connection]backendconfig.Connection
		ketchConsentCategoriesMap                 map[string][]string
		genericConsentManagementMap               SourceConsentMap
		batchDestinations                         []string
		configSubscriberLock                      sync.RWMutex
		enableDedup                               bool
		transformTimesPQLength                    int
		captureEventNameStats                     config.ValueLoader[bool]
		transformerURL                            string
		GWCustomVal                               string
		asyncInit                                 *misc.AsyncInit
		eventSchemaV2Enabled                      bool
		archivalEnabled                           config.ValueLoader[bool]
		eventAuditEnabled                         map[string]bool
		credentialsMap                            map[string][]types.Credential
		nonEventStreamSources                     map[string]bool
		enableConcurrentStore                     config.ValueLoader[bool]
		userTransformationMirroringSanitySampling config.ValueLoader[float64]
		userTransformationMirroringFireAndForget  config.ValueLoader[bool]
		storeSamplerEnabled                       config.ValueLoader[bool]
		archiveInPreProcess                       bool
	}

	drainConfig struct {
		jobRunIDs config.ValueLoader[[]string]
	}

	namespace  string
	instanceID string

	adaptiveLimit func(int64) int64
	storePlocker  kitsync.PartitionLocker

	sourceObservers      []sourceObserver
	trackedUsersReporter trackedUsersReporter
}
type processorStats struct {
	statGatewayDBR                func(partition string) stats.Measurement
	statGatewayDBW                func(partition string) stats.Measurement
	statDBR                       func(partition string) stats.Measurement
	statDBW                       func(partition string) stats.Measurement
	validateEventsTime            func(partition string) stats.Measurement // TODO: stop using it in dashboards and delete
	statNumRequests               func(partition string) stats.Measurement
	statNumEvents                 func(partition string) stats.Measurement
	statDBWriteRouterPayloadBytes func(partition string) stats.Measurement // TODO: stop using it in dashboards and delete
	statDBWriteBatchPayloadBytes  func(partition string) stats.Measurement // TODO: stop using it in dashboards and delete
	statDestNumOutputEvents       func(partition string) stats.Measurement
	statBatchDestNumOutputEvents  func(partition string) stats.Measurement
	trackedUsersReportGeneration  func(partition string) stats.Measurement // TODO: stop using it in dashboards and delete

	statReadStageCount         func(partition string) stats.Measurement
	statPretransformStageCount func(partition string) stats.Measurement
	statPreprocessStageCount   func(partition string) stats.Measurement
	statSrcHydrationStageCount func(partition string) stats.Measurement
	statUtransformStageCount   func(partition string) stats.Measurement
	statDtransformStageCount   func(partition string) stats.Measurement
	statStoreStageCount        func(partition string) stats.Measurement

	utMirroringEqualResponses     func(partition string) stats.Measurement
	utMirroringDifferentResponses func(partition string) stats.Measurement
}
type DestStatT struct {
	numEvents               stats.Measurement
	numOutputSuccessEvents  stats.Measurement
	numOutputFailedEvents   stats.Measurement
	numOutputFilteredEvents stats.Measurement
	transformTime           stats.Measurement
}

type ParametersT struct {
	SourceID                string      `json:"source_id"`
	SourceName              string      `json:"source_name"`
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
	TraceParent             string      `json:"traceparent"`
	ConnectionID            string      `json:"connection_id"`
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
	failedMetrics    []*reportingtypes.PUReportedMetric
	failedCountMap   map[string]int64
	filteredJobs     []*jobsdb.JobT
	filteredMetrics  []*reportingtypes.PUReportedMetric
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

func (proc *Handle) newUserTransformationStat(
	sourceID, workspaceID string, destination *backendconfig.DestinationT, mirroring bool,
) *DestStatT {
	tags := buildStatTags(sourceID, workspaceID, destination, UserTransformation)

	tags["transformation_id"] = destination.Transformations[0].ID
	tags["transformation_version_id"] = destination.Transformations[0].VersionID
	tags["error"] = "false"
	tags["mirroring"] = strconv.FormatBool(mirroring)

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
	ctx context.Context,
	backendConfig backendconfig.BackendConfig,
	gatewayDB, routerDB, batchRouterDB,
	eventSchemaDB, archivalDB jobsdb.JobsDB,
	reporting reportingtypes.Reporting,
	transientSources transientsource.Service,
	fileuploader fileuploader.Provider,
	rsourcesService rsources.JobService,
	transformerFeaturesService transformerFeaturesService.FeaturesService,
	destDebugger destinationdebugger.DestinationDebugger,
	transDebugger transformationdebugger.TransformationDebugger,
	enrichers []enricher.PipelineEnricher,
	trackedUsersReporter trackedusers.UsersReporter,
	pendingEventsRegistry rmetrics.PendingEventsRegistry,
) error {
	proc.reporting = reporting
	proc.destDebugger = destDebugger
	proc.transDebugger = transDebugger
	proc.reportingEnabled = proc.conf.GetBoolVar(reportingtypes.DefaultReportingEnabled, "Reporting.enabled")
	if proc.conf == nil {
		proc.conf = config.Default
	}
	// Stats
	if proc.statsFactory == nil {
		proc.statsFactory = stats.Default
	}

	proc.setupReloadableVars()
	proc.logger = logger.NewLogger().Child("processor")
	proc.backendConfig = backendConfig

	proc.gatewayDB = gatewayDB
	proc.routerDB = routerDB
	proc.batchRouterDB = batchRouterDB
	proc.eventSchemaDB = eventSchemaDB
	proc.archivalDB = archivalDB

	proc.pendingEventsRegistry = pendingEventsRegistry

	proc.transientSources = transientSources
	proc.fileuploader = fileuploader
	proc.rsourcesService = rsourcesService
	proc.enrichers = enrichers
	proc.transformerFeaturesService = transformerFeaturesService

	var err error
	proc.utSamplingFileManager, err = getUTSamplingUploader(proc.conf, proc.logger)
	if err != nil {
		proc.logger.Errorn("failed to create ut sampling file manager", obskit.Error(err))
		proc.utSamplingFileManager = nil
	}
	proc.storeSamplingFileManager, err = getStoreSamplingUploader(proc.conf, proc.logger)
	if err != nil {
		proc.logger.Errorn("failed to create store sampling file manager", obskit.Error(err))
	}

	if proc.adaptiveLimit == nil {
		proc.adaptiveLimit = func(limit int64) int64 { return limit }
	}
	proc.storePlocker = *kitsync.NewPartitionLocker()

	proc.namespace = config.GetKubeNamespace()
	proc.instanceID = misc.GetInstanceID()

	proc.trackedUsersReporter = trackedUsersReporter
	proc.tracer = tracing.New(proc.statsFactory.NewTracer("processor"), tracing.WithNamePrefix("proc"))
	proc.stats.statGatewayDBR = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_gateway_db_read", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statGatewayDBW = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_gateway_db_write", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statDBR = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_gateway_db_read_time", stats.TimerType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statDBW = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_gateway_db_write_time", stats.TimerType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.validateEventsTime = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_validate_events_time", stats.TimerType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statNumRequests = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_num_requests", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statNumEvents = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_num_events", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statDBWriteRouterPayloadBytes = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_db_write_payload_bytes", stats.HistogramType, stats.Tags{
			"module":    "router",
			"partition": partition,
		})
	}
	proc.stats.statDBWriteBatchPayloadBytes = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_db_write_payload_bytes", stats.HistogramType, stats.Tags{
			"module":    "batch_router",
			"partition": partition,
		})
	}
	proc.stats.statDestNumOutputEvents = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_num_output_events", stats.CountType, stats.Tags{
			"module":    "router",
			"partition": partition,
		})
	}
	proc.stats.statBatchDestNumOutputEvents = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_num_output_events", stats.CountType, stats.Tags{
			"module":    "batch_router",
			"partition": partition,
		})
	}
	proc.stats.trackedUsersReportGeneration = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_tracked_users_report_gen_seconds", stats.TimerType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statReadStageCount = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("proc_read_jobs", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statPretransformStageCount = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("proc_pretransform_jobs", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statPreprocessStageCount = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("proc_preprocess_jobs", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statSrcHydrationStageCount = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("proc_src_hydration_jobs", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statUtransformStageCount = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("proc_utransform_jobs", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statDtransformStageCount = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("proc_dtransform_jobs", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.statStoreStageCount = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("proc_store_jobs", stats.CountType, stats.Tags{
			"partition": partition,
		})
	}
	proc.stats.utMirroringEqualResponses = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_ut_mirroring_responses_count", stats.CountType, stats.Tags{
			"equal":     "true",
			"partition": partition,
		})
	}
	proc.stats.utMirroringDifferentResponses = func(partition string) stats.Measurement {
		return proc.statsFactory.NewTaggedStat("processor_ut_mirroring_responses_count", stats.CountType, stats.Tags{
			"equal":     "false",
			"partition": partition,
		})
	}

	if proc.config.enableDedup {
		var err error
		proc.dedup, err = dedup.New(proc.conf, proc.statsFactory, proc.logger)
		if err != nil {
			return err
		}
	}
	proc.sourceObservers = []sourceObserver{delayed.NewEventStats(proc.statsFactory, proc.conf)}
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	proc.backgroundCtx = ctx
	proc.backgroundWait = g.Wait
	proc.backgroundCancel = cancel

	proc.config.asyncInit = misc.NewAsyncInit(1)
	g.Go(crash.Wrapper(func() error {
		proc.backendConfigSubscriber(ctx)
		return nil
	}))

	// periodically publish a zero counter for ensuring that stuck processing pipeline alert
	// can always detect a stuck processor
	g.Go(crash.Wrapper(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(15 * time.Second):
				proc.stats.statGatewayDBW("").Count(0)
			}
		}
	}))

	proc.crashRecover()
	return nil
}

func (proc *Handle) setupReloadableVars() {
	proc.jobdDBQueryRequestTimeout = proc.conf.GetReloadableDurationVar(600, time.Second, "JobsDB.Processor.QueryRequestTimeout", "JobsDB.QueryRequestTimeout")
	proc.jobsDBCommandTimeout = proc.conf.GetReloadableDurationVar(600, time.Second, "JobsDB.Processor.CommandRequestTimeout", "JobsDB.CommandRequestTimeout")
	proc.jobdDBMaxRetries = proc.conf.GetReloadableIntVar(2, 1, "JobsDB.Processor.MaxRetries", "JobsDB.MaxRetries")
	proc.drainConfig.jobRunIDs = proc.conf.GetReloadableStringSliceVar([]string{}, "drain.jobRunIDs")
}

// Start starts this processor's main loops.
func (proc *Handle) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	var err error
	proc.logger.Infon("Starting processor in isolation mode", logger.NewStringField("isolationMode", string(proc.config.isolationMode)))
	if proc.isolationStrategy, err = isolation.GetStrategy(proc.config.isolationMode); err != nil {
		return fmt.Errorf("resolving isolation strategy for mode %q: %w", proc.config.isolationMode, err)
	}

	// limiters
	s := proc.statsFactory
	var limiterGroup sync.WaitGroup
	proc.limiter.read = kitsync.NewReloadableLimiter(ctx, &limiterGroup, "proc_read",
		proc.conf.GetReloadableIntVar(50, 1, "Processor.Limiter.read.limit"),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.read.dynamicPeriod", 1, time.Second)))
	proc.limiter.preprocess = kitsync.NewReloadableLimiter(ctx, &limiterGroup, "proc_preprocess",
		proc.conf.GetReloadableIntVar(50, 1, "Processor.Limiter.preprocess.limit"),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.preprocess.dynamicPeriod", 1, time.Second)))
	proc.limiter.srcHydration = kitsync.NewReloadableLimiter(ctx, &limiterGroup, "proc_srchydration",
		proc.conf.GetReloadableIntVar(50, 1, "Processor.Limiter.src_hydration.limit"),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.src_hydration.dynamicPeriod", 1, time.Second)))
	proc.limiter.pretransform = kitsync.NewReloadableLimiter(ctx, &limiterGroup, "proc_pretransform",
		proc.conf.GetReloadableIntVar(50, 1, "Processor.Limiter.pretransform.limit"),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.pretransform.dynamicPeriod", 1, time.Second)))
	proc.limiter.utransform = kitsync.NewReloadableLimiter(ctx, &limiterGroup, "proc_utransform",
		proc.conf.GetReloadableIntVar(50, 1, "Processor.Limiter.utransform.limit"),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.utransform.dynamicPeriod", 1, time.Second)))
	proc.limiter.dtransform = kitsync.NewReloadableLimiter(ctx, &limiterGroup, "proc_dtransform",
		proc.conf.GetReloadableIntVar(50, 1, "Processor.Limiter.dtransform.limit"),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.dtransform.dynamicPeriod", 1, time.Second)))
	proc.limiter.store = kitsync.NewReloadableLimiter(ctx, &limiterGroup, "proc_store",
		proc.conf.GetReloadableIntVar(50, 1, "Processor.Limiter.store.limit"),
		s,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Processor.Limiter.store.dynamicPeriod", 1, time.Second)))
	g.Go(func() error {
		limiterGroup.Wait()
		return nil
	})

	// pinger loop
	g.Go(crash.Wrapper(func() error {
		proc.logger.Infon("Starting pinger loop")
		proc.backendConfig.WaitForConfig(ctx)
		proc.logger.Infon("Backend config received")

		// waiting for init group
		proc.logger.Infon("Waiting for async init group")
		select {
		case <-ctx.Done():
			return nil
		case <-proc.config.asyncInit.Wait():
			// proceed
		}
		proc.logger.Infon("Async init group done")

		// waiting for transformer features
		proc.logger.Infon("Waiting for transformer features")
		select {
		case <-ctx.Done():
			return nil
		case <-proc.transformerFeaturesService.Wait():
			// proceed
		}
		proc.logger.Infon("Transformer features received")

		h := &workerHandleAdapter{proc}
		pool := workerpool.New(ctx, func(partition string) workerpool.Worker {
			return newPartitionWorker(partition, h, proc.statsFactory.NewTracer("partitionWorker"), proc.statsFactory)
		}, proc.logger)
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
	proc.pendingEventsRegistry.Reset()
}

func (proc *Handle) loadConfig() {
	proc.config.mainLoopTimeout = 200 * time.Millisecond

	defaultSubJobSize := 2000
	defaultMaxEventsToProcess := 10000
	defaultPayloadLimit := 100 * bytesize.MB

	defaultIsolationMode := isolation.ModeSource
	if proc.conf.IsSet("WORKSPACE_NAMESPACE") {
		defaultIsolationMode = isolation.ModeWorkspace
	}
	proc.config.isolationMode = isolation.Mode(proc.conf.GetString("Processor.isolationMode", string(defaultIsolationMode)))
	// If isolation mode is not none, we need to reduce the values for some of the config variables to more sensible defaults
	if proc.config.isolationMode != isolation.ModeNone {
		defaultSubJobSize = 400
		defaultMaxEventsToProcess = 2000
		defaultPayloadLimit = 20 * bytesize.MB
	}

	proc.config.enablePipelining = proc.conf.GetBoolVar(true, "Processor.enablePipelining")
	proc.config.pipelineBufferedItems = proc.conf.GetIntVar(0, 1, "Processor.pipelineBufferedItems")
	proc.config.subJobSize = proc.conf.GetIntVar(defaultSubJobSize, 1, "Processor.subJobSize")
	proc.config.pipelinesPerPartition = proc.conf.GetIntVar(1, 1, "Processor.pipelinesPerPartition")
	// Enable dedup of incoming events by default
	proc.config.enableDedup = proc.conf.GetBoolVar(false, "Dedup.enableDedup")
	proc.config.eventSchemaV2Enabled = proc.conf.GetBoolVar(false, "EventSchemas2.enabled")
	proc.config.batchDestinations = misc.BatchDestinations()
	proc.config.transformTimesPQLength = proc.conf.GetIntVar(5, 1, "Processor.transformTimesPQLength")
	// GWCustomVal is used as a key in the jobsDB customval column
	proc.config.GWCustomVal = proc.conf.GetStringVar("GW", "Gateway.CustomVal")
	proc.config.archiveInPreProcess = proc.conf.GetBoolVar(false, "Processor.archiveInPreProcess")
	proc.loadReloadableConfig(defaultPayloadLimit, defaultMaxEventsToProcess)
}

func (proc *Handle) loadReloadableConfig(defaultPayloadLimit int64, defaultMaxEventsToProcess int) {
	proc.payloadLimit = proc.conf.GetReloadableInt64Var(defaultPayloadLimit, 1, "Processor.payloadLimit")
	proc.config.maxLoopSleep = proc.conf.GetReloadableDurationVar(10000, time.Millisecond, "Processor.maxLoopSleep", "Processor.maxLoopSleepInMS")
	proc.config.storeTimeout = proc.conf.GetReloadableDurationVar(5, time.Minute, "Processor.storeTimeout")
	proc.config.pingerSleep = proc.conf.GetReloadableDurationVar(1000, time.Millisecond, "Processor.pingerSleep")
	proc.config.readLoopSleep = proc.conf.GetReloadableDurationVar(1000, time.Millisecond, "Processor.readLoopSleep")
	proc.config.maxEventsToProcess = proc.conf.GetReloadableIntVar(defaultMaxEventsToProcess, 1, "Processor.maxLoopProcessEvents")
	proc.config.archivalEnabled = proc.conf.GetReloadableBoolVar(true, "archival.Enabled")
	// Capture event name as a tag in event level stats
	proc.config.captureEventNameStats = proc.conf.GetReloadableBoolVar(false, "Processor.Stats.captureEventName")
	proc.config.enableConcurrentStore = proc.conf.GetReloadableBoolVar(false, "Processor.enableConcurrentStore")
	// UserTransformation mirroring settings
	proc.config.userTransformationMirroringSanitySampling = proc.conf.GetReloadableFloat64Var(0, "Processor.userTransformationMirroring.sanitySampling")
	proc.config.userTransformationMirroringFireAndForget = proc.conf.GetReloadableBoolVar(false, "Processor.userTransformationMirroring.fireAndForget")
	proc.config.storeSamplerEnabled = proc.conf.GetReloadableBoolVar(false, "Processor.storeSamplerEnabled")
}

type connection struct {
	sourceID, destinationID string
}

func (proc *Handle) backendConfigSubscriber(ctx context.Context) {
	var initDone bool
	ch := proc.backendConfig.Subscribe(ctx, backendconfig.TopicProcessConfig)
	for data := range ch {
		config := data.Data.(map[string]backendconfig.ConfigT)
		var (
			oneTrustConsentCategoriesMap = make(map[string][]string)
			ketchConsentCategoriesMap    = make(map[string][]string)
			genericConsentManagementMap  = make(SourceConsentMap)
			workspaceLibrariesMap        = make(map[string]backendconfig.LibrariesT, len(config))
			sourceIdDestinationMap       = make(map[string][]backendconfig.DestinationT)
			sourceIdSourceMap            = make(map[string]backendconfig.SourceT)
			eventAuditEnabled            = make(map[string]bool)
			credentialsMap               = make(map[string][]types.Credential)
			nonEventStreamSources        = make(map[string]bool)
			connectionConfigMap          = make(map[connection]backendconfig.Connection)
		)
		for workspaceID, wConfig := range config {
			for _, conn := range wConfig.Connections {
				connectionConfigMap[connection{sourceID: conn.SourceID, destinationID: conn.DestinationID}] = conn
			}
			for i := range wConfig.Sources {
				source := &wConfig.Sources[i]
				sourceIdSourceMap[source.ID] = *source
				if source.Enabled {
					sourceIdDestinationMap[source.ID] = source.Destinations
					genericConsentManagementMap[SourceID(source.ID)] = make(DestConsentMap)
					for j := range source.Destinations {
						destination := &source.Destinations[j]
						oneTrustConsentCategoriesMap[destination.ID] = getOneTrustConsentCategories(destination)
						ketchConsentCategoriesMap[destination.ID] = getKetchConsentCategories(destination)

						var err error
						genericConsentManagementMap[SourceID(source.ID)][DestinationID(destination.ID)], err = getGenericConsentManagementData(destination)
						if err != nil {
							proc.logger.Errorn("Error in pinger loop", obskit.Error(err))
						}
					}
				}
				if source.SourceDefinition.Category != "" && !strings.EqualFold(source.SourceDefinition.Category, sourceCategoryWebhook) {
					nonEventStreamSources[source.ID] = true
				}
			}
			workspaceLibrariesMap[workspaceID] = wConfig.Libraries
			eventAuditEnabled[workspaceID] = wConfig.Settings.EventAuditEnabled
			credentialsMap[workspaceID] = lo.MapToSlice(wConfig.Credentials, func(key string, value backendconfig.Credential) types.Credential {
				return types.Credential{
					ID:       key,
					Key:      value.Key,
					Value:    value.Value,
					IsSecret: value.IsSecret,
				}
			})
		}
		proc.config.configSubscriberLock.Lock()
		proc.config.connectionConfigMap = connectionConfigMap
		proc.config.oneTrustConsentCategoriesMap = oneTrustConsentCategoriesMap
		proc.config.ketchConsentCategoriesMap = ketchConsentCategoriesMap
		proc.config.genericConsentManagementMap = genericConsentManagementMap
		proc.config.workspaceLibrariesMap = workspaceLibrariesMap
		proc.config.sourceIdDestinationMap = sourceIdDestinationMap
		proc.config.sourceIdSourceMap = sourceIdSourceMap
		proc.config.eventAuditEnabled = eventAuditEnabled
		proc.config.credentialsMap = credentialsMap
		proc.config.nonEventStreamSources = nonEventStreamSources
		proc.config.configSubscriberLock.Unlock()
		if !initDone {
			initDone = true
			proc.config.asyncInit.Done()
		}
	}
}

func (proc *Handle) getWorkspaceLibraries(workspaceID string) backendconfig.LibrariesT {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.workspaceLibrariesMap[workspaceID]
}

func (proc *Handle) getConnectionConfig(conn connection) backendconfig.Connection {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.connectionConfigMap[conn]
}

func (proc *Handle) getSourceBySourceID(sourceId string) (*backendconfig.SourceT, error) {
	var err error
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	source, ok := proc.config.sourceIdSourceMap[sourceId]
	if !ok {
		err = errors.New("source not found for sourceId")
		proc.logger.Errorn("Processor : source not found for sourceId", obskit.SourceID(sourceId))
	}
	return &source, err
}

func (proc *Handle) getNonEventStreamSources() map[string]bool {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.nonEventStreamSources
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

func getTimestampFromEvent(event types.SingularEventT, field string, defaultTimestamp time.Time) time.Time {
	var timestamp time.Time
	var ok bool
	if timestamp, ok = misc.GetParsedTimestamp(event[field]); !ok {
		timestamp = defaultTimestamp
	}
	return timestamp
}

func enhanceWithTimeFields(event *types.TransformerEvent, singularEvent types.SingularEventT, receivedAt time.Time) {
	// set timestamp skew based on timestamp fields from SDKs
	originalTimestamp := getTimestampFromEvent(singularEvent, "originalTimestamp", receivedAt)
	sentAt := getTimestampFromEvent(singularEvent, "sentAt", receivedAt)
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

func (proc *Handle) singularEventMetadata(singularEvent types.SingularEventT, userID, partitionID string, jobId int64, receivedAt time.Time, source *backendconfig.SourceT, eventParams types.EventParams) *types.Metadata {
	commonMetadata := types.Metadata{}
	// job metadata
	commonMetadata.JobID = jobId
	commonMetadata.WorkspaceID = source.WorkspaceID
	commonMetadata.RudderID = userID
	commonMetadata.ReceivedAt = receivedAt.Format(misc.RFC3339Milli)
	commonMetadata.PartitionID = partitionID

	// event-related metadata
	commonMetadata.MessageID = stringify.Any(singularEvent["messageId"])
	commonMetadata.EventName, _ = misc.MapLookup(singularEvent, "event").(string)
	commonMetadata.EventType, _ = misc.MapLookup(singularEvent, "type").(string)

	// source metadata
	commonMetadata.SourceID = source.ID
	commonMetadata.OriginalSourceID = source.OriginalID
	commonMetadata.SourceDefinitionID = source.SourceDefinition.ID
	commonMetadata.SourceName = source.Name
	commonMetadata.SourceType = source.SourceDefinition.Name
	commonMetadata.SourceCategory = source.SourceDefinition.Category
	commonMetadata.SourceDefinitionType = source.SourceDefinition.Type

	// retl metadata
	commonMetadata.SourceJobID, _ = misc.MapLookup(singularEvent, "context", "sources", "job_id").(string)
	commonMetadata.SourceJobRunID = eventParams.SourceJobRunId
	commonMetadata.SourceTaskRunID = eventParams.SourceTaskRunId
	commonMetadata.RecordID = misc.MapLookup(singularEvent, "recordId")

	// other metadata
	commonMetadata.InstanceID = proc.instanceID
	commonMetadata.Namespace = proc.namespace
	commonMetadata.TraceParent = eventParams.TraceParent

	return &commonMetadata
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
			err := jsonrs.Unmarshal(job.Parameters, &params)
			if err != nil {
				proc.logger.Errorn("Error while UnMarshaling live event parameters", obskit.Error(err))
				continue
			}

			sourceID, _ := params["source_id"].(string)
			destID, _ := params["destination_id"].(string)
			procErr, _ := params["error"].(string)
			procErr = strconv.Quote(procErr)
			statusCode := fmt.Sprint(params["status_code"])
			sentAt := time.Now().Format(misc.RFC3339Milli)
			events := make([]map[string]interface{}, 0)
			err = jsonrs.Unmarshal(job.EventPayload, &events)
			if err != nil {
				proc.logger.Errorn("Error while UnMarshaling live event payload", obskit.Error(err))
				continue
			}
			for i := range events {
				event := &events[i]
				eventPayload, err := jsonrs.Marshal(*event)
				if err != nil {
					proc.logger.Errorn("Error while Marshaling live event payload", obskit.Error(err))
					continue
				}

				eventName := stringify.Any(gjson.GetBytes(eventPayload, "event").String())
				eventType := stringify.Any(gjson.GetBytes(eventPayload, "type").String())
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

func (proc *Handle) getTransformerEvents(
	response types.Response,
	commonMetaData *types.Metadata,
	eventsByMessageID map[string]types.SingularEventWithReceivedAt,
	destination *backendconfig.DestinationT,
	connection backendconfig.Connection,
	inPU, pu string,
) (
	[]types.TransformerEvent,
	[]*reportingtypes.PUReportedMetric,
	map[string]int64,
	map[string]MetricMetadata,
) {
	successMetrics := make([]*reportingtypes.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*reportingtypes.ConnectionDetails)
	statusDetailsMap := make(map[string]map[string]*reportingtypes.StatusDetail)
	successCountMap := make(map[string]int64)
	successCountMetadataMap := make(map[string]MetricMetadata)
	var eventsToTransform []types.TransformerEvent
	for i := range response.Events {
		// Update metrics maps
		userTransformedEvent := &response.Events[i]
		messages := lo.Map(
			userTransformedEvent.Metadata.GetMessagesIDs(),
			func(msgID string, _ int) types.SingularEventT {
				return eventsByMessageID[msgID].SingularEvent
			},
		)

		// Update metadata with updated event name before reporting
		updatedEventName := userTransformedEvent.Metadata.EventName
		if en, ok := userTransformedEvent.Output["event"].(string); ok {
			updatedEventName = en
		}
		userTransformedEvent.Metadata.EventName = updatedEventName

		updatedEventType := userTransformedEvent.Metadata.EventType
		if et, ok := userTransformedEvent.Output["type"].(string); ok {
			updatedEventType = et
		}
		userTransformedEvent.Metadata.EventType = updatedEventType

		for _, message := range messages {
			proc.updateMetricMaps(successCountMetadataMap, successCountMap, connectionDetailsMap, statusDetailsMap, userTransformedEvent, jobsdb.Succeeded.State, pu, func() json.RawMessage {
				if pu != reportingtypes.TRACKINGPLAN_VALIDATOR {
					return nil
				}
				if proc.transientSources.Apply(commonMetaData.SourceID) {
					return nil
				}

				sampleEvent, err := jsonrs.Marshal(message)
				if err != nil {
					proc.logger.Errorn("[Processor: getDestTransformerEvents] Failed to unmarshal first element in transformed events", obskit.Error(err))
					sampleEvent = nil
				}
				return sampleEvent
			},
				nil)
		}

		eventMetadata := commonMetaData
		//
		// fill non common metadata from response, effectively this strips out:
		// - tracking plan information after tracking plan validation
		// - user transformation information after user transformation (except for message IDs)

		// job metadata
		eventMetadata.JobID = userTransformedEvent.Metadata.JobID
		eventMetadata.RudderID = userTransformedEvent.Metadata.RudderID
		eventMetadata.ReceivedAt = userTransformedEvent.Metadata.ReceivedAt
		eventMetadata.PartitionID = userTransformedEvent.Metadata.PartitionID

		// event metadata
		eventMetadata.MessageID = userTransformedEvent.Metadata.MessageID
		eventMetadata.EventName = userTransformedEvent.Metadata.EventName
		eventMetadata.EventType = userTransformedEvent.Metadata.EventType

		// retl metadata
		eventMetadata.SourceJobID = userTransformedEvent.Metadata.SourceJobID
		eventMetadata.SourceJobRunID = userTransformedEvent.Metadata.SourceJobRunID
		eventMetadata.SourceTaskRunID = userTransformedEvent.Metadata.SourceTaskRunID
		eventMetadata.RecordID = userTransformedEvent.Metadata.RecordID

		// other metadata
		eventMetadata.TraceParent = userTransformedEvent.Metadata.TraceParent

		// user transformation message ids
		eventMetadata.MessageIDs = userTransformedEvent.Metadata.MessageIDs

		updatedEvent := types.TransformerEvent{
			Message:     userTransformedEvent.Output,
			Metadata:    *eventMetadata,
			Destination: *destination,
			Connection:  connection,
			Credentials: proc.config.credentialsMap[commonMetaData.WorkspaceID],
		}
		eventsToTransform = append(eventsToTransform, updatedEvent)
	}

	// REPORTING - START
	if proc.isReportingEnabled() {
		reportingtypes.AssertSameKeys(connectionDetailsMap, statusDetailsMap)

		for k, cd := range connectionDetailsMap {
			for _, sd := range statusDetailsMap[k] {
				m := &reportingtypes.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *reportingtypes.CreatePUDetails(inPU, pu, false, false),
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
	// countMetadataMap provides metadata context for getDiffMetrics to create detailed PUReportedMetric objects
	// storing rich context during event processing for diff metric reporting
	countMetadataMap map[string]MetricMetadata,

	// countMap accumulates event counts by unique key combinations, feeding into getDiffMetrics to capture diff metrics
	countMap map[string]int64,

	// connectionDetailsMap stores source-destination relationship data that becomes PUReportedMetric.ConnectionDetails
	// for constructing complete reporting metrics with workspace, source, and destination context
	connectionDetailsMap map[string]*reportingtypes.ConnectionDetails,

	// statusDetailsMap captures processing outcomes including error details, validation violations, and sample payloads
	// that become PUReportedMetric.StatusDetail
	statusDetailsMap map[string]map[string]*reportingtypes.StatusDetail,

	event *types.TransformerResponse,
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

	if countMap != nil {
		countMap[countKey] = countMap[countKey] + 1
	}

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
				trackingPlanID:          event.Metadata.TrackingPlanID,
				trackingPlanVersion:     event.Metadata.TrackingPlanVersion,
			}
		}
	}

	key := event.Metadata.SourceID + ":" +
		event.Metadata.DestinationID + ":" +
		event.Metadata.SourceJobRunID + ":" +
		event.Metadata.TransformationID + ":" +
		event.Metadata.TransformationVersionID + ":" +
		event.Metadata.TrackingPlanID + ":" +
		strconv.Itoa(event.Metadata.TrackingPlanVersion)

	if _, ok := connectionDetailsMap[key]; !ok {
		connectionDetailsMap[key] = &reportingtypes.ConnectionDetails{
			SourceID:                event.Metadata.SourceID,
			SourceTaskRunID:         event.Metadata.SourceTaskRunID,
			SourceJobID:             event.Metadata.SourceJobID,
			SourceJobRunID:          event.Metadata.SourceJobRunID,
			SourceDefinitionID:      event.Metadata.SourceDefinitionID,
			SourceCategory:          event.Metadata.SourceCategory,
			DestinationID:           event.Metadata.DestinationID,
			DestinationDefinitionID: event.Metadata.DestinationDefinitionID,
			TransformationID:        event.Metadata.TransformationID,
			TransformationVersionID: event.Metadata.TransformationVersionID,
			TrackingPlanID:          event.Metadata.TrackingPlanID,
			TrackingPlanVersion:     event.Metadata.TrackingPlanVersion,
		}
	}

	if _, ok := statusDetailsMap[key]; !ok {
		statusDetailsMap[key] = make(map[string]*reportingtypes.StatusDetail)
	}
	// create status details for each validation error
	// single event can have multiple validation errors of same type
	veCount := len(event.ValidationErrors)
	if stage == reportingtypes.TRACKINGPLAN_VALIDATOR && status == jobsdb.Succeeded.State {
		if veCount > 0 {
			status = reportingtypes.SUCCEEDED_WITH_VIOLATIONS
		} else {
			status = reportingtypes.SUCCEEDED_WITHOUT_VIOLATIONS
		}
	}
	sdkeySet := map[string]struct{}{}
	for _, ve := range event.ValidationErrors {
		sdkey := status + ":" + strconv.Itoa(event.StatusCode) + ":" + eventName + ":" + eventType + ":" + ve.Type
		sdkeySet[sdkey] = struct{}{}

		sd, ok := statusDetailsMap[key][sdkey]
		if !ok {
			sd = &reportingtypes.StatusDetail{
				Status:         status,
				StatusCode:     event.StatusCode,
				SampleResponse: event.Error,
				SampleEvent:    payload(),
				EventName:      eventName,
				EventType:      eventType,
				ErrorType:      ve.Type,
				StatTags:       event.StatTags,
			}
			statusDetailsMap[key][sdkey] = sd
		}
		sd.ViolationCount += incrementCount
	}
	for k := range sdkeySet {
		statusDetailsMap[key][k].Count += incrementCount
	}

	// create status details for a whole event
	sdkey := status + ":" + strconv.Itoa(event.StatusCode) + ":" + eventName + ":" + eventType + ":"
	sd, ok := statusDetailsMap[key][sdkey]
	if !ok {
		sd = &reportingtypes.StatusDetail{
			Status:         status,
			StatusCode:     event.StatusCode,
			SampleResponse: event.Error,
			SampleEvent:    payload(),
			EventName:      eventName,
			EventType:      eventType,
			StatTags:       event.StatTags,
		}
		statusDetailsMap[key][sdkey] = sd
	}

	sd.Count += incrementCount
	if status == jobsdb.Aborted.State {
		messageIDs := lo.Uniq(event.Metadata.GetMessagesIDs()) // this is called defensive programming... :(
		for _, messageID := range messageIDs {
			receivedAt := eventsByMessageID[messageID].ReceivedAt
			sd.FailedMessages = append(sd.FailedMessages, &reportingtypes.FailedMessage{MessageID: messageID, ReceivedAt: receivedAt})
		}
	}
	sd.ViolationCount += int64(veCount)
}

func (proc *Handle) getNonSuccessfulMetrics(
	response types.Response,
	inputEvents []types.TransformerEvent,
	commonMetaData *types.Metadata,
	eventsByMessageID map[string]types.SingularEventWithReceivedAt,
	inPU, pu string,
) *NonSuccessfulTransformationMetrics {
	m := &NonSuccessfulTransformationMetrics{}

	grouped := lo.GroupBy(
		response.FailedEvents,
		func(event types.TransformerResponse) bool {
			return event.StatusCode == reportingtypes.FilterEventCode
		},
	)
	filtered, failed := grouped[true], grouped[false]

	metadataByMessageID := make(map[string]*types.Metadata)
	for _, event := range inputEvents {
		metadataByMessageID[event.Metadata.MessageID] = &event.Metadata
	}

	m.filteredJobs, m.filteredMetrics, m.filteredCountMap = proc.getTransformationMetrics(
		filtered,
		jobsdb.Filtered.State,
		commonMetaData,
		eventsByMessageID,
		metadataByMessageID,
		inPU,
		pu,
	)

	m.failedJobs, m.failedMetrics, m.failedCountMap = proc.getTransformationMetrics(
		failed,
		jobsdb.Aborted.State,
		commonMetaData,
		eventsByMessageID,
		metadataByMessageID,
		inPU,
		pu,
	)

	return m
}

func procFilteredCountStat(destType, pu, statusCode string) {
	stats.Default.NewTaggedStat(
		"proc_filtered_counts",
		stats.CountType,
		stats.Tags{
			"destName":   destType,
			"statusCode": statusCode,
			"stage":      pu,
		},
	).Increment()
}

func procErrorCountsStat(destType, pu, statusCode string) {
	stats.Default.NewTaggedStat(
		"proc_error_counts",
		stats.CountType,
		stats.Tags{
			"destName":   destType,
			"statusCode": statusCode,
			"stage":      pu,
		},
	).Increment()
}

func (proc *Handle) getTransformationMetrics(
	transformerResponses []types.TransformerResponse,
	state string,
	commonMetaData *types.Metadata,
	eventsByMessageID map[string]types.SingularEventWithReceivedAt,
	metadataByMessageID map[string]*types.Metadata,
	inPU, pu string,
) ([]*jobsdb.JobT, []*reportingtypes.PUReportedMetric, map[string]int64) {
	metrics := make([]*reportingtypes.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*reportingtypes.ConnectionDetails)
	statusDetailsMap := make(map[string]map[string]*reportingtypes.StatusDetail)
	countMap := make(map[string]int64)
	var jobs []*jobsdb.JobT
	statFunc := procErrorCountsStat
	if state == jobsdb.Filtered.State {
		statFunc = procFilteredCountStat
	}
	for i := range transformerResponses {
		failedEvent := &transformerResponses[i]
		messages := lo.Map(
			failedEvent.Metadata.GetMessagesIDs(),
			func(msgID string, _ int) types.SingularEventT {
				return eventsByMessageID[msgID].SingularEvent
			},
		)
		payload, err := jsonrs.Marshal(messages)
		if err != nil {
			proc.logger.Errorn("[Processor: getTransformationMetrics] Failed to unmarshal list of failed events", obskit.Error(err))
			continue
		}

		for _, messageID := range failedEvent.Metadata.GetMessagesIDs() {
			message := eventsByMessageID[messageID].SingularEvent
			failedEventWithMetadata := *failedEvent
			if metadata, ok := metadataByMessageID[messageID]; ok {
				failedEventWithMetadata.Metadata = *metadata
			}

			proc.updateMetricMaps(
				nil,
				countMap,
				connectionDetailsMap,
				statusDetailsMap,
				&failedEventWithMetadata,
				state,
				pu,
				func() json.RawMessage {
					if proc.transientSources.Apply(commonMetaData.SourceID) {
						return nil
					}
					sampleEvent, err := jsonrs.Marshal(message)
					if err != nil {
						proc.logger.Errorn("[Processor: getTransformationMetrics] Failed to unmarshal first element in failed events", obskit.Error(err))
						sampleEvent = nil
					}
					return sampleEvent
				},
				eventsByMessageID)
		}

		proc.logger.Debugn(
			"[Processor: getTransformationMetrics] Error for source and destination",
			logger.NewIntField("statusCode", int64(failedEvent.StatusCode)),
			obskit.SourceID(commonMetaData.SourceID),
			obskit.DestinationID(commonMetaData.DestinationID),
			logger.NewStringField("error", failedEvent.Error),
		)

		id := misc.FastUUID()
		params := map[string]interface{}{
			"source_id":          commonMetaData.SourceID,
			"destination_id":     commonMetaData.DestinationID,
			"source_job_run_id":  failedEvent.Metadata.SourceJobRunID,
			"error":              failedEvent.Error,
			"status_code":        failedEvent.StatusCode,
			"stage":              pu,
			"record_id":          failedEvent.Metadata.RecordID,
			"source_task_run_id": failedEvent.Metadata.SourceTaskRunID,
			"connection_id":      generateConnectionID(commonMetaData.SourceID, commonMetaData.DestinationID),
		}
		if eventContext, castOk := failedEvent.Output["context"].(map[string]interface{}); castOk {
			params["violationErrors"] = eventContext["violationErrors"]
		}
		marshalledParams, err := jsonrs.Marshal(params)
		if err != nil {
			proc.logger.Errorn("[Processor] Failed to marshal parameters", logger.NewStringField("parameters", stringify.Any(params)))
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

		statFunc(commonMetaData.DestinationType, pu, strconv.Itoa(failedEvent.StatusCode))
	}

	// REPORTING - START
	if proc.isReportingEnabled() {
		reportingtypes.AssertSameKeys(connectionDetailsMap, statusDetailsMap)
		for k, cd := range connectionDetailsMap {
			for _, sd := range statusDetailsMap[k] {
				m := &reportingtypes.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *reportingtypes.CreatePUDetails(inPU, pu, false, false),
					StatusDetail:      sd,
				}
				metrics = append(metrics, m)
			}
		}
	}
	// REPORTING - END

	return jobs, metrics, countMap
}

func generateConnectionID(s1, s2 string) string {
	return s1 + ":" + s2
}

func (proc *Handle) updateSourceEventStatsDetailed(event types.SingularEventT, sourceId string) {
	// Any panics in this function are captured and ignore sending the stat
	defer func() {
		if r := recover(); r != nil {
			proc.logger.Error(r) // nolint:forbidigo
		}
	}()
	var eventType string
	var eventName string
	source, err := proc.getSourceBySourceID(sourceId)
	if err != nil {
		proc.logger.Errorn("[Processor] Failed to get source by source id", obskit.SourceID(sourceId))
		return
	}
	if val, ok := event["type"]; ok {
		eventType, _ = val.(string)
		tags := map[string]string{
			"writeKey":   source.WriteKey,
			"event_type": eventType,
		}
		statEventType := proc.statsFactory.NewSampledTaggedStat("processor_event_type", stats.CountType, tags)
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
			statEventTypeDetailed := proc.statsFactory.NewSampledTaggedStat("processor_event_type_detailed", stats.CountType, tagsDetailed)
			statEventTypeDetailed.Count(1)
		}
	}
}

func getDiffMetrics(
	inPU, pu string,
	inCountMetadataMap map[string]MetricMetadata,
	successCountMetadataMap map[string]MetricMetadata,
	inCountMap, successCountMap, failedCountMap, filteredCountMap map[string]int64,
	statFactory stats.Stats,
) []*reportingtypes.PUReportedMetric {
	diffMetrics := make([]*reportingtypes.PUReportedMetric, 0)

	// Helper function to create metric and record stats
	createMetricAndRecordStats := func(metadata MetricMetadata, key string, count int64) *reportingtypes.PUReportedMetric {
		var eventName, eventType string
		splitKey := strings.Split(key, MetricKeyDelimiter)
		if len(splitKey) >= 5 {
			eventName = splitKey[3]
			eventType = splitKey[4]
		}

		metric := &reportingtypes.PUReportedMetric{
			ConnectionDetails: reportingtypes.ConnectionDetails{
				SourceID:                metadata.sourceID,
				DestinationID:           metadata.destinationID,
				SourceTaskRunID:         metadata.sourceTaskRunID,
				SourceJobID:             metadata.sourceJobID,
				SourceJobRunID:          metadata.sourceJobRunID,
				SourceDefinitionID:      metadata.sourceDefinitionID,
				DestinationDefinitionID: metadata.destinationDefinitionID,
				SourceCategory:          metadata.sourceCategory,
				TransformationID:        metadata.transformationID,
				TransformationVersionID: metadata.transformationVersionID,
				TrackingPlanID:          metadata.trackingPlanID,
				TrackingPlanVersion:     metadata.trackingPlanVersion,
			},
			PUDetails: reportingtypes.PUDetails{
				InPU: inPU,
				PU:   pu,
			},
			StatusDetail: &reportingtypes.StatusDetail{
				Status:      reportingtypes.DiffStatus,
				Count:       count,
				SampleEvent: nil,
				EventName:   eventName,
				EventType:   eventType,
			},
		}

		statFactory.NewTaggedStat(
			"processor_diff_count",
			stats.CountType,
			stats.Tags{
				"stage":         metric.PU,
				"sourceId":      metric.ConnectionDetails.SourceID,
				"destinationId": metric.ConnectionDetails.DestinationID,
			},
		).Count(int(count))

		return metric
	}

	// Process input metrics
	for key, inCount := range inCountMap {
		successCount := successCountMap[key]
		failedCount := failedCountMap[key]
		filteredCount := filteredCountMap[key]
		diff := successCount + failedCount + filteredCount - inCount

		if diff != 0 {
			metric := createMetricAndRecordStats(inCountMetadataMap[key], key, diff)
			diffMetrics = append(diffMetrics, metric)
		}
	}

	// Process success metrics if enabled
	for key, successMetadata := range successCountMetadataMap {
		// Skip if this key was already processed from input metrics
		if _, exists := inCountMap[key]; exists {
			continue
		}

		successCount := successCountMap[key]
		if successCount > 0 {
			metric := createMetricAndRecordStats(successMetadata, key, successCount)
			diffMetrics = append(diffMetrics, metric)
		}
	}

	return diffMetrics
}

type dupStatKey struct {
	sourceID string
}

func (proc *Handle) eventAuditEnabled(workspaceID string) bool {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	return proc.config.eventAuditEnabled[workspaceID]
}

type preTransformationMessage struct {
	partition                     string
	subJobs                       subJob
	eventSchemaJobsBySourceId     map[SourceIDT][]*jobsdb.JobT
	archivalJobs                  []*jobsdb.JobT
	connectionDetailsMap          map[string]*reportingtypes.ConnectionDetails
	statusDetailsMap              map[string]map[string]*reportingtypes.StatusDetail
	enricherStatusDetailsMap      map[string]map[string]*reportingtypes.StatusDetail
	botManagementStatusDetailsMap map[string]map[string]*reportingtypes.StatusDetail
	eventBlockingStatusDetailsMap map[string]map[string]*reportingtypes.StatusDetail
	destFilterStatusDetailMap     map[string]map[string]*reportingtypes.StatusDetail
	reportMetrics                 []*reportingtypes.PUReportedMetric
	totalEvents                   int
	groupedEventsBySourceId       map[SourceIDT][]types.TransformerEvent
	eventsByMessageID             map[string]types.SingularEventWithReceivedAt
	jobIDToSpecificDestMapOnly    map[int64]string
	statusList                    []*jobsdb.JobStatusT
	jobList                       []*jobsdb.JobT
	sourceDupStats                map[dupStatKey]int
	dedupKeys                     map[string]struct{}
}

func (proc *Handle) preprocessStage(partition string, subJobs subJob, delay time.Duration) (*srcHydrationMessage, error) {
	spanTags := stats.Tags{"partition": partition}
	ctx, processJobsSpan := proc.tracer.Trace(subJobs.ctx, "preprocessStage", tracing.WithTraceTags(spanTags))
	defer processJobsSpan.End()

	var preprocessdelaySleeper preprocessdelay.Sleeper
	if proc.limiter.preprocess != nil {
		limiterExec := proc.limiter.preprocess.BeginWithSleepAndPriority(partition, proc.getLimiterPriority(partition))
		preprocessdelaySleeper = limiterExec.Sleep
		defer limiterExec.End()
		defer proc.stats.statPreprocessStageCount(partition).Count(len(subJobs.subJobs))
	}

	delayHandler := preprocessdelay.NewHandle(delay, preprocessdelaySleeper)
	jobList := subJobs.subJobs
	proc.stats.statNumRequests(partition).Count(len(jobList))

	var statusList []*jobsdb.JobStatusT
	groupedEventsBySourceId := make(map[SourceIDT][]types.TransformerEvent)
	eventsByMessageID := make(map[string]types.SingularEventWithReceivedAt)
	eventSchemaJobsBySourceId := make(map[SourceIDT][]*jobsdb.JobT)
	archivalJobs := make([]*jobsdb.JobT, 0)

	// Each block we receive from a client has a bunch of
	// requests. We parse the block and take out individual
	// requests, call the destination specific transformation
	// function and create jobs for them.
	// Transformation is called for a batch of jobs at a time
	// to speed-up execution.

	// Event count for performance stat monitoring
	totalEvents := 0

	proc.logger.Debugn("[Processor] Total jobs picked up", logger.NewIntField("jobCount", int64(len(jobList))))

	dedupKeys := make(map[string]struct{})
	sourceDupStats := make(map[dupStatKey]int)

	reportMetrics := make([]*reportingtypes.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*reportingtypes.ConnectionDetails)
	statusDetailsMap := make(map[string]map[string]*reportingtypes.StatusDetail)
	destFilterStatusDetailMap := make(map[string]map[string]*reportingtypes.StatusDetail)
	enricherStatusDetailsMap := make(map[string]map[string]*reportingtypes.StatusDetail)
	botManagementStatusDetailsMap := make(map[string]map[string]*reportingtypes.StatusDetail)
	eventBlockingStatusDetailsMap := make(map[string]map[string]*reportingtypes.StatusDetail)
	// map of jobID to destinationID: for messages that needs to be delivered to a specific destinations only
	jobIDToSpecificDestMapOnly := make(map[int64]string)

	spans := make([]stats.TraceSpan, 0, len(jobList))
	defer func() {
		for _, span := range spans {
			span.End()
		}
	}()

	type jobWithMetaData struct {
		jobID         int64
		workspaceID   string
		singularEvent types.SingularEventT
		messageID     string
		userId        string
		eventParams   types.EventParams
		dedupKey      dedup.BatchKey
		requestIP     string
		receivedAt    time.Time
		parameters    json.RawMessage
		span          stats.TraceSpan
		source        *backendconfig.SourceT
		uuid          uuid.UUID
		customVal     string
		partitionID   string
		payloadFunc   func() json.RawMessage
	}

	var jobsWithMetaData []jobWithMetaData
	var dedupBatchKeys []dedup.BatchKey
	var dedupBatchKeysIdx int
	for _, job := range jobList {
		var eventParams types.EventParams
		if err := jsonrs.Unmarshal(job.Parameters, &eventParams); err != nil {
			return nil, err
		}

		var span stats.TraceSpan
		traceParent := eventParams.TraceParent
		if traceParent == "" {
			proc.logger.Debugn("Missing traceParent in preprocessStage", logger.NewIntField("jobId", job.JobID))
		} else {
			ctx := stats.InjectTraceParentIntoContext(context.Background(), traceParent)
			_, span = proc.tracer.Trace(ctx, "preprocessStage", tracing.WithTraceKind(stats.SpanKindConsumer),
				tracing.WithTraceTags(stats.Tags{
					"workspaceId": job.WorkspaceId,
					"sourceId":    eventParams.SourceId,
					"partition":   partition,
				}),
			)
			spans = append(spans, span)
		}

		newStatus := jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      jobsdb.Succeeded.State,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{"success":"OK"}`),
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
			PartitionID:   job.PartitionID,
		}
		statusList = append(statusList, &newStatus)

		parameters := job.Parameters
		var gatewayBatchEvent types.GatewayBatchRequest
		if err := jsonrs.Unmarshal(job.EventPayload, &gatewayBatchEvent); err != nil {
			if span != nil {
				span.SetStatus(stats.SpanStatusError, "cannot unmarshal event payload")
			}
			proc.logger.Warnn("json parsing of event payload", logger.NewIntField("jobID", job.JobID), obskit.Error(err))
			gatewayBatchEvent.Batch = []types.SingularEventT{}
			continue
		}
		requestIP := gatewayBatchEvent.RequestIP
		receivedAt := gatewayBatchEvent.ReceivedAt

		proc.statsFactory.NewSampledTaggedStat("processor.event_pickup_lag_seconds", stats.TimerType, stats.Tags{
			"sourceId":    eventParams.SourceId,
			"workspaceId": job.WorkspaceId,
		}).Since(receivedAt)

		source, err := proc.getSourceBySourceID(eventParams.SourceId)
		if err != nil {
			if span != nil {
				span.SetStatus(stats.SpanStatusError, "source not found for sourceId")
			}
			continue
		}

		for _, e := range proc.enrichers {
			if err := e.Enrich(source, &gatewayBatchEvent, &eventParams); err != nil {
				proc.logger.Errorn("unable to enrich the gateway batch event", obskit.Error(err))
			}
		}

		for _, singularEvent := range gatewayBatchEvent.Batch {
			messageId := stringify.Any(singularEvent["messageId"])
			payloadFunc := ro.Memoize(func() json.RawMessage {
				payloadBytes, err := jsonrs.Marshal(singularEvent)
				if err != nil {
					return nil
				}
				return payloadBytes
			})
			dedupBatchKey := dedup.BatchKey{
				Index: dedupBatchKeysIdx,
				Key:   messageId + eventParams.SourceJobRunId,
			}
			dedupBatchKeysIdx++
			jobsWithMetaData = append(jobsWithMetaData, jobWithMetaData{
				jobID:         job.JobID,
				userId:        job.UserID,
				workspaceID:   job.WorkspaceId,
				partitionID:   job.PartitionID,
				singularEvent: singularEvent,
				messageID:     messageId,
				eventParams:   eventParams,
				dedupKey:      dedupBatchKey,
				requestIP:     requestIP,
				receivedAt:    receivedAt,
				parameters:    parameters,
				span:          span,
				source:        source,
				uuid:          job.UUID,
				customVal:     job.CustomVal,
				payloadFunc:   payloadFunc,
			})
			dedupBatchKeys = append(dedupBatchKeys, dedupBatchKey)
		}
		delayHandler.JobReceivedAt(receivedAt)
	}

	if err := delayHandler.Sleep(proc.backgroundCtx); err != nil {
		proc.logger.Infon("preprocess delay sleep interrupted", logger.NewStringField("partition", partition), obskit.Error(err))
		return nil, types.ErrProcessorStopping
	}

	var allowedBatchKeys map[dedup.BatchKey]bool
	var err error
	if proc.config.enableDedup {
		_ = proc.tracer.TraceFunc(ctx, "processJobsForDest.dedupAllowed", func(ctx context.Context) {
			allowedBatchKeys, err = proc.dedup.Allowed(dedupBatchKeys...)
		}, tracing.WithTraceTags(spanTags))
		if err != nil {
			return nil, err
		}
	}
	for _, event := range jobsWithMetaData {
		sourceId := event.eventParams.SourceId
		if event.eventParams.DestinationID != "" {
			jobIDToSpecificDestMapOnly[event.jobID] = event.eventParams.DestinationID
		}

		singularEventMetadata := proc.singularEventMetadata(
			event.singularEvent,
			event.userId,
			event.partitionID,
			event.jobID,
			event.receivedAt,
			event.source,
			event.eventParams,
		)

		// dummy event for metrics purposes only
		reportingEvent := &types.TransformerResponse{}
		reportingEvent.Metadata = *singularEventMetadata

		if event.eventParams.IsBot {
			// REPORTING - BOT_MANAGEMENT metrics - START
			if proc.isReportingEnabled() {
				status := reportingtypes.BotDetectedStatus
				reportingEvent.StatusCode = reportingtypes.SuccessEventCode
				if event.eventParams.BotAction == reportingtypes.DropBotEventAction {
					status = jobsdb.Filtered.State
					reportingEvent.StatusCode = reportingtypes.FilterEventCode
				}

				proc.updateMetricMaps(
					nil,
					nil,
					connectionDetailsMap,
					botManagementStatusDetailsMap,
					reportingEvent,
					status,
					reportingtypes.BOT_MANAGEMENT,
					func() json.RawMessage {
						return nil
					},
					nil,
				)
				// reset status code to 0 because transformerEvent is reused for other metrics
				reportingEvent.StatusCode = 0
			}
			// REPORTING - BOT_MANAGEMENT metrics - END

			if event.eventParams.BotAction == reportingtypes.DropBotEventAction {
				proc.logger.Debugn("Dropping event because it is a bot event and bot action is drop")
				continue
			}
		}

		if event.eventParams.IsEventBlocked {
			// REPORTING - EVENT_BLOCKING metrics - START
			if proc.isReportingEnabled() {
				reportingEvent.StatusCode = reportingtypes.FilterEventCode

				proc.updateMetricMaps(
					nil,
					nil,
					connectionDetailsMap,
					eventBlockingStatusDetailsMap,
					reportingEvent,
					jobsdb.Filtered.State,
					reportingtypes.EVENT_BLOCKING,
					func() json.RawMessage {
						return nil
					},
					nil,
				)
				// reset status code to 0 because transformerEvent is reused for other metrics
				reportingEvent.StatusCode = 0
			}
			// REPORTING - EVENT_BLOCKING metrics - END

			proc.logger.Debugn("Dropping event because it is blocked by event blocking")
			continue
		}

		if proc.config.enableDedup {
			if !allowedBatchKeys[event.dedupKey] {
				proc.logger.Debugn("Dropping event with duplicate key %s", logger.NewStringField("key", event.dedupKey.Key))
				sourceDupStats[dupStatKey{sourceID: event.eventParams.SourceId}] += 1
				continue
			}
			dedupKeys[event.dedupKey.Key] = struct{}{}
		}

		proc.updateSourceEventStatsDetailed(event.singularEvent, sourceId)
		totalEvents++
		eventsByMessageID[event.messageID] = types.SingularEventWithReceivedAt{
			SingularEvent: event.singularEvent,
			ReceivedAt:    event.receivedAt,
		}

		sourceIsTransient := proc.transientSources.Apply(sourceId)
		if proc.config.eventSchemaV2Enabled && // schemas enabled
			proc.eventAuditEnabled(event.workspaceID) &&
			// TODO: could use source.SourceDefinition.Category instead?
			singularEventMetadata.SourceJobRunID == "" &&
			!sourceIsTransient {
			if eventPayload := event.payloadFunc(); eventPayload != nil {
				eventSchemaJobsBySourceId[SourceIDT(sourceId)] = append(eventSchemaJobsBySourceId[SourceIDT(sourceId)],
					&jobsdb.JobT{
						UUID:         event.uuid,
						UserID:       event.userId,
						Parameters:   event.parameters,
						CustomVal:    event.customVal,
						EventPayload: eventPayload,
						CreatedAt:    time.Now(),
						ExpireAt:     time.Now(),
						WorkspaceId:  event.workspaceID,
					},
				)
			}
		}

		if proc.config.archivalEnabled.Load() &&
			singularEventMetadata.SourceJobRunID == "" && // archival enabled&&
			!sourceIsTransient {
			if eventPayload := event.payloadFunc(); eventPayload != nil {
				archivalJobs = append(archivalJobs,
					&jobsdb.JobT{
						UUID:         event.uuid,
						UserID:       event.userId,
						Parameters:   event.parameters,
						CustomVal:    event.customVal,
						EventPayload: eventPayload,
						CreatedAt:    time.Now(),
						ExpireAt:     time.Now(),
						WorkspaceId:  event.workspaceID,
					},
				)
			}
		}
		// REPORTING - GATEWAY metrics - START
		if proc.isReportingEnabled() {
			proc.updateMetricMaps(
				nil,
				nil,
				connectionDetailsMap,
				statusDetailsMap,
				reportingEvent,
				jobsdb.Succeeded.State,
				reportingtypes.GATEWAY,
				func() json.RawMessage {
					if sourceIsTransient {
						return nil
					}
					if payload := event.payloadFunc(); payload != nil {
						return payload
					}
					return nil
				},
				nil,
			)

			if event.eventParams.IsBot {
				botStatus := reportingtypes.BotDetectedStatus
				if event.eventParams.BotAction == reportingtypes.FlagBotEventAction {
					botStatus = reportingtypes.BotFlaggedStatus
				}

				// Pass nil for countMetadataMap and countMap as we don't want to capture diff metrics for bot enricher
				proc.updateMetricMaps(
					nil,
					nil,
					connectionDetailsMap,
					enricherStatusDetailsMap,
					reportingEvent,
					botStatus,
					reportingtypes.GATEWAY,
					func() json.RawMessage {
						return nil
					},
					nil,
				)
			}
		}
		// REPORTING - GATEWAY metrics - END
		// Getting all the destinations which are enabled for this event.
		// Event will be dropped if no valid destination is present
		// if empty destinationID is passed in this fn all the destinations for the source are validated
		// else only passed destinationID will be validated
		if !proc.isDestinationAvailable(event.singularEvent, sourceId, event.eventParams.DestinationID) {
			// REPORTING - DESTINATION_FILTER filtered metrics - START
			if proc.isReportingEnabled() {
				reportingEvent.StatusCode = reportingtypes.FilterEventCode
				proc.updateMetricMaps(
					nil,
					nil,
					connectionDetailsMap,
					destFilterStatusDetailMap,
					reportingEvent,
					jobsdb.Filtered.State,
					reportingtypes.DESTINATION_FILTER,
					func() json.RawMessage {
						return nil
					},
					nil,
				)
				reportingEvent.StatusCode = 0
			}
			// REPORTING - DESTINATION_FILTER filtered metrics - END
			continue
		}

		if _, ok := groupedEventsBySourceId[SourceIDT(sourceId)]; !ok {
			groupedEventsBySourceId[SourceIDT(sourceId)] = make([]types.TransformerEvent, 0)
		}
		transformerEvent := types.TransformerEvent{}
		transformerEvent.Message = event.singularEvent
		transformerEvent.Message["request_ip"] = event.requestIP
		transformerEvent.Metadata = *singularEventMetadata
		enhanceWithTimeFields(&transformerEvent, event.singularEvent, event.receivedAt)

		// fill in tracking plan details
		trackingPlanID := event.source.DgSourceTrackingPlanConfig.TrackingPlan.Id
		trackingPlanVersion := event.source.DgSourceTrackingPlanConfig.TrackingPlan.Version
		rudderTyperTPID := misc.MapLookup(event.singularEvent, "context", "ruddertyper", "trackingPlanId")
		rudderTyperTPVersion := misc.MapLookup(event.singularEvent, "context", "ruddertyper", "trackingPlanVersion")
		if rudderTyperTPID != nil && rudderTyperTPVersion != nil && rudderTyperTPID == trackingPlanID {
			if version, ok := rudderTyperTPVersion.(float64); ok && version > 0 {
				trackingPlanVersion = int(version)
			}
		}
		transformerEvent.Metadata.TrackingPlanID = trackingPlanID
		transformerEvent.Metadata.TrackingPlanVersion = trackingPlanVersion
		transformerEvent.Metadata.SourceTpConfig = event.source.DgSourceTrackingPlanConfig.Config
		transformerEvent.Metadata.MergedTpConfig = event.source.DgSourceTrackingPlanConfig.GetMergedConfig(singularEventMetadata.EventType)

		groupedEventsBySourceId[SourceIDT(sourceId)] = append(groupedEventsBySourceId[SourceIDT(sourceId)], transformerEvent)
	}

	if len(statusList) != len(jobList) {
		return nil, fmt.Errorf("len(statusList):%d != len(jobList):%d", len(statusList), len(jobList))
	}

	if proc.config.archiveInPreProcess {
		if err := proc.storeArchiveJobs(ctx, archivalJobs); err != nil {
			return nil, err
		}
		archivalJobs = nil
	}

	return &srcHydrationMessage{
		partition:                     partition,
		subJobs:                       subJobs,
		eventSchemaJobsBySourceId:     eventSchemaJobsBySourceId,
		archivalJobs:                  archivalJobs,
		connectionDetailsMap:          connectionDetailsMap,
		statusDetailsMap:              statusDetailsMap,
		enricherStatusDetailsMap:      enricherStatusDetailsMap,
		botManagementStatusDetailsMap: botManagementStatusDetailsMap,
		eventBlockingStatusDetailsMap: eventBlockingStatusDetailsMap,
		reportMetrics:                 reportMetrics,
		destFilterStatusDetailMap:     destFilterStatusDetailMap,
		totalEvents:                   totalEvents,
		groupedEventsBySourceId:       groupedEventsBySourceId,
		eventsByMessageID:             eventsByMessageID,
		jobIDToSpecificDestMapOnly:    jobIDToSpecificDestMapOnly,
		statusList:                    statusList,
		jobList:                       jobList,
		sourceDupStats:                sourceDupStats,
		dedupKeys:                     dedupKeys,
	}, nil
}

func (proc *Handle) pretransformStage(partition string, preTrans *preTransformationMessage) (*transformationMessage, error) {
	spanTags := stats.Tags{"partition": partition}
	ctx, mainSpan := proc.tracer.Trace(preTrans.subJobs.ctx, "pretransformStage", tracing.WithTraceTags(spanTags))
	defer mainSpan.End()

	if proc.limiter.pretransform != nil {
		defer proc.limiter.pretransform.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
		defer proc.stats.statPretransformStageCount(partition).Count(
			lo.Sum(lo.MapToSlice(preTrans.groupedEventsBySourceId, func(key SourceIDT, jobs []types.TransformerEvent) int {
				return len(jobs)
			})))
	}

	groupedEvents := make(map[string][]types.TransformerEvent)
	uniqueMessageIdsBySrcDestKey := make(map[string]map[string]struct{})

	if !proc.config.archiveInPreProcess {
		g, groupCtx := errgroup.WithContext(ctx)

		g.Go(func() error {
			return proc.storeEventSchemaJobs(groupCtx,
				lo.Flatten(lo.MapToSlice(preTrans.eventSchemaJobsBySourceId, func(_ SourceIDT, jobs []*jobsdb.JobT) []*jobsdb.JobT {
					return jobs
				})))
		})

		g.Go(func() error {
			return proc.storeArchiveJobs(groupCtx, preTrans.archivalJobs)
		})

		if err := g.Wait(); err != nil {
			return nil, err
		}
	} else {
		if err := proc.storeEventSchemaJobs(ctx,
			lo.Flatten(lo.MapToSlice(preTrans.eventSchemaJobsBySourceId, func(_ SourceIDT, jobs []*jobsdb.JobT) []*jobsdb.JobT {
				return jobs
			}))); err != nil {
			return nil, err
		}
	}

	// REPORTING - START
	if proc.isReportingEnabled() {

		reportingtypes.AssertKeysSubset(preTrans.connectionDetailsMap, preTrans.statusDetailsMap)
		reportingtypes.AssertKeysSubset(preTrans.connectionDetailsMap, preTrans.destFilterStatusDetailMap)
		reportingtypes.AssertKeysSubset(preTrans.connectionDetailsMap, preTrans.botManagementStatusDetailsMap)
		reportingtypes.AssertKeysSubset(preTrans.connectionDetailsMap, preTrans.eventBlockingStatusDetailsMap)
		reportingtypes.AssertKeysSubset(preTrans.connectionDetailsMap, preTrans.enricherStatusDetailsMap)

		for k, cd := range preTrans.connectionDetailsMap {
			for _, sd := range preTrans.botManagementStatusDetailsMap[k] {
				preTrans.reportMetrics = append(preTrans.reportMetrics, &reportingtypes.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *reportingtypes.CreatePUDetails("", reportingtypes.BOT_MANAGEMENT, false, false),
					StatusDetail:      sd,
				})
			}

			for _, sd := range preTrans.eventBlockingStatusDetailsMap[k] {
				preTrans.reportMetrics = append(preTrans.reportMetrics, &reportingtypes.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *reportingtypes.CreatePUDetails("", reportingtypes.EVENT_BLOCKING, false, false),
					StatusDetail:      sd,
				})
			}

			for _, sd := range preTrans.statusDetailsMap[k] {
				m := &reportingtypes.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *reportingtypes.CreatePUDetails("", reportingtypes.GATEWAY, false, true),
					StatusDetail:      sd,
				}
				preTrans.reportMetrics = append(preTrans.reportMetrics, m)
			}

			for _, sd := range preTrans.enricherStatusDetailsMap[k] {
				preTrans.reportMetrics = append(preTrans.reportMetrics, &reportingtypes.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *reportingtypes.CreatePUDetails("", reportingtypes.GATEWAY, false, true),
					StatusDetail:      sd,
				})
			}

			for _, dsd := range preTrans.destFilterStatusDetailMap[k] {
				destFilterMetric := &reportingtypes.PUReportedMetric{
					ConnectionDetails: *cd,
					PUDetails:         *reportingtypes.CreatePUDetails(reportingtypes.GATEWAY, reportingtypes.DESTINATION_FILTER, false, false),
					StatusDetail:      dsd,
				}
				preTrans.reportMetrics = append(preTrans.reportMetrics, destFilterMetric)
			}
		}
	}
	// REPORTING - END

	proc.stats.statNumEvents(preTrans.partition).Count(preTrans.totalEvents)

	for sourceID, events := range preTrans.groupedEventsBySourceId {
		source, err := proc.getSourceBySourceID(string(sourceID))
		if err != nil {
			continue
		}

		for _, obs := range proc.sourceObservers {
			obs.ObserveSourceEvents(source, events)
		}
	}

	// TRACKING PLAN - START
	// Placing the trackingPlan validation filters here.
	// Else further down events are duplicated by destId, so multiple validation takes places for same event
	validateEventsStart := time.Now()
	validatedEventsBySourceId, validatedReportMetrics, trackingPlanEnabledMap := proc.validateEvents(preTrans.groupedEventsBySourceId, preTrans.eventsByMessageID)
	validateEventsTime := time.Since(validateEventsStart)
	defer proc.stats.validateEventsTime(preTrans.partition).SendTiming(validateEventsTime)

	// Appending validatedReportMetrics to reportMetrics
	preTrans.reportMetrics = append(preTrans.reportMetrics, validatedReportMetrics...)
	// TRACKING PLAN - END

	// The below part further segregates events by sourceID and DestinationID.
	for sourceIdT, eventList := range validatedEventsBySourceId {
		for idx := range eventList {
			event := &eventList[idx]
			sourceId := string(sourceIdT)
			singularEvent := event.Message

			backendEnabledDestTypes := proc.getBackendEnabledDestinationTypes(sourceId)
			enabledDestTypes := integrations.FilterClientIntegrations(singularEvent, backendEnabledDestTypes)
			workspaceID := event.Metadata.WorkspaceID
			workspaceLibraries := proc.getWorkspaceLibraries(workspaceID)

			for _, destType := range enabledDestTypes {
				enabledDestinationsList := proc.getConsentFilteredDestinations(
					singularEvent,
					sourceId,
					lo.Filter(proc.getEnabledDestinations(sourceId, destType), func(item backendconfig.DestinationT, index int) bool {
						destId := preTrans.jobIDToSpecificDestMapOnly[event.Metadata.JobID]
						if destId != "" {
							return destId == item.ID
						}
						return destId == ""
					}),
				)

				// Adding a singular event multiple times if there are multiple destinations of same type
				for idx := range enabledDestinationsList {
					destination := &enabledDestinationsList[idx]
					destinationEvent := types.TransformerEvent{}
					destinationEvent.Connection = proc.getConnectionConfig(connection{sourceID: sourceId, destinationID: destination.ID})
					destinationEvent.Message = singularEvent
					destinationEvent.Destination = *destination
					destinationEvent.Libraries = workspaceLibraries
					destinationEvent.Metadata = event.Metadata

					// At the TP flow we are not having destination information, so adding it here.
					destinationEvent.Metadata.DestinationID = destination.ID
					destinationEvent.Metadata.DestinationName = destination.Name
					destinationEvent.Metadata.DestinationType = destination.DestinationDefinition.Name
					destinationEvent.Metadata.DestinationDefinitionID = destination.DestinationDefinition.ID
					if len(destination.Transformations) > 0 {
						destinationEvent.Metadata.TransformationID = destination.Transformations[0].ID
						destinationEvent.Metadata.TransformationVersionID = destination.Transformations[0].VersionID
					}
					destinationEvent.Credentials = proc.config.credentialsMap[destination.WorkspaceID]
					filterConfig(&destinationEvent)
					metadata := &destinationEvent.Metadata
					srcAndDestKey := getKeyFromSourceAndDest(metadata.SourceID, metadata.DestinationID)
					// We have at-least one event so marking it good
					_, ok := groupedEvents[srcAndDestKey]
					if !ok {
						groupedEvents[srcAndDestKey] = make([]types.TransformerEvent, 0)
					}
					groupedEvents[srcAndDestKey] = append(groupedEvents[srcAndDestKey], destinationEvent)
					if _, ok := uniqueMessageIdsBySrcDestKey[srcAndDestKey]; !ok {
						uniqueMessageIdsBySrcDestKey[srcAndDestKey] = make(map[string]struct{})
					}
					uniqueMessageIdsBySrcDestKey[srcAndDestKey][metadata.MessageID] = struct{}{}
				}
			}
		}
	}
	trackedUsersReportGenStart := time.Now()
	trackedUsersReports := proc.trackedUsersReporter.GenerateReportsFromJobs(preTrans.jobList, proc.getNonEventStreamSources())
	proc.stats.trackedUsersReportGeneration(preTrans.partition).SendTiming(time.Since(trackedUsersReportGenStart))

	return &transformationMessage{
		preTrans.subJobs.ctx,
		groupedEvents,
		trackingPlanEnabledMap,
		preTrans.eventsByMessageID,
		uniqueMessageIdsBySrcDestKey,
		preTrans.reportMetrics,
		preTrans.statusList,
		preTrans.sourceDupStats,
		preTrans.dedupKeys,

		preTrans.totalEvents,

		preTrans.subJobs.hasMore,
		preTrans.subJobs.rsourcesStats,
		trackedUsersReports,
	}, nil
}

func (proc *Handle) storeEventSchemaJobs(ctx context.Context, eventSchemaJobs []*jobsdb.JobT) error {
	if len(eventSchemaJobs) == 0 {
		return nil
	}
	err := misc.RetryWithNotify(
		ctx,
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
	proc.logger.Debugn("[Processor] Total jobs written to event_schema", logger.NewIntField("jobCount", int64(len(eventSchemaJobs))))
	return nil
}

func (proc *Handle) storeArchiveJobs(ctx context.Context, archivalJobs []*jobsdb.JobT) error {
	if len(archivalJobs) == 0 {
		return nil
	}
	err := misc.RetryWithNotify(
		ctx,
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
	proc.logger.Debugn("[Processor] Total jobs written to archiver", logger.NewIntField("jobCount", int64(len(archivalJobs))))
	return nil
}

type transformationMessage struct {
	ctx           context.Context
	groupedEvents map[string][]types.TransformerEvent

	trackingPlanEnabledMap       map[SourceIDT]bool
	eventsByMessageID            map[string]types.SingularEventWithReceivedAt
	uniqueMessageIdsBySrcDestKey map[string]map[string]struct{}
	reportMetrics                []*reportingtypes.PUReportedMetric
	statusList                   []*jobsdb.JobStatusT
	sourceDupStats               map[dupStatKey]int
	dedupKeys                    map[string]struct{}

	totalEvents int

	hasMore             bool
	rsourcesStats       rsources.StatsCollector
	trackedUsersReports []*trackedusers.UsersReport
}

type userTransformData struct {
	ctx                           context.Context
	userTransformAndFilterOutputs map[string]userTransformAndFilterOutput
	reportMetrics                 []*reportingtypes.PUReportedMetric
	statusList                    []*jobsdb.JobStatusT
	sourceDupStats                map[dupStatKey]int
	dedupKeys                     map[string]struct{}
	trackedUsersReports           []*trackedusers.UsersReport

	totalEvents int
	start       time.Time

	hasMore       bool
	rsourcesStats rsources.StatsCollector
	traces        map[string]stats.Tags
}

func (proc *Handle) userTransformStage(partition string, in *transformationMessage) *userTransformData {
	spanTags := stats.Tags{"partition": partition}
	_, mainSpan := proc.tracer.Trace(in.ctx, "userTransformStage", tracing.WithTraceTags(spanTags))
	defer mainSpan.End()

	if proc.limiter.utransform != nil {
		defer proc.limiter.utransform.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
		defer proc.stats.statUtransformStageCount(partition).Count(len(in.statusList))
	}
	// Now do the actual transformation. We call it in batches, once
	// for each destination ID

	ctx, task := trace.NewTask(context.Background(), "user_transformations")
	defer task.End()

	chOut := make(chan userTransformAndFilterOutput, 1)
	wg := sync.WaitGroup{}
	wg.Add(len(in.groupedEvents))

	spans := make([]stats.TraceSpan, 0, len(in.groupedEvents))
	defer func() {
		for _, span := range spans {
			span.End()
		}
	}()

	traces := make(map[string]stats.Tags)
	for _, eventList := range in.groupedEvents {
		for _, event := range eventList {
			if event.Metadata.TraceParent == "" {
				proc.logger.Debugn("Missing traceParent in transformations", logger.NewIntField("jobId", event.Metadata.JobID))
				continue
			}
			if _, ok := traces[event.Metadata.TraceParent]; ok {
				continue
			}
			tags := stats.Tags{
				"workspaceId":   event.Metadata.WorkspaceID,
				"sourceId":      event.Metadata.SourceID,
				"destinationId": event.Metadata.DestinationID,
				"destType":      event.Metadata.DestinationType,
			}
			ctx := stats.InjectTraceParentIntoContext(context.Background(), event.Metadata.TraceParent)
			_, span := proc.tracer.Trace(ctx, "user_transformations", tracing.WithTraceTags(tags))
			spans = append(spans, span)
			traces[event.Metadata.TraceParent] = tags
		}
	}

	for srcAndDestKey, eventList := range in.groupedEvents {
		srcAndDestKey, eventList := srcAndDestKey, eventList
		rruntime.Go(func() {
			defer wg.Done()
			chOut <- proc.userTransformAndFilter(
				ctx,
				partition,
				srcAndDestKey,
				eventList,
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

	userTransformAndFilterOutputs := make(map[string]userTransformAndFilterOutput, len(in.groupedEvents))
	for o := range chOut {
		userTransformAndFilterOutputs[o.srcAndDestKey] = o
	}

	return &userTransformData{
		ctx:                           in.ctx,
		userTransformAndFilterOutputs: userTransformAndFilterOutputs,
		reportMetrics:                 in.reportMetrics,
		statusList:                    in.statusList,
		sourceDupStats:                in.sourceDupStats,
		dedupKeys:                     in.dedupKeys,
		totalEvents:                   in.totalEvents,
		hasMore:                       in.hasMore,
		rsourcesStats:                 in.rsourcesStats,
		trackedUsersReports:           in.trackedUsersReports,
		traces:                        traces,
	}
}

func (proc *Handle) destinationTransformStage(partition string, in *userTransformData) *storeMessage {
	spanTags := stats.Tags{"partition": partition}
	_, mainSpan := proc.tracer.Trace(in.ctx, "destinationTransformStage", tracing.WithTraceTags(spanTags))
	defer mainSpan.End()

	if proc.limiter.dtransform != nil {
		defer proc.limiter.dtransform.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
		defer proc.stats.statDtransformStageCount(partition).Count(len(in.statusList))
	}

	procErrorJobsByDestID := make(map[string][]*jobsdb.JobT)
	var batchDestJobs []*jobsdb.JobT
	var destJobs []*jobsdb.JobT
	var droppedJobs []*jobsdb.JobT
	routerDestIDs := make(map[string]struct{})

	chOut := make(chan destTransformOutput, 1)
	ctx, task := trace.NewTask(context.Background(), "destination_transformations")
	defer task.End()

	spans := make([]stats.TraceSpan, 0, len(in.traces))
	defer func() {
		for _, span := range spans {
			span.End()
		}
	}()
	for traceParent, tags := range in.traces {
		ctx := stats.InjectTraceParentIntoContext(context.Background(), traceParent)
		_, span := proc.tracer.Trace(ctx, "destination_transformations", tracing.WithTraceTags(tags))
		spans = append(spans, span)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(in.userTransformAndFilterOutputs))

	// Start worker goroutines
	for _, userTransformAndFilterOutput := range in.userTransformAndFilterOutputs {
		userTransformAndFilterOutput := userTransformAndFilterOutput
		rruntime.Go(func() {
			defer wg.Done()
			chOut <- proc.destTransform(ctx, userTransformAndFilterOutput)
		})
	}

	// Start a goroutine to close the channel after all workers are done
	rruntime.Go(func() {
		wg.Wait()
		close(chOut)
	})

	// Collect results
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

	return &storeMessage{
		in.ctx,
		in.trackedUsersReports,
		in.statusList,
		destJobs,
		batchDestJobs,
		droppedJobs,

		procErrorJobsByDestID,
		lo.Keys(routerDestIDs),

		in.reportMetrics,
		in.sourceDupStats,
		in.dedupKeys,
		in.totalEvents,
		in.start,
		in.hasMore,
		in.rsourcesStats,
		in.traces,
	}
}

type storeMessage struct {
	ctx                 context.Context
	trackedUsersReports []*trackedusers.UsersReport
	statusList          []*jobsdb.JobStatusT
	destJobs            []*jobsdb.JobT
	batchDestJobs       []*jobsdb.JobT
	droppedJobs         []*jobsdb.JobT

	procErrorJobsByDestID map[string][]*jobsdb.JobT
	routerDestIDs         []string

	reportMetrics  []*reportingtypes.PUReportedMetric
	sourceDupStats map[dupStatKey]int
	dedupKeys      map[string]struct{}

	totalEvents int
	start       time.Time

	hasMore       bool
	rsourcesStats rsources.StatsCollector
	traces        map[string]stats.Tags
}

func (sm *storeMessage) merge(subJob *storeMessage) {
	sm.statusList = append(sm.statusList, subJob.statusList...)
	sm.destJobs = append(sm.destJobs, subJob.destJobs...)
	sm.batchDestJobs = append(sm.batchDestJobs, subJob.batchDestJobs...)
	sm.droppedJobs = append(sm.droppedJobs, subJob.droppedJobs...)

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

	sm.trackedUsersReports = append(sm.trackedUsersReports, subJob.trackedUsersReports...)
}

func (proc *Handle) sendRetryStoreStats(attempt int) {
	proc.logger.Warnn("Timeout during store jobs in processor module", logger.NewIntField("attempt", int64(attempt)))
	stats.Default.NewTaggedStat("jobsdb_store_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "processor"}).Count(1)
}

func (proc *Handle) sendRetryUpdateStats(attempt int) {
	proc.logger.Warnn("Timeout during update job status in processor module", logger.NewIntField("attempt", int64(attempt)))
	stats.Default.NewTaggedStat("jobsdb_update_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "processor"}).Count(1)
}

func (proc *Handle) sendQueryRetryStats(attempt int) {
	proc.logger.Warnn("Timeout during query jobs in processor module", logger.NewIntField("attempt", int64(attempt)))
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "processor"}).Count(1)
}

func (proc *Handle) storeStage(partition string, pipelineIndex int, in *storeMessage) {
	lockRouterDestIDs := func() func() {
		var deferred []func()
		if len(in.destJobs) == 0 {
			return func() {}
		}
		if len(in.routerDestIDs) > 0 {
			destIDs := lo.Uniq(in.routerDestIDs)
			slices.Sort(destIDs)
			for _, destID := range destIDs {
				// Use pipelineIndex in the lock pKey to avoid contention when multiple pipelines are running (each pipeline processes its own exclusive partition of userIDs)
				pKey := destID + "-" + strconv.Itoa(pipelineIndex)
				proc.storePlocker.Lock(pKey)
				deferred = append(deferred, func() { proc.storePlocker.Unlock(pKey) })
			}
		} else {
			proc.logger.Warnn("empty storeMessage.routerDestIDs",
				logger.NewStringField("partition", partition),
				logger.NewStringField("expected",
					strings.Join(
						lo.Uniq(
							lo.Map(in.destJobs, func(j *jobsdb.JobT, _ int) string { return gjson.GetBytes(j.Parameters, "destination_id").String() }),
						),
						", ",
					),
				),
			)
		}
		return func() {
			slices.Reverse(deferred)
			for _, fn := range deferred {
				fn()
			}
		}
	}

	spanTags := stats.Tags{"partition": partition}
	_, mainSpan := proc.tracer.Trace(in.ctx, "storeStage", tracing.WithTraceTags(spanTags))
	defer mainSpan.End()

	spans := make([]stats.TraceSpan, 0, len(in.traces))
	defer func() {
		for _, span := range spans {
			span.End()
		}
	}()
	for traceParent, tags := range in.traces {
		ctx := stats.InjectTraceParentIntoContext(context.Background(), traceParent)
		_, span := proc.tracer.Trace(ctx, "store", tracing.WithTraceTags(tags))
		spans = append(spans, span)
	}

	if proc.limiter.store != nil {
		defer proc.limiter.store.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
		defer proc.stats.statStoreStageCount(partition).Count(len(in.statusList))
	}

	statusList, destJobs, batchDestJobs := in.statusList, in.destJobs, in.batchDestJobs
	beforeStoreStatus := time.Now()
	g, ctx := errgroup.WithContext(context.Background())
	enableConcurrentStore := proc.config.enableConcurrentStore.Load()
	if !enableConcurrentStore {
		g.SetLimit(1)
	} else {
		// lock early to avoid deadlocks due to connection pool exhaustion
		defer lockRouterDestIDs()()
	}
	// XX: Need to do this in a transaction
	if len(batchDestJobs) > 0 {
		g.Go(func() error {
			err := misc.RetryWithNotify(
				ctx,
				proc.jobsDBCommandTimeout.Load(),
				proc.jobdDBMaxRetries.Load(),
				func(ctx context.Context) error {
					return proc.batchRouterDB.WithStoreSafeTx(
						ctx,
						func(tx jobsdb.StoreSafeTx) error {
							err := proc.batchRouterDB.StoreInTx(ctx, tx, batchDestJobs)
							if err != nil {
								storeErr := fmt.Errorf("storing batch router jobs: %w", err)
								if !proc.config.storeSamplerEnabled.Load() {
									return storeErr
								}
								if proc.storeSamplingFileManager == nil {
									proc.logger.Errorn("Cannot upload as store sampling file manager is nil")
								} else {
									batchDestJobsJSON, err := jsonrs.Marshal(batchDestJobs)
									if err != nil {
										return fmt.Errorf("marshalling batch router jobs: %w: %w ", storeErr, err)
									}

									objName := path.Join("proc-samples", proc.instanceID, uuid.NewString())
									uploadFile, err := proc.storeSamplingFileManager.UploadReader(ctx, objName, strings.NewReader(string(batchDestJobsJSON)))
									if err != nil {
										return fmt.Errorf("uploading sample batch router jobs: %w: %w", storeErr, err)
									}
									proc.logger.Infon("Successfully upload proc sample",
										logger.NewStringField("location", uploadFile.Location),
										logger.NewStringField("objectName", uploadFile.ObjectName),
									)
								}
								return storeErr
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
				return err
			}
			proc.logger.Debugn("[Processor] Total jobs written to batch router", logger.NewIntField("jobCount", int64(len(batchDestJobs))))
			proc.IncreasePendingEvents("batch_rt", getJobCountsByWorkspaceDestType(batchDestJobs))
			proc.stats.statBatchDestNumOutputEvents(partition).Count(len(batchDestJobs))
			proc.stats.statDBWriteBatchPayloadBytes(partition).Observe(
				float64(lo.SumBy(destJobs, func(j *jobsdb.JobT) int { return len(j.EventPayload) })),
			)
			return nil
		})
	}

	if len(destJobs) > 0 {
		g.Go(func() error {
			// Only one goroutine can store to a router destination at a time, otherwise we may have different transactions
			// committing at different timestamps which can cause events with lower jobIDs to appear after events with higher ones.
			// For that purpose, before storing, we lock the relevant destination IDs (in sorted order to avoid deadlocks).
			if !enableConcurrentStore {
				defer lockRouterDestIDs()()
			}
			err := misc.RetryWithNotify(
				ctx,
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
				return err
			}
			proc.logger.Debugn("[Processor] Total jobs written to router", logger.NewIntField("jobCount", int64(len(destJobs))))
			proc.IncreasePendingEvents("rt", getJobCountsByWorkspaceDestType(destJobs))
			proc.stats.statDestNumOutputEvents(partition).Count(len(destJobs))
			proc.stats.statDBWriteRouterPayloadBytes(partition).Observe(
				float64(lo.SumBy(destJobs, func(j *jobsdb.JobT) int { return len(j.EventPayload) })),
			)
			return nil
		})
	}

	if !enableConcurrentStore {
		if err := g.Wait(); err != nil {
			panic(err)
		}
	}
	in.rsourcesStats.CollectStats(statusList)
	err := misc.RetryWithNotify(context.Background(), proc.jobsDBCommandTimeout.Load(), proc.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return proc.gatewayDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
			err := proc.gatewayDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{proc.config.GWCustomVal}, nil)
			if err != nil {
				return fmt.Errorf("updating gateway jobs statuses: %w", err)
			}

			if proc.isReportingEnabled() {
				if err = proc.reporting.Report(ctx, in.reportMetrics, tx.Tx()); err != nil {
					return fmt.Errorf("reporting metrics: %w", err)
				}
			}

			err = proc.trackedUsersReporter.ReportUsers(ctx, in.trackedUsersReports, tx.Tx())
			if err != nil {
				return fmt.Errorf("storing tracked users: %w", err)
			}

			// this will publish stats for all sources involved in this batch
			err = in.rsourcesStats.Publish(ctx, tx.SqlTx())
			if err != nil {
				return fmt.Errorf("publishing rsources stats: %w", err)
			}
			// this will publish rudder source stats only for dropped jobs involved in this batch.
			// It needs to be called after rsourcesStats.Publish, otherwise, it may cause a deadlock
			// e.g. the batch contains two keys and there are dropped jobs only for the second key but not for the first one
			err = proc.saveDroppedJobs(ctx, in.droppedJobs, tx.Tx())
			if err != nil {
				return fmt.Errorf("saving dropped jobs: %w", err)
			}
			if enableConcurrentStore {
				return g.Wait()
			}
			return nil
		})
	}, proc.sendRetryUpdateStats)
	if err != nil {
		panic(err)
	}
	if proc.config.enableDedup {
		proc.updateSourceStats(in.sourceDupStats, "processor_write_key_duplicate_events")
		if len(in.dedupKeys) > 0 {
			if err := proc.dedup.Commit(lo.Keys(in.dedupKeys)); err != nil {
				panic(err)
			}
		}
	}
	if len(in.procErrorJobsByDestID) > 0 {
		proc.recordEventDeliveryStatus(in.procErrorJobsByDestID)
	}
	proc.stats.statDBW(partition).Since(beforeStoreStatus)
	proc.logger.Debugn("Processor GW DB Write Complete", logger.NewIntField("totalProcessed", int64(len(statusList))))
	proc.stats.statGatewayDBW(partition).Count(len(statusList))
}

func getStoreSamplingUploader(conf *config.Config, log logger.Logger) (*filemanager.S3Manager, error) {
	var (
		bucket           = conf.GetStringVar("rudder-customer-sample-payloads", "Processor.Store.Sampling.Bucket")
		regionHint       = conf.GetStringVar("us-east-1", "Processor.Store.Sampling.RegionHint", "AWS_S3_REGION_HINT")
		endpoint         = conf.GetStringVar("", "Processor.Store.Sampling.Endpoint")
		accessKeyID      = conf.GetStringVar("", "Processor.Store.Sampling.AccessKey", "AWS_ACCESS_KEY_ID")
		secretAccessKey  = conf.GetStringVar("", "Processor.Store.Sampling.SecretAccessKey", "AWS_SECRET_ACCESS_KEY")
		s3ForcePathStyle = conf.GetBoolVar(false, "Processor.Store.Sampling.S3ForcePathStyle")
		disableSSL       = conf.GetBoolVar(false, "Processor.Store.Sampling.DisableSSL")
		enableSSE        = conf.GetBoolVar(false, "Processor.Store.Sampling.EnableSSE", "AWS_ENABLE_SSE")
	)
	s3Config := map[string]any{
		"bucketName":       bucket,
		"regionHint":       regionHint,
		"endpoint":         endpoint,
		"accessKeyID":      accessKeyID,
		"secretAccessKey":  secretAccessKey,
		"s3ForcePathStyle": s3ForcePathStyle,
		"disableSSL":       disableSSL,
		"enableSSE":        enableSSE,
	}
	return filemanager.NewS3Manager(conf, s3Config, log.Withn(logger.NewStringField("component", "proc-uploader")), func() time.Duration {
		return conf.GetDuration("Processor.Store.Sampling.Timeout", 120, time.Second)
	})
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

type destTransformOutput struct {
	reportMetrics   []*reportingtypes.PUReportedMetric
	destJobs        []*jobsdb.JobT
	batchDestJobs   []*jobsdb.JobT
	errorsPerDestID map[string][]*jobsdb.JobT
	routerDestIDs   map[string]struct{}
	droppedJobs     []*jobsdb.JobT
}

// userTransformAndFilterOutput holds the data passed between preprocessing and postprocessing steps
type userTransformAndFilterOutput struct {
	eventsToTransform     []types.TransformerEvent
	commonMetaData        *types.Metadata
	reportMetrics         []*reportingtypes.PUReportedMetric
	procErrorJobsByDestID map[string][]*jobsdb.JobT
	droppedJobs           []*jobsdb.JobT
	eventsByMessageID     map[string]types.SingularEventWithReceivedAt
	srcAndDestKey         string
	response              types.Response
	transformAt           string
}

func (proc *Handle) userTransformAndFilter(
	ctx context.Context,
	partition string,
	srcAndDestKey string,
	eventList []types.TransformerEvent,
	trackingPlanEnabledMap map[SourceIDT]bool,
	eventsByMessageID map[string]types.SingularEventWithReceivedAt,
	uniqueMessageIdsBySrcDestKey map[string]map[string]struct{},
) userTransformAndFilterOutput {
	if len(eventList) == 0 {
		return userTransformAndFilterOutput{
			eventsToTransform: eventList,
		}
	}

	sourceID, destID := getSourceAndDestIDsFromKey(srcAndDestKey)
	destination := &eventList[0].Destination
	connection := eventList[0].Connection
	workspaceID := eventList[0].Metadata.WorkspaceID
	commonMetaData := eventList[0].Metadata.CommonMetadata()

	reportMetrics := make([]*reportingtypes.PUReportedMetric, 0)
	procErrorJobsByDestID := make(map[string][]*jobsdb.JobT)
	droppedJobs := make([]*jobsdb.JobT, 0)

	proc.config.configSubscriberLock.RLock()
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
					trackingPlanID:          event.Metadata.TrackingPlanID,
					trackingPlanVersion:     event.Metadata.TrackingPlanVersion,
				}
			}
			inCountMap[key] = inCountMap[key] + 1
		}
	}
	// REPORTING - END

	var response types.Response
	var eventsToTransform []types.TransformerEvent
	var inPU string
	if trackingPlanEnabled {
		inPU = reportingtypes.TRACKINGPLAN_VALIDATOR
	} else {
		inPU = reportingtypes.DESTINATION_FILTER
	}
	// Send to custom transformer only if the destination has a transformer enabled
	if transformationEnabled {
		noOfEvents := len(eventList)
		utMirroringEnabled, utMirroringSanityChecks := proc.isUserTransformMirroringEnabled()
		proc.logger.Debugn("UT mirroring setting", logger.NewBoolField("enabled", utMirroringEnabled))
		userTransformationStat := proc.newUserTransformationStat(sourceID, workspaceID, destination, false)
		var userTransformationMirroringStat *DestStatT
		userTransformationStat.numEvents.Count(noOfEvents)
		if utMirroringEnabled {
			userTransformationMirroringStat = proc.newUserTransformationStat(sourceID, workspaceID, destination, true)
			userTransformationMirroringStat.numEvents.Count(noOfEvents)
		}
		proc.logger.Debugn("Custom Transform input size", logger.NewIntField("eventCount", int64(noOfEvents)))

		trace.WithRegion(ctx, "UserTransform", func() {
			startedAt := time.Now()

			if utMirroringEnabled {
				go func() {
					response := proc.transformerClients.UserMirror().Transform(ctx, eventList)
					d := time.Since(startedAt)
					userTransformationMirroringStat.transformTime.SendTiming(d)
					userTransformationMirroringStat.numOutputSuccessEvents.Count(len(response.Events))
					filtered := lo.GroupBy(response.FailedEvents,
						func(event types.TransformerResponse) bool {
							return event.StatusCode == reportingtypes.FilterEventCode
						},
					)
					userTransformationMirroringStat.numOutputFailedEvents.Count(len(filtered[false]))
					userTransformationMirroringStat.numOutputFilteredEvents.Count(len(filtered[true]))
					if utMirroringSanityChecks != nil {
						utMirroringSanityChecks <- response
						close(utMirroringSanityChecks)
					}
				}()
			}

			response = proc.transformerClients.User().Transform(ctx, eventList)
			d := time.Since(startedAt)
			userTransformationStat.transformTime.SendTiming(d)

			if utMirroringEnabled && utMirroringSanityChecks != nil {
				// Let's create a copy of the response because it may be subject to changes later (see getTransformerEvents).
				// We want to block while creating a copy to avoid a race condition.
				// This shouldn't have a big impact on processor latency unless the sanity sampling is a very high percentage.
				responseCopy, err := jsonrs.Marshal(response)
				if err != nil {
					proc.logger.Warnn("Cannot create copy of transformer response", obskit.Error(err))
				}

				eventListCopy, err := jsonrs.Marshal(eventList)
				if err != nil {
					proc.logger.Warnn("Cannot create copy of transformer events", obskit.Error(err))
				}

				go func(responseCopy, eventListCopy []byte) {
					if len(responseCopy) == 0 || len(eventListCopy) == 0 {
						return
					}

					var response types.Response
					err := jsonrs.Unmarshal(responseCopy, &response)
					if err != nil {
						proc.logger.Warnn("Cannot unmarshal transformer response", obskit.Error(err))
						return
					}

					mirroredResponse := <-utMirroringSanityChecks
					diff, equal := response.Equal(&mirroredResponse)
					if equal {
						proc.stats.utMirroringEqualResponses(partition).Increment()
						return
					}

					defer proc.stats.utMirroringDifferentResponses(partition).Increment()

					var (
						tr  *types.TransformerResponse
						log = proc.logger
					)
					if len(mirroredResponse.Events) > 0 {
						tr = &mirroredResponse.Events[0]
					} else if len(mirroredResponse.FailedEvents) > 0 {
						tr = &mirroredResponse.FailedEvents[0]
					}
					if tr != nil {
						log = proc.logger.Withn( // adding more data to help with debugging
							obskit.WorkspaceID(tr.Metadata.WorkspaceID),
							obskit.SourceID(tr.Metadata.SourceID),
							logger.NewStringField("messageId", tr.Metadata.MessageID),
						)
					}

					if proc.utSamplingFileManager == nil { // Cannot upload, we should just report the issue with no diff
						log.Warnn("UserTransform sanity check failed")
						return
					}

					// Upload the diff file and the eventListCopy via the file manager
					// * eventListCopy is the original eventList that was passed to the transformer
					// * diff contains the difference between the transformer response and the mirrored response
					objNameSuffix := uuid.New().String()
					clientEventsObjName := objNameSuffix + "-events"
					diffObjName := objNameSuffix + "-diff"
					diffFile, err := proc.utSamplingFileManager.UploadReader(ctx, diffObjName, strings.NewReader(diff))
					if err != nil {
						log.Errorn("Error uploading UserTransform sanity check diff file", obskit.Error(err))
						return
					}

					clientEventsFile, err := proc.utSamplingFileManager.UploadReader(ctx, clientEventsObjName, bytes.NewReader(eventListCopy))
					if err != nil {
						log.Errorn("Error uploading UserTransform clientEvents file",
							obskit.Error(err),
							logger.NewStringField("diffLocation", diffFile.Location),
							logger.NewStringField("diffObjectName", diffFile.ObjectName),
						)
						return
					}

					log.Warnn("UserTransform sanity check failed",
						logger.NewStringField("diffLocation", diffFile.Location),
						logger.NewStringField("diffObjectName", diffFile.ObjectName),
						logger.NewStringField("clientEventsLocation", clientEventsFile.Location),
						logger.NewStringField("clientEventsObjectName", clientEventsFile.ObjectName),
					)
				}(responseCopy, eventListCopy)
			}

			var successMetrics []*reportingtypes.PUReportedMetric
			var successCountMap map[string]int64
			var successCountMetadataMap map[string]MetricMetadata
			eventsToTransform, successMetrics, successCountMap, successCountMetadataMap = proc.getTransformerEvents(response, commonMetaData, eventsByMessageID, destination, connection, inPU, reportingtypes.USER_TRANSFORMER)
			nonSuccessMetrics := proc.getNonSuccessfulMetrics(response, eventList, commonMetaData, eventsByMessageID, inPU, reportingtypes.USER_TRANSFORMER)
			droppedJobs = append(droppedJobs, append(proc.getDroppedJobs(response, eventList), append(nonSuccessMetrics.failedJobs, nonSuccessMetrics.filteredJobs...)...)...)
			if _, ok := procErrorJobsByDestID[destID]; !ok {
				procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
			}
			procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], nonSuccessMetrics.failedJobs...)
			userTransformationStat.numOutputSuccessEvents.Count(len(eventsToTransform))
			userTransformationStat.numOutputFailedEvents.Count(len(nonSuccessMetrics.failedJobs))
			userTransformationStat.numOutputFilteredEvents.Count(len(nonSuccessMetrics.filteredJobs))
			proc.logger.Debugn("Custom Transform output size", logger.NewIntField("size", int64(len(eventsToTransform))))
			trace.Logf(ctx, "UserTransform", "User Transform output size: %d", len(eventsToTransform))

			proc.transDebugger.UploadTransformationStatus(&transformationdebugger.TransformationStatusT{SourceID: sourceID, DestID: destID, Destination: destination, UserTransformedEvents: eventsToTransform, EventsByMessageID: eventsByMessageID, FailedEvents: response.FailedEvents, UniqueMessageIds: uniqueMessageIdsBySrcDestKey[srcAndDestKey]})

			// REPORTING - START
			if proc.isReportingEnabled() {
				diffMetrics := getDiffMetrics(
					inPU,
					reportingtypes.USER_TRANSFORMER,
					inCountMetadataMap,
					successCountMetadataMap,
					inCountMap,
					successCountMap,
					nonSuccessMetrics.failedCountMap,
					nonSuccessMetrics.filteredCountMap,
					proc.statsFactory,
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
			inPU = reportingtypes.USER_TRANSFORMER // for the next step in the pipeline
		})
	} else {
		proc.logger.Debugn("No custom transformation")
		eventsToTransform = eventList
	}

	if len(eventsToTransform) == 0 {
		return userTransformAndFilterOutput{
			eventsToTransform:     eventsToTransform,
			commonMetaData:        commonMetaData,
			reportMetrics:         reportMetrics,
			procErrorJobsByDestID: procErrorJobsByDestID,
			droppedJobs:           droppedJobs,
			eventsByMessageID:     eventsByMessageID,
			srcAndDestKey:         srcAndDestKey,
		}
	}

	transformAt := "processor"
	if val, ok := destination.DestinationDefinition.Config["transformAtV1"].(string); ok {
		transformAt = val
	}
	// Check for overrides through env
	transformAtOverrideFound := proc.conf.IsSet("Processor." + destination.DestinationDefinition.Name + ".transformAt")
	if transformAtOverrideFound {
		transformAt = proc.conf.GetString("Processor."+destination.DestinationDefinition.Name+".transformAt", "processor")
	}
	// Filtering events based on the supported message types - START
	s := time.Now()
	eventFilterInCount := len(eventsToTransform)
	proc.logger.Debugn("Supported messages filtering input size", logger.NewIntField("eventCount", int64(eventFilterInCount)))
	response = ConvertToFilteredTransformerResponse(
		eventsToTransform,
		transformAt != "none",
		func(event types.TransformerEvent) (bool, string) {
			if event.Metadata.SourceJobRunID != "" &&
				slices.Contains(
					proc.drainConfig.jobRunIDs.Load(),
					event.Metadata.SourceJobRunID,
				) {
				proc.logger.Debugn(
					"cancelled jobRunID",
					logger.NewStringField("jobRunID", event.Metadata.SourceJobRunID),
				)
				return true, "cancelled jobRunId"
			}
			return false, ""
		},
	)
	var successMetrics []*reportingtypes.PUReportedMetric
	var successCountMap map[string]int64
	var successCountMetadataMap map[string]MetricMetadata
	nonSuccessMetrics := proc.getNonSuccessfulMetrics(response, eventList, commonMetaData, eventsByMessageID, inPU, reportingtypes.EVENT_FILTER)
	droppedJobs = append(droppedJobs, append(proc.getDroppedJobs(response, eventsToTransform), append(nonSuccessMetrics.failedJobs, nonSuccessMetrics.filteredJobs...)...)...)
	if _, ok := procErrorJobsByDestID[destID]; !ok {
		procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
	}
	procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], nonSuccessMetrics.failedJobs...)
	eventsToTransform, successMetrics, successCountMap, successCountMetadataMap = proc.getTransformerEvents(response, commonMetaData, eventsByMessageID, destination, connection, inPU, reportingtypes.EVENT_FILTER)
	proc.logger.Debugn("Supported messages filtering output size", logger.NewIntField("eventCount", int64(len(eventsToTransform))))

	// REPORTING - START
	if proc.isReportingEnabled() {
		reportMetrics = append(reportMetrics, successMetrics...)
		reportMetrics = append(reportMetrics, nonSuccessMetrics.failedMetrics...)
		reportMetrics = append(reportMetrics, nonSuccessMetrics.filteredMetrics...)

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

	return userTransformAndFilterOutput{
		eventsToTransform:     eventsToTransform,
		commonMetaData:        commonMetaData,
		reportMetrics:         reportMetrics,
		procErrorJobsByDestID: procErrorJobsByDestID,
		droppedJobs:           droppedJobs,
		eventsByMessageID:     eventsByMessageID,
		srcAndDestKey:         srcAndDestKey,
		response:              response,
		transformAt:           transformAt,
	}
}

func (proc *Handle) destTransform(ctx context.Context, data userTransformAndFilterOutput) destTransformOutput {
	if len(data.eventsToTransform) == 0 {
		return destTransformOutput{
			destJobs:        nil,
			batchDestJobs:   nil,
			errorsPerDestID: data.procErrorJobsByDestID,
			reportMetrics:   data.reportMetrics,
			routerDestIDs:   make(map[string]struct{}),
			droppedJobs:     data.droppedJobs,
		}
	}

	response := data.response
	eventsByMessageID := data.eventsByMessageID
	sourceID := data.commonMetaData.SourceID
	destID := data.commonMetaData.DestinationID
	sourceName := data.commonMetaData.SourceName
	workspaceID := data.commonMetaData.WorkspaceID
	destType := data.commonMetaData.DestinationType
	transformAt := data.transformAt

	destJobs := make([]*jobsdb.JobT, 0)
	batchDestJobs := make([]*jobsdb.JobT, 0)
	routerDestIDs := make(map[string]struct{})
	transformAtFromFeaturesFile := proc.transformerFeaturesService.RouterTransform(destType)

	// Destination transformation - START
	// Send to transformer only if is
	// a. transformAt is processor
	// OR
	// b. transformAt is router and transformer doesn't support router transform
	if transformAt == "processor" || (transformAt == "router" && !transformAtFromFeaturesFile) {
		trace.WithRegion(ctx, "Dest Transform", func() {
			trace.Logf(ctx, "Dest Transform", "input size %d", len(data.eventsToTransform))
			proc.logger.Debugn("Dest Transform input size", logger.NewIntField("inputSize", int64(len(data.eventsToTransform))))
			s := time.Now()
			response = proc.transformerClients.Destination().Transform(ctx, data.eventsToTransform)

			destTransformationStat := proc.newDestinationTransformationStat(sourceID, workspaceID, transformAt, &data.eventsToTransform[0].Destination)
			destTransformationStat.transformTime.Since(s)
			transformAt = "processor"

			proc.logger.Debugn("Dest Transform output size", logger.NewIntField("outputSize", int64(len(response.Events))))
			trace.Logf(ctx, "DestTransform", "output size %d", len(response.Events))

			nonSuccessMetrics := proc.getNonSuccessfulMetrics(
				response, data.eventsToTransform, data.commonMetaData, eventsByMessageID,
				reportingtypes.EVENT_FILTER, reportingtypes.DEST_TRANSFORMER,
			)
			destTransformationStat.numEvents.Count(len(data.eventsToTransform))
			destTransformationStat.numOutputSuccessEvents.Count(len(response.Events))
			destTransformationStat.numOutputFailedEvents.Count(len(nonSuccessMetrics.failedJobs))
			destTransformationStat.numOutputFilteredEvents.Count(len(nonSuccessMetrics.filteredJobs))
			data.droppedJobs = append(data.droppedJobs, append(proc.getDroppedJobs(response, data.eventsToTransform), append(nonSuccessMetrics.failedJobs, nonSuccessMetrics.filteredJobs...)...)...)

			if _, ok := data.procErrorJobsByDestID[destID]; !ok {
				data.procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
			}
			data.procErrorJobsByDestID[destID] = append(data.procErrorJobsByDestID[destID], nonSuccessMetrics.failedJobs...)

			// REPORTING - PROCESSOR metrics - START
			if proc.isReportingEnabled() {
				successMetrics := make([]*reportingtypes.PUReportedMetric, 0)
				connectionDetailsMap := make(map[string]*reportingtypes.ConnectionDetails)
				statusDetailsMap := make(map[string]map[string]*reportingtypes.StatusDetail)
				for i := range response.Events {
					// Update metrics maps
					proc.updateMetricMaps(nil, nil, connectionDetailsMap, statusDetailsMap, &response.Events[i], jobsdb.Succeeded.State, reportingtypes.DEST_TRANSFORMER, func() json.RawMessage { return nil }, nil)
				}
				reportingtypes.AssertSameKeys(connectionDetailsMap, statusDetailsMap)

				for k, cd := range connectionDetailsMap {
					for _, sd := range statusDetailsMap[k] {
						m := &reportingtypes.PUReportedMetric{
							ConnectionDetails: *cd,
							PUDetails:         *reportingtypes.CreatePUDetails(reportingtypes.EVENT_FILTER, reportingtypes.DEST_TRANSFORMER, false, false),
							StatusDetail:      sd,
						}
						successMetrics = append(successMetrics, m)
					}
				}

				data.reportMetrics = append(data.reportMetrics, nonSuccessMetrics.failedMetrics...)
				data.reportMetrics = append(data.reportMetrics, nonSuccessMetrics.filteredMetrics...)
				data.reportMetrics = append(data.reportMetrics, successMetrics...)
			}
			// REPORTING - PROCESSOR metrics - END
		})
	}

	trace.WithRegion(ctx, "MarshalForDB", func() {
		// Save the JSON in DB. This is what the router uses
		for i := range response.Events {
			destEventJSON, err := jsonrs.Marshal(response.Events[i].Output)
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
			workspaceID := metadata.WorkspaceID
			partitionID := metadata.PartitionID
			// If the response from the transformer does not have userID in metadata, setting userID to random-uuid.
			// This is done to respect findWorker logic in router.
			if rudderID == "" {
				rudderID = "random-" + id.String()
			}

			params := ParametersT{
				SourceID:                sourceID,
				SourceName:              sourceName,
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
				WorkspaceId:             workspaceID,
				TraceParent:             metadata.TraceParent,
				ConnectionID:            generateConnectionID(sourceID, destID),
			}
			marshalledParams, err := jsonrs.Marshal(params)
			if err != nil {
				proc.logger.Errorn("[Processor] Failed to marshal parameters object", logger.NewStringField("parameters", stringify.Any(params)))
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
				WorkspaceId:  workspaceID,
				PartitionID:  partitionID,
			}
			if slices.Contains(proc.config.batchDestinations, newJob.CustomVal) {
				batchDestJobs = append(batchDestJobs, &newJob)
			} else {
				destJobs = append(destJobs, &newJob)
				routerDestIDs[destID] = struct{}{}
			}
		}
	})

	return destTransformOutput{
		destJobs:        destJobs,
		batchDestJobs:   batchDestJobs,
		errorsPerDestID: data.procErrorJobsByDestID,
		reportMetrics:   data.reportMetrics,
		routerDestIDs:   routerDestIDs,
		droppedJobs:     data.droppedJobs,
	}
}

func (proc *Handle) saveDroppedJobs(ctx context.Context, droppedJobs []*jobsdb.JobT, tx *Tx) error {
	if len(droppedJobs) > 0 {
		for i := range droppedJobs { // each dropped job should have a unique jobID in the scope of the batch
			droppedJobs[i].JobID = int64(i)
		}
		rsourcesStats := rsources.NewDroppedJobsCollector(
			proc.rsourcesService,
			"processor",
			proc.statsFactory,
			rsources.IgnoreDestinationID(),
		)
		rsourcesStats.JobsDropped(droppedJobs)
		return rsourcesStats.Publish(ctx, tx.Tx)
	}
	return nil
}

func (proc *Handle) isUserTransformMirroringEnabled() (bool, chan types.Response) {
	mirroringSanityChecksSampling := proc.config.userTransformationMirroringSanitySampling.Load()
	mirroringFireAndForget := proc.config.userTransformationMirroringFireAndForget.Load()

	if !mirroringFireAndForget && mirroringSanityChecksSampling <= 0 {
		return false, nil // Mirroring is disabled.
	}
	if mirroringFireAndForget && mirroringSanityChecksSampling > 0 {
		proc.logger.Errorn(
			"UT mirroring sanity checks and fire&forget are enabled (they are mutually exclusive). Disabling UT mirroring.",
		)
		return false, nil
	}

	if mirroringFireAndForget { // Mirroring is enabled in fire&forget mode and no sanity checks
		return true, nil
	}

	// Determine if mirroring should be enabled based on sampling percentage.
	// Sampling percentage precision can be with two decimals like 12.34%
	if ok := shouldSample(mirroringSanityChecksSampling); !ok {
		// Sanity checks were enabled but the random value was less than the sampling percentage.
		// Disabling mirroring altogether.
		return false, nil
	}

	return true, make(chan types.Response, 1)
}

func ConvertToFilteredTransformerResponse(
	events []types.TransformerEvent,
	filter bool,
	drainFunc func(types.TransformerEvent) (bool, string),
) types.Response {
	var responses []types.TransformerResponse
	var failedEvents []types.TransformerResponse

	type cacheValue struct {
		values []string
		ok     bool
	}
	supportedMessageTypesCache := make(map[string]*cacheValue)
	supportedMessageEventsCache := make(map[string]*cacheValue)

	// filter unsupported message types
	for i := range events {
		event := &events[i]

		// drain events
		if drain, reason := drainFunc(*event); drain {
			failedEvents = append(
				failedEvents,
				types.TransformerResponse{
					Output:     event.Message,
					StatusCode: reportingtypes.DrainEventCode,
					Metadata:   event.Metadata,
					Error:      reason,
				},
			)
			continue
		}

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
					failedEvents = append(
						failedEvents,
						types.TransformerResponse{
							Output:     event.Message,
							StatusCode: 400,
							Metadata:   event.Metadata,
							Error:      "Invalid message event. Type assertion failed",
						},
					)
					continue
				}
				if !slices.Contains(supportedEvents.values, messageEvent) {
					failedEvents = append(
						failedEvents,
						types.TransformerResponse{
							Output:     event.Message,
							StatusCode: reportingtypes.FilterEventCode,
							Metadata:   event.Metadata,
							Error:      "Event not supported",
						},
					)
					continue
				}
			}

		}
		// allow event
		responses = append(
			responses,
			types.TransformerResponse{
				Output:     event.Message,
				StatusCode: 200,
				Metadata:   event.Metadata,
			},
		)
	}

	return types.Response{Events: responses, FailedEvents: failedEvents}
}

func (proc *Handle) getJobsStage(ctx context.Context, partition string) jobsdb.JobsResult {
	s := time.Now()

	_, span := proc.tracer.Trace(ctx, "getJobsStage", tracing.WithTraceTags(stats.Tags{
		"partition": partition,
	}))
	defer span.End()

	if proc.limiter.read != nil {
		defer proc.limiter.read.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
	}

	proc.logger.Debugn("Processor DB Read size", logger.NewIntField("maxEventsToProcess", int64(proc.config.maxEventsToProcess.Load())))

	queryParams := jobsdb.GetQueryParams{
		CustomValFilters: []string{proc.config.GWCustomVal},
		JobsLimit:        proc.config.maxEventsToProcess.Load(),
		EventsLimit:      proc.config.maxEventsToProcess.Load(),
		PayloadSizeLimit: proc.adaptiveLimit(proc.payloadLimit.Load()),
	}
	proc.isolationStrategy.AugmentQueryParams(partition, &queryParams)

	unprocessedList, err := misc.QueryWithRetriesAndNotify(context.Background(), proc.jobdDBQueryRequestTimeout.Load(), proc.jobdDBMaxRetries.Load(), func(ctx context.Context) (jobsdb.JobsResult, error) {
		return proc.gatewayDB.GetUnprocessed(ctx, queryParams)
	}, proc.sendQueryRetryStats)
	if err != nil {
		proc.logger.Errorn("Failed to get unprocessed jobs from DB", obskit.Error(err))
		panic(err)
	}

	dbReadTime := time.Since(s)
	defer proc.stats.statDBR(partition).SendTiming(dbReadTime)

	var firstJob *jobsdb.JobT
	var lastJob *jobsdb.JobT
	if len(unprocessedList.Jobs) > 0 {
		firstJob = unprocessedList.Jobs[0]
		lastJob = unprocessedList.Jobs[len(unprocessedList.Jobs)-1]
	}
	proc.pipelineDelayStats(partition, firstJob, lastJob)

	// check if there is work to be done
	if len(unprocessedList.Jobs) == 0 {
		proc.logger.Debugn("Processor DB Read Complete. No GW Jobs to process.")
		return unprocessedList
	}

	proc.logger.Debugn("Processor DB Read Complete",
		logger.NewIntField("unprocessedJobs", int64(len(unprocessedList.Jobs))),
		logger.NewIntField("totalEvents", int64(unprocessedList.EventsCount)))
	proc.stats.statGatewayDBR(partition).Count(len(unprocessedList.Jobs))
	proc.stats.statReadStageCount(partition).Count(len(unprocessedList.Jobs))

	return unprocessedList
}

func (proc *Handle) markExecuting(ctx context.Context, partition string, jobs []*jobsdb.JobT) error {
	_, span := proc.tracer.Trace(ctx, "markExecuting", tracing.WithTraceTags(stats.Tags{
		"partition": partition,
	}))
	defer span.End()

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
			PartitionID:   job.PartitionID,
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
	ctx := context.TODO()
	unprocessedList := proc.getJobsStage(ctx, partition) // context is used for tracing

	if len(unprocessedList.Jobs) == 0 {
		return false
	}

	rsourcesStats := rsources.NewStatsCollector(proc.rsourcesService, "processor", proc.statsFactory, rsources.IgnoreDestinationID())
	rsourcesStats.BeginProcessing(unprocessedList.Jobs)

	var transMessage *transformationMessage
	var err error
	srcHydrationMsg, err := proc.preprocessStage(
		partition,
		subJob{
			ctx:           ctx,
			subJobs:       unprocessedList.Jobs,
			hasMore:       false,
			rsourcesStats: rsourcesStats,
		},
		proc.conf.GetReloadableDurationVar(0, time.Second, "Processor.preprocessDelay."+partition).Load(),
	)
	if err != nil {
		panic(err)
	}

	preTransMessage, err := proc.srcHydrationStage(partition, srcHydrationMsg)
	if err != nil {
		panic(err)
	}

	transMessage, err = proc.pretransformStage(partition, preTransMessage)
	if err != nil {
		panic(err)
	}
	proc.storeStage(partition, 0,
		proc.destinationTransformStage(partition,
			proc.userTransformStage(partition, transMessage)),
	)
	return true
}

// `jobSplitter` func Splits the read Jobs into sub-batches after reading from DB to process.
// `subJobMerger` func merges the split jobs into a single batch before writing to DB.
// So, to keep track of sub-batch we have `hasMore` variable.
// each sub-batch has `hasMore`. If, a sub-batch is the last one from the batch it's marked as `false`, else `true`.
type subJob struct {
	ctx           context.Context
	subJobs       []*jobsdb.JobT
	hasMore       bool
	rsourcesStats rsources.StatsCollector
}

func (proc *Handle) jobSplitter(
	ctx context.Context, jobs []*jobsdb.JobT, rsourcesStats rsources.StatsCollector, //nolint:unparam
) []subJob {
	chunks := lo.Chunk(jobs, proc.config.subJobSize)
	return lo.Map(chunks, func(subJobs []*jobsdb.JobT, index int) subJob {
		return subJob{
			ctx:           ctx,
			subJobs:       subJobs,
			hasMore:       index+1 < len(chunks),
			rsourcesStats: rsourcesStats,
		}
	})
}

func (proc *Handle) crashRecover() {
	proc.gatewayDB.DeleteExecuting()
}

func (proc *Handle) updateSourceStats(sourceStats map[dupStatKey]int, bucket string) {
	for dupStat, count := range sourceStats {
		tags := map[string]string{
			"source": dupStat.sourceID,
		}
		sourceStatsD := proc.statsFactory.NewTaggedStat(bucket, stats.CountType, tags)
		sourceStatsD.Count(count)
	}
}

func (proc *Handle) isReportingEnabled() bool {
	return proc.reporting != nil && proc.reportingEnabled
}

func (proc *Handle) updateRudderSourcesStats(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) error {
	rsourcesStats := rsources.NewStatsCollector(proc.rsourcesService, "processor", proc.statsFactory)
	rsourcesStats.JobsStored(jobs)
	err := rsourcesStats.Publish(ctx, tx.SqlTx())
	return err
}

func filterConfig(eventCopy *types.TransformerEvent) {
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

// check if event has eligible destinations to send to
//
// event will be dropped if no destination is found
func (proc *Handle) isDestinationAvailable(event types.SingularEventT, sourceId, destinationID string) bool {
	enabledDestTypes := integrations.FilterClientIntegrations(
		event,
		proc.getBackendEnabledDestinationTypes(sourceId),
	)
	if len(enabledDestTypes) == 0 {
		proc.logger.Debugn("No enabled destination types")
		return false
	}

	if enabledDestinationsList := lo.Filter(proc.getConsentFilteredDestinations(
		event,
		sourceId,
		lo.Flatten(
			lo.Map(
				enabledDestTypes,
				func(destType string, _ int) []backendconfig.DestinationT {
					return proc.getEnabledDestinations(sourceId, destType)
				},
			),
		),
	), func(dest backendconfig.DestinationT, index int) bool {
		return len(destinationID) == 0 || dest.ID == destinationID
	}); len(enabledDestinationsList) == 0 {
		proc.logger.Debugn("No destination to route this event to")
		return false
	}

	return true
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
			proc.pendingEventsRegistry.IncreasePendingEvents(tablePrefix, workspace, destType, float64(stats[workspace][destType]))
		}
	}
}

func (proc *Handle) countPendingEvents(ctx context.Context) error {
	dbs := map[string]jobsdb.JobsDB{"rt": proc.routerDB, "batch_rt": proc.batchRouterDB}
	jobdDBQueryRequestTimeout := proc.conf.GetDurationVar(600, time.Second, "JobsDB.GetPileUpCounts.QueryRequestTimeout", "JobsDB.QueryRequestTimeout")
	jobdDBMaxRetries := proc.conf.GetReloadableIntVar(2, 1, "JobsDB.Processor.MaxRetries", "JobsDB.MaxRetries")

	err := misc.RetryWithNotify(ctx,
		jobdDBQueryRequestTimeout,
		jobdDBMaxRetries.Load(),
		func(ctx context.Context) error {
			startTime := time.Now()
			proc.pendingEventsRegistry.Reset()
			defer proc.pendingEventsRegistry.Publish()
			g, ctx := errgroup.WithContext(ctx)
			for tablePrefix, db := range dbs {
				g.Go(func() error {
					if err := db.GetPileUpCounts(ctx, startTime, proc.pendingEventsRegistry.IncreasePendingEvents); err != nil {
						return fmt.Errorf("pileup counts for %s: %w", tablePrefix, err)
					}
					return nil
				})
			}
			return g.Wait()
		}, func(attempt int) {
			proc.logger.Warnn("Timeout during GetPileUpCounts",
				logger.NewIntField("attempt", int64(attempt)))
			stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": strconv.Itoa(attempt), "module": "pileup"}).Increment()
		})
	if err != nil && ctx.Err() == nil { // ignore context cancellation
		return err
	}
	return nil
}

// shouldSample sampling percentage precision can be with two decimals like 12.34%.
// To sample everything use 100. To sample nothing use 0.
func shouldSample(samplingPercentage float64) bool {
	if samplingPercentage == 0 || samplingPercentage == 100 {
		return samplingPercentage == 100
	}
	return float64(rand.Intn(10000))/100 <= samplingPercentage
}

// getUTSamplingUploader can be completely removed once we get rid of UT sampling
func getUTSamplingUploader(conf *config.Config, log logger.Logger) (*filemanager.S3Manager, error) {
	var (
		bucket           = conf.GetString("UTSampling.Bucket", "processor-ut-mirroring-diffs")
		endpoint         = conf.GetString("UTSampling.Endpoint", "")
		accessKeyID      = conf.GetStringVar("", "UTSampling.AccessKeyId", "AWS_ACCESS_KEY_ID")
		accessKey        = conf.GetStringVar("", "UTSampling.AccessKey", "AWS_SECRET_ACCESS_KEY")
		s3ForcePathStyle = conf.GetBool("UTSampling.S3ForcePathStyle", false)
		disableSSL       = conf.GetBool("UTSampling.DisableSsl", false)
		enableSSE        = conf.GetBoolVar(false, "UTSampling.EnableSse", "AWS_ENABLE_SSE")
		useGlue          = conf.GetBool("UTSampling.UseGlue", false)
		region           = conf.GetStringVar("us-east-1", "UTSampling.Region", "AWS_DEFAULT_REGION")
	)
	s3Config := map[string]any{
		"bucketName":       bucket,
		"endpoint":         endpoint,
		"accessKeyID":      accessKeyID,
		"accessKey":        accessKey,
		"s3ForcePathStyle": s3ForcePathStyle,
		"disableSSL":       disableSSL,
		"enableSSE":        enableSSE,
		"useGlue":          useGlue,
		"region":           region,
	}
	return filemanager.NewS3Manager(conf, s3Config, log.Withn(logger.NewStringField("component", "ut-uploader")), func() time.Duration {
		return conf.GetDuration("UTSampling.Timeout", 120, time.Second)
	})
}
