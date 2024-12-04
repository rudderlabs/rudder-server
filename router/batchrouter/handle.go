package batchrouter

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/isolation"
	"github.com/rudderlabs/rudder-server/router/rterror"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const module = "batch_router"

type Handle struct {
	destType string
	// dependencies

	conf               *config.Config
	logger             logger.Logger
	netHandle          *http.Client
	jobsDB             jobsdb.JobsDB
	errorDB            jobsdb.JobsDB
	reporting          types.Reporting
	backendConfig      backendconfig.BackendConfig
	fileManagerFactory filemanager.Factory
	transientSources   transientsource.Service
	rsourcesService    rsources.JobService
	warehouseClient    *client.Warehouse
	debugger           destinationdebugger.DestinationDebugger
	Diagnostics        diagnostics.DiagnosticsI
	adaptiveLimit      func(int64) int64
	isolationStrategy  isolation.Strategy
	now                func() time.Time

	// configuration

	maxEventsInABatch            int
	maxPayloadSizeInBytes        int
	maxFailedCountForJob         config.ValueLoader[int]
	maxFailedCountForSourcesJob  config.ValueLoader[int]
	asyncUploadTimeout           config.ValueLoader[time.Duration]
	asyncUploadWorkerTimeout     config.ValueLoader[time.Duration]
	retryTimeWindow              config.ValueLoader[time.Duration]
	sourcesRetryTimeWindow       config.ValueLoader[time.Duration]
	reportingEnabled             bool
	jobQueryBatchSize            config.ValueLoader[int]
	pollStatusLoopSleep          config.ValueLoader[time.Duration]
	payloadLimit                 config.ValueLoader[int64]
	jobsDBCommandTimeout         config.ValueLoader[time.Duration]
	jobdDBQueryRequestTimeout    config.ValueLoader[time.Duration]
	jobdDBMaxRetries             config.ValueLoader[int]
	minIdleSleep                 config.ValueLoader[time.Duration]
	uploadFreq                   config.ValueLoader[time.Duration]
	mainLoopFreq                 config.ValueLoader[time.Duration]
	disableEgress                bool
	warehouseServiceMaxRetryTime config.ValueLoader[time.Duration]
	transformerURL               string
	datePrefixOverride           config.ValueLoader[string]
	customDatePrefix             config.ValueLoader[string]

	drainer routerutils.Drainer

	// state

	backgroundGroup  *errgroup.Group
	backgroundCtx    context.Context
	backgroundCancel context.CancelFunc
	backgroundWait   func() error

	backendConfigInitializedOnce sync.Once
	backendConfigInitialized     chan bool

	configSubscriberMu       sync.RWMutex                                   // protects the following fields
	destinationsMap          map[string]*routerutils.DestinationWithSources // destinationID -> destination
	connectionWHNamespaceMap map[string]string                              // connectionIdentifier -> warehouseConnectionIdentifier(+namepsace)
	uploadIntervalMap        map[string]time.Duration

	encounteredMergeRuleMapMu sync.Mutex
	encounteredMergeRuleMap   map[string]map[string]bool

	limiter struct {
		read    kitsync.Limiter
		process kitsync.Limiter
		upload  kitsync.Limiter
	}

	lastExecTimesMu sync.RWMutex
	lastExecTimes   map[string]time.Time

	failingDestinationsMu sync.RWMutex
	failingDestinations   map[string]bool

	batchRequestsMetricMu sync.RWMutex
	batchRequestsMetric   []batchRequestMetric

	warehouseServiceFailedTimeMu sync.RWMutex
	warehouseServiceFailedTime   time.Time

	dateFormatProvider *storageDateFormatProvider

	diagnosisTicker          *time.Ticker
	uploadedRawDataJobsCache map[string]map[string]bool
	asyncDestinationStruct   map[string]*asynccommon.AsyncDestinationStruct

	asyncPollTimeStat       stats.Measurement
	asyncFailedJobsTimeStat stats.Measurement
	asyncSuccessfulJobCount stats.Measurement
	asyncFailedJobCount     stats.Measurement
	asyncAbortedJobCount    stats.Measurement
}

// mainLoop is responsible for pinging the workers periodically for every active partition
func (brt *Handle) mainLoop(ctx context.Context) {
	if brt.now == nil {
		brt.now = timeutil.Now
	}

	pool := workerpool.New(ctx, func(partition string) workerpool.Worker { return newWorker(partition, brt.logger, brt) }, brt.logger)
	defer pool.Shutdown()
	mainLoopSleep := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(mainLoopSleep):
			for _, partition := range brt.activePartitions(ctx) {
				pool.PingWorker(partition)
			}
			mainLoopSleep = brt.mainLoopFreq.Load()
		}
	}
}

// activePartitions returns the list of active partitions, depending on the active isolation strategy
func (brt *Handle) activePartitions(ctx context.Context) []string {
	statTags := map[string]string{"destType": brt.destType}
	defer stats.Default.NewTaggedStat("brt_active_partitions_time", stats.TimerType, statTags).RecordDuration()()
	keys, err := brt.isolationStrategy.ActivePartitions(ctx, brt.jobsDB)
	if err != nil && ctx.Err() == nil {
		panic(err)
	}
	stats.Default.NewTaggedStat("brt_active_partitions", stats.GaugeType, statTags).Gauge(len(keys))
	return keys
}

// getWorkerJobs returns the list of jobs for a given partition. Jobs are grouped by destination
func (brt *Handle) getWorkerJobs(partition string) (workerJobs []*DestinationJobs) {
	if brt.skipFetchingJobs(partition) {
		return
	}

	defer brt.limiter.read.Begin("")()

	brt.configSubscriberMu.RLock()
	destinationsMap := brt.destinationsMap
	brt.configSubscriberMu.RUnlock()
	var jobs []*jobsdb.JobT

	var firstJob *jobsdb.JobT
	var lastJob *jobsdb.JobT

	brtQueryStat := stats.Default.NewTaggedStat("batch_router.jobsdb_query_time", stats.TimerType, stats.Tags{"function": "getJobs", "destType": brt.destType, "partition": partition})
	queryStart := time.Now()
	queryParams := jobsdb.GetQueryParams{
		CustomValFilters: []string{brt.destType},
		JobsLimit:        brt.jobQueryBatchSize.Load(),
		PayloadSizeLimit: brt.adaptiveLimit(brt.payloadLimit.Load()),
	}
	brt.isolationStrategy.AugmentQueryParams(partition, &queryParams)
	var limitsReached bool

	toProcess, err := misc.QueryWithRetriesAndNotify(context.Background(), brt.jobdDBQueryRequestTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) (jobsdb.JobsResult, error) {
		return brt.jobsDB.GetJobs(ctx, []string{jobsdb.Failed.State, jobsdb.Unprocessed.State}, queryParams)
	}, brt.sendQueryRetryStats)
	if err != nil {
		brt.logger.Errorf("BRT: %s: Error while reading from DB: %v", brt.destType, err)
		panic(err)
	}
	jobs = toProcess.Jobs
	limitsReached = toProcess.LimitsReached

	brtQueryStat.Since(queryStart)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].JobID < jobs[j].JobID
	})
	if len(jobs) > 0 {
		firstJob = jobs[0]
		lastJob = jobs[len(jobs)-1]
	}
	brt.pipelineDelayStats(partition, firstJob, lastJob)
	jobsByDesID := lo.GroupBy(jobs, func(job *jobsdb.JobT) string {
		return gjson.GetBytes(job.Parameters, "destination_id").String()
	})
	for destID, destJobs := range jobsByDesID {
		if batchDest, ok := destinationsMap[destID]; ok {
			var processJobs bool
			brt.lastExecTimesMu.Lock()
			brt.failingDestinationsMu.RLock()
			if limitsReached && !brt.failingDestinations[destID] { // if limits are reached and the destination is not failing, process all jobs regardless of their upload frequency
				processJobs = true
			} else { // honour upload frequency
				lastExecTime := brt.lastExecTimes[destID]
				if lastExecTime.IsZero() || time.Since(lastExecTime) >= brt.uploadFreq.Load() {
					processJobs = true
					brt.lastExecTimes[destID] = time.Now()
				}
			}
			brt.failingDestinationsMu.RUnlock()
			brt.lastExecTimesMu.Unlock()
			if processJobs {
				workerJobs = append(workerJobs, &DestinationJobs{destWithSources: *batchDest, jobs: destJobs})
			}
		} else {
			brt.logger.Errorf("BRT: %s: Destination %s not found in destinationsMap", brt.destType, destID)
		}
	}

	return
}

// upload the given batch of jobs to the given object storage provider
func (brt *Handle) upload(provider string, batchJobs *BatchedJobs, isWarehouse bool) UploadResult {
	if brt.disableEgress {
		return UploadResult{Error: rterror.DisabledEgress}
	}

	var localTmpDirName string
	if isWarehouse {
		localTmpDirName = fmt.Sprintf(`/%s/`, misc.RudderWarehouseStagingUploads)
	} else {
		localTmpDirName = fmt.Sprintf(`/%s/`, misc.RudderRawDataDestinationLogs)
	}

	uuid := uuid.New()
	brt.logger.Debugf("BRT: Starting logging to %s", provider)

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	gzipFilePath := filepath.Join(
		tmpDirPath,
		localTmpDirName,
		fmt.Sprintf(
			"%v.%v.%v.json.gz",
			time.Now().Unix(),
			batchJobs.Connection.Source.ID,
			uuid,
		),
	)

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
	connIdentifier := connectionIdentifier(*batchJobs.Connection)
	brt.configSubscriberMu.RLock()
	warehouseConnIdentifier := brt.connectionWHNamespaceMap[connIdentifier]
	brt.configSubscriberMu.RUnlock()
	var totalBytes int
	for _, job := range batchJobs.Jobs {
		// do not add to staging file if the event is a rudder_identity_merge_rules record
		// and has been previously added to it
		if isWarehouse && warehouseutils.IDResolutionEnabled() && gjson.GetBytes(job.EventPayload, "metadata.isMergeRule").Bool() {
			mergeProp1 := gjson.GetBytes(job.EventPayload, "metadata.mergePropOne").String()
			mergeProp2 := gjson.GetBytes(job.EventPayload, "metadata.mergePropTwo").String()
			ruleIdentifier := fmt.Sprintf(`%s::%s`, mergeProp1, mergeProp2)
			brt.encounteredMergeRuleMapMu.Lock()
			if _, ok := brt.encounteredMergeRuleMap[warehouseConnIdentifier]; !ok {
				brt.encounteredMergeRuleMap[warehouseConnIdentifier] = make(map[string]bool)
			}
			if _, ok := brt.encounteredMergeRuleMap[warehouseConnIdentifier][ruleIdentifier]; ok {
				brt.encounteredMergeRuleMapMu.Unlock()
				dedupedIDMergeRuleJobs++
				continue
			}
			brt.encounteredMergeRuleMap[warehouseConnIdentifier][ruleIdentifier] = true
			brt.encounteredMergeRuleMapMu.Unlock()
		}

		eventID := gjson.GetBytes(job.EventPayload, "messageId").String()
		var ok bool
		interruptedEventsMap, isDestInterrupted := brt.uploadedRawDataJobsCache[batchJobs.Connection.Destination.ID]
		if isDestInterrupted {
			if _, ok = interruptedEventsMap[eventID]; !ok {
				eventsFound = true
				line := string(job.EventPayload) + "\n"
				totalBytes += len(line)
				_ = gzWriter.WriteGZ(line)
			}
		} else {
			eventsFound = true
			line := string(job.EventPayload) + "\n"
			totalBytes += len(line)
			_ = gzWriter.WriteGZ(line)
		}
	}
	_ = gzWriter.CloseGZ()
	if !eventsFound {
		brt.logger.Infof("BRT: No events in this batch for upload to %s. Events are either de-deuplicated or skipped", provider)
		return UploadResult{
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
	useRudderStorage := isWarehouse && misc.IsConfiguredToUseRudderObjectStorage(batchJobs.Connection.Destination.Config)
	uploader, err := brt.fileManagerFactory(&filemanager.Settings{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           batchJobs.Connection.Destination.Config,
			UseRudderStorage: useRudderStorage,
			WorkspaceID:      batchJobs.Connection.Destination.WorkspaceID,
		}),
	})
	if err != nil {
		return UploadResult{
			Error:          err,
			LocalFilePaths: []string{gzipFilePath},
		}
	}

	outputFile, err := os.Open(gzipFilePath)
	if err != nil {
		panic(err)
	}

	brt.logger.Debugf("BRT: Starting upload to %s", provider)
	var folderName string
	if isWarehouse {
		folderName = config.GetString("WAREHOUSE_STAGING_BUCKET_FOLDER_NAME", "rudder-warehouse-staging-logs")
	} else {
		folderName = config.GetString("DESTINATION_BUCKET_FOLDER_NAME", "rudder-logs")
	}

	var datePrefixLayout string
	if brt.datePrefixOverride.Load() != "" {
		datePrefixLayout = brt.datePrefixOverride.Load()
	} else {
		dateFormat, _ := brt.dateFormatProvider.GetFormat(brt.logger, uploader, batchJobs.Connection, folderName)
		datePrefixLayout = dateFormat
	}

	workspaceID := batchJobs.Connection.Destination.WorkspaceID
	customTimezone := brt.conf.GetString("BatchRouter.customTimezone."+workspaceID, "")

	now := brt.now()
	if customTimezone != "" {
		loc, err := time.LoadLocation(customTimezone)
		if err != nil {
			brt.logger.Errorn(
				"Error loading custom timezone",
				obskit.Error(err),
				obskit.WorkspaceID(workspaceID),
				logger.NewStringField("customTimezone", customTimezone),
			)
		}
		now = now.In(loc)
	}

	brt.logger.Debugf("BRT: Date prefix layout is %s", datePrefixLayout)
	switch datePrefixLayout {
	case "MM-DD-YYYY": // used to be earlier default
		datePrefixLayout = now.Format("01-02-2006")
	default:
		datePrefixLayout = now.Format("2006-01-02")
	}

	keyPrefixes := []string{folderName, batchJobs.Connection.Source.ID, brt.customDatePrefix.Load() + datePrefixLayout}

	_, fileName := filepath.Split(gzipFilePath)
	var (
		opID      int64
		opPayload stdjson.RawMessage
	)
	if !isWarehouse {
		opPayload, _ = json.Marshal(&ObjectStorageDefinition{
			Config:          batchJobs.Connection.Destination.Config,
			Key:             strings.Join(append(keyPrefixes, fileName), "/"),
			Provider:        provider,
			DestinationID:   batchJobs.Connection.Destination.ID,
			DestinationType: batchJobs.Connection.Destination.DestinationDefinition.Name,
		})
		opID, err = brt.jobsDB.JournalMarkStart(jobsdb.RawDataDestUploadOperation, opPayload)
		if err != nil {
			panic(fmt.Errorf("BRT: Error marking start of upload operation in journal: %v", err))
		}
	}

	startTime := time.Now()
	uploadOutput, err := uploader.Upload(context.TODO(), outputFile, keyPrefixes...)
	uploadSuccess := err == nil
	brtUploadTimeStat := stats.Default.NewTaggedStat("brt_upload_time", stats.TimerType, map[string]string{
		"success":     strconv.FormatBool(uploadSuccess),
		"destType":    brt.destType,
		"destination": batchJobs.Connection.Destination.ID,
	})
	brtUploadTimeStat.Since(startTime)

	if err != nil {
		brt.logger.Errorf("BRT: Error uploading to %s: Error: %v", provider, err)
		return UploadResult{
			Error:          err,
			JournalOpID:    opID,
			LocalFilePaths: []string{gzipFilePath},
		}
	}

	return UploadResult{
		Config:           batchJobs.Connection.Destination.Config,
		Key:              uploadOutput.ObjectName,
		FileLocation:     uploadOutput.Location,
		LocalFilePaths:   []string{gzipFilePath},
		JournalOpID:      opID,
		FirstEventAt:     firstEventAt,
		LastEventAt:      lastEventAt,
		TotalEvents:      len(batchJobs.Jobs) - dedupedIDMergeRuleJobs,
		TotalBytes:       totalBytes,
		UseRudderStorage: useRudderStorage,
	}
}

// pingWarehouse notifies the warehouse about a new data upload (staging files)
func (brt *Handle) pingWarehouse(batchJobs *BatchedJobs, output UploadResult) (err error) {
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
	var sampleParameters routerutils.JobParameters
	err = json.Unmarshal(batchJobs.Jobs[0].Parameters, &sampleParameters)
	if err != nil {
		brt.logger.Error("Unmarshal of job parameters failed in postToWarehouse function. ", string(batchJobs.Jobs[0].Parameters))
	}

	payload := client.StagingFile{
		WorkspaceID:           batchJobs.Jobs[0].WorkspaceId,
		Schema:                schemaMap,
		SourceID:              batchJobs.Connection.Source.ID,
		DestinationID:         batchJobs.Connection.Destination.ID,
		Location:              output.Key,
		FirstEventAt:          output.FirstEventAt,
		LastEventAt:           output.LastEventAt,
		TotalEvents:           output.TotalEvents,
		TotalBytes:            output.TotalBytes,
		UseRudderStorage:      output.UseRudderStorage,
		SourceTaskRunID:       sampleParameters.SourceTaskRunID,
		SourceJobID:           sampleParameters.SourceJobID,
		SourceJobRunID:        sampleParameters.SourceJobRunID,
		DestinationRevisionID: batchJobs.Connection.Destination.RevisionID,
	}

	if slices.Contains(warehouseutils.TimeWindowDestinations, brt.destType) {
		payload.TimeWindow = batchJobs.TimeWindow
	}

	err = brt.warehouseClient.Process(context.TODO(), payload)
	if err != nil {
		brt.logger.Errorf("BRT: Failed to route staging file: %v", err)
		return
	}
	brt.logger.Infof("BRT: Routed successfully staging file URL to warehouse service")
	return
}

// updateJobStatus updates the statuses for the provided batch of jobs in jobsDB
func (brt *Handle) updateJobStatus(batchJobs *BatchedJobs, isWarehouse bool, errOccurred error, notifyWarehouseErr bool) {
	var (
		batchJobState string
		errorResp     []byte
	)
	batchRouterWorkspaceJobStatusCount := make(map[string]int)
	var abortedEvents []*jobsdb.JobT
	var batchReqMetric batchRequestMetric
	if errOccurred != nil {
		switch {
		case errors.Is(errOccurred, rterror.DisabledEgress):
			brt.logger.Debugf("BRT: Outgoing traffic disabled : %v at %v", batchJobs.Connection.Source.ID,
				time.Now().Format("01-02-2006"))
			batchJobState = jobsdb.Succeeded.State
			errorResp = []byte(fmt.Sprintf(`{"success":"%s"}`, errOccurred.Error())) // skipcq: GO-R4002
		case errors.Is(errOccurred, filemanager.ErrInvalidServiceProvider):
			brt.logger.Warnf("BRT: Destination %s : %s for destination ID : %v at %v",
				batchJobs.Connection.Destination.DestinationDefinition.DisplayName, errOccurred.Error(),
				batchJobs.Connection.Destination.ID, time.Now().Format("01-02-2006"))
			batchJobState = jobsdb.Aborted.State
			errorResp = []byte(fmt.Sprintf(`{"reason":"%s"}`, errOccurred.Error())) // skipcq: GO-R4002
		default:
			brt.logger.Errorf("BRT: Error uploading to object storage: %v %v", errOccurred, batchJobs.Connection.Source.ID)
			if batchJobs.JobState != "" {
				batchJobState = batchJobs.JobState
			} else {
				batchJobState = jobsdb.Failed.State
			}
			errorResp, _ = json.Marshal(ErrorResponse{Error: errOccurred.Error()})
			batchReqMetric.batchRequestFailed = 1
			// We keep track of number of failed attempts in case of failure and number of events uploaded in case of success in stats
		}
	} else {
		brt.logger.Debugf("BRT: Uploaded to object storage : %v at %v", batchJobs.Connection.Source.ID, time.Now().Format("01-02-2006"))
		batchJobState = jobsdb.Succeeded.State
		errorResp = []byte(`{"success":"OK"}`)
		batchReqMetric.batchRequestSuccess = 1
	}
	brt.trackRequestMetrics(batchReqMetric)
	brt.failingDestinationsMu.Lock()
	brt.failingDestinations[batchJobs.Connection.Destination.ID] = batchReqMetric.batchRequestFailed > 0
	brt.failingDestinationsMu.Unlock()
	var statusList []*jobsdb.JobStatusT

	if isWarehouse && notifyWarehouseErr {
		brt.warehouseServiceFailedTimeMu.Lock()
		if brt.warehouseServiceFailedTime.IsZero() {
			brt.warehouseServiceFailedTime = time.Now()
		}
		brt.warehouseServiceFailedTimeMu.Unlock()
	} else if isWarehouse {
		brt.warehouseServiceFailedTimeMu.Lock()
		brt.warehouseServiceFailedTime = time.Time{}
		brt.warehouseServiceFailedTimeMu.Unlock()
	}

	var err error
	reportMetrics := make([]*types.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*types.ConnectionDetails)
	transformedAtMap := make(map[string]string)
	statusDetailsMap := make(map[string]*types.StatusDetail)
	jobStateCounts := make(map[string]int)
	jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionDetails)
	for _, job := range batchJobs.Jobs {
		jobState := batchJobState

		firstAttemptedAt := getFirstAttemptAtFromErrorResponse(job.LastJobStatus.ErrorResponse)
		if enhancedErrorResp, err := sjson.SetBytes(errorResp, "firstAttemptedAt", firstAttemptedAt.Format(misc.RFC3339Milli)); err == nil {
			errorResp = enhancedErrorResp
		}

		var parameters routerutils.JobParameters
		err = json.Unmarshal(job.Parameters, &parameters)
		if err != nil {
			brt.logger.Error("Unmarshal of job parameters failed. ", string(job.Parameters))
		}

		var failedMessage *types.FailedMessage
		var errorCode string
		switch jobState {
		case jobsdb.Failed.State:
			if !notifyWarehouseErr && brt.retryLimitReached(&job.LastJobStatus) {
				job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "stage", "batch_router")
				job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "reason", errOccurred.Error())
				abortedEvents = append(abortedEvents, job)
				failedMessage = &types.FailedMessage{MessageID: parameters.MessageID, ReceivedAt: parameters.ParseReceivedAtTime()}
				jobState = jobsdb.Aborted.State
				errorCode = routerutils.DRAIN_ERROR_CODE
			}
			if notifyWarehouseErr && isWarehouse {
				// change job state to abort state after warehouse service is continuously failing more than warehouseServiceMaxRetryTimeinHr time
				brt.warehouseServiceFailedTimeMu.RLock()
				if time.Since(brt.warehouseServiceFailedTime) > brt.warehouseServiceMaxRetryTime.Load() {
					job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "stage", "batch_router")
					job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "reason", errOccurred.Error())
					abortedEvents = append(abortedEvents, job)
					failedMessage = &types.FailedMessage{MessageID: parameters.MessageID, ReceivedAt: parameters.ParseReceivedAtTime()}
					jobState = jobsdb.Aborted.State
					errorCode = routerutils.DRAIN_ERROR_CODE
				}
				brt.warehouseServiceFailedTimeMu.RUnlock()
			}
		case jobsdb.Aborted.State:
			job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "stage", "batch_router")
			job.Parameters = misc.UpdateJSONWithNewKeyVal(job.Parameters, "reason", errOccurred.Error())
			failedMessage = &types.FailedMessage{MessageID: parameters.MessageID, ReceivedAt: parameters.ParseReceivedAtTime()}
			abortedEvents = append(abortedEvents, job)
		}
		attemptNum := job.LastJobStatus.AttemptNum + 1
		jobIDConnectionDetailsMap[job.JobID] = jobsdb.ConnectionDetails{
			SourceID:      parameters.SourceID,
			DestinationID: parameters.DestinationID,
		}
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    attemptNum,
			JobState:      jobState,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     errorCode,
			ErrorResponse: errorResp,
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
		}
		statusList = append(statusList, &status)
		jobStateCounts[jobState] = jobStateCounts[jobState] + 1

		// REPORTING - START
		if brt.reporting != nil && brt.reportingEnabled {
			// Update metrics maps
			errorCode := getBRTErrorCode(jobState)
			var cd *types.ConnectionDetails
			workspaceID := job.WorkspaceId
			key := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s", parameters.SourceID, parameters.DestinationID, parameters.SourceJobRunID, jobState, strconv.Itoa(errorCode), parameters.EventName, parameters.EventType)
			if _, ok := connectionDetailsMap[key]; !ok {
				cd = &types.ConnectionDetails{
					SourceID:                parameters.SourceID,
					DestinationID:           parameters.DestinationID,
					SourceTaskRunID:         parameters.SourceTaskRunID,
					SourceJobID:             parameters.SourceJobID,
					SourceJobRunID:          parameters.SourceJobRunID,
					SourceDefinitionID:      parameters.SourceDefinitionID,
					DestinationDefinitionID: parameters.DestinationDefinitionID,
					SourceCategory:          parameters.SourceCategory,
				}
				connectionDetailsMap[key] = cd
				transformedAtMap[key] = parameters.TransformAt
			}
			sd, ok := statusDetailsMap[key]
			if !ok {
				sampleEvent := job.EventPayload
				if brt.transientSources.Apply(parameters.SourceID) {
					sampleEvent = []byte(`{}`)
				}
				sd = &types.StatusDetail{
					Status:         jobState,
					StatusCode:     errorCode,
					SampleResponse: string(errorResp),
					SampleEvent:    sampleEvent,
					EventName:      parameters.EventName,
					EventType:      parameters.EventType,
				}
				statusDetailsMap[key] = sd
			}
			if status.JobState == jobsdb.Failed.State && status.AttemptNum == 1 {
				sd.Count++
			}
			if status.JobState != jobsdb.Failed.State {
				if status.JobState == jobsdb.Succeeded.State || status.JobState == jobsdb.Aborted.State {
					batchRouterWorkspaceJobStatusCount[workspaceID] += 1
				}
				sd.Count++
				if failedMessage != nil {
					sd.FailedMessages = append(sd.FailedMessages, failedMessage)
				}
			}
		}
		// REPORTING - END
	}

	for workspace, jobCount := range batchRouterWorkspaceJobStatusCount {
		rmetrics.DecreasePendingEvents(
			"batch_rt",
			workspace,
			brt.destType,
			float64(jobCount),
		)
	}
	// tracking batch router errors
	if diagnostics.EnableDestinationFailuresMetric {
		if batchJobState == jobsdb.Failed.State {
			brt.Diagnostics.Track(diagnostics.BatchRouterFailed, map[string]interface{}{
				diagnostics.BatchRouterDestination: brt.destType,
				diagnostics.ErrorResponse:          string(errorResp),
			})
		}
	}

	parameterFilters := []jobsdb.ParameterFilterT{
		{
			Name:  "destination_id",
			Value: batchJobs.Connection.Destination.ID,
		},
	}

	// Store the aborted jobs to errorDB
	if abortedEvents != nil {
		err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
			return brt.errorDB.Store(ctx, abortedEvents)
		}, brt.sendRetryStoreStats)
		if err != nil {
			brt.logger.Errorf("[Batch Router] Store into proc error table failed with error: %v", err)
			brt.logger.Errorf("abortedEvents: %v", abortedEvents)
			panic(err)
		}
	}

	// REPORTING - START
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
	// REPORTING - END

	// Mark the status of the jobs
	err = misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
			err = brt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{brt.destType}, parameterFilters)
			if err != nil {
				brt.logger.Errorf("[Batch Router] Error occurred while updating %s jobs statuses. Panicking. Err: %v", brt.destType, err)
				return err
			}

			// rsources stats
			err = brt.updateRudderSourcesStats(ctx, tx, batchJobs.Jobs, statusList)
			if err != nil {
				return err
			}

			if brt.reporting != nil && brt.reportingEnabled {
				if err = brt.reporting.Report(ctx, reportMetrics, tx.Tx()); err != nil {
					return fmt.Errorf("reporting metrics: %w", err)
				}
			}
			return nil
		})
	}, brt.sendRetryUpdateStats)
	if err != nil {
		panic(err)
	}
	routerutils.UpdateProcessedEventsMetrics(stats.Default, module, brt.destType, statusList, jobIDConnectionDetailsMap)
	sendDestStatusStats(batchJobs.Connection, jobStateCounts, brt.destType, isWarehouse)
}

// uploadInterval calculates the upload interval for the destination
func (brt *Handle) uploadInterval(destinationConfig map[string]interface{}) time.Duration {
	uploadInterval, ok := destinationConfig["uploadInterval"]
	if !ok {
		brt.logger.Debugf("BRT: uploadInterval not found in destination config, falling back to default: %s", brt.asyncUploadTimeout)
		return brt.asyncUploadTimeout.Load()
	}
	dur, ok := uploadInterval.(string)
	if !ok {
		brt.logger.Warnf("BRT: not found string type uploadInterval, falling back to default: %s", brt.asyncUploadTimeout)
		return brt.asyncUploadTimeout.Load()
	}
	parsedTime, err := strconv.ParseInt(dur, 10, 64)
	if err != nil {
		brt.logger.Warnf("BRT: Couldn't parseint uploadInterval, falling back to default: %s", brt.asyncUploadTimeout)
		return brt.asyncUploadTimeout.Load()
	}
	return time.Duration(parsedTime * int64(time.Minute))
}

// skipFetchingJobs returns true if the destination type is async and the there are still jobs in [importing] state for this destination type
func (brt *Handle) skipFetchingJobs(partition string) bool {
	if asynccommon.IsAsyncDestination(brt.destType) {
		queryParams := jobsdb.GetQueryParams{
			CustomValFilters: []string{brt.destType},
			JobsLimit:        1,
			PayloadSizeLimit: brt.adaptiveLimit(brt.payloadLimit.Load()),
		}
		brt.isolationStrategy.AugmentQueryParams(partition, &queryParams)
		importingList, err := misc.QueryWithRetriesAndNotify(context.Background(), brt.jobdDBQueryRequestTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) (jobsdb.JobsResult, error) {
			return brt.jobsDB.GetImporting(ctx, queryParams)
		}, brt.sendQueryRetryStats)
		if err != nil {
			brt.logger.Errorf("BRT: Failed to get importing jobs for %s with error: %v", brt.destType, err)
			panic(err)
		}
		return len(importingList.Jobs) != 0
	}
	return false
}

// splitBatchJobsOnTimeWindow splits the batchJobs based on a timeWindow if the destination requires so, otherwise a single entry is returned using the zero value as key
func (brt *Handle) splitBatchJobsOnTimeWindow(batchJobs BatchedJobs) map[time.Time]*BatchedJobs {
	splitBatches := map[time.Time]*BatchedJobs{}
	if !slices.Contains(warehouseutils.TimeWindowDestinations, brt.destType) {
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
			brt.logger.Errorf("Invalid value '%s' for receivedAt : %v ", receivedAtStr, err)
			panic(err)
		}
		timeWindow := warehouseutils.GetTimeWindow(receivedAt)

		// create batchJob for timeWindow if it does not exist
		if _, ok := splitBatches[timeWindow]; !ok {
			splitBatches[timeWindow] = &BatchedJobs{
				Jobs:       make([]*jobsdb.JobT, 0),
				Connection: batchJobs.Connection,
				TimeWindow: timeWindow,
			}
		}

		splitBatches[timeWindow].Jobs = append(splitBatches[timeWindow].Jobs, job)
	}
	return splitBatches
}

func (brt *Handle) retryLimitReached(status *jobsdb.JobStatusT) bool {
	firstAttemptedAtTime := getFirstAttemptAtFromErrorResponse(status.ErrorResponse)

	maxFailedCountForJob := brt.maxFailedCountForJob.Load()
	retryTimeWindow := brt.retryTimeWindow.Load()

	if gjson.GetBytes(status.JobParameters, "source_job_run_id").Str != "" {
		maxFailedCountForJob = brt.maxFailedCountForSourcesJob.Load()
		retryTimeWindow = brt.sourcesRetryTimeWindow.Load()
	}

	return time.Since(firstAttemptedAtTime) > retryTimeWindow &&
		status.AttemptNum >= maxFailedCountForJob // retry time window exceeded
}
