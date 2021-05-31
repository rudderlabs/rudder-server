package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/services/dedup"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	event_schema "github.com/rudderlabs/rudder-server/event-schema"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
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

//HandleT is an handle to this object used in main.go
type HandleT struct {
	backendConfig                  backendconfig.BackendConfig
	processSessions                bool
	sessionThresholdEvents         int
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
	statActiveUsers                stats.RudderStats
	userJobListMap                 map[string][]*jobsdb.JobT
	userEventsMap                  map[string][]types.SingularEventT
	userPQItemMap                  map[string]*pqItemT
	transformEventsByTimeMutex     sync.RWMutex
	destTransformEventsByTimeTaken transformRequestPQ
	userTransformEventsByTimeTaken transformRequestPQ
	statDBR                        stats.RudderStats
	statDBW                        stats.RudderStats
	statLoopTime                   stats.RudderStats
	eventSchemasTime               stats.RudderStats
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
	destStats                      map[string]*DestStatT
	userToSessionIDMap             map[string]string
	userJobPQ                      pqT
	userPQLock                     sync.Mutex
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

type DestStatT struct {
	id               string
	numEvents        stats.RudderStats
	numOutputEvents  stats.RudderStats
	sessionTransform stats.RudderStats
	userTransform    stats.RudderStats
	destTransform    stats.RudderStats
}

type ParametersT struct {
	SourceID        string `json:"source_id"`
	DestinationID   string `json:"destination_id"`
	ReceivedAt      string `json:"received_at"`
	TransformAt     string `json:"transform_at"`
	MessageID       string `json:"message_id"`
	GatewayJobID    int64  `json:"gateway_job_id"`
	SourceBatchID   string `json:"source_batch_id"`
	SourceTaskID    string `json:"source_task_id"`
	SourceTaskRunID string `json:"source_task_run_id"`
	SourceJobID     string `json:"source_job_id"`
	SourceJobRunID  string `json:"source_job_run_id"`
}

type MetricMetadata struct {
	sourceID        string
	destinationID   string
	sourceBatchID   string
	sourceTaskID    string
	sourceTaskRunID string
	sourceJobID     string
	sourceJobRunID  string
}

func (proc *HandleT) newDestinationStat(destination backendconfig.DestinationT) *DestStatT {
	destinationTag := misc.GetTagName(destination.ID, destination.Name)
	var module = "router"
	if batchrouter.IsObjectStorageDestination(destination.DestinationDefinition.Name) {
		module = "batch_router"
	}
	if batchrouter.IsWarehouseDestination(destination.DestinationDefinition.Name) {
		module = "warehouse"
	}
	tags := map[string]string{
		"module":      module,
		"destination": destinationTag,
		"destType":    destination.DestinationDefinition.Name,
	}
	numEvents := proc.stats.NewTaggedStat("proc_num_events", stats.CountType, tags)
	numOutputEvents := proc.stats.NewTaggedStat("proc_num_output_events", stats.CountType, tags)
	sessionTransform := proc.stats.NewTaggedStat("proc_session_transform", stats.TimerType, tags)
	userTransform := proc.stats.NewTaggedStat("proc_user_transform", stats.TimerType, tags)
	destTransform := proc.stats.NewTaggedStat("proc_dest_transform", stats.TimerType, tags)
	return &DestStatT{
		id:               destination.ID,
		numEvents:        numEvents,
		numOutputEvents:  numOutputEvents,
		sessionTransform: sessionTransform,
		userTransform:    userTransform,
		destTransform:    destTransform,
	}
}

//Print the internal structure
func (proc *HandleT) Print() {
	if !proc.logger.IsDebugLevel() {
		return
	}
	proc.logger.Debug("PriorityQueue")
	proc.userJobPQ.Print()
	proc.logger.Debug("JobList")
	for k, v := range proc.userJobListMap {
		proc.logger.Debug(k, ":", len(v))
	}
	proc.logger.Debug("EventLength")
	for k, v := range proc.userEventsMap {
		proc.logger.Debug(k, ":", len(v))
	}
	proc.logger.Debug("PQItem")
	for k, v := range proc.userPQItemMap {
		proc.logger.Debug(k, ":", *v)
	}
	proc.logger.Debug("Session")
	for k, v := range proc.userToSessionIDMap {
		proc.logger.Debug(k, " : ", v)
	}
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("processor")
}

// NewProcessor creates a new Processor intanstace
func NewProcessor() *HandleT {
	return &HandleT{
		transformer:            transformer.NewTransformer(),
		processSessions:        configProcessSessions,
		sessionThresholdEvents: configSessionThresholdEvents,
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
	return statusRes
}

//Setup initializes the module
func (proc *HandleT) Setup(backendConfig backendconfig.BackendConfig, gatewayDB jobsdb.JobsDB, routerDB jobsdb.JobsDB, batchRouterDB jobsdb.JobsDB, errorDB jobsdb.JobsDB, clearDB *bool, reporting types.ReportingI) {
	proc.reporting = reporting
	proc.reportingEnabled = config.GetBool("Reporting.enabled", true)
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
	proc.userJobListMap = make(map[string][]*jobsdb.JobT)
	proc.userEventsMap = make(map[string][]types.SingularEventT)
	proc.userPQItemMap = make(map[string]*pqItemT)
	proc.userToSessionIDMap = make(map[string]string)
	proc.userTransformEventsByTimeTaken = make([]*TransformRequestT, 0, transformTimesPQLength)
	proc.destTransformEventsByTimeTaken = make([]*TransformRequestT, 0, transformTimesPQLength)
	proc.userJobPQ = make(pqT, 0)
	proc.pStatsJobs.Setup("ProcessorJobs")
	proc.pStatsDBR.Setup("ProcessorDBRead")
	proc.pStatsDBW.Setup("ProcessorDBWrite")

	proc.statGatewayDBR = proc.stats.NewStat("processor.gateway_db_read", stats.CountType)
	proc.statGatewayDBW = proc.stats.NewStat("processor.gateway_db_write", stats.CountType)
	proc.statRouterDBW = proc.stats.NewStat("processor.router_db_write", stats.CountType)
	proc.statBatchRouterDBW = proc.stats.NewStat("processor.batch_router_db_write", stats.CountType)
	proc.statActiveUsers = proc.stats.NewStat("processor.active_users", stats.GaugeType)
	proc.statDBR = proc.stats.NewStat("processor.gateway_db_read_time", stats.TimerType)
	proc.statDBW = proc.stats.NewStat("processor.gateway_db_write_time", stats.TimerType)
	proc.statProcErrDBW = proc.stats.NewStat("processor.proc_err_db_write", stats.CountType)
	proc.statLoopTime = proc.stats.NewStat("processor.loop_time", stats.TimerType)
	proc.eventSchemasTime = proc.stats.NewStat("processor.event_schemas_time", stats.TimerType)
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
	proc.destStats = make(map[string]*DestStatT)
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
	if proc.processSessions {
		proc.logger.Info("Starting session processor")
		rruntime.Go(func() {
			proc.createSessions()
		})
	}
}

// Start starts this processor's main loops.
func (proc *HandleT) Start() {

	rruntime.Go(func() {
		proc.mainLoop()
	})
	rruntime.Go(func() {
		st := stash.New()
		st.Setup(proc.errorDB)
		st.Start()
	})
}

var (
	loopSleep                           time.Duration
	maxLoopSleep                        time.Duration
	fixedLoopSleep                      time.Duration
	maxEventsToProcess                  int
	avgEventsInRequest                  int
	dbReadBatchSize                     int
	transformBatchSize                  int
	userTransformBatchSize              int
	sessionInactivityThreshold          time.Duration
	configSessionThresholdEvents        int
	configProcessSessions               bool
	writeKeyDestinationMap              map[string][]backendconfig.DestinationT
	writeKeySourceMap                   map[string]backendconfig.SourceT
	destinationIDtoTypeMap              map[string]string
	destinationTransformationEnabledMap map[string]bool
	rawDataDestinations                 []string
	configSubscriberLock                sync.RWMutex
	customDestinations                  []string
	pkgLogger                           logger.LoggerI
	enableEventSchemasFeature           bool
	enableDedup                         bool
	transformTimesPQLength              int
	captureEventNameStats               bool
	transformerURL                      string
	pollInterval                        time.Duration
	isUnLocked                          bool
)

func loadConfig() {
	config.RegisterDurationConfigVariable(time.Duration(5000), &maxLoopSleep, true, time.Millisecond, "Processor.maxLoopSleepInMS")
	config.RegisterDurationConfigVariable(time.Duration(10), &loopSleep, true, time.Millisecond, "Processor.loopSleepInMS")
	config.RegisterDurationConfigVariable(time.Duration(0), &fixedLoopSleep, true, time.Millisecond, "Processor.fixedLoopSleepInMS")
	config.RegisterIntConfigVariable(100, &transformBatchSize, true, 1, "Processor.transformBatchSize")
	config.RegisterIntConfigVariable(200, &userTransformBatchSize, true, 1, "Processor.userTransformBatchSize")
	configSessionThresholdEvents = config.GetInt("Processor.sessionThresholdEvents", 100)
	sessionInactivityThreshold = config.GetDuration("Processor.sessionInactivityThresholdInS", time.Duration(120)) * time.Second
	configProcessSessions = config.GetBool("Processor.processSessions", false)
	// Enable dedup of incoming events by default
	enableDedup = config.GetBool("Dedup.enableDedup", true)
	rawDataDestinations = []string{"S3", "GCS", "MINIO", "RS", "BQ", "AZURE_BLOB", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE", "DIGITAL_OCEAN_SPACES", "MSSQL"}
	customDestinations = []string{"KAFKA", "KINESIS", "AZURE_EVENT_HUB", "CONFLUENT_CLOUD"}
	// EventSchemas feature. false by default
	enableEventSchemasFeature = config.GetBool("EventSchemas.enableEventSchemasFeature", false)
	config.RegisterIntConfigVariable(10000, &maxEventsToProcess, true, 1, "Processor.maxLoopProcessEvents")
	config.RegisterIntConfigVariable(1, &avgEventsInRequest, true, 1, "Processor.avgEventsInRequest")
	// assuming every job in gw_jobs has atleast one event, max value for dbReadBatchSize can be maxEventsToProcess
	dbReadBatchSize = int(math.Ceil(float64(maxEventsToProcess) / float64(avgEventsInRequest)))
	transformTimesPQLength = config.GetInt("Processor.transformTimesPQLength", 5)
	// Capture event name as a tag in event level stats
	config.RegisterBoolConfigVariable(false, &captureEventNameStats, true, "Processor.Stats.captureEventName")
	transformerURL = config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090")
	pollInterval = config.GetDuration("Processor.pollIntervalInS", time.Duration(5)) * time.Second

}

func (proc *HandleT) getTransformerFeatureJson() {
	const attempts = 10
	for {
		for i := 0; i < attempts; i++ {
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
				body, err := ioutil.ReadAll(res.Body)
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

func (proc *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	proc.backendConfig.Subscribe(ch, backendconfig.TopicProcessConfig)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		writeKeyDestinationMap = make(map[string][]backendconfig.DestinationT)
		writeKeySourceMap = map[string]backendconfig.SourceT{}
		destinationIDtoTypeMap = make(map[string]string)
		destinationTransformationEnabledMap = make(map[string]bool)
		sources := config.Data.(backendconfig.ConfigT)
		for _, source := range sources.Sources {
			writeKeySourceMap[source.WriteKey] = source
			if source.Enabled {
				writeKeyDestinationMap[source.WriteKey] = source.Destinations
				for _, destination := range source.Destinations {
					destinationIDtoTypeMap[destination.ID] = destination.DestinationDefinition.Name
					destinationTransformationEnabledMap[destination.ID] = len(destination.Transformations) > 0
					_, ok := proc.destStats[destination.ID]
					if !ok {
						proc.destStats[destination.ID] = proc.newDestinationStat(destination)
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

func (proc *HandleT) addJobsToSessions(jobList []*jobsdb.JobT) {

	proc.logger.Debug("[Processor: addJobsToSessions] adding jobs to session")
	proc.userPQLock.Lock()

	//List of users whose jobs need to be processed
	processUserIDs := make(map[string]bool)

	for _, job := range jobList {
		//Append to job to list. If over threshold, just process them
		eventList, ok := misc.ParseRudderEventBatch(job.EventPayload)
		if !ok {
			//bad event
			proc.logger.Debug("[Processor: addJobsToSessions] bad event")
			continue
		}
		userID, ok := misc.GetRudderID(eventList[0])
		if !ok {
			proc.logger.Error("[Processor: addJobsToSessions] Failed to get userID for job")
			continue
		}

		//Prefixing write key to userID. This is done to create session per user per source
		userID = gjson.GetBytes(job.EventPayload, "writeKey").Str + ":" + userID

		_, ok = proc.userJobListMap[userID]
		if !ok {
			proc.userJobListMap[userID] = make([]*jobsdb.JobT, 0)
			proc.userEventsMap[userID] = make([]types.SingularEventT, 0)
		}
		// Adding a new session id for the user, if not present
		proc.logger.Debug("[Processor: addJobsToSessions] Adding a new session id for the user")
		_, ok = proc.userToSessionIDMap[userID]
		if !ok {
			proc.userToSessionIDMap[userID] = fmt.Sprintf("%s:%s", userID, strconv.FormatInt(time.Now().UnixNano()/1000000, 10))
		}
		//Add the job to the userID specific lists
		proc.userJobListMap[userID] = append(proc.userJobListMap[userID], job)
		proc.userEventsMap[userID] = append(proc.userEventsMap[userID], eventList...)
		//If we have enough events from that user, we process jobs
		if len(proc.userEventsMap[userID]) > proc.sessionThresholdEvents {
			processUserIDs[userID] = true
		}

		//Setting/updating pqItem lastTS with event received timestamp
		receivedAtResult := gjson.Get(string(job.EventPayload), "receivedAt")
		timestamp := time.Now()
		if receivedAtResult.Type != gjson.Null {
			timestamp = receivedAtResult.Time()
		}
		pqItem, ok := proc.userPQItemMap[userID]
		if !ok {
			pqItem := &pqItemT{
				userID: userID,
				lastTS: timestamp,
				index:  -1,
			}
			proc.userPQItemMap[userID] = pqItem
			proc.userJobPQ.Add(pqItem)
		} else {
			if pqItem.index == -1 {
				panic(fmt.Errorf("pqItem.index is -1"))
			}
			proc.userJobPQ.Update(pqItem, timestamp)
		}

	}

	if len(processUserIDs) > 0 {
		userJobsToProcess := make(map[string][]*jobsdb.JobT)
		userEventsToProcess := make(map[string][]types.SingularEventT)
		userToSessionMap := make(map[string]string)

		proc.logger.Debug("Post Add Processing")
		proc.Print()

		//We clear the data structure for these users
		for userID := range processUserIDs {
			userJobsToProcess[userID] = proc.userJobListMap[userID]
			userEventsToProcess[userID] = proc.userEventsMap[userID]
			userToSessionMap[userID] = proc.userToSessionIDMap[userID]
			delete(proc.userJobListMap, userID)
			delete(proc.userEventsMap, userID)
			delete(proc.userToSessionIDMap, userID)
		}
		proc.logger.Debug("Processing")
		proc.Print()
		//We release the lock before actually processing
		proc.userPQLock.Unlock()
		proc.processUserJobs(userJobsToProcess, userEventsToProcess, userToSessionMap)
		return
	}
	proc.userPQLock.Unlock()
}

func (proc *HandleT) processUserJobs(userJobs map[string][]*jobsdb.JobT, userEvents map[string][]types.SingularEventT, userToSessionMap map[string]string) {
	proc.logger.Debug("[Processor: processUserJobs] in processUserJobs")

	totalJobs := 0
	allJobIDs := make(map[int64]bool)
	for userID := range userJobs {
		for _, job := range userJobs[userID] {
			totalJobs++
			allJobIDs[job.JobID] = true
		}
	}

	//Create a list of list of user events which is passed to transformer
	userEventsList := make([][]types.SingularEventT, 0)
	userIDList := make([]string, 0) //Order of users which are added to list
	userEventsMap := make(map[string][]types.SingularEventT)
	for userID := range userEvents {
		// add the session_id field to each event before sending it downstream
		userEventsMap[userID] = make([]types.SingularEventT, 0)
		for _, event := range userEvents[userID] {
			event["session_id"] = userToSessionMap[userID]
			userEventsMap[userID] = append(userEventsMap[userID], event)
		}

		userEventsList = append(userEventsList, userEventsMap[userID])
		userIDList = append(userIDList, userID)
	}

	if len(userEventsList) != len(userEventsMap) {
		panic(fmt.Errorf("len(userEventsList):%d != len(userEventsMap):%d", len(userEventsList), len(userEventsMap)))
	}

	//Create jobs that can be processed further
	toProcessJobs, toProcessEvents := createUserTransformedJobsFromEvents(userEventsList, userIDList, userJobs)
	//Some sanity check to make sure we have all the jobs
	if len(toProcessJobs) != totalJobs {
		panic(fmt.Errorf("len(toProcessJobs):%d != totalJobs:%d", len(toProcessJobs), totalJobs))
	}
	if len(toProcessEvents) != totalJobs {
		panic(fmt.Errorf("len(toProcessEvents):%d != totalJobs:%d", len(toProcessEvents), totalJobs))
	}
	for _, job := range toProcessJobs {
		_, ok := allJobIDs[job.JobID]
		if !ok {
			panic(fmt.Errorf("key %d not found in map allJobIDs", job.JobID))
		}
		delete(allJobIDs, job.JobID)
	}
	if len(allJobIDs) != 0 {
		panic(fmt.Errorf("len(allJobIDs):%d != 0", len(allJobIDs)))
	}

	//Process
	proc.processJobsForDest(toProcessJobs, toProcessEvents)
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

func (proc *HandleT) createSessions() {
	proc.logger.Debug("[Processor: createSessions] starting sessions")
	for {
		proc.userPQLock.Lock()
		//Now jobs
		if proc.userJobPQ.Len() == 0 {
			proc.userPQLock.Unlock()
			time.Sleep(loopSleep)
			continue
		}

		proc.statActiveUsers.Gauge(len(proc.userJobListMap))
		//Enough time hasn't transpired since last
		oldestItem := proc.userJobPQ.Top()
		if time.Since(oldestItem.lastTS) < time.Duration(sessionInactivityThreshold) {
			proc.userPQLock.Unlock()
			sleepTime := time.Duration(sessionInactivityThreshold) - time.Since(oldestItem.lastTS)
			proc.logger.Debug("Sleeping", sleepTime)
			time.Sleep(sleepTime)
			continue
		}

		userJobsToProcess := make(map[string][]*jobsdb.JobT)
		userEventsToProcess := make(map[string][]types.SingularEventT)
		userToSessionMap := make(map[string]string)
		//Find all jobs that need to be processed
		for {
			if proc.userJobPQ.Len() == 0 {
				break
			}
			oldestItem := proc.userJobPQ.Top()
			if time.Since(oldestItem.lastTS) > time.Duration(sessionInactivityThreshold) {
				userID := oldestItem.userID
				pqItem, ok := proc.userPQItemMap[userID]
				if !(ok && pqItem == oldestItem) {
					panic(fmt.Errorf("userID is not found in userPQItemMap or pqItem is not oldestItem"))
				}
				userJobsToProcess[userID] = proc.userJobListMap[userID]
				userEventsToProcess[userID] = proc.userEventsMap[userID]
				// it is guaranteed that user will have a session even if one job is present
				// Refer addJobsToSession
				userToSessionMap[userID] = proc.userToSessionIDMap[userID]
				//Clear from the map
				delete(proc.userJobListMap, userID)
				delete(proc.userEventsMap, userID)
				proc.userJobPQ.Remove(proc.userPQItemMap[userID])
				delete(proc.userPQItemMap, userID)
				// A session ends when a user is inactive for a period of sessionInactivityThreshold
				// or session limit on number of jobs has been achievd
				// Refer addJobsToSession
				delete(proc.userToSessionIDMap, userID)
				continue
			}
			break
		}
		proc.Print()
		proc.userPQLock.Unlock()
		if len(userJobsToProcess) > 0 {
			proc.logger.Debug("Processing Session Check")
			proc.Print()
			proc.processUserJobs(userJobsToProcess, userEventsToProcess, userToSessionMap)
		}
	}
}

func getSourceByWriteKey(writeKey string) backendconfig.SourceT {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	source, ok := writeKeySourceMap[writeKey]
	if !ok {
		panic(fmt.Errorf(`source not found for writeKey: %s`, writeKey))
	}
	return source
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
		panic(fmt.Errorf("[Processor] couldn't marshal singularEvent. singularEvent: %v\n", singularEvent))
	}
	commonMetadata.SourceID = gjson.GetBytes(batchEvent.Parameters, "source_id").Str
	commonMetadata.RudderID = batchEvent.UserID
	commonMetadata.JobID = batchEvent.JobID
	commonMetadata.MessageID = singularEvent["messageId"].(string)
	commonMetadata.ReceivedAt = receivedAt.Format(misc.RFC3339Milli)
	commonMetadata.SourceBatchID = gjson.GetBytes(eventBytes, "context.sources.batch_id").String()
	commonMetadata.SourceTaskID = gjson.GetBytes(eventBytes, "context.sources.task_id").String()
	commonMetadata.SourceTaskRunID = gjson.GetBytes(eventBytes, "context.sources.task_run_id").String()
	commonMetadata.SourceJobID = gjson.GetBytes(eventBytes, "context.sources.job_id").String()
	commonMetadata.SourceJobRunID = gjson.GetBytes(eventBytes, "context.sources.job_run_id").String()
	commonMetadata.SourceType = source.SourceDefinition.Name
	commonMetadata.SourceCategory = source.SourceDefinition.Category

	return &commonMetadata
}

// add metadata to each singularEvent which will be returned by transformer in response
func enhanceWithMetadata(commonMetadata *transformer.MetadataT, event *transformer.TransformerEventT, destination backendconfig.DestinationT) {
	metadata := transformer.MetadataT{}
	metadata.SourceType = commonMetadata.SourceType
	metadata.SourceCategory = commonMetadata.SourceCategory
	metadata.SourceID = commonMetadata.SourceID
	metadata.RudderID = commonMetadata.RudderID
	metadata.JobID = commonMetadata.JobID
	metadata.MessageID = commonMetadata.MessageID
	metadata.ReceivedAt = commonMetadata.ReceivedAt
	metadata.SourceBatchID = commonMetadata.SourceBatchID
	metadata.SourceTaskID = commonMetadata.SourceTaskID
	metadata.SourceTaskRunID = commonMetadata.SourceTaskRunID
	metadata.SourceJobID = commonMetadata.SourceJobID
	metadata.SourceJobRunID = commonMetadata.SourceJobRunID

	metadata.DestinationID = destination.ID
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
				panic(err)
			}

			sourceID, _ := params["source_id"].(string)
			destID, _ := params["destination_id"].(string)
			procErr, _ := params["error"].(string)
			procErr = strconv.Quote(procErr)
			statusCode, _ := params["status_code"].(string)

			deliveryStatus := destinationdebugger.DeliveryStatusT{
				DestinationID: destID,
				SourceID:      sourceID,
				Payload:       job.EventPayload,
				AttemptNum:    1,
				JobState:      jobsdb.Aborted.State,
				ErrorCode:     statusCode,
				ErrorResponse: []byte(fmt.Sprintf(`{"error": %s}`, procErr)),
			}
			destinationdebugger.RecordEventDeliveryStatus(destID, &deliveryStatus)
		}
	}
}

func (proc *HandleT) getDestTransformerEvents(response transformer.ResponseT, commonMetaData transformer.MetadataT, destination backendconfig.DestinationT) ([]transformer.TransformerEventT, []*types.PUReportedMetric, map[string]int64, map[string]MetricMetadata) {
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
		updatedEvent := transformer.TransformerEventT{
			Message:     userTransformedEvent.Output,
			Metadata:    eventMetadata,
			Destination: destination,
		}
		eventsToTransform = append(eventsToTransform, updatedEvent)
	}

	//REPORTING - START
	if proc.reporting != nil && proc.reportingEnabled {
		types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)

		for k, cd := range connectionDetailsMap {
			m := &types.PUReportedMetric{
				ConnectionDetails: *cd,
				PUDetails:         *types.CreatePUDetails(types.GATEWAY, types.USER_TRANSFORMER, false, false),
				StatusDetail:      statusDetailsMap[k],
			}
			successMetrics = append(successMetrics, m)
		}
	}
	//REPORTING - END

	return eventsToTransform, successMetrics, successCountMap, successCountMetadataMap
}

func (proc *HandleT) updateMetricMaps(countMetadataMap map[string]MetricMetadata, countMap map[string]int64, connectionDetailsMap map[string]*types.ConnectionDetails, statusDetailsMap map[string]*types.StatusDetail, event transformer.TransformerResponseT, status string, payload json.RawMessage) {
	if proc.reporting != nil && proc.reportingEnabled {
		countKey := fmt.Sprintf("%s:%s:%s", event.Metadata.SourceID, event.Metadata.DestinationID, event.Metadata.SourceBatchID)
		if _, ok := countMap[countKey]; !ok {
			countMap[countKey] = 0
		}
		countMap[countKey] = countMap[countKey] + 1
		if countMetadataMap != nil {
			if _, ok := countMetadataMap[countKey]; !ok {
				countMetadataMap[countKey] = MetricMetadata{sourceID: event.Metadata.SourceID, destinationID: event.Metadata.DestinationID, sourceBatchID: event.Metadata.SourceBatchID, sourceTaskID: event.Metadata.SourceTaskID, sourceTaskRunID: event.Metadata.SourceTaskRunID, sourceJobID: event.Metadata.SourceJobID, sourceJobRunID: event.Metadata.SourceJobRunID}
			}
		}

		key := fmt.Sprintf("%s:%s:%s:%s:%d", event.Metadata.SourceID, event.Metadata.DestinationID, event.Metadata.SourceBatchID, status, event.StatusCode)
		cd, ok := connectionDetailsMap[key]
		if !ok {
			cd = types.CreateConnectionDetail(event.Metadata.SourceID, event.Metadata.DestinationID, event.Metadata.SourceBatchID, event.Metadata.SourceTaskID, event.Metadata.SourceTaskRunID, event.Metadata.SourceJobID, event.Metadata.SourceJobRunID)
			connectionDetailsMap[key] = cd
		}
		sd, ok := statusDetailsMap[key]
		if !ok {
			sd = types.CreateStatusDetail(status, 0, event.StatusCode, event.Error, payload)
			statusDetailsMap[key] = sd
		}
		sd.Count++
	}
}

func (proc *HandleT) getFailedEventJobs(response transformer.ResponseT, commonMetaData transformer.MetadataT, eventsByMessageID map[string]types.SingularEventWithReceivedAt, stage string, transformationEnabled bool) ([]*jobsdb.JobT, []*types.PUReportedMetric, map[string]int64) {
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
	if proc.reporting != nil && proc.reportingEnabled {
		types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)

		var inPU, pu string
		if stage == transformer.DestTransformerStage {
			if transformationEnabled {
				inPU = types.USER_TRANSFORMER
			} else {
				inPU = types.GATEWAY
			}
			pu = types.DEST_TRANSFORMER
		} else if stage == transformer.UserTransformerStage {
			inPU = types.GATEWAY
			pu = types.USER_TRANSFORMER
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
				ConnectionDetails: *types.CreateConnectionDetail(metricMetadata.sourceID, metricMetadata.destinationID, metricMetadata.sourceBatchID, metricMetadata.sourceTaskID, metricMetadata.sourceTaskRunID, metricMetadata.sourceJobID, metricMetadata.sourceJobRunID),
				PUDetails:         *types.CreatePUDetails(inPU, pu, false, false),
				StatusDetail:      types.CreateStatusDetail(types.DiffStatus, diff, 0, "", []byte(`{}`)),
			}
			diffMetrics = append(diffMetrics, metric)
		}
	}

	return diffMetrics
}

func (proc *HandleT) processJobsForDest(jobList []*jobsdb.JobT, parsedEventList [][]types.SingularEventT) {

	proc.pStatsJobs.Start()

	proc.statNumRequests.Count(len(jobList))

	var destJobs []*jobsdb.JobT
	var batchDestJobs []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	var groupedEvents = make(map[string][]transformer.TransformerEventT)
	var eventsByMessageID = make(map[string]types.SingularEventWithReceivedAt)
	var procErrorJobsByDestID = make(map[string][]*jobsdb.JobT)

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
					allMessageIdsInBatch = append(allMessageIdsInBatch, singularEvent["messageId"].(string))
				}
				duplicateIndexes = proc.dedupHandler.FindDuplicates(allMessageIdsInBatch, uniqueMessageIds)
			}

			//Iterate through all the events in the batch
			for eventIndex, singularEvent := range singularEvents {
				messageId := singularEvent["messageId"].(string)
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
				workspaceID := proc.backendConfig.GetWorkspaceIDForWriteKey(writeKey)
				workspaceLibraries := proc.backendConfig.GetWorkspaceLibrariesForWorkspaceID(workspaceID)
				sourceForSingularEvent := getSourceByWriteKey(writeKey)

				// proc.logger.Debug("=== enabledDestTypes ===", enabledDestTypes)
				if len(enabledDestTypes) == 0 {
					proc.logger.Debug("No enabled destinations")
					continue
				}

				commonMetadataFromSingularEvent := makeCommonMetadataFromSingularEvent(singularEvent, batchEvent, receivedAt, sourceForSingularEvent)
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
						//TODO: Test for multiple workspaces ex: hosted data plane
						/* Stream destinations does not need config in transformer. As the Kafka destination config
						holds the ca-certificate and it depends on user input, it may happen that they provide entire
						certificate chain. So, that will make the payload huge while sending a batch of events to transformer,
						it may result into payload larger than accepted by transformer. So, discarding destination config from being
						sent to transformer for such destination. */
						if misc.ContainsString(customDestinations, destType) {
							shallowEventCopy.Destination.Config = nil
						}

						shallowEventCopy.Message["request_ip"] = requestIP

						enhanceWithTimeFields(&shallowEventCopy, singularEvent, receivedAt)
						enhanceWithMetadata(commonMetadataFromSingularEvent, &shallowEventCopy, destination)

						metadata := shallowEventCopy.Metadata
						srcAndDestKey := getKeyFromSourceAndDest(metadata.SourceID, metadata.DestinationID)
						//We have at-least one event so marking it good
						_, ok = groupedEvents[srcAndDestKey]
						if !ok {
							groupedEvents[srcAndDestKey] = make([]transformer.TransformerEventT, 0)
						}
						groupedEvents[srcAndDestKey] = append(groupedEvents[srcAndDestKey],
							shallowEventCopy)
					}
				}

				//REPORTING - GATEWAY metrics - START
				if proc.reporting != nil && proc.reportingEnabled {
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
						cd = types.CreateConnectionDetail(commonMetadataFromSingularEvent.SourceID, "", commonMetadataFromSingularEvent.SourceBatchID, commonMetadataFromSingularEvent.SourceTaskID, commonMetadataFromSingularEvent.SourceTaskRunID, commonMetadataFromSingularEvent.SourceJobID, commonMetadataFromSingularEvent.SourceJobRunID)
						connectionDetailsMap[key] = cd
					}
					sd, ok := statusDetailsMap[key]
					if !ok {
						sd = types.CreateStatusDetail(jobsdb.Succeeded.State, 0, 200, "", []byte(`{}`))
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
		}
		statusList = append(statusList, &newStatus)
	}

	//REPORTING - GATEWAY metrics - START
	if proc.reporting != nil && proc.reportingEnabled {
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

	//Now do the actual transformation. We call it in batches, once
	//for each destination ID

	proc.destProcessing.Start()
	proc.logger.Debug("[Processor: processJobsForDest] calling transformations")
	var startedAt, endedAt time.Time
	var timeTaken float64
	for srcAndDestKey, eventList := range groupedEvents {
		sourceID, destID := getSourceAndDestIDsFromKey(srcAndDestKey)
		destination := eventList[0].Destination
		//TODO: should trackingplanID be set here? what if tp is changed after events are sent? replay case?
		commonMetaData := transformer.MetadataT{
			SourceID:        sourceID,
			SourceType:      eventList[0].Metadata.SourceType,
			SourceCategory:  eventList[0].Metadata.SourceCategory,
			DestinationID:   destID,
			DestinationType: destination.DestinationDefinition.Name,
		}
		configT, ok := proc.backendConfig.Get()
		if(!ok) {
			pkgLogger.Errorf("unable to get backend config")
		}
		sourceTpId := ""
		for _, source := range configT.Sources {
			if (source.ID == sourceID && !source.TrackingPlanConnection.Deleted) {
				sourceTpId = source.TrackingPlanConnection.TrackingPlanId
			}
		}
		var response transformer.ResponseT
		//Doing it here before passed to user/dest transformation
		if (sourceTpId != "") {
			startedAt = time.Now()
			response = proc.transformer.Transform(eventList, integrations.GetTrackingPlanValidationURL(), userTransformBatchSize, false)
			endedAt = time.Now()
			timeTaken = endedAt.Sub(startedAt).Seconds()
			fmt.Println(response)
		}
		failedJobs, failedMetrics, failedCountMap := proc.getFailedEventJobs(response, commonMetaData, eventsByMessageID, transformer.DestTransformerStage, false)
		if _, ok := procErrorJobsByDestID[destID]; !ok {
			procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
		}
		procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], failedJobs...)

		var procErrorJobs []*jobsdb.JobT
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
		err := proc.gatewayDB.UpdateJobStatusInTxn(txn, statusList, []string{gateway.CustomVal}, nil)
		if err != nil {
			pkgLogger.Errorf("Error occurred while updating gateway jobs statuses. Panicking. Err: %v", err)
			panic(err)
		}
		proc.gatewayDB.CommitTransaction(txn)
		proc.gatewayDB.ReleaseUpdateJobStatusLocks()
		var jobIds []int64
		for _, job := range failedJobs {
			jobIds = append(jobIds, job.JobID)
		}
		var newEventList []transformer.TransformerEventT
		for _, event := range eventList{
			if !misc.Contains(jobIds, event.Metadata.JobID) {
				newEventList = append(newEventList, event)
			}
		}

		eventList = newEventList


		destStat := proc.destStats[destID]
		destStat.numEvents.Count(len(eventList))

		configSubscriberLock.RLock()
		destType := destinationIDtoTypeMap[destID]
		transformationEnabled := destinationTransformationEnabledMap[destID]
		configSubscriberLock.RUnlock()

		//REPORTING - START
		if proc.reporting != nil && proc.reportingEnabled {
			//Grouping events by sourceid + destinationid + source batch id to find the count
			inCountMap = make(map[string]int64)
			inCountMetadataMap = make(map[string]MetricMetadata)
			for _, event := range eventList {
				key := fmt.Sprintf("%s:%s:%s", event.Metadata.SourceID, event.Metadata.DestinationID, event.Metadata.SourceBatchID)
				if _, ok := inCountMap[key]; !ok {
					inCountMap[key] = 0
				}
				if _, ok := inCountMetadataMap[key]; !ok {
					inCountMetadataMap[key] = MetricMetadata{sourceID: event.Metadata.SourceID, destinationID: event.Metadata.DestinationID, sourceBatchID: event.Metadata.SourceBatchID, sourceTaskID: event.Metadata.SourceTaskID, sourceTaskRunID: event.Metadata.SourceTaskRunID, sourceJobID: event.Metadata.SourceJobID, sourceJobRunID: event.Metadata.SourceJobRunID}
				}
				inCountMap[key] = inCountMap[key] + 1
			}
		}
		//REPORTING - END

		url := integrations.GetDestinationURL(destType)
		var eventsToTransform []transformer.TransformerEventT
		// Send to custom transformer only if the destination has a transformer enabled
		if transformationEnabled {
			proc.logger.Debug("Custom Transform input size", len(eventList))
			if proc.processSessions {
				// If processSessions is true, Transform should break into a new batch only when user changes.
				// This way all the events of a user session are never broken into separate batches
				// Note: Assumption is events from a user's session are together in destEventList, which is guaranteed by the way destEventList is created
				destStat.sessionTransform.Start()
				response = proc.transformer.Transform(eventList, integrations.GetUserTransformURL(proc.processSessions), userTransformBatchSize, true)
				destStat.sessionTransform.End()
			} else {
				// We need not worry about breaking up a single user sessions in this case
				destStat.userTransform.Start()
				startedAt = time.Now()
				response = proc.transformer.Transform(eventList, integrations.GetUserTransformURL(proc.processSessions), userTransformBatchSize, false)
				endedAt = time.Now()
				timeTaken = endedAt.Sub(startedAt).Seconds()
				destStat.userTransform.End()
				proc.addToTransformEventByTimePQ(&TransformRequestT{Event: eventList, Stage: transformer.UserTransformerStage, ProcessingTime: timeTaken, Index: -1}, &proc.userTransformEventsByTimeTaken)
			}

			var successMetrics []*types.PUReportedMetric
			var successCountMap map[string]int64
			var successCountMetadataMap map[string]MetricMetadata
			eventsToTransform, successMetrics, successCountMap, successCountMetadataMap = proc.getDestTransformerEvents(response, commonMetaData, destination)
			failedJobs, failedMetrics, failedCountMap := proc.getFailedEventJobs(response, commonMetaData, eventsByMessageID, transformer.UserTransformerStage, transformationEnabled)
			if _, ok := procErrorJobsByDestID[destID]; !ok {
				procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
			}
			procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], failedJobs...)
			proc.logger.Debug("Custom Transform output size", len(eventsToTransform))

			transformationdebugger.UploadTransformationStatus(&transformationdebugger.TransformationStatusT{SourceID: sourceID, DestID: destID, Destination: &destination, UserTransformedEvents: eventsToTransform, EventsByMessageID: eventsByMessageID, FailedEvents: response.FailedEvents, UniqueMessageIds: uniqueMessageIds})

			//REPORTING - START
			if proc.reporting != nil && proc.reportingEnabled {
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
		destStat.destTransform.Start()
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

		//If transformAt is none
		// OR
		//router and transformer supports router transform, then no destination transformation happens.
		if transformAt == "none" || (transformAt == "router" && transformAtFromFeaturesFile != "") {
			response = convertToTransformerResponse(eventsToTransform)
		} else {
			response = proc.transformer.Transform(eventsToTransform, url, transformBatchSize, false)
			transformAt = "processor"
		}

		endedAt = time.Now()
		timeTaken = endedAt.Sub(startedAt).Seconds()
		destStat.destTransform.End()
		proc.addToTransformEventByTimePQ(&TransformRequestT{Event: eventsToTransform, Stage: "destination-transformer", ProcessingTime: timeTaken, Index: -1}, &proc.destTransformEventsByTimeTaken)

		destTransformEventList := response.Events
		proc.logger.Debug("Dest Transform output size", len(destTransformEventList))
		destStat.numOutputEvents.Count(len(destTransformEventList))

		failedJobs, failedMetrics, failedCountMap = proc.getFailedEventJobs(response, commonMetaData, eventsByMessageID, transformer.DestTransformerStage, transformationEnabled)
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
			//If the response from the transformer does not have userID in metadata, setting userID to random-uuid.
			//This is done to respect findWorker logic in router.
			if rudderID == "" {
				rudderID = "random-" + id.String()
			}

			params := ParametersT{
				SourceID:        sourceID,
				DestinationID:   destID,
				ReceivedAt:      receivedAt,
				TransformAt:     transformAt,
				MessageID:       messageId,
				GatewayJobID:    jobId,
				SourceBatchID:   sourceBatchId,
				SourceTaskID:    sourceTaskId,
				SourceTaskRunID: sourceTaskRunId,
				SourceJobID:     sourceJobId,
				SourceJobRunID:  sourceJobRunId,
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
			if misc.Contains(rawDataDestinations, newJob.CustomVal) {
				batchDestJobs = append(batchDestJobs, &newJob)
			} else {
				destJobs = append(destJobs, &newJob)
			}
		}

		//REPORTING - PROCESSOR metrics - START
		if proc.reporting != nil && proc.reportingEnabled {
			types.AssertSameKeys(connectionDetailsMap, statusDetailsMap)

			inPU := types.GATEWAY
			if transformationEnabled {
				inPU = types.USER_TRANSFORMER
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

	var procErrorJobs []*jobsdb.JobT
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
	err := proc.gatewayDB.UpdateJobStatusInTxn(txn, statusList, []string{gateway.CustomVal}, nil)
	if err != nil {
		pkgLogger.Errorf("Error occurred while updating gateway jobs statuses. Panicking. Err: %v", err)
		panic(err)
	}
	if proc.reporting != nil && proc.reportingEnabled {
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
}

func convertToTransformerResponse(events []transformer.TransformerEventT) transformer.ResponseT {
	var responses []transformer.TransformerResponseT
	for _, event := range events {
		resp := transformer.TransformerResponseT{Output: event.Message, StatusCode: 200, Metadata: event.Metadata}
		responses = append(responses, resp)
	}

	return transformer.ResponseT{Events: responses}
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

	unTruncatedRetryList := proc.gatewayDB.GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{gateway.CustomVal}, Count: toQuery})
	retryList, totalRetryEvents = getTruncatedEventList(unTruncatedRetryList, maxEventsToProcess)

	if len(unTruncatedRetryList) >= dbReadBatchSize || totalRetryEvents >= maxEventsToProcess {
		// skip querying for unprocessed jobs if either retreived dbReadBatchSize or retreived maxEventToProcess
	} else {
		eventsLeftToProcess := maxEventsToProcess - totalRetryEvents
		toQuery = misc.MinInt(eventsLeftToProcess, dbReadBatchSize)
		unTruncatedUnProcessedList := proc.gatewayDB.GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{gateway.CustomVal}, Count: toQuery})
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
	if enableEventSchemasFeature {
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

	if proc.processSessions {
		//Mark all as executing so next query doesn't pick it up
		var statusList []*jobsdb.JobStatusT
		for _, batchEvent := range combinedList {
			newStatus := jobsdb.JobStatusT{
				JobID:         batchEvent.JobID,
				JobState:      jobsdb.Executing.State,
				AttemptNum:    1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: []byte(`{"success":"OK"}`),
			}
			statusList = append(statusList, &newStatus)
		}
		err := proc.gatewayDB.UpdateJobStatus(statusList, []string{gateway.CustomVal}, nil)
		if err != nil {
			pkgLogger.Errorf("Error occurred while marking gateway jobs statuses as executing. Panicking. Err: %v", err)
			panic(err)
		}

		proc.addJobsToSessions(combinedList)
	} else {
		proc.processJobsForDest(combinedList, nil)
	}
	proc.statLoopTime.End()

	return true
}

func (proc *HandleT) mainLoop() {
	//waiting till the backend config is received
	proc.backendConfig.WaitForConfig()
	//waiting for reporting client setup
	if proc.reporting != nil {
		proc.reporting.WaitForSetup(types.CORE_REPORTING_CLIENT)
	}

	proc.logger.Info("Processor loop started")
	currLoopSleep := time.Duration(0)

	for {
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

func (proc *HandleT) crashRecover() {
	for {
		execList := proc.gatewayDB.GetExecuting(jobsdb.GetQueryParamsT{CustomValFilters: []string{gateway.CustomVal}, Count: dbReadBatchSize})

		if len(execList) == 0 {
			break
		}
		proc.logger.Debug("Processor crash recovering", len(execList))

		var statusList []*jobsdb.JobStatusT

		for _, job := range execList {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				JobState:      jobsdb.Failed.State,
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`), // check
			}
			statusList = append(statusList, &status)
		}
		err := proc.gatewayDB.UpdateJobStatus(statusList, []string{gateway.CustomVal}, nil)
		if err != nil {
			pkgLogger.Errorf("Error occurred while marking gateway jobs statuses as failed. Panicking. Err: %v", err)
			panic(err)
		}
	}
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
