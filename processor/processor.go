package processor

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
)

//HandleT is an handle to this object used in main.go
type HandleT struct {
	gatewayDB          *jobsdb.HandleT
	routerDB           *jobsdb.HandleT
	batchRouterDB      *jobsdb.HandleT
	transformer        *transformerHandleT
	statsJobs          *misc.PerfStats
	statsDBR           *misc.PerfStats
	statGatewayDBR     *stats.RudderStats
	statsDBW           *misc.PerfStats
	statGatewayDBW     *stats.RudderStats
	statRouterDBW      *stats.RudderStats
	statBatchRouterDBW *stats.RudderStats
	statActiveUsers    *stats.RudderStats
	userJobListMap     map[string][]*jobsdb.JobT
	userEventsMap      map[string][]interface{}
	userPQItemMap      map[string]*pqItemT
	userJobPQ          pqT
	userPQLock         sync.Mutex
	replayProcessor    *ReplayProcessorT
}

//Print the internal structure
func (proc *HandleT) Print() {
	if !logger.IsDebugLevel() {
		return
	}
	logger.Debug("PriorityQueue")
	proc.userJobPQ.Print()
	logger.Debug("JobList")
	for k, v := range proc.userJobListMap {
		logger.Debug(k, ":", len(v))
	}
	logger.Debug("EventLength")
	for k, v := range proc.userEventsMap {
		logger.Debug(k, ":", len(v))
	}
	logger.Debug("PQItem")
	for k, v := range proc.userPQItemMap {
		logger.Debug(k, ":", *v)
	}
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes the module
func (proc *HandleT) Setup(gatewayDB *jobsdb.HandleT, routerDB *jobsdb.HandleT, batchRouterDB *jobsdb.HandleT) {
	proc.gatewayDB = gatewayDB
	proc.routerDB = routerDB
	proc.batchRouterDB = batchRouterDB
	proc.transformer = &transformerHandleT{}
	proc.statsJobs = &misc.PerfStats{}
	proc.statsDBR = &misc.PerfStats{}
	proc.statsDBW = &misc.PerfStats{}
	proc.userJobListMap = make(map[string][]*jobsdb.JobT)
	proc.userEventsMap = make(map[string][]interface{})
	proc.userPQItemMap = make(map[string]*pqItemT)
	proc.userJobPQ = make(pqT, 0)
	proc.statsJobs.Setup("ProcessorJobs")
	proc.statsDBR.Setup("ProcessorDBRead")
	proc.statsDBW.Setup("ProcessorDBWrite")

	proc.statGatewayDBR = stats.NewStat("processor.gateway_db_read", stats.CountType)
	proc.statGatewayDBW = stats.NewStat("processor.gateway_db_write", stats.CountType)
	proc.statRouterDBW = stats.NewStat("processor.router_db_write", stats.CountType)
	proc.statBatchRouterDBW = stats.NewStat("processor.batch_router_db_write", stats.CountType)
	proc.statActiveUsers = stats.NewStat("processor.active_users", stats.GaugeType)

	proc.replayProcessor = NewReplayProcessor()
	proc.replayProcessor.Setup()

	go proc.backendConfigSubscriber()
	proc.transformer.Setup()

	proc.replayProcessor.CrashRecover()
	proc.crashRecover()

	go proc.mainLoop()
	if processSessions {
		logger.Info("Starting session processor")
		go proc.createSessions()
	}
}

var (
	loopSleep                  time.Duration
	dbReadBatchSize            int
	transformBatchSize         int
	sessionThresholdInS        time.Duration
	sessionThresholdEvents     int
	processSessions            bool
	writeKeyDestinationMap     map[string][]backendconfig.DestinationT
	copyWriteKeyDestinationMap map[string][]backendconfig.DestinationT
	rawDataDestinations        []string
	configSubscriberLock       sync.RWMutex
	processReplays             []replayT
)

func loadConfig() {
	loopSleep = config.GetDuration("Processor.loopSleepInMS", time.Duration(10)) * time.Millisecond
	dbReadBatchSize = config.GetInt("Processor.dbReadBatchSize", 100000)
	transformBatchSize = config.GetInt("Processor.transformBatchSize", 50)
	sessionThresholdEvents = config.GetInt("Processor.sessionThresholdEvents", 20)
	sessionThresholdInS = config.GetDuration("Processor.sessionThresholdInS", time.Duration(20)) * time.Second
	processSessions = config.GetBool("Processor.processSessions", true)
	maxChanSize = config.GetInt("Processor.maxChanSize", 2048)
	numTransformWorker = config.GetInt("Processor.numTransformWorker", 32)
	maxRetry = config.GetInt("Processor.maxRetry", 3)
	retrySleep = config.GetDuration("Processor.retrySleepInMS", time.Duration(100)) * time.Millisecond
	rawDataDestinations = []string{"S3"}
	processReplays = []replayT{}
}

type replayT struct {
	sourceID      string
	destinationID string
	notifyURL     string
}

func (proc *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		writeKeyDestinationMap = make(map[string][]backendconfig.DestinationT)
		sources := config.Data.(backendconfig.SourcesT)
		for _, source := range sources.Sources {
			if source.Enabled {
				writeKeyDestinationMap[source.WriteKey] = source.Destinations
			}

			var replays = []replayT{}
			for _, dest := range source.Destinations {
				if dest.Config.(map[string]interface{})["Replay"] == true {
					notifyURL, ok := dest.Config.(map[string]interface{})["ReplayURL"].(string)
					if !ok {
						notifyURL = ""
					}
					replays = append(replays, replayT{sourceID: source.ID, destinationID: dest.ID, notifyURL: notifyURL})
				}
			}

			if len(replays) > 0 {
				processReplays = proc.replayProcessor.GetReplaysToProcess(replays)
			}
		}
		configSubscriberLock.Unlock()
	}
}

func (proc *HandleT) addJobsToSessions(jobList []*jobsdb.JobT) {

	proc.userPQLock.Lock()

	//List of users whose jobs need to be processed
	processUserIDs := make(map[string]bool)

	for _, job := range jobList {
		//Append to job to list. If over threshold, just process them
		eventList, ok := misc.ParseRudderEventBatch(job.EventPayload)
		if !ok {
			//bad event
			continue
		}
		userID, ok := misc.GetRudderEventUserID(eventList)
		if !ok {
			logger.Error("Failed to get userID for job")
			continue
		}
		_, ok = proc.userJobListMap[userID]
		if !ok {
			proc.userJobListMap[userID] = make([]*jobsdb.JobT, 0)
			proc.userEventsMap[userID] = make([]interface{}, 0)
		}
		//Add the job to the userID specific lists
		proc.userJobListMap[userID] = append(proc.userJobListMap[userID], job)
		proc.userEventsMap[userID] = append(proc.userEventsMap[userID], eventList...)
		//If we have enough events from that user, we process jobs
		if len(proc.userEventsMap[userID]) > sessionThresholdEvents {
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
			misc.Assert(pqItem.index != -1)
			proc.userJobPQ.Update(pqItem, timestamp)
		}

	}

	if len(processUserIDs) > 0 {
		userJobsToProcess := make(map[string][]*jobsdb.JobT)
		userEventsToProcess := make(map[string][]interface{})
		logger.Debug("Post Add Processing")
		proc.Print()

		//We clear the data structure for these users
		for userID := range processUserIDs {
			userJobsToProcess[userID] = proc.userJobListMap[userID]
			userEventsToProcess[userID] = proc.userEventsMap[userID]
			delete(proc.userJobListMap, userID)
			delete(proc.userEventsMap, userID)
			proc.userJobPQ.Remove(proc.userPQItemMap[userID])
			delete(proc.userPQItemMap, userID)
		}
		logger.Debug("Processing")
		proc.Print()
		//We release the block before actually processing
		proc.userPQLock.Unlock()
		proc.processUserJobs(userJobsToProcess, userEventsToProcess)
		return
	}
	proc.userPQLock.Unlock()
}

func (proc *HandleT) processUserJobs(userJobs map[string][]*jobsdb.JobT, userEvents map[string][]interface{}) {

	misc.Assert(len(userEvents) == len(userJobs))

	totalJobs := 0
	allJobIDs := make(map[int64]bool)
	for userID := range userJobs {
		for _, job := range userJobs[userID] {
			totalJobs++
			allJobIDs[job.JobID] = true
		}
	}

	//Create a list of list of user events which is passed to transformer
	userEventsList := make([]interface{}, 0)
	userIDList := make([]string, 0) //Order of users which are added to list
	for userID := range userEvents {
		userEventsList = append(userEventsList, userEvents[userID])
		userIDList = append(userIDList, userID)
	}
	misc.Assert(len(userEventsList) == len(userEvents))

	//Create jobs that can be processed further
	toProcessJobs, toProcessEvents := createUserTransformedJobsFromEvents(userEventsList, userIDList, userJobs)

	//Some sanity check to make sure we have all the jobs
	misc.Assert(len(toProcessJobs) == totalJobs)
	misc.Assert(len(toProcessEvents) == totalJobs)
	for _, job := range toProcessJobs {
		_, ok := allJobIDs[job.JobID]
		misc.Assert(ok)
		delete(allJobIDs, job.JobID)
	}
	misc.Assert(len(allJobIDs) == 0)

	//Process
	proc.processJobsForDest(toProcessJobs, toProcessEvents)
}

//We create sessions (of individul events) from set of input jobs  from a user
//Those sesssion events are transformed and we have a transformed set of
//events that must be processed further via destination specific transformations
//(in processJobsForDest). This function creates jobs from eventList
func createUserTransformedJobsFromEvents(transformUserEventList []interface{},
	userIDList []string, userJobs map[string][]*jobsdb.JobT) ([]*jobsdb.JobT, [][]interface{}) {

	transJobList := make([]*jobsdb.JobT, 0)
	transEventList := make([][]interface{}, 0)
	misc.Assert(len(transformUserEventList) == len(userIDList))
	for idx, userID := range userIDList {
		userEvents := transformUserEventList[idx]
		userEventsList, ok := userEvents.([]interface{})
		misc.Assert(ok)
		for idx, job := range userJobs[userID] {
			//We put all the transformed event on the first job
			//and empty out the remaining payloads
			transJobList = append(transJobList, job)
			if idx == 0 {
				transEventList = append(transEventList, userEventsList)
			} else {
				transEventList = append(transEventList, nil)
			}
		}
	}
	return transJobList, transEventList
}

func (proc *HandleT) createSessions() {

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
		if time.Since(oldestItem.lastTS) < time.Duration(sessionThresholdInS) {
			proc.userPQLock.Unlock()
			sleepTime := time.Duration(sessionThresholdInS) - time.Since(oldestItem.lastTS)
			logger.Debug("Sleeping", sleepTime)
			time.Sleep(sleepTime)
			continue
		}

		userJobsToProcess := make(map[string][]*jobsdb.JobT)
		userEventsToProcess := make(map[string][]interface{})
		//Find all jobs that need to be processed
		for {
			if proc.userJobPQ.Len() == 0 {
				break
			}
			oldestItem := proc.userJobPQ.Top()
			if time.Since(oldestItem.lastTS) > time.Duration(sessionThresholdInS) {
				userID := oldestItem.userID
				pqItem, ok := proc.userPQItemMap[userID]
				misc.Assert(ok && pqItem == oldestItem)
				userJobsToProcess[userID] = proc.userJobListMap[userID]
				userEventsToProcess[userID] = proc.userEventsMap[userID]
				//Clear from the map
				delete(proc.userJobListMap, userID)
				delete(proc.userEventsMap, userID)
				proc.userJobPQ.Remove(proc.userPQItemMap[userID])
				delete(proc.userPQItemMap, userID)
				continue
			}
			break
		}
		proc.userPQLock.Unlock()
		if len(userJobsToProcess) > 0 {
			proc.processUserJobs(userJobsToProcess, userEventsToProcess)
		}
	}
}

func getEnabledDestinations(writeKey string, destinationName string) []backendconfig.DestinationT {
	var enabledDests []backendconfig.DestinationT
	for _, dest := range copyWriteKeyDestinationMap[writeKey] {
		if destinationName == dest.DestinationDefinition.Name && dest.Enabled {
			enabledDests = append(enabledDests, dest)
		}
	}
	return enabledDests
}

func getEnabledDestinationTypes(writeKey string) map[string]backendconfig.DestinationDefinitionT {
	var enabledDestinationTypes = make(map[string]backendconfig.DestinationDefinitionT)
	for _, destination := range copyWriteKeyDestinationMap[writeKey] {
		if destination.Enabled {
			enabledDestinationTypes[destination.DestinationDefinition.DisplayName] = destination.DestinationDefinition
		}
	}
	return enabledDestinationTypes
}

func getTimestampFromEvent(event map[string]interface{}, field string) time.Time {
	var timestamp time.Time
	var err error
	if _, ok := event[field]; ok {
		timestampStr, typecasted := event[field].(string)
		if typecasted {
			timestamp, err = dateparse.ParseAny(timestampStr)
		}
		if !typecasted || err != nil {
			timestamp = time.Now()
		}
	} else {
		timestamp = time.Now()
	}
	return timestamp
}

func (proc *HandleT) processJobsForDest(jobList []*jobsdb.JobT, parsedEventList [][]interface{}) {

	proc.statsJobs.Start()

	var destJobs []*jobsdb.JobT
	var batchDestJobs []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	var eventsByDest = make(map[string][]interface{})

	misc.Assert(parsedEventList == nil || len(jobList) == len(parsedEventList))
	//Each block we receive from a client has a bunch of
	//requests. We parse the block and take out individual
	//requests, call the destination specific transformation
	//function and create jobs for them.
	//Transformation is called for a batch of jobs at a time
	//to speed-up execution.

	//Event count for performance stat monitoring
	totalEvents := 0

	for idx, batchEvent := range jobList {

		var eventList []interface{}
		var ok bool
		if parsedEventList == nil {
			eventList, ok = misc.ParseRudderEventBatch(batchEvent.EventPayload)
		} else {
			eventList = parsedEventList[idx]
			ok = (eventList != nil)
		}
		writeKey := gjson.Get(string(batchEvent.EventPayload), "writeKey").Str
		requestIP := gjson.Get(string(batchEvent.EventPayload), "requestIP").Str
		receivedAt := gjson.Get(string(batchEvent.EventPayload), "receivedAt").Time()

		if ok {
			//Iterate through all the events in the batch
			for _, singularEvent := range eventList {
				//We count this as one, not destination specific ones
				totalEvents++
				//Getting all the destinations which are enabled for this
				//event
				destTypesFromConfig := getEnabledDestinationTypes(writeKey)
				destTypes := integrations.GetDestinationIDs(singularEvent, destTypesFromConfig)

				if len(destTypes) == 0 {
					logger.Debug("No enabled destinations")
					continue
				}
				enabledDestinationsMap := map[string][]backendconfig.DestinationT{}
				for _, destType := range destTypes {
					enabledDestinationsList := getEnabledDestinations(writeKey, destType)
					enabledDestinationsMap[destType] = enabledDestinationsList
					// Adding a singular event multiple times if there are multiple destinations of same type
					if len(destTypes) == 0 {
						logger.Debugf("No enabled destinations for type %v", destType)
						continue
					}
					for _, destination := range enabledDestinationsList {
						shallowEventCopy := make(map[string]interface{})
						singularEventMap, ok := singularEvent.(map[string]interface{})
						misc.Assert(ok)
						shallowEventCopy["message"] = singularEventMap
						shallowEventCopy["destination"] = reflect.ValueOf(destination).Interface()
						shallowEventCopy["message"].(map[string]interface{})["request_ip"] = requestIP
						shallowEventCopy["message"].(map[string]interface{})["source_id"] = gjson.GetBytes(batchEvent.Parameters, "source_id").Str

						// set timestamp skew based on timestamp fields from SDKs
						originalTimestamp := getTimestampFromEvent(singularEventMap, "originalTimestamp")
						sentAt := getTimestampFromEvent(singularEventMap, "sentAt")

						// set all timestamps in RFC3339 format
						shallowEventCopy["message"].(map[string]interface{})["receivedAt"] = receivedAt.Format(time.RFC3339)
						shallowEventCopy["message"].(map[string]interface{})["originalTimestamp"] = originalTimestamp.Format(time.RFC3339)
						shallowEventCopy["message"].(map[string]interface{})["sentAt"] = sentAt.Format(time.RFC3339)
						shallowEventCopy["message"].(map[string]interface{})["timestamp"] = misc.GetChronologicalTimeStamp(receivedAt, sentAt, originalTimestamp).Format(time.RFC3339)

						//We have at-least one event so marking it good
						_, ok = eventsByDest[destType]
						if !ok {
							eventsByDest[destType] = make([]interface{}, 0)
						}
						eventsByDest[destType] = append(eventsByDest[destType],
							shallowEventCopy)
					}
				}
			}
		}

		//Mark the batch event as processed
		newStatus := jobsdb.JobStatusT{
			JobID:         batchEvent.JobID,
			JobState:      jobsdb.SucceededState,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{"success":"OK"}`),
		}
		statusList = append(statusList, &newStatus)
	}

	//Now do the actual transformation. We call it in batches, once
	//for each destination ID
	for destID, destEventList := range eventsByDest {
		//Call transform for this destination. Returns
		//the JSON we can send to the destination
		url := integrations.GetDestinationURL(destID)
		logger.Debug("Transform input size", len(destEventList))
		response := proc.transformer.Transform(destEventList, url, transformBatchSize)
		destTransformEventList := response.Events
		logger.Debug("Transform output size", len(destTransformEventList))
		if !response.Success {
			continue
		}

		//Save the JSON in DB. This is what the rotuer uses
		for idx, destEvent := range destTransformEventList {
			destEventJSON, err := json.Marshal(destEvent)
			//Should be a valid JSON since its our transformation
			//but we handle anyway
			if err != nil {
				continue
			}

			//Need to replace UUID his with messageID from client
			id := uuid.NewV4()
			sourceID := response.SourceIDList[idx]
			newJob := jobsdb.JobT{
				UUID:         id,
				Parameters:   []byte(fmt.Sprintf(`{"source_id": "%v"}`, sourceID)),
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
				CustomVal:    destID,
				EventPayload: destEventJSON,
			}
			if misc.Contains(rawDataDestinations, newJob.CustomVal) {
				batchDestJobs = append(batchDestJobs, &newJob)
			} else {
				destJobs = append(destJobs, &newJob)
			}
		}
	}

	misc.Assert(len(statusList) == len(jobList))

	proc.statsDBW.Start()
	//XX: Need to do this in a transaction
	proc.routerDB.Store(destJobs)
	proc.batchRouterDB.Store(batchDestJobs)
	proc.gatewayDB.UpdateJobStatus(statusList, []string{gateway.CustomVal})
	//XX: End of transaction
	proc.statsDBW.End(len(statusList))
	proc.statsJobs.End(totalEvents)

	proc.statGatewayDBW.Count(len(statusList))
	proc.statRouterDBW.Count(len(destJobs))
	proc.statBatchRouterDBW.Count(len(batchDestJobs))

	proc.statsJobs.Print()
	proc.statsDBW.Print()
}

/*
 * If there is a new replay destination, compute the min JobID that the data plane would be routing for that source
 */
func (proc *HandleT) handleReplay(combinedList []*jobsdb.JobT) {

	if len(processReplays) > 0 {
		maxDSIndex := proc.gatewayDB.GetMaxDSIndex()
		misc.Assert(len(combinedList) > 0)
		replayMinJobID := combinedList[0].JobID

		proc.replayProcessor.ProcessNewReplays(processReplays, replayMinJobID, maxDSIndex)
		processReplays = []replayT{}
	}
}

func (proc *HandleT) mainLoop() {

	logger.Info("Processor loop started")
	for {

		proc.statsDBR.Start()

		toQuery := dbReadBatchSize
		//Should not have any failure while processing (in v0) so
		//retryList should be empty. Remove the assert
		retryList := proc.gatewayDB.GetToRetry([]string{gateway.CustomVal}, toQuery)

		unprocessedList := proc.gatewayDB.GetUnprocessed([]string{gateway.CustomVal}, toQuery)

		if len(unprocessedList)+len(retryList) == 0 {
			proc.statsDBR.End(0)
			time.Sleep(loopSleep)
			continue
		}

		combinedList := append(unprocessedList, retryList...)
		proc.statsDBR.End(len(combinedList))
		proc.statGatewayDBR.Count(len(combinedList))

		proc.statsDBR.Print()

		//Sort by JOBID
		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		// Make a copy of configuration for the processor loop
		// Need to process minJobID and new destinations at once
		configSubscriberLock.RLock()
		copyWriteKeyDestinationMap = writeKeyDestinationMap
		proc.handleReplay(combinedList)
		configSubscriberLock.RUnlock()

		if processSessions {
			//Mark all as executing so next query doesn't pick it up
			var statusList []*jobsdb.JobStatusT
			for _, batchEvent := range combinedList {
				newStatus := jobsdb.JobStatusT{
					JobID:         batchEvent.JobID,
					JobState:      jobsdb.ExecutingState,
					AttemptNum:    1,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "200",
					ErrorResponse: []byte(`{"success":"OK"}`),
				}
				statusList = append(statusList, &newStatus)
			}
			proc.gatewayDB.UpdateJobStatus(statusList, []string{gateway.CustomVal})
			proc.addJobsToSessions(combinedList)
		} else {
			proc.processJobsForDest(combinedList, nil)
		}

		copyWriteKeyDestinationMap = nil
	}
}

func (proc *HandleT) crashRecover() {

	for {
		execList := proc.gatewayDB.GetExecuting([]string{gateway.CustomVal}, dbReadBatchSize)

		if len(execList) == 0 {
			break
		}
		logger.Debug("Processor crash recovering", len(execList))

		var statusList []*jobsdb.JobStatusT

		for _, job := range execList {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				JobState:      jobsdb.FailedState,
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`), // check
			}
			statusList = append(statusList, &status)
		}
		proc.gatewayDB.UpdateJobStatus(statusList, []string{gateway.CustomVal})
	}
}
