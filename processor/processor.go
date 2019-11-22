package processor

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	"github.com/jpillora/backoff"
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
	"github.com/thoas/go-funk"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

//HandleT is an handle to this object used in main.go
type HandleT struct {
	gatewayDB            *jobsdb.HandleT
	failedGatewayDB      *jobsdb.HandleT
	abortedGatewayDB     *jobsdb.HandleT
	routerDB             *jobsdb.HandleT
	batchRouterDB        *jobsdb.HandleT
	transformer          *transformerHandleT
	statsJobs            *misc.PerfStats
	statsDBR             *misc.PerfStats
	statGatewayDBR       *stats.RudderStats
	statsDBW             *misc.PerfStats
	statGatewayDBW       *stats.RudderStats
	statRouterDBW        *stats.RudderStats
	statBatchRouterDBW   *stats.RudderStats
	statActiveUsers      *stats.RudderStats
	userJobListMap       map[string][]*jobsdb.JobT
	userEventsMap        map[string][]interface{}
	userPQItemMap        map[string]*pqItemT
	userJobPQ            pqT
	userPQLock           sync.Mutex
	failedUserDestJobMap map[string]int64
	destRetryBackoffMap  map[string]DestRetryT
}

type DestRetryT struct {
	NextProcessTime time.Time
	Backoff         *backoff.Backoff
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
func (proc *HandleT) Setup(gatewayDB *jobsdb.HandleT, failedGatewayDB *jobsdb.HandleT, abortedGatewayDB *jobsdb.HandleT, routerDB *jobsdb.HandleT, batchRouterDB *jobsdb.HandleT) {
	proc.gatewayDB = gatewayDB
	proc.failedGatewayDB = failedGatewayDB
	proc.abortedGatewayDB = abortedGatewayDB
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
	proc.failedUserDestJobMap = make(map[string]int64)
	proc.destRetryBackoffMap = make(map[string]DestRetryT)
	proc.statsJobs.Setup("ProcessorJobs")
	proc.statsDBR.Setup("ProcessorDBRead")
	proc.statsDBW.Setup("ProcessorDBWrite")

	proc.statGatewayDBR = stats.NewStat("processor.gateway_db_read", stats.CountType)
	proc.statGatewayDBW = stats.NewStat("processor.gateway_db_write", stats.CountType)
	proc.statRouterDBW = stats.NewStat("processor.router_db_write", stats.CountType)
	proc.statBatchRouterDBW = stats.NewStat("processor.batch_router_db_write", stats.CountType)
	proc.statActiveUsers = stats.NewStat("processor.active_users", stats.GaugeType)

	go backendConfigSubscriber()
	proc.transformer.Setup()
	proc.crashRecover()
	go proc.mainLoop()
	if processSessions {
		logger.Info("Starting session processor")
		go proc.createSessions()
	}
}

var (
	loopSleep              time.Duration
	dbReadBatchSize        int
	transformBatchSize     int
	sessionThresholdInS    time.Duration
	sessionThresholdEvents int
	processSessions        bool
	writeKeyDestinationMap map[string][]backendconfig.DestinationT
	rawDataDestinations    []string
	configSubscriberLock   sync.RWMutex
	maxFailedCountForJob   int
	backoffIncrementInS    int
	maxBackoffInS          int
	backoffFactor          int
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
	maxFailedCountForJob = config.GetInt("Processor.maxFailedCountForJob", 8)
	backoffIncrementInS = config.GetInt("Processor.backoffIncrementInS", 20)
	maxBackoffInS = config.GetInt("Processor.backoffIncrementInS", 300)
	backoffFactor = config.GetInt("Processor.backoffFactor", 2)
}

func backendConfigSubscriber() {
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

func getEnabledDestinationTypes(writeKey string) map[string]backendconfig.DestinationDefinitionT {
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
	var failedGWJobs []*jobsdb.JobT
	var abortedGWJobs []*jobsdb.JobT
	var failedGWJobStatusList []*jobsdb.JobStatusT
	var failedGWStatusCustomVals []string
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

		prevFailedJob := false
		var failedDestType, failedDestID string
		if gjson.GetBytes(batchEvent.Parameters, "prev_failed").Exists() {
			prevFailedJob = true
			failedDestType = gjson.GetBytes(batchEvent.Parameters, "failed_destination_type").String()
			failedDestID = gjson.GetBytes(batchEvent.Parameters, "failed_destination_id").String()
		}

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

				// Getting all the destinations which are enabled for this
				// event
				// If job is from failed_gw ds, select destination only for which the job has failed
				var destTypes []string
				if prevFailedJob {
					destTypes = []string{failedDestType}
				} else {
					destTypesFromConfig := getEnabledDestinationTypes(writeKey)
					destTypes = integrations.GetDestinationCodes(singularEvent, destTypesFromConfig)
				}

				if len(destTypes) == 0 {
					logger.Debug("No enabled destinations")
					continue
				}
				// enabledDestinationsMap := map[string][]backendconfig.DestinationT{}
				for _, destType := range destTypes {
					enabledDestinationsList := getEnabledDestinations(writeKey, destType)
					// If job is from failed_gw ds, select destination only for which the job has failed
					// Source can have multiple GA destiantions but might have failed for one of them due to
					// user transformation attached to it
					if prevFailedJob {
						failedDestI := funk.Find(enabledDestinationsList, func(dest backendconfig.DestinationT) bool {
							return dest.ID == failedDestID
						})
						if failedDestI != nil {
							enabledDestinationsList = []backendconfig.DestinationT{failedDestI.(backendconfig.DestinationT)}
						}
					}

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

						// add metadata to each singularEvent which will be returned by transformer in response
						shallowEventCopy["metadata"] = make(map[string]interface{})
						shallowEventCopy["metadata"].(map[string]interface{})["source_id"] = gjson.GetBytes(batchEvent.Parameters, "source_id").Str
						shallowEventCopy["metadata"].(map[string]interface{})["job_id"] = batchEvent.JobID
						shallowEventCopy["metadata"].(map[string]interface{})["destination_id"] = destination.ID
						shallowEventCopy["metadata"].(map[string]interface{})["destination_type"] = destination.DestinationDefinition.Name
						shallowEventCopy["metadata"].(map[string]interface{})["message_id"] = shallowEventCopy["message"].(map[string]interface{})["messageId"].(string)
						shallowEventCopy["metadata"].(map[string]interface{})["anonymous_id"] = shallowEventCopy["message"].(map[string]interface{})["anonymousId"].(string)

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

		// do not mark again in gateway status if job is from failed_gw ds
		if !prevFailedJob {
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
	}

	//Now do the actual transformation. We call it in batches, once
	//for each destination ID
	for destType, destEventList := range eventsByDest {
		//Call transform for this destination. Returns
		//the JSON we can send to the destination
		url := integrations.GetDestinationURL(destType)
		logger.Debug("Transform input size", len(destEventList))
		response := proc.transformer.Transform(destEventList, integrations.GetUserTransformURL(), 0)
		response = proc.transformer.Transform(response.Events, url, transformBatchSize)
		destTransformEventList := response.Events
		failedJobIDs := response.FailedJobIDs
		logger.Debug("Transform output size", len(destTransformEventList))
		if !response.Success {
			continue
		}

		// map to save setting status of job/abort in failed_gw
		// do not process multiple failed events from same job
		currentRespJobStateMap := make(map[int64]string)
		// map to save setting backoff times for a destination
		// do not increment backoff counter for same destination
		// if multiple events failed in single response from transformer
		failedDestIDMap := make(map[string]bool)
		//Save the JSON in DB. This is what the rotuer uses
		for idx, destEvent := range destTransformEventList {
			// actual transformed event json
			destEventJSON, err := json.Marshal(destEvent.(map[string]interface{})["output"])
			sourceID := response.SourceIDList[idx]

			// variables from metdata of the event returned by transformer
			destEventJobID := int64(destEvent.(map[string]interface{})["metadata"].(map[string]interface{})["job_id"].(float64))
			destID := destEvent.(map[string]interface{})["metadata"].(map[string]interface{})["destination_id"].(string)
			destType := destEvent.(map[string]interface{})["metadata"].(map[string]interface{})["destination_type"].(string)
			userID := destEvent.(map[string]interface{})["metadata"].(map[string]interface{})["anonymous_id"].(string)
			messageID := destEvent.(map[string]interface{})["metadata"].(map[string]interface{})["message_id"].(string)

			userDestEventKey := userID + "_" + destID
			// check if we have failed event present for user+dest combination
			previousFailedJobID, isPrevFailedUserDest := proc.failedUserDestJobMap[userDestEventKey]

			// get the corresponding job from jobList for an event
			destEventJobI := funk.Find(jobList, func(job *jobsdb.JobT) bool {
				return job.JobID == destEventJobID
			})
			var destEventJob *jobsdb.JobT
			if destEventJobI != nil {
				destEventJob = destEventJobI.(*jobsdb.JobT)
			}

			hasJobFailed := misc.Contains(failedJobIDs, destEventJobID)
			isJobFromFailedGW := gjson.GetBytes(destEventJob.Parameters, "prev_failed").Exists()

			// job_id for the event
			// retrieve job_id of original event for a failed_gw job under job_id in parameters
			originalJobID := destEventJob.JobID
			if isJobFromFailedGW {
				originalJobID = gjson.GetBytes(destEventJob.Parameters, "job_id").Int()
			}

			// create job status as waiting in failed_gw ds if its behind a failed event for same user+dest combination
			// and continue without creating rt job
			if isPrevFailedUserDest && (previousFailedJobID != originalJobID) {
				// do not create again if event from same job is encountered again in this transformer response
				if state, ok := currentRespJobStateMap[originalJobID]; ok && state == "marked_waiting" {
					continue
				}
				currentRespJobStateMap[originalJobID] = "marked_waiting"

				// create new job record in failed_gw if not exists
				// else create job_status record in failed_gw ds
				if isJobFromFailedGW {
					waitingStatus := jobsdb.JobStatusT{
						JobID:         destEventJob.JobID,
						JobState:      jobsdb.WaitingState,
						AttemptNum:    1,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "200",
						ErrorResponse: []byte(`{"success":"OK"}`),
					}
					failedGWJobStatusList = append(failedGWJobStatusList, &waitingStatus)
					failedGWStatusCustomVals = append(failedGWStatusCustomVals, destID)
				} else {
					newWaitingJob := jobsdb.JobT{
						UUID:         destEventJob.UUID,
						Parameters:   []byte(fmt.Sprintf(`{"source_id": "%v", "failed_destination_id": "%v", "failed_destination_type": "%v", "anonymous_id": "%v", "prev_failed": true, "job_id": %v}`, sourceID, destID, destType, userID, destEventJob.JobID)),
						CreatedAt:    time.Now(),
						ExpireAt:     time.Now(),
						CustomVal:    destID,
						EventPayload: destEventJob.EventPayload,
					}
					failedGWJobs = append(failedGWJobs, &newWaitingJob)
				}
				continue
			}

			if hasJobFailed {
				// set in map, so that same user's event to same destID are processed in order
				proc.failedUserDestJobMap[userDestEventKey] = originalJobID

				// increment/create backoff only if new set of jobs are failing for a given destID
				if _, ok := failedDestIDMap[destID]; !ok {
					if retryConfig, ok := proc.destRetryBackoffMap[destID]; ok {
						retryConfig.NextProcessTime = time.Now().Add(time.Duration(retryConfig.Backoff.Duration().Seconds()) * time.Second)
						proc.destRetryBackoffMap[destID] = retryConfig
					} else {
						b := &backoff.Backoff{
							Min:    time.Duration(backoffIncrementInS) * time.Second,
							Max:    time.Duration(maxBackoffInS) * time.Second,
							Factor: float64(backoffFactor),
							Jitter: false,
						}
						proc.destRetryBackoffMap[destID] = DestRetryT{
							Backoff:         b,
							NextProcessTime: time.Now().Add(time.Duration(b.Duration().Seconds()) * time.Second),
						}
					}
				}
				failedDestIDMap[destID] = true

				retries := 0
				if isJobFromFailedGW {
					retries = destEventJob.LastJobStatus.AttemptNum
				}

				// increment AttemptNum if retry attemp is below configured limit
				// create record or update status (increment attempt) in failed_gw ds
				if retries < maxFailedCountForJob { // less than abort retries
					// if already marked fail for current set of destTransformEventList, do not do anything
					if state, ok := currentRespJobStateMap[originalJobID]; ok && state == "marked_fail" {
						continue
					}
					currentRespJobStateMap[originalJobID] = "marked_fail"
					if !isJobFromFailedGW {
						newFailedJob := jobsdb.JobT{
							UUID:         destEventJob.UUID,
							Parameters:   []byte(fmt.Sprintf(`{"source_id": "%v", "failed_destination_id": "%v", "failed_destination_type": "%v", "anonymous_id": "%v", "prev_failed": true, "job_id": %v}`, sourceID, destID, destType, userID, destEventJob.JobID)),
							CreatedAt:    time.Now(),
							ExpireAt:     time.Now(),
							CustomVal:    destID,
							EventPayload: destEventJob.EventPayload,
						}
						failedGWJobs = append(failedGWJobs, &newFailedJob)
					} else {
						failedStatus := jobsdb.JobStatusT{
							JobID:         destEventJob.JobID,
							JobState:      jobsdb.FailedState,
							AttemptNum:    retries + 1,
							ExecTime:      time.Now(),
							RetryTime:     time.Now(),
							ErrorCode:     "200",
							ErrorResponse: []byte(`{"success":"OK"}`),
						}
						failedGWJobStatusList = append(failedGWJobStatusList, &failedStatus)
						failedGWStatusCustomVals = append(failedGWStatusCustomVals, destID)
					}
					continue
				} else {
					// if the job is not set to abort status yet, do it
					if state, ok := currentRespJobStateMap[destEventJob.JobID]; !ok || state != "marked_abort" {
						currentRespJobStateMap[destEventJob.JobID] = "marked_abort"
						failedStatus := jobsdb.JobStatusT{
							JobID:         destEventJob.JobID,
							JobState:      jobsdb.AbortedState,
							AttemptNum:    retries + 1,
							ExecTime:      time.Now(),
							RetryTime:     time.Now(),
							ErrorCode:     "200",
							ErrorResponse: []byte(`{"success":"OK"}`),
						}
						failedGWJobStatusList = append(failedGWJobStatusList, &failedStatus)
						failedGWStatusCustomVals = append(failedGWStatusCustomVals, destID)
					}
					// unblock other jobs for user+dest combination
					delete(proc.failedUserDestJobMap, userDestEventKey)

					// store failed event in abort_gw table
					respElemMap, castOk := destEvent.(map[string]interface{})
					if castOk {
						if statusCode, ok := respElemMap["statusCode"]; ok && fmt.Sprintf("%v", statusCode) == "400" {
							// write to aborted db
							batch := gjson.GetBytes(destEventJob.EventPayload, "batch")
							var index int
							var found bool
							batch.ForEach(func(_, _ gjson.Result) bool {
								if gjson.GetBytes(destEventJob.EventPayload, fmt.Sprintf(`batch.%v.messageId`, index)).Str == messageID {
									found = true
									return false
								}
								index++
								return true // keep iterating
							})
							if found {
								singleEvent := gjson.GetBytes(destEventJob.EventPayload, fmt.Sprintf(`batch.%v`, index))
								destEventJob.EventPayload, _ = sjson.SetRawBytes(destEventJob.EventPayload, `batch`, []byte(fmt.Sprintf(`[%v]`, singleEvent)))
								newAbortedJob := jobsdb.JobT{
									UUID:         uuid.NewV4(),
									Parameters:   []byte(fmt.Sprintf(`{"source_id": "%v", "failed_destination_id": "%v", "failed_destination_type": "%v", "anonymous_id": "%v", "prev_failed": true, "job_id": %v}`, sourceID, destID, destType, userID, destEventJob.JobID)),
									CreatedAt:    time.Now(),
									ExpireAt:     time.Now(),
									CustomVal:    destID,
									EventPayload: destEventJob.EventPayload,
								}
								abortedGWJobs = append(abortedGWJobs, &newAbortedJob)
							}
							continue
						}
					}
				}
			} else if isPrevFailedUserDest {
				delete(proc.failedUserDestJobMap, userDestEventKey)
			}

			// job transformation is successful
			// unblock other jobs for user+dest combination
			delete(proc.failedUserDestJobMap, userDestEventKey)
			// reset backoff counter for the destination
			delete(proc.destRetryBackoffMap, destID)
			delete(failedDestIDMap, destID)

			//Should be a valid JSON since its our transformation
			//but we handle anyway
			if err != nil {
				continue
			}

			//Need to replace UUID his with messageID from client
			id := uuid.NewV4()
			newJob := jobsdb.JobT{
				UUID:         id,
				Parameters:   []byte(fmt.Sprintf(`{"source_id": "%v"}`, sourceID)),
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
	}

	misc.Assert(len(statusList) == len(jobList))

	proc.statsDBW.Start()
	//XX: Need to do this in a transaction
	proc.failedGatewayDB.Store(failedGWJobs)
	proc.failedGatewayDB.UpdateJobStatus(failedGWJobStatusList, funk.UniqString(failedGWStatusCustomVals))
	proc.abortedGatewayDB.Store(abortedGWJobs)
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

func (proc *HandleT) mainLoop() {

	logger.Info("Processor loop started")
	for {

		proc.statsDBR.Start()

		toQuery := dbReadBatchSize

		// pick up jobs failed_gw table only if it exceeds backoff time
		var toFetchDestIDs []string
		for destID, retryConfig := range proc.destRetryBackoffMap {
			if time.Now().After(retryConfig.NextProcessTime) {
				toFetchDestIDs = append(toFetchDestIDs, destID)
			}
		}

		var failedGWList []*jobsdb.JobT
		if len(toFetchDestIDs) > 0 {
			failedList := proc.failedGatewayDB.GetToRetry(toFetchDestIDs, toQuery)
			toQuery -= len(failedList)
			unprocList := proc.failedGatewayDB.GetUnprocessed(toFetchDestIDs, toQuery)
			toQuery -= len(unprocList)
			waitList := proc.failedGatewayDB.GetWaiting(toFetchDestIDs, toQuery)
			toQuery -= len(waitList)

			failedGWList = append(failedGWList, append(waitList, append(unprocList, failedList...)...)...)
		}

		//Should not have any failure while processing (in v0) so
		//retryList should be empty. Remove the assert
		retryList := proc.gatewayDB.GetToRetry([]string{gateway.CustomVal}, toQuery)
		toQuery -= len(retryList)

		unprocessedList := proc.gatewayDB.GetUnprocessed([]string{gateway.CustomVal}, toQuery)

		if len(unprocessedList)+len(retryList)+len(failedGWList) == 0 {
			proc.statsDBR.End(0)
			time.Sleep(loopSleep)
			continue
		}

		// combinedList := append(unprocessedList, append(retryList, append(unprocList, append(waitList, failedList...)...)...)...)
		combinedList := append(unprocessedList, append(retryList, failedGWList...)...)
		proc.statsDBR.End(len(combinedList))
		proc.statGatewayDBR.Count(len(combinedList))

		proc.statsDBR.Print()

		//Sort by JOBID
		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

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
