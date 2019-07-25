package processor

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/integrations"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/misc"
	uuid "github.com/satori/go.uuid"
)

//HandleT is an handle to this object used in main.go
type HandleT struct {
	gatewayDB          *jobsdb.HandleT
	routerDB           *jobsdb.HandleT
	transformer        *transformerHandleT
	statsJobs          *misc.PerfStats
	statsDBR           *misc.PerfStats
	statsDBW           *misc.PerfStats
	userJobListMap     map[string][]*jobsdb.JobT
	userEventLengthMap map[string]int
	userPQItemMap      map[string]*pqItemT
	userJobPQ          pqT
	userPQLock         sync.Mutex
}

//Print the internal structure
func (proc *HandleT) Print() {
	log.Println("PriorityQueue")
	proc.userJobPQ.Print()
	log.Println("JobList")
	for k, v := range proc.userJobListMap {
		log.Println(k, ":", len(v))
	}
	log.Println("EventLength")
	for k, v := range proc.userEventLengthMap {
		log.Println(k, ":", v)
	}
	log.Println("PQItem")
	for k, v := range proc.userPQItemMap {
		log.Println(k, ":", *v)
	}
}

//Setup initializes the module
func (proc *HandleT) Setup(gatewayDB *jobsdb.HandleT, routerDB *jobsdb.HandleT) {
	loadConfig()
	proc.gatewayDB = gatewayDB
	proc.routerDB = routerDB
	proc.transformer = &transformerHandleT{}
	proc.statsJobs = &misc.PerfStats{}
	proc.statsDBR = &misc.PerfStats{}
	proc.statsDBW = &misc.PerfStats{}
	proc.userJobListMap = make(map[string][]*jobsdb.JobT)
	proc.userEventLengthMap = make(map[string]int)
	proc.userPQItemMap = make(map[string]*pqItemT)
	proc.userJobPQ = make(pqT, 0)
	proc.statsJobs.Setup("ProcessorJobs")
	proc.statsDBR.Setup("ProcessorDBRead")
	proc.statsDBW.Setup("ProcessorDBWrite")
	proc.transformer.Setup()
	go proc.mainLoop()
	if processSessions {
		log.Println("Starting session processor")
		go proc.createSessions()
	}
}

var (
	loopSleep              time.Duration
	batchSize              int
	sessionThresholdInS    time.Duration
	sessionThresholdEvents int
	processSessions        bool
)

func loadConfig() {
	loopSleep = config.GetDuration("Processor.loopSleepInMS", time.Duration(10)) * time.Millisecond
	batchSize = config.GetInt("Processor.batchSize", 1000)
	sessionThresholdEvents = config.GetInt("Processor.sessionThresholdEvents", 20)
	processSessions = config.GetBool("Processor.processSessions", false)
	sessionThresholdInS = config.GetDuration("Processor.sessionThresholdInS", time.Duration(20)) * time.Second
	maxChanSize = config.GetInt("Processor.maxChanSize", 2048)
	numTransformWorker = config.GetInt("Processor.numTransformWorker", 32)
	maxRetry = config.GetInt("Processor.maxRetry", 3)
	retrySleep = config.GetDuration("Processor.retrySleepInMS", time.Duration(100)) * time.Millisecond

}

func (proc *HandleT) addJobsToSessions(jobList []*jobsdb.JobT) {

	proc.userPQLock.Lock()

	//List of users whose jobs need to be processed
	processUserIDs := make(map[string]bool)

	for _, job := range jobList {
		userID, ok := misc.GetRudderEventUserID(job.EventPayload)
		if !ok {
			log.Println("Failed to get userID for job")
			continue
		}
		_, ok = proc.userJobListMap[userID]
		if !ok {
			proc.userJobListMap[userID] = make([]*jobsdb.JobT, 0)
			proc.userEventLengthMap[userID] = 0
		}
		//Append to job to list. If over threshold, just process them
		eventList, ok := misc.ParseRudderEventBatch(job.EventPayload)
		if !ok {
			//bad event
			continue
		}
		//Add the job to the userID specific lists
		proc.userJobListMap[userID] = append(proc.userJobListMap[userID], job)
		proc.userEventLengthMap[userID] += len(eventList)
		//If we have enough events from that user, we process jobs
		if proc.userEventLengthMap[userID] > sessionThresholdEvents {
			processUserIDs[userID] = true
		}
		pqItem, ok := proc.userPQItemMap[userID]
		if !ok {
			pqItem := &pqItemT{
				userID: userID,
				lastTS: time.Now(),
				index:  -1,
			}
			proc.userPQItemMap[userID] = pqItem
			proc.userJobPQ.Add(pqItem)
		} else {
			misc.Assert(pqItem.index != -1)
			proc.userJobPQ.Update(pqItem, time.Now())
		}

	}

	if len(processUserIDs) > 0 {
		userJobsToProcess := make(map[string][]*jobsdb.JobT)
		log.Println("Post Add Processing")
		proc.Print()

		//We clear the data structure for these users
		for userID := range processUserIDs {
			userJobsToProcess[userID] = proc.userJobListMap[userID]
			delete(proc.userJobListMap, userID)
			delete(proc.userEventLengthMap, userID)
			proc.userJobPQ.Remove(proc.userPQItemMap[userID])
			delete(proc.userPQItemMap, userID)
		}
		log.Println("Processing")
		proc.Print()
		//We release the block before actually processing
		proc.userPQLock.Unlock()
		proc.processUserJobs(userJobsToProcess)
		return
	}
	proc.userPQLock.Unlock()
}

func (proc *HandleT) processUserJobs(userJobs map[string][]*jobsdb.JobT) {

	userEventsMap := make(map[string][]interface{})

	totalJobs := 0
	allJobIDs := make(map[int64]bool)
	//We extract the individual events from the batch structure
	//and create a single list of events per user
	for userID := range userJobs {
		userEventsMap[userID] = make([]interface{}, 0)
		for _, job := range userJobs[userID] {
			totalJobs++
			allJobIDs[job.JobID] = true
			eventList, ok := misc.ParseRudderEventBatch(job.EventPayload)
			if !ok {
				continue
			}
			userEventsMap[userID] = append(userEventsMap[userID], eventList...)
		}
	}

	//Create a list of list of user events which is passed to transformer
	userEventsList := make([]interface{}, 0)
	userIDList := make([]string, 0) //Order of users which are added to list
	for userID := range userEventsMap {
		userEventsList = append(userEventsList, userEventsMap[userID])
		userIDList = append(userIDList, userID)
	}
	misc.Assert(len(userEventsList) == len(userEventsMap))

	//Call the transformation function
	transformUserEventList, ok := proc.transformer.Transform(userEventsList,
		integrations.GetUserTransformURL(), false)
	misc.Assert(ok)

	//Create jobs that can be processed further
	toProcessJobs := createUserTransformedJobsFromEvents(transformUserEventList, userIDList, userJobs)

	//Some sanity check to make sure we have all the jobs
	misc.Assert(len(toProcessJobs) == totalJobs)
	for _, job := range toProcessJobs {
		_, ok := allJobIDs[job.JobID]
		misc.Assert(ok)
		delete(allJobIDs, job.JobID)
	}
	misc.Assert(len(allJobIDs) == 0)

	//Process
	proc.processJobsForDest(toProcessJobs)
}

//We create sessions (of individul events) from set of input jobs  from a user
//Those sesssion events are transformed and we have a transformed set of
//events that must be processed further via destination specific transformations
//(in processJobsForDest). This function creates jobs from eventList
func createUserTransformedJobsFromEvents(transformUserEventList []interface{},
	userIDList []string, userJobs map[string][]*jobsdb.JobT) []*jobsdb.JobT {

	transJobList := make([]*jobsdb.JobT, 0)

	misc.Assert(len(transformUserEventList) == len(userIDList))
	for idx, userID := range userIDList {
		userEvents := transformUserEventList[idx]
		transPayload := map[string]interface{}{"batch": userEvents}
		transPayloadRaw, err := json.Marshal(transPayload)
		misc.AssertError(err)
		for idx, job := range userJobs[userID] {
			//We put all the transformed event on the first job
			//and empty out the remaining payloads
			if idx == 0 {
				job.EventPayload = transPayloadRaw
			} else {
				job.EventPayload = make([]byte, 0)
			}
			transJobList = append(transJobList, job)
		}
	}
	return transJobList
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

		//Enough time hasn't transpired since last
		oldestItem := proc.userJobPQ.Top()
		if time.Since(oldestItem.lastTS) < time.Duration(sessionThresholdInS) {
			proc.userPQLock.Unlock()
			sleepTime := time.Duration(sessionThresholdInS) - time.Since(oldestItem.lastTS)
			log.Println("Sleeping", sleepTime)
			time.Sleep(sleepTime)
			continue
		}

		userJobsToProcess := make(map[string][]*jobsdb.JobT)

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

				//Clear from the map
				delete(proc.userJobListMap, userID)
				delete(proc.userEventLengthMap, userID)
				proc.userJobPQ.Remove(proc.userPQItemMap[userID])
				delete(proc.userPQItemMap, userID)
				continue
			}
			break
		}
		proc.userPQLock.Unlock()
		if len(userJobsToProcess) > 0 {
			log.Println("Processing Session Check")
			proc.Print()
			proc.processUserJobs(userJobsToProcess)
		}
	}
}

func (proc *HandleT) processJobsForDest(jobList []*jobsdb.JobT) {

	proc.statsJobs.Start()

	var destJobs []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	var eventsByDest = make(map[string][]interface{})

	//Sort by JOBID
	sort.Slice(jobList, func(i, j int) bool {
		return jobList[i].JobID < jobList[j].JobID
	})

	//Each block we receive from a client has a bunch of
	//requests. We parse the block and take out individual
	//requests, call the destination specific transformation
	//function and create jobs for them.
	//Transformation is called for a batch of jobs at a time
	//to speed-up execution.

	//Event count for performance stat monitoring
	totalEvents := 0

	for _, batchEvent := range jobList {

		eventList, ok := misc.ParseRudderEventBatch(batchEvent.EventPayload)

		if ok {
			//Iterate through all the events in the batch
			for _, singularEvent := range eventList {
				//We count this as one, not destination specific ones
				totalEvents++
				//Getting all the destinations which are enabled for this
				//event
				destIDs := integrations.GetDestinationIDs(singularEvent)
				if len(destIDs) == 0 {
					continue
				}

				for _, destID := range destIDs {
					//We have at-least one event so marking it good
					_, ok := eventsByDest[destID]
					if !ok {
						eventsByDest[destID] = make([]interface{}, 0)
					}
					eventsByDest[destID] = append(eventsByDest[destID],
						singularEvent)
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
		url, ok := integrations.GetDestinationURL(destID)
		if !ok {
			continue
		}
		destTransformEventList, ok := proc.transformer.Transform(destEventList, url, true)
		if !ok {
			continue
		}

		//Save the JSON in DB. This is what the rotuer uses
		for _, destEvent := range destTransformEventList {
			destEventJSON, err := json.Marshal(destEvent)
			//Should be a valid JSON since its our transformation
			//but we handle anyway
			if err != nil {
				continue
			}

			//Need to replace UUID his with messageID from client
			id := uuid.NewV4()
			newJob := jobsdb.JobT{
				UUID:         id,
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
				CustomVal:    destID,
				EventPayload: destEventJSON,
			}
			destJobs = append(destJobs, &newJob)
		}
	}

	misc.Assert(len(statusList) == len(jobList))

	proc.statsDBW.Start()
	//XX: Need to do this in a transaction
	proc.routerDB.Store(destJobs)
	proc.gatewayDB.UpdateJobStatus(statusList, []string{gateway.CustomVal})
	//XX: End of transaction
	proc.statsDBW.End(len(statusList))
	proc.statsJobs.End(totalEvents)
	proc.statsJobs.Print()
	proc.statsDBW.Print()
}

func (proc *HandleT) mainLoop() {

	fmt.Println("Processor loop started")
	for {

		proc.statsDBR.Start()

		toQuery := batchSize
		//Should not have any failure while processing (in v0) so
		//retryList should be empty. Remove the assert
		retryList := proc.gatewayDB.GetToRetry([]string{gateway.CustomVal}, toQuery)
		misc.Assert(len(retryList) == 0)

		unprocessedList := proc.gatewayDB.GetUnprocessed([]string{gateway.CustomVal}, toQuery)

		if len(unprocessedList)+len(retryList) == 0 {
			proc.statsDBR.End(0)
			time.Sleep(loopSleep)
			continue
		}

		combinedList := append(unprocessedList, retryList...)
		proc.statsDBR.End(len(combinedList))
		proc.statsDBR.Print()

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
			proc.processJobsForDest(combinedList)
		}

	}
}
