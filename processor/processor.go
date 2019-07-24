package processor

import (
	"container/heap"
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

//We keep a priority queue of user_id to last event
//timestamp from that user
type pqItemT struct {
	userID string    //userID
	lastTS time.Time //last timestamp
	index  int       //index in priority queue
}

type pqT []*pqItemT

func (pq pqT) Len() int { return len(pq) }

func (pq pqT) Less(i, j int) bool {
	return pq[i].lastTS.Before(pq[j].lastTS)
}

func (pq pqT) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *pqT) Push(x interface{}) {
	n := len(*pq)
	item := x.(*pqItemT)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *pqT) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *pqT) Top() *pqItemT {
	item := (*pq)[len(*pq)-1]
	return item
}

func (pq *pqT) Remove(item *pqItemT) {
	heap.Remove(pq, item.index)
}

func (pq *pqT) Update(item *pqItemT, newTS time.Time) {
	item.lastTS = newTS
	heap.Fix(pq, item.index)
}

func (pq *pqT) Print() {
	for _, v := range *pq {
		log.Println(*v)
	}
}

//HandleT is an handle to this object used in main.go
type HandleT struct {
	gatewayDB          *jobsdb.HandleT
	routerDB           *jobsdb.HandleT
	integ              *integrations.HandleT
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
	proc.integ = &integrations.HandleT{}
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
	proc.integ.Setup()
	go proc.mainLoop()
	if processSessions {
		log.Println("Starting session processor")
		go proc.processSessionJobs()
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
	processSessions = config.GetBool("Processor.processSessions", true)
	sessionThresholdInS = config.GetDuration("Processor.sessionThresholdInS", time.Duration(20)) * time.Second

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
		proc.userJobListMap[userID] = append(proc.userJobListMap[userID], job)
		proc.userEventLengthMap[userID] += len(eventList)
		//If we have a lot of events from that user, we process jobs
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
			heap.Push(&proc.userJobPQ, pqItem)
		} else {
			misc.Assert(pqItem.index != -1)
			proc.userJobPQ.Update(pqItem, time.Now())
		}

	}

	log.Println("Post Add")
	proc.Print()

	//Now process events from those users
	var toProcessJobs []*jobsdb.JobT
	for userID := range processUserIDs {
		toProcessJobs = append(toProcessJobs, proc.userJobListMap[userID]...)
		delete(proc.userJobListMap, userID)
		delete(proc.userEventLengthMap, userID)
		proc.userJobPQ.Remove(proc.userPQItemMap[userID])
		delete(proc.userPQItemMap, userID)
	}
	//We can release the lock
	proc.userPQLock.Unlock()
	if len(toProcessJobs) > 0 {
		log.Println("Processing")
		proc.Print()
		proc.processJobs(toProcessJobs)
	}
}

func (proc *HandleT) processSessionJobs() {

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

		var toProcessJobs []*jobsdb.JobT
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
				toProcessJobs = append(toProcessJobs, proc.userJobListMap[userID]...)
				delete(proc.userJobListMap, userID)
				delete(proc.userEventLengthMap, userID)
				proc.userJobPQ.Remove(proc.userPQItemMap[userID])
				delete(proc.userPQItemMap, userID)
			} else {
				break
			}
		}
		proc.userPQLock.Unlock()
		if len(toProcessJobs) > 0 {
			log.Println("Processing ession Check")
			proc.Print()
			proc.processJobs(toProcessJobs)
		}
	}
}

func (proc *HandleT) processJobs(jobList []*jobsdb.JobT) {

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
	goodJSON := false

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
					goodJSON = true
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
		state := "aborted"
		if goodJSON {
			state = "succeeded"
		}
		newStatus := jobsdb.JobStatusT{
			JobID:         batchEvent.JobID,
			JobState:      state,
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
		destTransformEventList, ok := proc.integ.Transform(destEventList, destID)
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
					JobState:      "executing",
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
			proc.processJobs(combinedList)
		}

	}
}
