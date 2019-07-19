package processor

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/spf13/viper"

	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/integrations"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/misc"
	uuid "github.com/satori/go.uuid"
)

//HandleT is an handle to this object used in main.go
type HandleT struct {
	gatewayDB *jobsdb.HandleT
	routerDB  *jobsdb.HandleT
	integ     *integrations.HandleT
	statsAll  *misc.PerfStats
	statsDBR  *misc.PerfStats
	statsDBW  *misc.PerfStats
}

//Setup initializes the module
func (proc *HandleT) Setup(gatewayDB *jobsdb.HandleT, routerDB *jobsdb.HandleT) {
	loadConfig()
	proc.gatewayDB = gatewayDB
	proc.routerDB = routerDB
	proc.integ = &integrations.HandleT{}
	proc.statsAll = &misc.PerfStats{}
	proc.statsDBR = &misc.PerfStats{}
	proc.statsDBW = &misc.PerfStats{}
	proc.statsAll.Setup("ProcessorAll")
	proc.statsDBR.Setup("ProcessorDBRead")
	proc.statsDBW.Setup("ProcessorDBWrite")
	proc.integ.Setup()
	go proc.mainLoop()
}

var (
	loopSleep time.Duration
	batchSize int
	//DestEndPointVal is a placeholder for now. Need to replace with destination
	//specific keys
)

func loadConfig() {
	loopSleep = viper.GetDuration("Processor.loopSleepInMS") * time.Millisecond
	batchSize = viper.GetInt("Processor.batchSize")
}

func (proc *HandleT) mainLoop() {

	fmt.Println("Processor loop started")
	for {
		time.Sleep(loopSleep)

		proc.statsAll.Start()
		proc.statsDBR.Start()

		toQuery := batchSize

		//Should not have any failure while processing (in v0) so
		//retryList should be empty. Remove the assert
		retryList := proc.gatewayDB.GetToRetry([]string{gateway.CustomVal}, toQuery)
		misc.Assert(len(retryList) == 0)

		unprocessedList := proc.gatewayDB.GetUnprocessed([]string{gateway.CustomVal}, toQuery)

		if len(unprocessedList)+len(retryList) == 0 {
			time.Sleep(loopSleep)
			continue
		}

		combinedList := append(unprocessedList, retryList...)

		proc.statsDBR.End(len(combinedList))

		//Sort by JOBID
		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		var destJobs []*jobsdb.JobT
		var statusList []*jobsdb.JobStatusT
		var eventsByDest = make(map[string][]*interface{})

		//Each block we receive from a client has a bunch of
		//requests. We parse the block and take out individual
		//requests, call the destination specific transformation
		//function and create jobs for them.
		//Transformation is called for a batch of jobs at a time
		//to speed-up execution.

		//Event count for performance stat monitoring
		totalEvents := 0
		goodJSON := false

		for _, batchEvent := range combinedList {

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
							eventsByDest[destID] = make([]*interface{}, 0)
						}
						eventsByDest[destID] = append(eventsByDest[destID],
							&singularEvent)
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
			destTransformEventList, ok := proc.integ.TransformJS(destEventList, destID)
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

		misc.Assert(len(statusList) == len(combinedList))

		proc.statsDBW.Start()
		//XX: Need to do this in a transaction
		proc.routerDB.Store(destJobs)
		proc.gatewayDB.UpdateJobStatus(statusList)
		//XX: End of transaction
		proc.statsDBW.End(len(statusList))

		proc.statsAll.End(totalEvents)
		proc.statsAll.Print()
		proc.statsDBR.Print()
		proc.statsDBW.Print()

	}
}
