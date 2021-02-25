package main

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	uuid "github.com/satori/go.uuid"
)

var sampleEvent string = `
[
  {
    "Channel": null,
    "Type": null,
    "MessageId": "69e3bd01-fef1-4d14-856d-a00a5a551c18",
    "Timestamp": "01-01-0001 00:00:00",
    "Context": {
      "App": {
        "Build": "101",
        "Name": "Analytics Unity Client",
        "Namespace": "com.rudderlabs.analytics.unity.client",
        "Version": "1.0.0"
      },
      "Traits": {
        "AnonymousId": "6250c1b1-e70c-4e1c-aa2a-48309370408f"
      },
      "Library": {
        "Name": "analytics-unity",
        "Version": "1.0.0"
      },
      "OS": {
        "Name": "Windows",
        "Version": ""
      },
      "Screen": {
        "Density": 96,
        "Width": 698,
        "Height": 458
      },
      "UserAgent": null,
      "Locale": "en-IN",
      "Device": {
        "Id": "8d1c660b6a28f43864764cf1d729d0a6920e68a8",
        "Manufacturer": "HP Notebook (Hewlett-Packard)",
        "Model": "HP Notebook (Hewlett-Packard)",
        "Name": "DESKTOP-9CQ5IAJ"
      },
      "Network": null
    },
    "AnonymousId": null,
    "Event": "Track",
    "Properties": null
  }
]`

var endPoint string = "4"

var batchSize int = 100
var numLoops int = 100
var numBatchesPerLoop int = 10000

var numQuery int = 10000
var failRatio int = 5

var wg sync.WaitGroup

var storeDone bool = false

var uuidWriteMap map[uuid.UUID]bool
var uuidReadMap map[uuid.UUID]bool

func storeProcess(jd *jobsdb.HandleT) {

	var totalTime float64
	storeSleep := 2 * time.Millisecond

	for i := 0; i < numBatchesPerLoop; i++ {
		time.Sleep(storeSleep)
		var jobList []*jobsdb.JobT
		for i := 0; i < batchSize; i++ {
			id := uuid.NewV4()
			uuidWriteMap[id] = true
			newJob := jobsdb.JobT{
				UUID:         id,
				UserID:       id.String(),
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
				CustomVal:    endPoint,
				EventPayload: []byte(sampleEvent),
			}
			jobList = append(jobList, &newJob)
		}
		start := time.Now()
		jd.Store(jobList)
		elapsed := time.Since(start)
		totalTime += float64(elapsed.Seconds())
		if i%1000 == 0 {
			fmt.Println("**********Average save time:", i, batchSize, totalTime/float64(i+1))
		}
	}
	storeDone = true
	wg.Done()
}

func readProcess(jd *jobsdb.HandleT) {

	var totalReadTime float64
	var totalUpdateTime float64
	var totalLoop int

	readSleep := 1 * time.Second

	for {

		time.Sleep(readSleep)

		start := time.Now()

		toQuery := numQuery
		retryList := jd.GetToRetry([]string{endPoint}, toQuery, []jobsdb.ParameterFilterT{})
		toQuery -= len(retryList)

		unprocessedList := jd.GetUnprocessed([]string{endPoint}, toQuery, []jobsdb.ParameterFilterT{})

		elapsed := time.Since(start)
		totalReadTime += float64(elapsed.Seconds())

		if len(unprocessedList)+len(retryList) == 0 {
			if storeDone {
				break
			} else {
				time.Sleep(readSleep)
				continue
			}

		}

		//Mark call as failed
		var statusList []*jobsdb.JobStatusT

		combinedList := append(unprocessedList, retryList...)
		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		for _, job := range combinedList {

			//Save UUID Map
			uuidReadMap[job.UUID] = true

			stat := jobsdb.Succeeded.State
			if rand.Intn(failRatio) == 0 {
				stat = jobsdb.Failed.State
			}
			newStatus := jobsdb.JobStatusT{
				JobID:         job.JobID,
				JobState:      stat,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "202",
				ErrorResponse: []byte(`{"success":"OK"}`),
			}
			statusList = append(statusList, &newStatus)
		}

		start = time.Now()
		jd.UpdateJobStatus(statusList, []string{}, []jobsdb.ParameterFilterT{})
		elapsed = time.Since(start)
		totalUpdateTime += float64(elapsed.Seconds())

		totalLoop += 1
		if totalLoop%2 == 0 {
			fmt.Println("**********Average read time:", totalLoop, numQuery, totalReadTime/float64(totalLoop))
			fmt.Println("**********Average update time:", totalLoop, numQuery, totalUpdateTime/float64(totalLoop))
		}
	}
	fmt.Println("**********Breaking read thread\n")
	wg.Done()
}

// skipcq: SCC-compile
func main() {
	var jd jobsdb.HandleT

	jd.Setup(true, "prof", time.Duration(0))

	for i := 0; i < numLoops; i++ {

		fmt.Println("Starting loop", i)

		uuidReadMap = make(map[uuid.UUID]bool)
		uuidWriteMap = make(map[uuid.UUID]bool)

		wg.Add(2)
		go storeProcess(&jd)
		go readProcess(&jd)
		wg.Wait()

		//Verify if everything was proper
		for k := range uuidWriteMap {
			_, ok := uuidReadMap[k]
			if !ok {
				fmt.Println("Not found during read", k)
			}
		}

		for k := range uuidReadMap {
			_, ok := uuidWriteMap[k]
			if !ok {
				fmt.Println("Spurious read", k)
			}
		}
	}
}
