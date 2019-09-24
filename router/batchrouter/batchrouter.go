package batchrouter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
)

var (
	jobQueryBatchSize    int
	noOfWorkers          int
	mainLoopSleepInS     int
	batchDestinations    []BatchDestinationT
	configSubscriberLock sync.RWMutex
	rawDataDestinations  []string
	inProgressMap        map[string]bool
)

type HandleT struct {
	processQ  chan BatchJobsT
	jobsDB    *jobsdb.HandleT
	isEnabled bool
}

func backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Eb.Subscribe("backendconfig", ch)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		batchDestinations = []BatchDestinationT{}
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if source.Enabled && len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if misc.Contains(rawDataDestinations, destination.DestinationDefinition.Name) {
						batchDestinations = append(batchDestinations, BatchDestinationT{Source: source, Destination: destination})
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

func (brt *HandleT) copyJobsToS3(batchJobs BatchJobsT) {
	uuid := uuid.NewV4()
	path := fmt.Sprintf("%v%v.json", config.GetEnv("TMPDIR", "/home/ubuntu/s3/"), fmt.Sprintf("%v.%v.%v", time.Now().Unix(), batchJobs.BatchDestination.Source.ID, uuid))
	unzippedFile, err := os.Create(path)
	misc.AssertError(err)
	for _, job := range batchJobs.Jobs {
		trimmedPayload := bytes.TrimLeft(job.EventPayload, " \t\r\n")
		isArray := len(trimmedPayload) > 0 && trimmedPayload[0] == '['
		if isArray {
			var events []interface{}
			err := json.Unmarshal(trimmedPayload, &events)
			misc.AssertError(err)
			for _, event := range events {
				jsonEvent, err := json.Marshal((event))
				misc.AssertError(err)
				_, err = fmt.Fprintln(unzippedFile, string(jsonEvent))
				misc.AssertError(err)
			}
		} else {
			_, err := fmt.Fprintln(unzippedFile, string(job.EventPayload))
			misc.AssertError(err)
		}
	}
	unzippedFile.Close()

	zipFilePath := fmt.Sprintf(`%v.gz`, path)
	err = misc.ZipFiles(zipFilePath, []string{path})
	misc.AssertError(err)

	zipFile, err := os.Open(zipFilePath)
	defer zipFile.Close()

	bucketName := batchJobs.BatchDestination.Destination.Config.(map[string]interface{})["bucketName"].(string)
	uploader, err := fileuploader.NewFileUploader(&fileuploader.SettingsT{
		Provider:       "s3",
		AmazonS3Bucket: bucketName,
	})
	err = uploader.Upload(zipFile, config.GetEnv("DESTINATION_S3_BUCKET_FOLDER_NAME", "rudder-logs"), batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006"))
	var jobState string
	if err != nil {
		logger.Debug(err)
		jobState = jobsdb.FailedState
	} else {
		jobState = jobsdb.SucceededState
	}

	var statusList []*jobsdb.JobStatusT

	//Identify jobs which can be processed
	for _, job := range batchJobs.Jobs {
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			JobState:      jobState,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "",
			ErrorResponse: []byte(`{}`), // check
		}
		statusList = append(statusList, &status)
	}

	//Mark the jobs as executing
	brt.jobsDB.UpdateJobStatus(statusList, []string{batchJobs.BatchDestination.Destination.DestinationDefinition.Name})

	err = os.Remove(zipFilePath)
	misc.AssertError(err)
	err = os.Remove(path)
	misc.AssertError(err)
}

func (brt *HandleT) initWorkers() {
	for i := 0; i < noOfWorkers; i++ {
		go func() {
			for {
				select {
				case batchJobs := <-brt.processQ:
					switch batchJobs.BatchDestination.Destination.DestinationDefinition.Name {
					case "S3":
						brt.copyJobsToS3(batchJobs)
						delete(inProgressMap, batchJobs.BatchDestination.Source.ID)
					}
				}
			}
		}()
	}
}

type BatchDestinationT struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type BatchJobsT struct {
	Jobs             []*jobsdb.JobT
	BatchDestination BatchDestinationT
}

func (brt *HandleT) mainLoop() {
	for {
		if !brt.isEnabled {
			time.Sleep(time.Duration(2*mainLoopSleepInS) * time.Second)
			continue
		}
		time.Sleep(time.Duration(mainLoopSleepInS) * time.Second)
		for _, batchDestination := range batchDestinations {
			if inProgressMap[batchDestination.Source.ID] {
				continue
			}
			toQuery := jobQueryBatchSize
			retryList := brt.jobsDB.GetToRetry([]string{batchDestination.Destination.DestinationDefinition.Name}, toQuery, batchDestination.Source.ID)
			toQuery -= len(retryList)
			waitList := brt.jobsDB.GetWaiting([]string{batchDestination.Destination.DestinationDefinition.Name}, toQuery, batchDestination.Source.ID) //Jobs send to waiting state
			toQuery -= len(waitList)
			unprocessedList := brt.jobsDB.GetUnprocessed([]string{batchDestination.Destination.DestinationDefinition.Name}, toQuery, batchDestination.Source.ID)
			if len(waitList)+len(unprocessedList)+len(retryList) == 0 {
				continue
			}

			combinedList := append(waitList, append(unprocessedList, retryList...)...)

			var statusList []*jobsdb.JobStatusT

			//Identify jobs which can be processed
			for _, job := range combinedList {
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum,
					JobState:      jobsdb.ExecutingState,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: []byte(`{}`), // check
				}
				statusList = append(statusList, &status)
			}

			//Mark the jobs as executing
			brt.jobsDB.UpdateJobStatus(statusList, []string{batchDestination.Destination.DestinationDefinition.Name})
			inProgressMap[batchDestination.Source.ID] = true
			brt.processQ <- BatchJobsT{Jobs: combinedList, BatchDestination: batchDestination}
		}
	}
}

//Enable enables a router :)
func (brt *HandleT) Enable() {
	brt.isEnabled = true
}

//Disable disables a router:)
func (brt *HandleT) Disable() {
	brt.isEnabled = false
}

func (brt *HandleT) crashRecover() {

	for {
		execList := brt.jobsDB.GetExecuting([]string{}, jobQueryBatchSize)

		if len(execList) == 0 {
			break
		}
		logger.Debug("Batch Router crash recovering", len(execList))

		var statusList []*jobsdb.JobStatusT

		for _, job := range execList {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				JobState:      jobsdb.FailedState,
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`), // check
			}
			statusList = append(statusList, &status)
		}
		brt.jobsDB.UpdateJobStatus(statusList, []string{})
	}
}

func loadConfig() {
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize", 10000)
	noOfWorkers = config.GetInt("BatchRouter.noOfWorkers", 8)
	mainLoopSleepInS = config.GetInt("BatchRouter.mainLoopSleepInS", 5)
	rawDataDestinations = []string{"S3"}
	inProgressMap = map[string]bool{}
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes this module
func (brt *HandleT) Setup(jobsDB *jobsdb.HandleT) {
	logger.Info("Batch Router started")
	brt.jobsDB = jobsDB
	brt.processQ = make(chan BatchJobsT)
	brt.crashRecover()
	brt.isEnabled = false

	go brt.initWorkers()
	go backendConfigSubscriber()
	go brt.mainLoop()
}
