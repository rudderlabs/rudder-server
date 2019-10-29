package batchrouter

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
)

var (
	jobQueryBatchSize    int
	noOfWorkers          int
	mainLoopSleepInS     int
	batchDestinations    []BatchDestinationT
	configSubscriberLock sync.RWMutex
	rawDataDestinations  []string
	inProgressMap        map[string]bool
	uploadedS3JobsCache  map[string]bool
	errorsCountStat      *stats.RudderStats
)

type HandleT struct {
	processQ  chan BatchJobsT
	jobsDB    *jobsdb.HandleT
	isEnabled bool
}

type S3ObjectT struct {
	Bucket string
	Key    string
}

func backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch)
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

type ErrorResponseT struct {
	Error string
}

func updateDestStatusStats(id string, count int, isSuccess bool) {
	var destStatsD *stats.RudderStats
	if isSuccess {
		destStatsD = stats.NewBatchDestStat("batch_router.dest_successful_events", stats.CountType, id)
	} else {
		destStatsD = stats.NewBatchDestStat("batch_router.dest_failed_attempts", stats.CountType, id)
		errorsCountStat.Count(count)
	}
	destStatsD.Count(count)
}

func (brt *HandleT) copyJobsToS3(batchJobs BatchJobsT) {
	bucketName := batchJobs.BatchDestination.Destination.Config.(map[string]interface{})["bucketName"].(string)
	uuid := uuid.NewV4()
	logger.Debugf("BRT: Starting logging to S3 bucket: %v", bucketName)

	dirName := "/rudder-s3-destination-logs/"
	tmpdirPath := strings.TrimSuffix(config.GetEnv("RUDDER_TMPDIR", ""), "/")
	var err error
	if tmpdirPath == "" {
		tmpdirPath, err = os.UserHomeDir()
		misc.AssertError(err)
	}
	path := fmt.Sprintf("%v%v.json", tmpdirPath+dirName, fmt.Sprintf("%v.%v.%v", time.Now().Unix(), batchJobs.BatchDestination.Source.ID, uuid))
	var contentSlice [][]byte
	for _, job := range batchJobs.Jobs {
		trimmedPayload := bytes.TrimLeft(job.EventPayload, " \t\r\n")
		isArray := len(trimmedPayload) > 0 && trimmedPayload[0] == '['
		if isArray {
			var events []interface{}
			err := json.Unmarshal(trimmedPayload, &events)
			misc.AssertError(err)
			for _, event := range events {
				jsonEvent, err := json.Marshal((event))
				eventID := gjson.GetBytes(jsonEvent, "messageId").String()
				misc.AssertError(err)
				var ok bool
				if _, ok = uploadedS3JobsCache[eventID]; !ok {
					contentSlice = append(contentSlice, jsonEvent)
				}
			}
		} else {
			eventID := gjson.GetBytes(job.EventPayload, "messageId").String()
			misc.AssertError(err)
			var ok bool
			if _, ok = uploadedS3JobsCache[eventID]; !ok {
				contentSlice = append(contentSlice, job.EventPayload)
			}
		}
	}
	content := bytes.Join(contentSlice[:], []byte("\n"))

	gzipFilePath := fmt.Sprintf(`%v.gz`, path)
	err = os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	misc.AssertError(err)
	gzipFile, err := os.Create(gzipFilePath)

	gzipWriter := gzip.NewWriter(gzipFile)
	_, err = gzipWriter.Write(content)
	misc.AssertError(err)
	gzipWriter.Close()

	logger.Debugf("BRT: Logged to local file: %v", gzipFilePath)

	uploader, err := fileuploader.NewFileUploader(&fileuploader.SettingsT{
		Provider:       "s3",
		AmazonS3Bucket: bucketName,
	})
	gzipFile, err = os.Open(gzipFilePath)
	misc.AssertError(err)

	logger.Debugf("BRT: Starting upload to S3 bucket: %v", bucketName)
	keyPrefixes := []string{config.GetEnv("DESTINATION_S3_BUCKET_FOLDER_NAME", "rudder-logs"), batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006")}
	_, fileName := filepath.Split(gzipFilePath)
	opPayload, err := json.Marshal(&S3ObjectT{
		Bucket: bucketName,
		Key:    strings.Join(append(keyPrefixes, fileName), "/"),
	})
	opID := brt.jobsDB.JournalMarkStart(jobsdb.S3DestUploadOperation, opPayload)
	err = uploader.Upload(gzipFile, keyPrefixes...)
	var (
		jobState  string
		errorResp []byte
	)
	brt.jobsDB.JournalMarkDone(opID)
	if err != nil {
		logger.Errorf("BRT: %v", err)
		jobState = jobsdb.FailedState
		errorResp, _ = json.Marshal(ErrorResponseT{Error: err.Error()})
		// We keep track of number of failed attempts in case of failure and number of events uploaded in case of success in stats
		updateDestStatusStats(batchJobs.BatchDestination.Destination.ID, 1, false)
	} else {
		logger.Debugf("BRT: Uploaded to S3 bucket: %v %v %v", bucketName, batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006"))
		jobState = jobsdb.SucceededState
		errorResp = []byte(`{"success":"OK"}`)
		updateDestStatusStats(batchJobs.BatchDestination.Destination.ID, len(batchJobs.Jobs), true)
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
			ErrorResponse: errorResp,
		}
		statusList = append(statusList, &status)
	}

	//Mark the jobs as executing
	brt.jobsDB.UpdateJobStatus(statusList, []string{batchJobs.BatchDestination.Destination.DestinationDefinition.Name})

	err = os.Remove(gzipFilePath)
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
						s3DestUploadStat := stats.NewStat("batch_router.S3_dest_upload_time", stats.TimerType)
						s3DestUploadStat.Start()
						brt.copyJobsToS3(batchJobs)
						s3DestUploadStat.End()
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
			inProgressMap[batchDestination.Source.ID] = true
			toQuery := jobQueryBatchSize
			retryList := brt.jobsDB.GetToRetry([]string{batchDestination.Destination.DestinationDefinition.Name}, toQuery, batchDestination.Source.ID)
			toQuery -= len(retryList)
			waitList := brt.jobsDB.GetWaiting([]string{batchDestination.Destination.DestinationDefinition.Name}, toQuery, batchDestination.Source.ID) //Jobs send to waiting state
			toQuery -= len(waitList)
			unprocessedList := brt.jobsDB.GetUnprocessed([]string{batchDestination.Destination.DestinationDefinition.Name}, toQuery, batchDestination.Source.ID)
			if len(waitList)+len(unprocessedList)+len(retryList) == 0 {
				delete(inProgressMap, batchDestination.Source.ID)
				continue
			}

			combinedList := append(waitList, append(unprocessedList, retryList...)...)

			var statusList []*jobsdb.JobStatusT

			//Identify jobs which can be processed
			for _, job := range combinedList {
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum + 1,
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

func (brt *HandleT) dedupS3DestJobsOnCrash() {
	entries := brt.jobsDB.GetJouranlEntries(jobsdb.S3DestUploadOperation)
	for _, entry := range entries {
		var s3Object S3ObjectT
		err := json.Unmarshal(entry.OpPayload, &s3Object)
		misc.AssertError(err)
		downloader, err := fileuploader.NewFileUploader(&fileuploader.SettingsT{
			Provider:       "s3",
			AmazonS3Bucket: s3Object.Bucket,
		})

		dirName := "/rudder-s3-dest-upload-crash-recovery/"
		tmpdirPath := strings.TrimSuffix(config.GetEnv("RUDDER_TMPDIR", ""), "/")
		if tmpdirPath == "" {
			tmpdirPath, err = os.UserHomeDir()
			misc.AssertError(err)
		}
		jsonPath := fmt.Sprintf("%v%v.json", tmpdirPath+dirName, fmt.Sprintf("%v.%v", time.Now().Unix(), uuid.NewV4().String()))

		err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
		jsonFile, err := os.Create(jsonPath)
		misc.AssertError(err)
		err = downloader.Download(jsonFile, s3Object.Key)
		if err != nil {
			continue
		}

		jsonFile.Close()
		defer os.Remove(jsonPath)

		rawf, err := os.Open(jsonPath)
		reader, _ := gzip.NewReader(rawf)

		sc := bufio.NewScanner(reader)

		for sc.Scan() {
			lineBytes := sc.Bytes()
			eventID := gjson.GetBytes(lineBytes, "messageId").String()
			uploadedS3JobsCache[eventID] = true
		}
	}
}

func (brt *HandleT) crashRecover() {

	for {
		execList := brt.jobsDB.GetExecuting([]string{}, jobQueryBatchSize)

		if len(execList) == 0 {
			break
		}
		logger.Debug("BRT: Batch Router crash recovering", len(execList))

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
	brt.dedupS3DestJobsOnCrash()
}

func loadConfig() {
	jobQueryBatchSize = config.GetInt("BatchRouter.jobQueryBatchSize", 100000)
	noOfWorkers = config.GetInt("BatchRouter.noOfWorkers", 8)
	mainLoopSleepInS = config.GetInt("BatchRouter.mainLoopSleepInS", 5)
	rawDataDestinations = []string{"S3"}
	inProgressMap = map[string]bool{}
}

func init() {
	config.Initialize()
	loadConfig()
	uploadedS3JobsCache = make(map[string]bool)
	errorsCountStat = stats.NewStat("batch_router.errors", stats.CountType)
}

//Setup initializes this module
func (brt *HandleT) Setup(jobsDB *jobsdb.HandleT) {
	logger.Info("BRT: Batch Router started")
	brt.jobsDB = jobsDB
	brt.processQ = make(chan BatchJobsT)
	brt.crashRecover()

	go brt.initWorkers()
	go backendConfigSubscriber()
	go brt.mainLoop()
}
