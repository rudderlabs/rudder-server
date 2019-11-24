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
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
)

var (
	jobQueryBatchSize        int
	noOfWorkers              int
	mainLoopSleepInS         int
	batchDestinations        []BatchDestinationT
	configSubscriberLock     sync.RWMutex
	rawDataDestinations      []string
	inProgressMap            map[string]bool
	inProgressMapLock        sync.RWMutex
	uploadedRawDataJobsCache map[string]bool
	errorsCountStat          *stats.RudderStats
)

type HandleT struct {
	processQ  chan BatchJobsT
	jobsDB    *jobsdb.HandleT
	isEnabled bool
}

type ObjectStorageT struct {
	Bucket   string
	Key      string
	Provider string
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

func (brt *HandleT) copyJobsToStorage(provider string, batchJobs BatchJobsT) {
	bucketName := batchJobs.BatchDestination.Destination.Config.(map[string]interface{})["bucketName"].(string)
	uuid := uuid.NewV4()
	logger.Debugf("BRT: Starting logging to %s: %s", provider, bucketName)

	dirName := "/rudder-raw-data-destination-logs/"
	tmpdirPath := strings.TrimSuffix(config.GetEnv("RUDDER_TMPDIR", ""), "/")
	var err error
	if tmpdirPath == "" {
		tmpdirPath, err = os.UserHomeDir()
		misc.AssertError(err)
	}
	path := fmt.Sprintf("%v%v.json", tmpdirPath+dirName, fmt.Sprintf("%v.%v.%v", time.Now().Unix(), batchJobs.BatchDestination.Source.ID, uuid))
	var contentSlice [][]byte
	for _, job := range batchJobs.Jobs {
		eventID := gjson.GetBytes(job.EventPayload, "messageId").String()
		misc.AssertError(err)
		var ok bool
		if _, ok = uploadedRawDataJobsCache[eventID]; !ok {
			contentSlice = append(contentSlice, job.EventPayload)
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

	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: provider,
		Bucket:   bucketName,
	})
	gzipFile, err = os.Open(gzipFilePath)
	misc.AssertError(err)

	logger.Debugf("BRT: Starting upload to %s: %s", provider, bucketName)
	keyPrefixes := []string{config.GetEnv("DESTINATION_S3_BUCKET_FOLDER_NAME", "rudder-logs"), batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006")}
	_, fileName := filepath.Split(gzipFilePath)
	opPayload, err := json.Marshal(&ObjectStorageT{
		Bucket:   bucketName,
		Key:      strings.Join(append(keyPrefixes, fileName), "/"),
		Provider: provider,
	})
	opID := brt.jobsDB.JournalMarkStart(jobsdb.RawDataDestUploadOperation, opPayload)
	err = uploader.Upload(gzipFile, keyPrefixes...)
	var (
		jobState  string
		errorResp []byte
	)
	if err != nil {
		logger.Errorf("BRT: Error uploading to %s: %v", provider, err)
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
	brt.jobsDB.UpdateJobStatus(statusList, []string{batchJobs.BatchDestination.Destination.DestinationDefinition.Name}, batchJobs.BatchDestination.Source.ID)
	brt.jobsDB.JournalDeleteEntry(opID)

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
						brt.copyJobsToStorage("S3", batchJobs)
						s3DestUploadStat.End()
						setSourceInProgress(batchJobs.BatchDestination.Source.ID, batchJobs.BatchDestination.Destination.ID, false)
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

func isSourceInProgress(sourceID, destinationID string) bool {
	inProgressMapLock.RLock()
	if inProgressMap[sourceID+"-"+destinationID] {
		inProgressMapLock.RUnlock()
		return true
	}
	inProgressMapLock.RUnlock()
	return false
}

func setSourceInProgress(sourceID, destinationID string, starting bool) {
	inProgressMapLock.Lock()
	if starting {
		inProgressMap[sourceID+"-"+destinationID] = true
	} else {
		delete(inProgressMap, sourceID+"-"+destinationID)
	}
	inProgressMapLock.Unlock()
}

func (brt *HandleT) mainLoop() {
	for {
		if !brt.isEnabled {
			time.Sleep(time.Duration(2*mainLoopSleepInS) * time.Second)
			continue
		}
		time.Sleep(time.Duration(mainLoopSleepInS) * time.Second)
		for _, batchDestination := range batchDestinations {
			if isSourceInProgress(batchDestination.Source.ID, batchDestination.Destination.ID) {
				continue
			}
			setSourceInProgress(batchDestination.Source.ID, batchDestination.Destination.ID, true)
			toQuery := jobQueryBatchSize

			retryList := brt.jobsDB.GetToRetry([]string{batchDestination.Destination.DestinationDefinition.Name}, toQuery, batchDestination.Source.ID+"-"+batchDestination.Destination.ID)
			toQuery -= len(retryList)
			waitList := brt.jobsDB.GetWaiting([]string{batchDestination.Destination.DestinationDefinition.Name}, toQuery, batchDestination.Source.ID+"-"+batchDestination.Destination.ID) //Jobs send to waiting state
			toQuery -= len(waitList)
			unprocessedList := brt.jobsDB.GetUnprocessed([]string{batchDestination.Destination.DestinationDefinition.Name}, toQuery, batchDestination.Source.ID+"-"+batchDestination.Destination.ID)

			if len(waitList)+len(unprocessedList)+len(retryList) == 0 {
				delete(inProgressMap, batchDestination.Source.ID+"-"+batchDestination.Destination.ID)
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
			brt.jobsDB.UpdateJobStatus(statusList, []string{batchDestination.Destination.DestinationDefinition.Name}, batchDestination.Source.ID+"-"+batchDestination.Destination.ID)
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

func (brt *HandleT) dedupRawDataDestJobsOnCrash() {
	logger.Debugf("BRT: Checking for incomplete journal entries to recover from...")
	entries := brt.jobsDB.GetJournalEntries(jobsdb.RawDataDestUploadOperation)
	for _, entry := range entries {
		var object ObjectStorageT
		err := json.Unmarshal(entry.OpPayload, &object)
		misc.AssertError(err)
		downloader, err := filemanager.New(&filemanager.SettingsT{
			Provider: object.Provider,
			Bucket:   object.Bucket,
		})

		dirName := "/rudder-raw-data-dest-upload-crash-recovery/"
		tmpdirPath := strings.TrimSuffix(config.GetEnv("RUDDER_TMPDIR", ""), "/")
		if tmpdirPath == "" {
			tmpdirPath, err = os.UserHomeDir()
			misc.AssertError(err)
		}
		jsonPath := fmt.Sprintf("%v%v.json", tmpdirPath+dirName, fmt.Sprintf("%v.%v", time.Now().Unix(), uuid.NewV4().String()))

		err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
		jsonFile, err := os.Create(jsonPath)
		misc.AssertError(err)

		logger.Debugf("BRT: Downloading data for incomplete journal entry to recover from %s in bucket: %s at key: %s", object.Provider, object.Bucket, object.Key)
		err = downloader.Download(jsonFile, object.Key)
		if err != nil {
			logger.Debugf("BRT: Failed to download data for incomplete journal entry to recover from %s in bucket: %s at key: %s with error: %v", object.Provider, object.Bucket, object.Key, err)
			continue
		}

		jsonFile.Close()
		defer os.Remove(jsonPath)

		rawf, err := os.Open(jsonPath)
		reader, _ := gzip.NewReader(rawf)

		sc := bufio.NewScanner(reader)

		logger.Debugf("BRT: Setting go map cache for incomplete journal entry to recover from...")
		for sc.Scan() {
			lineBytes := sc.Bytes()
			eventID := gjson.GetBytes(lineBytes, "messageId").String()
			uploadedRawDataJobsCache[eventID] = true
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
	brt.dedupRawDataDestJobsOnCrash()
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
	uploadedRawDataJobsCache = make(map[string]bool)
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
