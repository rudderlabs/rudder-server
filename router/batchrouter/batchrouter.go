package batchrouter

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/services/diagnostics"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/destination-debugger"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
)

var (
	jobQueryBatchSize                  int
	noOfWorkers                        int
	maxFailedCountForJob               int
	mainLoopSleep, diagnosisTickerTime time.Duration
	uploadFreqInS                      int64
	configSubscriberLock               sync.RWMutex
	objectStorageDestinations          []string
	warehouseDestinations              []string
	inProgressMap                      map[string]bool
	inProgressMapLock                  sync.RWMutex
	lastExecMap                        map[string]int64
	lastExecMapLock                    sync.RWMutex
	uploadedRawDataJobsCache           map[string]map[string]bool
	warehouseURL                       string
	warehouseMode                      string
	warehouseServiceFailedTime         time.Time
	warehouseServiceFailedTimeLock     sync.RWMutex
	warehouseServiceMaxRetryTimeinHr   time.Duration
)

type HandleT struct {
	destType                string
	batchDestinations       []DestinationT
	netHandle               *http.Client
	processQ                chan BatchJobsT
	jobsDB                  *jobsdb.HandleT
	isEnabled               bool
	batchRequestsMetricLock sync.RWMutex
	diagnosisTicker         *time.Ticker
	batchRequestsMetric     []batchRequestMetric
}

type ObjectStorageT struct {
	Config          map[string]interface{}
	Key             string
	Provider        string
	DestinationID   string
	DestinationType string
}

func (brt *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		brt.batchDestinations = []DestinationT{}
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.DestinationDefinition.Name == brt.destType {
						brt.batchDestinations = append(brt.batchDestinations, DestinationT{Source: source, Destination: destination})
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

type batchRequestMetric struct {
	batchRequestSuccess int
	batchRequestFailed  int
}

type StorageUploadOutput struct {
	Config         map[string]interface{}
	Key            string
	LocalFilePaths []string
	JournalOpID    int64
	Error          error
	FirstEventAt   string
	LastEventAt    string
	TotalEvents    int
}

type ErrorResponseT struct {
	Error string
}

func updateDestStatusStats(id string, count int, isSuccess bool) {
	var destStatsD stats.RudderStats
	if isSuccess {
		destStatsD = stats.NewBatchDestStat("batch_router.dest_successful_events", stats.CountType, id)
	} else {
		destStatsD = stats.NewBatchDestStat("batch_router.dest_failed_attempts", stats.CountType, id)
	}
	destStatsD.Count(count)
}

func (brt *HandleT) copyJobsToStorage(provider string, batchJobs BatchJobsT, makeJournalEntry bool, isWarehouse bool) StorageUploadOutput {
	var localTmpDirName string
	if isWarehouse {
		localTmpDirName = "/rudder-warehouse-staging-uploads/"
	} else {
		localTmpDirName = "/rudder-raw-data-destination-logs/"
	}

	uuid := uuid.NewV4()
	logger.Debugf("BRT: Starting logging to %s", provider)

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	path := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v.%v", time.Now().Unix(), batchJobs.BatchDestination.Source.ID, uuid))

	gzipFilePath := fmt.Sprintf(`%v.gz`, path)
	err = os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	gzWriter, err := misc.CreateGZ(gzipFilePath)
	if err != nil {
		panic(err)
	}

	eventsFound := false
	for _, job := range batchJobs.Jobs {
		eventID := gjson.GetBytes(job.EventPayload, "messageId").String()
		var ok bool
		interruptedEventsMap, isDestInterrupted := uploadedRawDataJobsCache[batchJobs.BatchDestination.Destination.ID]
		if isDestInterrupted {
			if _, ok = interruptedEventsMap[eventID]; !ok {
				eventsFound = true
				gzWriter.WriteGZ(fmt.Sprintf(`%s`, job.EventPayload) + "\n")
			}
		} else {
			eventsFound = true
			gzWriter.WriteGZ(fmt.Sprintf(`%s`, job.EventPayload) + "\n")
		}
	}
	gzWriter.CloseGZ()
	if !isWarehouse && !eventsFound {
		logger.Infof("BRT: All events in this batch for %s are de-deuplicated...", provider)
		return StorageUploadOutput{
			LocalFilePaths: []string{gzipFilePath},
		}
	}
	// assumes events from warehouse have receivedAt in metadata
	var firstEventAt, lastEventAt string
	if isWarehouse {
		firstEventAt = gjson.GetBytes(batchJobs.Jobs[0].EventPayload, "metadata.receivedAt").String()
		lastEventAt = gjson.GetBytes(batchJobs.Jobs[len(batchJobs.Jobs)-1].EventPayload, "metadata.receivedAt").String()
	}

	logger.Debugf("BRT: Logged to local file: %v", gzipFilePath)
	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: provider,
		Config:   misc.GetObjectStorageConfig(provider, batchJobs.BatchDestination.Destination.Config),
	})
	if err != nil {
		panic(err)
	}

	outputFile, err := os.Open(gzipFilePath)
	if err != nil {
		panic(err)
	}

	logger.Debugf("BRT: Starting upload to %s", provider)

	var keyPrefixes []string
	if isWarehouse {
		keyPrefixes = []string{config.GetEnv("WAREHOUSE_STAGING_BUCKET_FOLDER_NAME", "rudder-warehouse-staging-logs"), batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006")}
	} else {
		keyPrefixes = []string{config.GetEnv("DESTINATION_BUCKET_FOLDER_NAME", "rudder-logs"), batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006")}
	}

	_, fileName := filepath.Split(gzipFilePath)
	var (
		opID      int64
		opPayload json.RawMessage
	)
	if !isWarehouse {
		opPayload, _ = json.Marshal(&ObjectStorageT{
			Config:          batchJobs.BatchDestination.Destination.Config,
			Key:             strings.Join(append(keyPrefixes, fileName), "/"),
			Provider:        provider,
			DestinationID:   batchJobs.BatchDestination.Destination.ID,
			DestinationType: batchJobs.BatchDestination.Destination.DestinationDefinition.Name,
		})
		opID = brt.jobsDB.JournalMarkStart(jobsdb.RawDataDestUploadOperation, opPayload)
	}
	uploadOutput, err := uploader.Upload(outputFile, keyPrefixes...)

	if err != nil {
		logger.Errorf("BRT: Error uploading to %s: Error: %v", provider, err)
		return StorageUploadOutput{
			Error:       err,
			JournalOpID: opID,
		}
	}

	return StorageUploadOutput{
		Config:         batchJobs.BatchDestination.Destination.Config,
		Key:            uploadOutput.ObjectName,
		LocalFilePaths: []string{gzipFilePath},
		JournalOpID:    opID,
		FirstEventAt:   firstEventAt,
		LastEventAt:    lastEventAt,
		TotalEvents:    len(batchJobs.Jobs),
	}
}

func (brt *HandleT) postToWarehouse(batchJobs BatchJobsT, output StorageUploadOutput) (err error) {
	schemaMap := make(map[string]map[string]interface{})
	for _, job := range batchJobs.Jobs {
		var payload map[string]interface{}
		err := json.Unmarshal(job.EventPayload, &payload)
		if err != nil {
			panic(err)
		}
		tableName := payload["metadata"].(map[string]interface{})["table"].(string)
		var ok bool
		if _, ok = schemaMap[tableName]; !ok {
			schemaMap[tableName] = make(map[string]interface{})
		}
		columns := payload["metadata"].(map[string]interface{})["columns"].(map[string]interface{})
		for columnName, columnType := range columns {
			if _, ok := schemaMap[tableName][columnName]; !ok {
				schemaMap[tableName][columnName] = columnType
			}
		}
	}
	payload := warehouseutils.StagingFileT{
		Schema: schemaMap,
		BatchDestination: warehouseutils.DestinationT{
			Source:      batchJobs.BatchDestination.Source,
			Destination: batchJobs.BatchDestination.Destination,
		},
		Location:     output.Key,
		FirstEventAt: output.FirstEventAt,
		LastEventAt:  output.LastEventAt,
		TotalEvents:  output.TotalEvents,
	}

	jsonPayload, err := json.Marshal(&payload)

	uri := fmt.Sprintf(`%s/v1/process`, warehouseURL)
	_, err = brt.netHandle.Post(uri, "application/json; charset=utf-8",
		bytes.NewBuffer(jsonPayload))
	if err != nil {
		logger.Errorf("BRT: Failed to route staging file URL to warehouse service@%v, error:%v", uri, err)
	} else {
		logger.Infof("BRT: Routed successfully staging file URL to warehouse service@%v", uri)
	}
	return
}

func (brt *HandleT) setJobStatus(batchJobs BatchJobsT, isWarehouse bool, err error, postToWarehouseErr bool) {
	var (
		batchJobState string
		errorResp     []byte
	)

	var batchReqMetric batchRequestMetric
	if err != nil {
		logger.Errorf("BRT: Error uploading to object storage: %v %v", err, batchJobs.BatchDestination.Source.ID)
		batchJobState = jobsdb.Failed.State
		errorResp, _ = json.Marshal(ErrorResponseT{Error: err.Error()})
		batchReqMetric.batchRequestFailed = 1
		// We keep track of number of failed attempts in case of failure and number of events uploaded in case of success in stats
		updateDestStatusStats(batchJobs.BatchDestination.Destination.ID, 1, false)
	} else {
		logger.Debugf("BRT: Uploaded to object storage : %v at %v", batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006"))
		batchJobState = jobsdb.Succeeded.State
		errorResp = []byte(`{"success":"OK"}`)
		batchReqMetric.batchRequestSuccess = 1
		updateDestStatusStats(batchJobs.BatchDestination.Destination.ID, len(batchJobs.Jobs), true)
	}
	brt.trackRequestMetrics(batchReqMetric)
	var statusList []*jobsdb.JobStatusT

	if isWarehouse && postToWarehouseErr {
		warehouseServiceFailedTimeLock.Lock()
		if warehouseServiceFailedTime.IsZero() {
			warehouseServiceFailedTime = time.Now()
		}
		warehouseServiceFailedTimeLock.Unlock()
	} else if isWarehouse {
		warehouseServiceFailedTimeLock.Lock()
		warehouseServiceFailedTime = time.Time{}
		warehouseServiceFailedTimeLock.Unlock()
	}

	for _, job := range batchJobs.Jobs {
		jobState := batchJobState

		if jobState == jobsdb.Failed.State && job.LastJobStatus.AttemptNum >= maxFailedCountForJob && !postToWarehouseErr {
			jobState = jobsdb.Aborted.State
		} else {
			// change job state to abort state after warehouse service is continuously failing more than warehouseServiceMaxRetryTimeinHr time
			if jobState == jobsdb.Failed.State && isWarehouse && postToWarehouseErr {
				warehouseServiceFailedTimeLock.RLock()
				if time.Now().Sub(warehouseServiceFailedTime) > warehouseServiceMaxRetryTimeinHr {
					jobState = jobsdb.Aborted.State
				}
				warehouseServiceFailedTimeLock.RUnlock()
			}
		}
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum + 1,
			JobState:      jobState,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "",
			ErrorResponse: errorResp,
		}
		statusList = append(statusList, &status)
	}

	//tracking batch router errors
	if diagnostics.EnableDestinationFailuresMetric {
		if batchJobState == jobsdb.Failed.State {
			diagnostics.Track(diagnostics.BatchRouterFailed, map[string]interface{}{
				diagnostics.BatchRouterDestination: brt.destType,
				diagnostics.ErrorResponse:          string(errorResp),
			})
		}
	}

	parameterFilters := []jobsdb.ParameterFilterT{
		jobsdb.ParameterFilterT{
			Name:  "source_id",
			Value: batchJobs.BatchDestination.Source.ID,
		},
		jobsdb.ParameterFilterT{
			Name:     "destination_id",
			Value:    batchJobs.BatchDestination.Destination.ID,
			Optional: true,
		},
	}
	//Mark the status of the jobs
	brt.jobsDB.UpdateJobStatus(statusList, []string{brt.destType}, parameterFilters)
}

func (brt *HandleT) trackRequestMetrics(batchReqDiagnostics batchRequestMetric) {
	if diagnostics.EnableBatchRouterMetric {
		brt.batchRequestsMetricLock.Lock()
		if brt.batchRequestsMetric == nil {
			var batchRequestsMetric []batchRequestMetric
			brt.batchRequestsMetric = append(batchRequestsMetric, batchReqDiagnostics)
		} else {
			brt.batchRequestsMetric = append(brt.batchRequestsMetric, batchReqDiagnostics)
		}
		brt.batchRequestsMetricLock.Unlock()
	}
}

func (brt *HandleT) recordDeliveryStatus(batchDestination DestinationT, err error, isWarehouse bool) {
	var (
		jobState  string
		errorResp []byte
	)

	if err != nil {
		jobState = jobsdb.Failed.State
		if isWarehouse {
			jobState = warehouseutils.GeneratingStagingFileFailed
		}
		errorResp, _ = json.Marshal(ErrorResponseT{Error: err.Error()})
	} else {
		jobState = jobsdb.Succeeded.State
		if isWarehouse {
			jobState = warehouseutils.GeneratedStagingFile
		}
		errorResp = []byte(`{"success":"OK"}`)
	}

	//Payload and AttemptNum don't make sense in recording batch router delivery status,
	//So they are set to default values.
	deliveryStatus := destinationdebugger.DeliveryStatusT{
		DestinationID: batchDestination.Destination.ID,
		SourceID:      batchDestination.Source.ID,
		Payload:       []byte(`{}`),
		AttemptNum:    1,
		JobState:      jobState,
		ErrorCode:     "",
		ErrorResponse: errorResp,
	}
	destinationdebugger.RecordEventDeliveryStatus(batchDestination.Destination.ID, &deliveryStatus)
}

func (brt *HandleT) initWorkers() {
	for i := 0; i < noOfWorkers; i++ {
		rruntime.Go(func() {
			func() {
				for {
					select {
					case batchJobs := <-brt.processQ:
						switch {
						case misc.ContainsString(objectStorageDestinations, brt.destType):
							destUploadStat := stats.NewStat(fmt.Sprintf(`batch_router.%s_dest_upload_time`, brt.destType), stats.TimerType)
							destUploadStat.Start()
							output := brt.copyJobsToStorage(brt.destType, batchJobs, true, false)
							brt.recordDeliveryStatus(batchJobs.BatchDestination, output.Error, false)
							brt.setJobStatus(batchJobs, false, output.Error, false)
							misc.RemoveFilePaths(output.LocalFilePaths...)
							if output.JournalOpID > 0 {
								brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
							}
							destUploadStat.End()
							setDestInProgress(batchJobs.BatchDestination, false)
						case misc.ContainsString(warehouseDestinations, brt.destType):
							objectStorageType := warehouseutils.ObjectStorageType(brt.destType, batchJobs.BatchDestination.Destination.Config)
							destUploadStat := stats.NewStat(fmt.Sprintf(`batch_router.%s_%s_dest_upload_time`, brt.destType, objectStorageType), stats.TimerType)
							destUploadStat.Start()
							output := brt.copyJobsToStorage(objectStorageType, batchJobs, true, true)
							postToWarehouseErr := false
							if output.Error == nil && output.Key != "" {
								output.Error = brt.postToWarehouse(batchJobs, output)
								if output.Error != nil {
									postToWarehouseErr = true
								}
								warehouseutils.DestStat(stats.CountType, "generate_staging_files", batchJobs.BatchDestination.Destination.ID).Count(1)
								warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", batchJobs.BatchDestination.Destination.ID).Count(len(batchJobs.Jobs))
							}
							brt.recordDeliveryStatus(batchJobs.BatchDestination, output.Error, true)
							brt.setJobStatus(batchJobs, true, output.Error, postToWarehouseErr)
							misc.RemoveFilePaths(output.LocalFilePaths...)
							destUploadStat.End()
							setDestInProgress(batchJobs.BatchDestination, false)
						}

					}
				}
			}()
		})
	}
}

type DestinationT struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type BatchJobsT struct {
	Jobs             []*jobsdb.JobT
	BatchDestination DestinationT
}

func connectionString(batchDestination DestinationT) string {
	return fmt.Sprintf(`source:%s:destination:%s`, batchDestination.Source.ID, batchDestination.Destination.ID)
}

func isDestInProgress(batchDestination DestinationT) bool {
	inProgressMapLock.RLock()
	if inProgressMap[connectionString(batchDestination)] {
		inProgressMapLock.RUnlock()
		return true
	}
	inProgressMapLock.RUnlock()
	return false
}

func setDestInProgress(batchDestination DestinationT, starting bool) {
	inProgressMapLock.Lock()
	if starting {
		inProgressMap[connectionString(batchDestination)] = true
	} else {
		delete(inProgressMap, connectionString(batchDestination))
	}
	inProgressMapLock.Unlock()
}

func uploadFrequencyExceeded(batchDestination DestinationT) bool {
	lastExecMapLock.Lock()
	defer lastExecMapLock.Unlock()
	if lastExecTime, ok := lastExecMap[connectionString(batchDestination)]; ok && time.Now().Unix()-lastExecTime < uploadFreqInS {
		return true
	}
	lastExecMap[connectionString(batchDestination)] = time.Now().Unix()
	return false
}

func (brt *HandleT) mainLoop() {
	for {
		time.Sleep(mainLoopSleep)
		for _, batchDestination := range brt.batchDestinations {
			if isDestInProgress(batchDestination) {
				logger.Debugf("BRT: Skipping batch router upload loop since destination %s:%s is in progress", batchDestination.Destination.DestinationDefinition.Name, batchDestination.Destination.ID)
				continue
			}
			if uploadFrequencyExceeded(batchDestination) {
				logger.Debugf("BRT: Skipping batch router upload loop since %s:%s upload freq not exceeded", batchDestination.Destination.DestinationDefinition.Name, batchDestination.Destination.ID)
				continue
			}
			setDestInProgress(batchDestination, true)

			toQuery := jobQueryBatchSize
			parameterFilters := []jobsdb.ParameterFilterT{
				jobsdb.ParameterFilterT{
					Name:  "source_id",
					Value: batchDestination.Source.ID,
				},
				jobsdb.ParameterFilterT{
					Name:     "destination_id",
					Value:    batchDestination.Destination.ID,
					Optional: true,
				},
			}
			brtQueryStat := stats.NewStat("batch_router.jobsdb_query_time", stats.TimerType)
			brtQueryStat.Start()

			retryList := brt.jobsDB.GetToRetry([]string{brt.destType}, toQuery, parameterFilters)
			toQuery -= len(retryList)
			waitList := brt.jobsDB.GetWaiting([]string{brt.destType}, toQuery, parameterFilters) //Jobs send to waiting state
			toQuery -= len(waitList)
			unprocessedList := brt.jobsDB.GetUnprocessed([]string{brt.destType}, toQuery, parameterFilters)
			brtQueryStat.End()

			combinedList := append(waitList, append(unprocessedList, retryList...)...)
			if len(combinedList) == 0 {
				logger.Debugf("BRT: DB Read Complete. No BRT Jobs to process for parameter Filters: %v", parameterFilters)
				setDestInProgress(batchDestination, false)
				continue
			}
			logger.Debugf("BRT: %s: DB Read Complete for parameter Filters: %v retryList: %v, waitList: %v unprocessedList: %v, total: %v", parameterFilters, brt.destType, len(retryList), len(waitList), len(unprocessedList), len(combinedList))

			var statusList []*jobsdb.JobStatusT

			for _, job := range combinedList {
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum + 1,
					JobState:      jobsdb.Executing.State,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: []byte(`{}`), // check
				}
				statusList = append(statusList, &status)
			}

			parameterFilters = []jobsdb.ParameterFilterT{
				jobsdb.ParameterFilterT{
					Name:  "source_id",
					Value: batchDestination.Source.ID,
				},
				jobsdb.ParameterFilterT{
					Name:     "destination_id",
					Value:    batchDestination.Destination.ID,
					Optional: true,
				},
			}
			//Mark the jobs as executing
			brt.jobsDB.UpdateJobStatus(statusList, []string{brt.destType}, parameterFilters)
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
	logger.Debug("BRT: Checking for incomplete journal entries to recover from...")
	entries := brt.jobsDB.GetJournalEntries(jobsdb.RawDataDestUploadOperation)
	for _, entry := range entries {
		var object ObjectStorageT
		err := json.Unmarshal(entry.OpPayload, &object)
		if err != nil {
			panic(err)
		}
		if len(object.Config) == 0 {
			//Backward compatibility. If old entries dont have config, just delete journal entry
			brt.jobsDB.JournalDeleteEntry(entry.OpID)
			continue
		}
		downloader, err := filemanager.New(&filemanager.SettingsT{
			Provider: object.Provider,
			Config:   object.Config,
		})
		if err != nil {
			panic(err)
		}

		localTmpDirName := "/rudder-raw-data-dest-upload-crash-recovery/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			panic(err)
		}
		jsonPath := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v", time.Now().Unix(), uuid.NewV4().String()))

		err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
		jsonFile, err := os.Create(jsonPath)
		if err != nil {
			panic(err)
		}

		logger.Debugf("BRT: Downloading data for incomplete journal entry to recover from %s at key: %s\n", object.Provider, object.Key)
		err = downloader.Download(jsonFile, object.Key)
		if err != nil {
			logger.Debugf("BRT: Failed to download data for incomplete journal entry to recover from %s at key: %s with error: %v\n", object.Provider, object.Key, err)
			continue
		}

		jsonFile.Close()
		defer os.Remove(jsonPath)

		rawf, err := os.Open(jsonPath)
		if err != nil {
			panic(err)
		}
		reader, err := gzip.NewReader(rawf)
		if err != nil {
			panic(err)
		}

		sc := bufio.NewScanner(reader)

		logger.Debug("BRT: Setting go map cache for incomplete journal entry to recover from...")
		for sc.Scan() {
			lineBytes := sc.Bytes()
			eventID := gjson.GetBytes(lineBytes, "messageId").String()
			if _, ok := uploadedRawDataJobsCache[object.DestinationID]; !ok {
				uploadedRawDataJobsCache[object.DestinationID] = make(map[string]bool)
			}
			uploadedRawDataJobsCache[object.DestinationID][eventID] = true
		}
		reader.Close()
		brt.jobsDB.JournalDeleteEntry(entry.OpID)
	}
}

func (brt *HandleT) crashRecover() {

	for {
		execList := brt.jobsDB.GetExecuting([]string{brt.destType}, jobQueryBatchSize, nil)

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
				JobState:      jobsdb.Failed.State,
				ErrorCode:     "",
				ErrorResponse: []byte(`{"Error": "Rudder server crashed while copying jobs to storage"}`), // check
			}
			statusList = append(statusList, &status)
		}
		brt.jobsDB.UpdateJobStatus(statusList, []string{}, nil)
	}
	if misc.Contains(objectStorageDestinations, brt.destType) {
		brt.dedupRawDataDestJobsOnCrash()
	}
}

func getWarehouseURL() (url string) {
	if warehouseMode == config.EmbeddedMode {
		url = fmt.Sprintf(`http://localhost:%d`, config.GetInt("Warehouse.webPort", 8082))
	} else {
		url = config.GetEnv("WAREHOUSE_URL", "http://localhost:8082")
	}
	return
}

func (brt *HandleT) collectMetrics() {
	if diagnostics.EnableBatchRouterMetric {
		for {
			select {
			case _ = <-brt.diagnosisTicker.C:
				brt.batchRequestsMetricLock.RLock()
				var diagnosisProperties map[string]interface{}
				success := 0
				failed := 0
				for _, batchReqMetric := range brt.batchRequestsMetric {
					success = success + batchReqMetric.batchRequestSuccess
					failed = failed + batchReqMetric.batchRequestFailed
				}
				if len(brt.batchRequestsMetric) > 0 {
					diagnosisProperties = map[string]interface{}{
						brt.destType: map[string]interface{}{
							diagnostics.BatchRouterSuccess: success,
							diagnostics.BatchRouterFailed:  failed,
						},
					}

					diagnostics.Track(diagnostics.BatchRouterEvents, diagnosisProperties)
				}

				brt.batchRequestsMetric = nil
				brt.batchRequestsMetricLock.RUnlock()
			}
		}
	}
}

func loadConfig() {
	jobQueryBatchSize = config.GetInt("BatchRouter.jobQueryBatchSize", 100000)
	noOfWorkers = config.GetInt("BatchRouter.noOfWorkers", 8)
	maxFailedCountForJob = config.GetInt("BatchRouter.maxFailedCountForJob", 128)
	mainLoopSleep = config.GetDuration("BatchRouter.mainLoopSleepInS", 2) * time.Second
	uploadFreqInS = config.GetInt64("BatchRouter.uploadFreqInS", 30)
	objectStorageDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO"}
	warehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE", "POSTGRES"}
	inProgressMap = map[string]bool{}
	lastExecMap = map[string]int64{}
	warehouseMode = config.GetString("Warehouse.mode", "embedded")
	warehouseURL = getWarehouseURL()
	// Time period for diagnosis ticker
	diagnosisTickerTime = config.GetDuration("Diagnostics.batchRouterTimePeriodInS", 600) * time.Second
	warehouseServiceMaxRetryTimeinHr = config.GetDuration("batchRouter.warehouseServiceMaxRetryTimeinHr", 3) * time.Hour
}

func init() {
	loadConfig()
	uploadedRawDataJobsCache = make(map[string]map[string]bool)
}

//Setup initializes this module
func (brt *HandleT) Setup(jobsDB *jobsdb.HandleT, destType string) {
	logger.Infof("BRT: Batch Router started: %s", destType)
	brt.diagnosisTicker = time.NewTicker(diagnosisTickerTime)
	brt.destType = destType
	brt.jobsDB = jobsDB
	brt.isEnabled = true

	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	brt.netHandle = client

	brt.processQ = make(chan BatchJobsT)
	brt.crashRecover()

	rruntime.Go(func() {
		brt.collectMetrics()
	})
	rruntime.Go(func() {
		brt.initWorkers()
	})
	rruntime.Go(func() {
		brt.backendConfigSubscriber()
	})
	rruntime.Go(func() {
		brt.mainLoop()
	})
}
