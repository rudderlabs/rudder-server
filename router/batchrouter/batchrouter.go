package batchrouter

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/services/diagnosis"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	warehouseutils "github.com/rudderlabs/rudder-server/router/warehouse/utils"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
)

var (
	jobQueryBatchSize          int
	noOfWorkers                int
	mainLoopSleepInS           int
	configSubscriberLock       sync.RWMutex
	objectStorageDestinations  []string
	warehouseDestinations      []string
	inProgressMap              map[string]bool
	inProgressMapLock          sync.RWMutex
	uploadedRawDataJobsCache   map[string]map[string]bool
	warehouseStagingFilesTable string
)

type HandleT struct {
	destType          string
	batchDestinations []DestinationT
	processQ          chan BatchJobsT
	jobsDB            *jobsdb.HandleT
	jobsDBHandle      *sql.DB
	isEnabled         bool
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
	backendconfig.Subscribe(ch)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		brt.batchDestinations = []DestinationT{}
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if source.Enabled && len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.Enabled && destination.DestinationDefinition.Name == brt.destType {
						brt.batchDestinations = append(brt.batchDestinations, DestinationT{Source: source, Destination: destination})
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

type batchRequestDiagnosis struct {
	batchRequestSuccess int `json:"batch_request_success"`
	batchRequestFailed  int `json:"batch_request_failed"`
}

var (
	batchRequestsDiagnosisLock sync.Mutex
	diagnosisTicker            *time.Ticker
	batchRequestsDiagnosis     map[string][]batchRequestDiagnosis
)

type StorageUploadOutput struct {
	Config         map[string]interface{}
	Key            string
	LocalFilePaths []string
	Error          error
	JournalOpID    int64
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
	}
	destStatsD.Count(count)
}

func (brt *HandleT) copyJobsToStorage(provider string, batchJobs BatchJobsT, makeJournalEntry bool, isWarehouse bool) StorageUploadOutput {
	var localTmpDirName string
	destinationConfig := batchJobs.BatchDestination.Destination.Config.(map[string]interface{})
	if isWarehouse {
		localTmpDirName = "/rudder-warehouse-staging-uploads/"
	} else {
		localTmpDirName = "/rudder-raw-data-destination-logs/"
	}

	uuid := uuid.NewV4()
	logger.Debugf("BRT: Starting logging to %s: %s\n", provider, destinationConfig)

	tmpDirPath := misc.CreateTMPDIR()
	path := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v.%v", time.Now().Unix(), batchJobs.BatchDestination.Source.ID, uuid))

	gzipFilePath := fmt.Sprintf(`%v.gz`, path)
	err := os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	misc.AssertError(err)
	gzWriter, err := misc.CreateGZ(gzipFilePath)

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
		logger.Infof("BRT: All events in this batch for %s are de-deuplicated...\n", provider)
		return StorageUploadOutput{
			LocalFilePaths: []string{gzipFilePath},
		}
	}

	logger.Debugf("BRT: Logged to local file: %v\n", gzipFilePath)

	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: provider,
		Config:   batchJobs.BatchDestination.Destination.Config.(map[string]interface{}),
	})
	misc.AssertError(err)

	outputFile, err := os.Open(gzipFilePath)
	misc.AssertError(err)

	logger.Debugf("BRT: Starting upload to %s: config:%s\n", provider, destinationConfig)

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
			Config:          batchJobs.BatchDestination.Destination.Config.(map[string]interface{}),
			Key:             strings.Join(append(keyPrefixes, fileName), "/"),
			Provider:        provider,
			DestinationID:   batchJobs.BatchDestination.Destination.ID,
			DestinationType: batchJobs.BatchDestination.Destination.DestinationDefinition.Name,
		})
		opID = brt.jobsDB.JournalMarkStart(jobsdb.RawDataDestUploadOperation, opPayload)
	}
	_, err = uploader.Upload(outputFile, keyPrefixes...)

	if err != nil {
		logger.Errorf("BRT: Error uploading to %s: config:%s: %v\n", provider, destinationConfig, err)
		return StorageUploadOutput{
			Error:       err,
			JournalOpID: opID,
		}
	}

	return StorageUploadOutput{
		Config:         batchJobs.BatchDestination.Destination.Config.(map[string]interface{}),
		Key:            strings.Join(keyPrefixes, "/") + "/" + fileName,
		LocalFilePaths: []string{gzipFilePath},
		JournalOpID:    opID,
	}
}

func (brt *HandleT) updateWarehouseMetadata(batchJobs BatchJobsT, location string) (err error) {
	schemaMap := make(map[string]map[string]interface{})
	for _, job := range batchJobs.Jobs {
		var payload map[string]interface{}
		err := json.Unmarshal(job.EventPayload, &payload)
		misc.AssertError(err)
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
	logger.Debugf("BRT: Creating record for uploaded json in %s table with schema: %+v\n", warehouseStagingFilesTable, schemaMap)
	schemaPayload, err := json.Marshal(schemaMap)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (location, schema, source_id, destination_id, status, created_at, updated_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $6)`, warehouseStagingFilesTable)
	stmt, err := brt.jobsDBHandle.Prepare(sqlStatement)
	misc.AssertError(err)
	defer stmt.Close()

	_, err = stmt.Exec(location, schemaPayload, batchJobs.BatchDestination.Source.ID, batchJobs.BatchDestination.Destination.ID, warehouseutils.StagingFileWaitingState, time.Now())
	misc.AssertError(err)
	return err
}

func (brt *HandleT) setJobStatus(batchJobs BatchJobsT, isWarehouse bool, err error) {
	var (
		jobState          string
		errorResp         []byte
		destinationConfig map[string]interface{}
	)
	destinationConfig = batchJobs.BatchDestination.Destination.Config.(map[string]interface{})
	var batchReqDiagnosis batchRequestDiagnosis
	if err != nil {
		logger.Errorf("BRT: Error uploading to object storage: %v %v %v\n", err, destinationConfig, batchJobs.BatchDestination.Source.ID)
		jobState = jobsdb.FailedState
		errorResp, _ = json.Marshal(ErrorResponseT{Error: err.Error()})
		batchReqDiagnosis.batchRequestFailed = 1
		// We keep track of number of failed attempts in case of failure and number of events uploaded in case of success in stats
		updateDestStatusStats(batchJobs.BatchDestination.Destination.ID, 1, false)
	} else {
		logger.Debugf("BRT: Uploaded to object storage with config: %v %v %v\n", destinationConfig, batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006"))
		jobState = jobsdb.SucceededState
		errorResp = []byte(`{"success":"OK"}`)
		batchReqDiagnosis.batchRequestSuccess = 1
		updateDestStatusStats(batchJobs.BatchDestination.Destination.ID, len(batchJobs.Jobs), true)
	}
	brt.requestDiagnosis(batchReqDiagnosis)
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
	brt.jobsDB.UpdateJobStatus(statusList, []string{brt.destType}, batchJobs.BatchDestination.Source.ID)
}

func (brt *HandleT) requestDiagnosis(batchReqDiagnosis batchRequestDiagnosis) {
	if diagnosis.EnableDiagnosis {
		batchRequestsDiagnosisLock.Lock()
		if _, ok := batchRequestsDiagnosis[brt.destType]; ok {
			batchRequestsDiagnosis[brt.destType] = append(batchRequestsDiagnosis[brt.destType], batchReqDiagnosis)
		} else {
			batchRequestsDiagnosis = map[string][]batchRequestDiagnosis{
				brt.destType: {batchReqDiagnosis},
			}
		}
		batchRequestsDiagnosisLock.Unlock()
	}
}

func (brt *HandleT) initWorkers() {
	for i := 0; i < noOfWorkers; i++ {
		go func() {
			for {
				select {
				case batchJobs := <-brt.processQ:
					switch {
					case misc.ContainsString(objectStorageDestinations, brt.destType):
						destUploadStat := stats.NewStat(fmt.Sprintf(`batch_router.%s_dest_upload_time`, brt.destType), stats.TimerType)
						destUploadStat.Start()
						output := brt.copyJobsToStorage(brt.destType, batchJobs, true, false)
						brt.setJobStatus(batchJobs, false, output.Error)
						if output.JournalOpID != 0 {
							brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
						}
						misc.RemoveFilePaths(output.LocalFilePaths...)
						destUploadStat.End()
						setSourceInProgress(batchJobs.BatchDestination, false)
					case misc.ContainsString(warehouseDestinations, brt.destType):
						destUploadStat := stats.NewStat(fmt.Sprintf(`batch_router.%s_%s_dest_upload_time`, brt.destType, warehouseutils.ObjectStorageMap[brt.destType]), stats.TimerType)
						destUploadStat.Start()
						output := brt.copyJobsToStorage(warehouseutils.ObjectStorageMap[brt.destType], batchJobs, true, true)
						if output.Error == nil && output.Key != "" {
							brt.updateWarehouseMetadata(batchJobs, output.Key)
						}
						brt.setJobStatus(batchJobs, true, output.Error)
						misc.RemoveFilePaths(output.LocalFilePaths...)
						destUploadStat.End()
						setSourceInProgress(batchJobs.BatchDestination, false)
					}

				}
			}
		}()
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

func isSourceInProgress(batchDestination DestinationT) bool {
	inProgressMapLock.RLock()
	if inProgressMap[batchDestination.Source.ID+"_"+batchDestination.Destination.ID] {
		inProgressMapLock.RUnlock()
		return true
	}
	inProgressMapLock.RUnlock()
	return false
}

func setSourceInProgress(batchDestination DestinationT, starting bool) {
	inProgressMapLock.Lock()
	if starting {
		inProgressMap[batchDestination.Source.ID+"_"+batchDestination.Destination.ID] = true
	} else {
		delete(inProgressMap, batchDestination.Source.ID+"_"+batchDestination.Destination.ID)
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
		for _, batchDestination := range brt.batchDestinations {
			if isSourceInProgress(batchDestination) {
				continue
			}
			setSourceInProgress(batchDestination, true)
			toQuery := jobQueryBatchSize
			retryList := brt.jobsDB.GetToRetry([]string{brt.destType}, toQuery, batchDestination.Source.ID)
			toQuery -= len(retryList)
			waitList := brt.jobsDB.GetWaiting([]string{brt.destType}, toQuery, batchDestination.Source.ID) //Jobs send to waiting state
			toQuery -= len(waitList)
			unprocessedList := brt.jobsDB.GetUnprocessed([]string{brt.destType}, toQuery, batchDestination.Source.ID)

			combinedList := append(waitList, append(unprocessedList, retryList...)...)
			if len(combinedList) == 0 {
				setSourceInProgress(batchDestination, false)
				continue
			}

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
			brt.jobsDB.UpdateJobStatus(statusList, []string{brt.destType}, batchDestination.Source.ID)
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
		misc.AssertError(err)
		if len(object.Config) == 0 {
			//Backward compatibility. If old entries dont have config, just delete journal entry
			brt.jobsDB.JournalDeleteEntry(entry.OpID)
			continue
		}
		downloader, err := filemanager.New(&filemanager.SettingsT{
			Provider: object.Provider,
			Config:   object.Config,
		})
		misc.AssertError(err)

		localTmpDirName := "/rudder-raw-data-dest-upload-crash-recovery/"
		tmpDirPath := misc.CreateTMPDIR()
		jsonPath := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v", time.Now().Unix(), uuid.NewV4().String()))

		err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
		jsonFile, err := os.Create(jsonPath)
		misc.AssertError(err)

		logger.Debugf("BRT: Downloading data for incomplete journal entry to recover from %s with config: %s at key: %s\n", object.Provider, object.Config, object.Key)
		err = downloader.Download(jsonFile, object.Key)
		if err != nil {
			logger.Debugf("BRT: Failed to download data for incomplete journal entry to recover from %s with config: %s at key: %s with error: %v\n", object.Provider, object.Config, object.Key, err)
			continue
		}

		jsonFile.Close()
		defer os.Remove(jsonPath)

		rawf, err := os.Open(jsonPath)
		reader, _ := gzip.NewReader(rawf)

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
	if misc.Contains(objectStorageDestinations, brt.destType) {
		brt.dedupRawDataDestJobsOnCrash()
	}
}

func (brt *HandleT) setupWarehouseStagingFilesTable() {

	sqlStatement := `DO $$ BEGIN
                                CREATE TYPE wh_staging_state_type
                                     AS ENUM(
                                              'waiting',
                                              'executing',
											  'failed',
											  'succeeded');
                                     EXCEPTION
                                        WHEN duplicate_object THEN null;
                            END $$;`

	_, err := brt.jobsDBHandle.Exec(sqlStatement)
	misc.AssertError(err)

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
                                      id BIGSERIAL PRIMARY KEY,
									  location TEXT NOT NULL,
									  source_id VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  schema JSONB NOT NULL,
									  error TEXT,
									  status wh_staging_state_type,
									  created_at TIMESTAMP NOT NULL,
									  updated_at TIMESTAMP NOT NULL);`, warehouseStagingFilesTable)

	_, err = brt.jobsDBHandle.Exec(sqlStatement)
	misc.AssertError(err)

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_id_index ON %[1]s (source_id, destination_id);`, warehouseStagingFilesTable)
	_, err = brt.jobsDBHandle.Exec(sqlStatement)
	misc.AssertError(err)
}
func startDiagnosis() {
	if diagnosis.EnableDiagnosis {
		for {
			select {
			case _ = <-diagnosisTicker.C:
				batchRequestsDiagnosisLock.Lock()
				var diagnosisProperties map[string]interface{}
				for destName, batchReqsDiagnosis := range batchRequestsDiagnosis {
					success := 0
					failed := 0
					for _, batchReqDiagnosis := range batchReqsDiagnosis {
						success = success + batchReqDiagnosis.batchRequestSuccess
						failed = failed + batchReqDiagnosis.batchRequestFailed
					}
					if diagnosisProperties == nil {
						diagnosisProperties = map[string]interface{}{
							destName: map[string]interface{}{
								diagnosis.BatchRouterSuccess: success,
								diagnosis.BatchRouterFailed:  failed,
							},
						}

					} else {
						diagnosisProperties[destName] = map[string]interface{}{
							diagnosis.BatchRouterSuccess: success,
							diagnosis.BatchRouterFailed:  failed,
						}
					}
				}
				if diagnosisProperties != nil {
					diagnosis.Track(diagnosis.BatchRouterEvents, diagnosisProperties)
				}

				batchRequestsDiagnosis = nil
				batchRequestsDiagnosisLock.Unlock()
			}
		}
	}
}
func loadConfig() {
	jobQueryBatchSize = config.GetInt("BatchRouter.jobQueryBatchSize", 100000)
	noOfWorkers = config.GetInt("BatchRouter.noOfWorkers", 8)
	mainLoopSleepInS = config.GetInt("BatchRouter.mainLoopSleepInS", 5)
	warehouseStagingFilesTable = config.GetString("Warehouse.stagingFilesTable", "wh_staging_files")
	objectStorageDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO"}
	warehouseDestinations = []string{"RS", "BQ"}
	inProgressMap = map[string]bool{}
	diagnosisTicker = time.NewTicker(config.GetDuration("Diagnosis.batchRouterTimePeriod", 1) * time.Minute)
}

func init() {
	config.Initialize()
	loadConfig()
	uploadedRawDataJobsCache = make(map[string]map[string]bool)
}

//Setup initializes this module
func (brt *HandleT) Setup(jobsDB *jobsdb.HandleT, destType string) {
	logger.Infof("BRT: Batch Router started: %s\n", destType)
	brt.destType = destType
	brt.jobsDB = jobsDB
	brt.jobsDBHandle = brt.jobsDB.GetDBHandle()
	brt.isEnabled = true
	brt.setupWarehouseStagingFilesTable()
	brt.processQ = make(chan BatchJobsT)
	brt.crashRecover()
	go startDiagnosis()
	go brt.initWorkers()
	go brt.backendConfigSubscriber()
	go brt.mainLoop()
}
