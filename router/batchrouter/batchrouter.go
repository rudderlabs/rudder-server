package batchrouter

import (
	"bytes"
	"compress/gzip"
	"database/sql"
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
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
)

var (
	jobQueryBatchSize         int
	noOfWorkers               int
	mainLoopSleepInS          int
	batchDestinations         []DestinationT
	configSubscriberLock      sync.RWMutex
	rawDataDestinations       []string
	inProgressMap             map[string]bool
	warehouseJSONUploadsTable string
)

type HandleT struct {
	processQ     chan BatchJobsT
	jobsDB       *jobsdb.HandleT
	jobsDBHandle *sql.DB
	isEnabled    bool
}

func backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		batchDestinations = []DestinationT{}
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if source.Enabled && len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if misc.Contains(rawDataDestinations, destination.DestinationDefinition.Name) && destination.Enabled {
						batchDestinations = append(batchDestinations, DestinationT{Source: source, Destination: destination})
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

type S3UploadOutput struct {
	Bucket         string
	Key            string
	LocalFilePaths []string
	Error          error
}

func (brt *HandleT) copyJobsToS3(batchJobs BatchJobsT, bucket string, isWarehouse bool) S3UploadOutput {

	uuid := uuid.NewV4()
	fileName := fmt.Sprintf("%v.%v.%v.json", time.Now().Unix(), batchJobs.BatchDestination.Source.ID, uuid)
	var subDir string
	if isWarehouse {
		subDir = "warehousejsons/"
	} else {
		subDir = "destjsons/"
	}
	path := fmt.Sprintf("%v%v%v", config.GetEnv("S3_UPLOADS_DIR", "/home/ubuntu/s3/"), subDir, fileName)
	var content string
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
				content += string(jsonEvent) + "\n"
			}
		} else {
			content += string(job.EventPayload) + "\n"
		}
	}

	gzipFilePath := fmt.Sprintf(`%v.gz`, path)
	err := os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	misc.AssertError(err)
	gzipFile, err := os.Create(gzipFilePath)

	gzipWriter := gzip.NewWriter(gzipFile)
	_, err = gzipWriter.Write([]byte(content))
	misc.AssertError(err)
	gzipWriter.Close()

	uploader, err := fileuploader.NewFileUploader(&fileuploader.SettingsT{
		Provider:       "s3",
		AmazonS3Bucket: bucket,
	})
	var uploadLocation []string
	if isWarehouse {
		uploadLocation = []string{config.GetEnv("WAREHOUSE_S3_BUCKET_FOLDER_NAME", "rudder-warehouse-logs"), batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006")}
	} else {
		uploadLocation = []string{config.GetEnv("DESTINATION_S3_BUCKET_FOLDER_NAME", "rudder-logs"), batchJobs.BatchDestination.Source.ID, time.Now().Format("01-02-2006")}
	}
	gzipFile, err = os.Open(gzipFilePath)
	misc.AssertError(err)
	uploader.Upload(gzipFile, uploadLocation...)
	if err != nil {
		logger.Debug(err)
		return S3UploadOutput{Error: err}
	}
	return S3UploadOutput{Bucket: bucket, Key: strings.Join(uploadLocation, "/") + "/" + fileName + ".gz", LocalFilePaths: []string{gzipFilePath}}
}

func (brt *HandleT) updateWarehouseMetadata(batchJobs BatchJobsT, location string) (err error) {
	schemaMap := make(map[string]map[string]interface{})
	for _, job := range batchJobs.Jobs {
		trimmedPayload := bytes.TrimLeft(job.EventPayload, " \t\r\n")
		isArray := len(trimmedPayload) > 0 && trimmedPayload[0] == '['
		if isArray {
			var payloads []map[string]interface{}
			err := json.Unmarshal(trimmedPayload, &payloads)
			misc.AssertError(err)
			for _, payload := range payloads {
				tableName := payload["metadata"].(map[string]interface{})["table"].(string)
				columns := payload["metadata"].(map[string]interface{})["columns"].(map[string]interface{})
				if schemaMap[tableName] != nil {
					for columnName, columnType := range columns {
						schemaMap[tableName][columnName] = columnType
					}
				} else {
					schemaMap[tableName] = columns
				}
			}
		} else {
			var payload map[string]interface{}
			err := json.Unmarshal(job.EventPayload, &payload)
			misc.AssertError(err)
			schemaMap[payload["metadata"].(map[string]interface{})["table"].(string)] = payload["metadata"].(map[string]interface{})["columns"].(map[string]interface{})
		}
	}
	schemaPayload, err := json.Marshal(schemaMap)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (location, schema, source_id, status, created_at)
									   VALUES ($1, $2, $3, $4, $5)`, warehouseJSONUploadsTable)
	stmt, err := brt.jobsDBHandle.Prepare(sqlStatement)
	misc.AssertError(err)
	defer stmt.Close()

	_, err = stmt.Exec(location, schemaPayload, batchJobs.BatchDestination.Source.ID, "waiting", time.Now())
	misc.AssertError(err)
	return err
}

func (brt *HandleT) setJobStatus(batchJobs BatchJobsT, err error) {
	var jobState string
	if err != nil {
		logger.Error(err)
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
}

func (brt *HandleT) initWorkers() {
	for i := 0; i < noOfWorkers; i++ {
		go func() {
			for {
				select {
				case batchJobs := <-brt.processQ:
					switch batchJobs.BatchDestination.Destination.DestinationDefinition.Name {
					case "S3":
						output := brt.copyJobsToS3(batchJobs, batchJobs.BatchDestination.Destination.Config.(map[string]interface{})["bucketName"].(string), false)
						brt.setJobStatus(batchJobs, output.Error)
						misc.RemoveFilePaths(output.LocalFilePaths...)
						delete(inProgressMap, batchJobs.BatchDestination.Destination.ID)
					case "RS":
						output := brt.copyJobsToS3(batchJobs, "rl-redshift-json-dump", true)
						err := output.Error
						if err == nil {
							err = brt.updateWarehouseMetadata(batchJobs, output.Key)
						}
						brt.setJobStatus(batchJobs, err)
						misc.RemoveFilePaths(output.LocalFilePaths...)
						delete(inProgressMap, batchJobs.BatchDestination.Destination.ID)
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

func (brt *HandleT) mainLoop() {
	for {
		if !brt.isEnabled {
			time.Sleep(time.Duration(2*mainLoopSleepInS) * time.Second)
			continue
		}
		time.Sleep(time.Duration(mainLoopSleepInS) * time.Second)
		for _, batchDestination := range batchDestinations {
			if inProgressMap[batchDestination.Destination.ID] {
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
			inProgressMap[batchDestination.Destination.ID] = true
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

func (brt *HandleT) setupWarehouseJSONUploadsTable() {

	sqlStatement := `DO $$ BEGIN
                                CREATE TYPE wh_json_upload_state_type
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
									  schema JSONB NOT NULL,
									  status wh_json_upload_state_type,
									  created_at TIMESTAMP NOT NULL);`, warehouseJSONUploadsTable)

	_, err = brt.jobsDBHandle.Exec(sqlStatement)
	misc.AssertError(err)
}

func loadConfig() {
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize", 10000)
	noOfWorkers = config.GetInt("BatchRouter.noOfWorkers", 8)
	mainLoopSleepInS = config.GetInt("BatchRouter.mainLoopSleepInS", 5)
	warehouseJSONUploadsTable = config.GetString("Warehouse.jsonUploadsTable", "wh_json_uploads")
	rawDataDestinations = []string{"S3", "RS"}
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
	brt.jobsDBHandle = brt.jobsDB.GetDBHandle()
	brt.setupWarehouseJSONUploadsTable()
	brt.processQ = make(chan BatchJobsT)
	brt.crashRecover()
	brt.isEnabled = false

	go brt.initWorkers()
	go backendConfigSubscriber()
	go brt.mainLoop()
}
