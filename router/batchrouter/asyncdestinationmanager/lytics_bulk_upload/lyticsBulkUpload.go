package lyticsBulkUpload

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func NewLyticsBulkUploader(destinationName, authorization, baseEndpoint string) *LyticsBulkUploader {
	return &LyticsBulkUploader{
		destName:      destinationName,
		logger:        logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("Lytics").Child("LyticsBulkUploader"),
		authorization: authorization,
		baseEndpoint:  baseEndpoint,
		fileSizeLimit: common.GetBatchRouterConfigInt64("MaxUploadLimit", destinationName, 10*bytesize.MB),
		jobToCSVMap:   map[int64]int64{},
	}
}

func NewManager(destination *backendconfig.DestinationT) (*LyticsBulkUploader, error) {
	destConfig := DestinationConfig{}
	jsonConfig, err := stdjson.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	err = stdjson.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling destination config: %v", err)
	}
	destName := destination.DestinationDefinition.Name
	baseEndpoint := fmt.Sprintf("https://bulk.lytics.io/collect/bulk/%s?timestamp_field=%s", destConfig.LyticsStreamName, destConfig.TimestampField)
	unableToGetBaseEndpointStat := stats.Default.NewTaggedStat("unable_to_get_base_endpoint", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": destName,
	})
	if err != nil {
		unableToGetBaseEndpointStat.Count(1)
		return nil, fmt.Errorf("error in getting base endpoint: %v", err)
	}
	return NewLyticsBulkUploader(destName, destConfig.LyticsApiKey, baseEndpoint), nil
}

// Poll return a success response for the poll request every time by default
func (ym *LyticsBulkUploader) Poll(_ common.AsyncPoll) common.PollStatusResponse {
	return common.PollStatusResponse{}
}

// GetUploadStats return a success response for the getUploadStats request every time by default
func (ym *LyticsBulkUploader) GetUploadStats(_ common.GetUploadStatsInput) common.GetUploadStatsResponse {
	return common.GetUploadStatsResponse{}
}

func (*LyticsBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
}

// Helper function to retrieve the value of RudderProperty from uploadData
func getUploadDataValue(uploadData []byte, rudderProperty string) (string, bool) {
	var data map[string]interface{}
	if err := json.Unmarshal(uploadData, &data); err != nil {
		return "", false
	}

	if value, exists := data[rudderProperty]; exists {
		if strValue, ok := value.(string); ok {
			return strValue, true
		}
	}
	return "", false
}

func (b *LyticsBulkUploader) populateZipFile(actionFile *ActionFileInfo, streamTraitsMapping []StreamTraitMapping, line string, data Data) error {
	newFileSize := actionFile.FileSize + int64(len(line))
	if newFileSize < b.fileSizeLimit {
		actionFile.FileSize = newFileSize
		actionFile.EventCount += 1

		// Create a map for quick lookups of LyticsProperty based on RudderProperty
		propertyMap := make(map[string]string)
		for _, mapping := range streamTraitsMapping {
			propertyMap[mapping.RudderProperty] = mapping.LyticsProperty
		}

		// Unmarshal Fields into a slice of json.RawMessage
		var fields []json.RawMessage
		if err := json.Unmarshal(data.Message.Fields, &fields); err != nil {
			return err
		}

		// Initialize an empty CSV row
		csvRow := []string{}

		// Populate the CSV row based on streamTraitsMapping
		for _, mapping := range streamTraitsMapping {
			found := false
			for _, uploadData := range fields {
				if value, exists := getUploadDataValue(uploadData, mapping.RudderProperty); exists {
					csvRow = append(csvRow, value)
					found = true
					break
				}
			}
			if !found {
				// Append an empty string if the RudderProperty is not found in uploadData
				csvRow = append(csvRow, "")
			}
		}

		err := actionFile.CSVWriter.Write(csvRow)
		if err != nil {
			return err
		}
		actionFile.SuccessfulJobIDs = append(actionFile.SuccessfulJobIDs, data.Metadata.JobID)
	} else {
		actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
	}
	return nil
}

func createCSVWriter(filePath string) (*ActionFileInfo, error) {
	// Open or create the file where the CSV will be written
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %v", err)
	}

	// Create a new CSV writer using the file
	csvWriter := csv.NewWriter(file)

	// Return the ActionFileInfo struct with the CSV writer and file
	return &ActionFileInfo{
		CSVWriter: csvWriter,
		File:      file,
	}, nil
}

func (b *LyticsBulkUploader) createCSVFile(filePath string, streamTraitsMapping []StreamTraitMapping) (*ActionFileInfo, error) {
	// Call the createCSVWriter function to initialize the CSV writer
	actionFile, err := createCSVWriter(filePath)
	if err != nil {
		return nil, err
	}
	defer actionFile.File.Close() // Ensure the file is closed when done

	// Create a new scanner using the file in actionFile
	scanner := bufio.NewScanner(actionFile.File)
	scanner.Buffer(nil, 50000*1024) // Adjust the buffer size if necessary

	for scanner.Scan() {
		line := scanner.Text()
		var data Data
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			// Collect the failed job ID
			actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
			continue
		}

		// Calculate the payload size and observe it
		payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.HistogramType,
			map[string]string{
				"module":   "batch_router",
				"destType": b.destName,
			})
		payloadSizeStat.Observe(float64(len(data.Message.Fields)))

		// Populate the CSV file and collect success/failure job IDs
		err := b.populateZipFile(actionFile, streamTraitsMapping, line, data)
		if err != nil {
			actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
		} else {
			actionFile.SuccessfulJobIDs = append(actionFile.SuccessfulJobIDs, data.Metadata.JobID)
			actionFile.EventCount++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error while scanning file: %v", err)
	}

	// After processing, calculate the final file size
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve file info: %v", err)
	}
	actionFile.FileSize = fileInfo.Size()

	return actionFile, nil
}

func convertGjsonToStreamTraitMapping(result gjson.Result) ([]StreamTraitMapping, error) {
	var mappings []StreamTraitMapping

	// Iterate through the array in the result
	result.ForEach(func(key, value gjson.Result) bool {
		mapping := StreamTraitMapping{
			RudderProperty: value.Get("rudderProperty").String(),
			LyticsProperty: value.Get("lyticsProperty").String(),
		}
		mappings = append(mappings, mapping)
		return true // Continue iteration
	})

	return mappings, nil
}

func (e *LyticsBulkUploader) MakeHTTPRequest(data *HttpRequestData) ([]byte, int, error) {
	req, err := http.NewRequest(data.Method, data.Endpoint, data.Body)
	if err != nil {
		return nil, 500, err
	}
	req.Header.Add("Authorization", data.Authorization)
	req.Header.Add("content-type", data.ContentType)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, 500, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, 500, err
	}
	return body, res.StatusCode, err
}

func (e *LyticsBulkUploader) UploadBulkFile(data *HttpRequestData, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	data.Endpoint = data.Endpoint
	data.Method = http.MethodPost
	data.ContentType = "application/csv"
	data.Body = file
	_, statusCode, err := e.MakeHTTPRequest(data)
	if err != nil {
		return err
	}
	if statusCode != 200 {
		return fmt.Errorf("Upload failed with status code: %d", statusCode)
	}
	return nil
}

func (b *LyticsBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	// startTime := time.Now()
	filePath := asyncDestStruct.FileName
	destConfig, err := json.Marshal(destination.Config)
	if err != nil {
		// return b.generateErrorOutput("Error while marshalling destination config. ", err, asyncDestStruct.ImportingJobIDs,asyncDestStruct.Destination.ID )
		eventsAbortedStat := stats.Default.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})
		eventsAbortedStat.Count(len(asyncDestStruct.ImportingJobIDs))
		return common.AsyncUploadOutput{
			AbortCount:    len(asyncDestStruct.ImportingJobIDs),
			DestinationID: asyncDestStruct.Destination.ID,
			AbortJobIDs:   asyncDestStruct.ImportingJobIDs,
			AbortReason:   fmt.Sprintf("%s %v", "Error while marshalling destination config", err.Error()),
		}
	}
	var failedJobs []int64
	var successJobs []int64
	var errors []string

	destConfigJson := string(destConfig)
	// Convert gjson.Result to []StreamTraitMapping
	streamTraitsMapping, err := convertGjsonToStreamTraitMapping(gjson.Get(destConfigJson, "streamTraitsMapping"))
	if err != nil {
		// ßreturn nil, fmt.Errorf("failed to convert streamTraitsMapping: %v", err)
	}
	actionFiles, err := b.createCSVFile(filePath, streamTraitsMapping)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while transforming the file. %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	uploadRetryableStat := stats.Default.NewTaggedStat("events_over_prescribed_limit", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})

	uploadRetryableStat.Count(len(actionFiles.FailedJobIDs))
	if err != nil {
		b.logger.Error("Error in getting bulk upload url: %w", err)
		failedJobs = append(append(failedJobs, actionFiles.SuccessfulJobIDs...), actionFiles.FailedJobIDs...)
		errors = append(errors, fmt.Sprintf("%s:error in getting bulk upload url: %s", actionFiles.Action, err.Error()))
	}

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})

	uploadDataData := HttpRequestData{
		Endpoint:      b.baseEndpoint,
		Authorization: b.authorization,
	}

	startTime := time.Now()
	errorDuringUpload := b.UploadBulkFile(&uploadDataData, actionFiles.CSVFilePath)
	uploadTimeStat.Since(startTime)

	if errorDuringUpload != nil {
		b.logger.Error("error in uploading the bulk file: %v", errorDuringUpload)
		failedJobs = append(append(failedJobs, actionFiles.SuccessfulJobIDs...), actionFiles.FailedJobIDs...)
		// remove the file that could not be uploaded
		err = os.Remove(actionFiles.CSVFilePath)
		if err != nil {
			b.logger.Error("Error in removing zip file: %v", err)
		}
	}

	failedJobs = append(failedJobs, actionFiles.FailedJobIDs...)
	successJobs = append(successJobs, actionFiles.SuccessfulJobIDs...)

	err = os.Remove(actionFiles.CSVFilePath)
	if err != nil {
		b.logger.Error("Error in removing zip file: %v", err)
	}

	return common.AsyncUploadOutput{
		ImportingJobIDs: successJobs,
		FailedJobIDs:    append(asyncDestStruct.FailedJobIDs, failedJobs...),
		FailedReason:    fmt.Sprintf("unable to upload the data. "+"%v", err),
		ImportingCount:  len(successJobs),
		FailedCount:     len(asyncDestStruct.FailedJobIDs) + len(failedJobs),
		DestinationID:   destination.ID,
	}
}
