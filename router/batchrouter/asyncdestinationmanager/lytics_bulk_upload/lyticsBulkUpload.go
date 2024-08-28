package lyticsBulkUpload

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func NewLyticsBulkUploader(destinationName, authorization, baseEndpoint string) common.AsyncUploadAndTransformManager {
	return &LyticsBulkUploader{
		destName:      destinationName,
		logger:        logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("Lytics").Child("LyticsBulkUploader"),
		authorization: authorization,
		baseEndpoint:  baseEndpoint,
		fileSizeLimit: common.GetBatchRouterConfigInt64("MaxUploadLimit", destinationName, 10*bytesize.MB),
		jobToCSVMap:   map[int64]int64{},
	}
}

func NewManager(destination *backendconfig.DestinationT) (common.AsyncDestinationManager, error) {
	destConfig := DestinationConfig{}
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
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
	return common.SimpleAsyncDestinationManager{UploaderAndTransformer: NewLyticsBulkUploader(destName, destConfig.LyticsApiKey, baseEndpoint)}, nil
}

func (*LyticsBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (b *LyticsBulkUploader) PopulateCsvFile(actionFile *ActionFileInfo, streamTraitsMapping []StreamTraitMapping, line string, data Data) error {
	newFileSize := actionFile.FileSize + int64(len(line))
	if newFileSize < b.fileSizeLimit {
		actionFile.FileSize = newFileSize
		actionFile.EventCount += 1

		// Create a map for quick lookups of LyticsProperty based on RudderProperty
		propertyMap := make(map[string]string)
		for _, mapping := range streamTraitsMapping {
			propertyMap[mapping.RudderProperty] = mapping.LyticsProperty
		}

		// Unmarshal Properties into a map of json.RawMessage
		var fields map[string]interface{}
		if err := jsoniter.Unmarshal(data.Message.Properties, &fields); err != nil {
			return err
		}

		// Initialize an empty CSV row
		csvRow := make([]string, len(streamTraitsMapping))

		// Populate the CSV row based on streamTraitsMapping
		for i, mapping := range streamTraitsMapping {
			if value, exists := fields[mapping.RudderProperty]; exists {
				// Convert the json.RawMessage value to a string
				switch v := value.(type) {
				case jsoniter.RawMessage:
					// Convert the json.RawMessage value to a string
					var valueStr string
					if err := jsoniter.Unmarshal(v, &valueStr); err == nil {
						csvRow[i] = valueStr
					} else {
						csvRow[i] = string(v)
					}
				case []byte:
					// Handle if the value is directly a []byte
					var valueStr string
					if err := jsoniter.Unmarshal(v, &valueStr); err == nil {
						csvRow[i] = valueStr
					} else {
						csvRow[i] = string(v)
					}
				case string:
					// If the value is already a string, use it directly
					csvRow[i] = v
				default:
					// Handle other types (e.g., numbers, booleans) by converting to a string
					csvRow[i] = fmt.Sprintf("%v", v)
				}
			} else {
				// Append an empty string if the RudderProperty is not found in fields
				csvRow[i] = ""
			}
		}

		// Write the CSV header only once
		if actionFile.EventCount == 1 {
			headers := make([]string, len(streamTraitsMapping))
			for i, mapping := range streamTraitsMapping {
				headers[i] = mapping.LyticsProperty
			}
			if err := actionFile.CSVWriter.Write(headers); err != nil {
				return err
			}
		}

		// Write the CSV row
		if err := actionFile.CSVWriter.Write(csvRow); err != nil {
			return err
		}

		actionFile.CSVWriter.Flush()
		actionFile.SuccessfulJobIDs = append(actionFile.SuccessfulJobIDs, data.Metadata.JobID)
	} else {
		actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
	}
	return nil
}

func createCSVWriter(fileName string) (*ActionFileInfo, error) {
	// Open or create the file where the CSV will be written
	file, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %v", err)
	}

	// Create a new CSV writer using the file
	csvWriter := csv.NewWriter(file)

	// Return the ActionFileInfo struct with the CSV writer, file, and file path
	return &ActionFileInfo{
		CSVWriter:   csvWriter,
		File:        file,
		CSVFilePath: fileName,
	}, nil
}

func (b *LyticsBulkUploader) createCSVFile(existingFilePath string, streamTraitsMapping []StreamTraitMapping) (*ActionFileInfo, error) {
	// Create a temporary directory using misc.CreateTMPDIR
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary directory: %v", err)
	}

	// Define a local directory name within the temp directory
	localTmpDirName := fmt.Sprintf("/%s/", misc.RudderAsyncDestinationLogs)

	// Combine the temporary directory with the local directory name and generate a unique file path
	path := filepath.Join(tmpDirPath, localTmpDirName, uuid.NewString())
	csvFilePath := fmt.Sprintf("%v.csv", path)

	// Initialize the CSV writer with the generated file path
	actionFile, err := createCSVWriter(csvFilePath)
	if err != nil {
		return nil, err
	}
	defer actionFile.File.Close() // Ensure the file is closed when done

	// Store the CSV file path in the ActionFileInfo struct
	actionFile.CSVFilePath = csvFilePath

	// Create a scanner to read the existing file line by line
	existingFile, err := os.Open(existingFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open existing file: %v", err)
	}
	defer existingFile.Close()

	scanner := bufio.NewScanner(existingFile)
	scanner.Buffer(nil, 50000*1024) // Adjust the buffer size if necessary

	for scanner.Scan() {
		line := scanner.Text()
		var data Data
		if err := jsoniter.Unmarshal([]byte(line), &data); err != nil {
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
		payloadSizeStat.Observe(float64(len(data.Message.Properties)))

		// Populate the CSV file and collect success/failure job IDs
		err := b.PopulateCsvFile(actionFile, streamTraitsMapping, line, data)
		if err != nil {
			actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error while scanning file: %v", err)
	}

	// After processing, calculate the final file size
	fileInfo, err := os.Stat(actionFile.CSVFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve file info: %v", err)
	}
	actionFile.FileSize = fileInfo.Size()

	return actionFile, nil
}

func convertGjsonToStreamTraitMapping(result gjson.Result) []StreamTraitMapping {
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

	return mappings
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
	filePath := asyncDestStruct.FileName
	destConfig, err := jsoniter.Marshal(destination.Config)
	if err != nil {
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

	destConfigJson := string(destConfig)
	streamTraitsMapping := convertGjsonToStreamTraitMapping(gjson.Get(destConfigJson, "streamTraitsMapping"))
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("failed to convert streamTraitsMapping: %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
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
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
				FailedReason:  fmt.Sprintf("Error in removing zip file: %v", err.Error()),
				FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
				DestinationID: destination.ID,
			}
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
		ImportingCount:  len(successJobs),
		FailedCount:     len(asyncDestStruct.FailedJobIDs) + len(failedJobs),
		DestinationID:   destination.ID,
	}
}
