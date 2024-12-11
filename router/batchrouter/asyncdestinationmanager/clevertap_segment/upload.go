package clevertapSegment

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type ClevertapBulkUploader struct {
	destName                  string
	logger                    logger.Logger
	statsFactory              stats.Stats
	appKey                    string
	accessToken               string
	presignedURLEndpoint      string
	notifyEndpoint            string
	fileSizeLimit             int64
	jobToCSVMap               map[int64]int64
	service                   ClevertapService
	clevertapConnectionConfig *ConnectionConfig
}

func (*ClevertapBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (u *ClevertapBulkUploader) PopulateCsvFile(actionFile *ActionFileInfo, line string, data Data) error {
	newFileSize := actionFile.FileSize + int64(len(line))
	if newFileSize < u.fileSizeLimit {
		actionFile.FileSize = newFileSize
		actionFile.EventCount += 1

		// Unmarshal Properties into a map of jsonLib.RawMessage
		var fields map[string]interface{}
		if err := jsonFast.Unmarshal(data.Message.Fields, &fields); err != nil {
			return err
		}

		// Check for presence of "i" and "g" values
		if valueG, okG := fields["g"]; okG {
			// If "g" exists, prioritize it and omit "i"
			csvRow := []string{"g", fmt.Sprintf("%v", valueG)} // Type: g
			if err := actionFile.CSVWriter.Write(csvRow); err != nil {
				return err
			}
		} else if valueI, okI := fields["i"]; okI {
			// Write "i" value only if "g" does not exist
			csvRow := []string{"i", fmt.Sprintf("%v", valueI)} // Type: i
			if err := actionFile.CSVWriter.Write(csvRow); err != nil {
				return err
			}
		}

		// Write the CSV header only once
		if actionFile.EventCount == 1 {
			// Fixed headers
			headers := []string{"Type", "Identity"}
			if err := actionFile.CSVWriter.Write(headers); err != nil {
				return err
			}
		}
		actionFile.CSVWriter.Flush()
		actionFile.SuccessfulJobIDs = append(actionFile.SuccessfulJobIDs, data.Metadata.JobID)
	} else {
		// fmt.println("size exceeding")
		actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
	}
	return nil
}

func (u *ClevertapBulkUploader) createCSVFile(existingFilePath string) (*ActionFileInfo, error) {
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
	actionFile, err := CreateCSVWriter(csvFilePath)
	if err != nil {
		return nil, err
	}
	defer actionFile.File.Close() // Ensure the file is closed when done

	// Store the CSV file path in the ActionFileInfo struct
	actionFile.CSVFilePath = csvFilePath

	// Create a scanner to read the existing file line by line
	existingFile, err := os.Open(existingFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open existing file")
	}
	defer existingFile.Close()

	scanner := bufio.NewScanner(existingFile)
	scanner.Buffer(nil, 50000*1024) // Adjust the buffer size if necessary

	for scanner.Scan() {
		line := scanner.Text()
		var data Data
		if err := jsonFast.Unmarshal([]byte(line), &data); err != nil {
			// Collect the failed job ID only if it's valid
			if data.Metadata.JobID != 0 { // Check if JobID is not zero
				actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
			}
			continue
		}

		// Calculate the payload size and observe it
		payloadSizeStat := u.statsFactory.NewTaggedStat("payload_size", stats.HistogramType,
			map[string]string{
				"module":   "batch_router",
				"destType": u.destName,
			})
		payloadSizeStat.Observe(float64(len(data.Message.Fields)))

		// Populate the CSV file and collect success/failure job IDs
		err := u.PopulateCsvFile(actionFile, line, data)
		if err != nil {
			// Collect the failed job ID only if it's valid
			if data.Metadata.JobID != 0 { // Check if JobID is not zero
				actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
			}
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

/*
1. Read CSV file from the path
2. API call to get the presigned URL
3. API call to upload the file to the presigned URL
4. Check if the file is uploaded successfully
5. API call to name the segment
6. Remove the file after uploading
7. Return the success and failed job IDs
*/
func (u *ClevertapBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	filePath := asyncDestStruct.FileName
	var failedJobs []int64
	var successJobs []int64

	actionFiles, err := u.createCSVFile(filePath)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while transforming the file. %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	uploadRetryableStat := u.statsFactory.NewTaggedStat("events_over_prescribed_limit", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": u.destName,
	})

	uploadRetryableStat.Count(len(actionFiles.FailedJobIDs))

	uploadTimeStat := u.statsFactory.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": u.destName,
	})

	presignedURL, urlErr := u.getPresignedS3URL(u.appKey, u.accessToken) // API

	if urlErr != nil {
		eventsAbortedStat := u.statsFactory.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": u.destName,
		})
		eventsAbortedStat.Count(len(asyncDestStruct.ImportingJobIDs))
		return common.AsyncUploadOutput{
			AbortCount:    len(asyncDestStruct.ImportingJobIDs),
			DestinationID: asyncDestStruct.Destination.ID,
			AbortJobIDs:   asyncDestStruct.ImportingJobIDs,
			AbortReason:   fmt.Sprintf("%s %v", "Error while fetching presigned url", urlErr),
		}
	}

	startTime := time.Now()
	errorDuringUpload := u.service.UploadBulkFile(actionFiles.CSVFilePath, presignedURL) // API
	uploadTimeStat.Since(startTime)

	if errorDuringUpload != nil {
		failedJobs = append(failedJobs, actionFiles.SuccessfulJobIDs...)

		// Append only failed job IDs if they exist
		if len(actionFiles.FailedJobIDs) > 0 {
			fmt.Println("Here")
			failedJobs = append(failedJobs, actionFiles.FailedJobIDs...)
		}
		// remove the file that could not be uploaded
		err = os.Remove(actionFiles.CSVFilePath)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
				FailedReason:  fmt.Sprintf("Error in removing zip file: %v", err.Error()),
				FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
				DestinationID: destination.ID,
			}
		} else {
			return common.AsyncUploadOutput{
				FailedJobIDs:  failedJobs,
				FailedReason:  fmt.Sprintf("error in uploading the bulk file: %v", errorDuringUpload.Error()),
				FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
				DestinationID: destination.ID,
			}
		}
	}

	failedJobs = append(failedJobs, actionFiles.FailedJobIDs...)
	successJobs = append(successJobs, actionFiles.SuccessfulJobIDs...)

	errorDuringNaming := u.namingSegment(presignedURL, actionFiles.CSVFilePath, u.appKey, u.accessToken) // API

	if errorDuringNaming != nil {
		return common.AsyncUploadOutput{
			AbortCount:    len(asyncDestStruct.ImportingJobIDs),
			AbortJobIDs:   asyncDestStruct.ImportingJobIDs,
			AbortReason:   fmt.Sprintf("%s %v", "Error while creating the segment", errorDuringNaming),
			DestinationID: destination.ID,
		}
	}

	err = os.Remove(actionFiles.CSVFilePath)
	if err != nil {
		u.logger.Error("Error in removing zip file: %v", err)
	}

	return common.AsyncUploadOutput{
		ImportingJobIDs: successJobs,
		FailedJobIDs:    append(asyncDestStruct.FailedJobIDs, failedJobs...),
		ImportingCount:  len(successJobs),
		FailedCount:     len(asyncDestStruct.FailedJobIDs) + len(failedJobs),
		DestinationID:   destination.ID,
	}
}

func (u *ClevertapBulkUploader) getPresignedS3URL(appKey, accessToken string) (string, error) {
	data := &HttpRequestData{
		Method:      http.MethodPost,
		Endpoint:    u.presignedURLEndpoint,
		ContentType: "application/json",
		appKey:      appKey,
		accessToken: accessToken,
	}

	body, statusCode, err := u.service.MakeHTTPRequest(data)
	if err != nil {
		return "", err
	}

	// Parse the response
	var result struct {
		PresignedS3URL string `json:"presignedS3URL"`
		Expiry         string `json:"expiry"`
		Status         string `json:"status"`
		Error          string `json:"error"`
		Code           int    `json:"code"`
	}
	if err := jsonFast.Unmarshal(body, &result); err != nil {
		return "", err
	}

	if statusCode != 200 {
		err := fmt.Errorf("Error while fetching preSignedUrl: %s", result.Error)
		return "", err
	}

	if result.PresignedS3URL == "" {
		err := fmt.Errorf("presigned URL is empty after parsing")
		return "", err
	}

	return result.PresignedS3URL, nil
}

func (u *ClevertapBulkUploader) namingSegment(presignedURL, csvFilePath, appKey, accessToken string) error {
	url := u.notifyEndpoint

	// Construct the request payload
	payload := map[string]interface{}{
		"name":     u.clevertapConnectionConfig.Config.Destination.SegmentName,
		"email":    u.clevertapConnectionConfig.Config.Destination.AdminEmail,
		"filename": csvFilePath,
		"creator":  u.clevertapConnectionConfig.Config.Destination.SenderName,
		"url":      presignedURL,
		"replace":  true,
	}

	payloadBytes, err := jsonFast.Marshal(payload)
	if err != nil {
		return err
	}

	// Create HttpRequestData
	data := &HttpRequestData{
		Method:      http.MethodPost,
		Endpoint:    url,
		Body:        bytes.NewBuffer(payloadBytes),
		ContentType: "application/json",
		appKey:      appKey,
		accessToken: accessToken,
	}

	// Use MakeHTTPRequest to send the request
	body, statusCode, err := u.service.MakeHTTPRequest(data)
	if err != nil {
		return err
	}
	// Parse the response
	var result struct {
		SegmentID int    `json:"Segment ID"`
		Status    string `json:"status"`
		Error     string `json:"error"`
		Code      int    `json:"code"`
	}
	if err := jsonFast.Unmarshal(body, &result); err != nil {
		return err
	}

	if statusCode != 200 {
		err := fmt.Errorf("Error while namimng segment: %s", result.Error)
		return err
	}

	if result.SegmentID == 0 {
		err := fmt.Errorf("Segment Creation is Unsuccessful")
		return err
	}

	return nil
}
