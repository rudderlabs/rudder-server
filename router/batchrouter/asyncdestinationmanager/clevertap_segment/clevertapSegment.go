package clevertapSegment

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type clevertapServiceImpl struct {
	BulkApi          string
	NotifyApi        string
	ConnectionConfig *ConnectionConfig
}

// GetCleverTapEndpoint returns the API endpoint for the given region
func (u *clevertapServiceImpl) getCleverTapEndpoint(region string) (string, error) {
	// Mapping of regions to endpoints
	endpoints := map[string]string{
		"IN":        "in1.api.clevertap.com",
		"SINGAPORE": "sg1.api.clevertap.com",
		"US":        "us1.api.clevertap.com",
		"INDONESIA": "aps3.api.clevertap.com",
		"UAE":       "mec1.api.clevertap.com",
		"EU":        "api.clevertap.com",
	}

	// Normalize the region input to uppercase for case-insensitivity
	region = strings.ToUpper(region)

	// Check if the region exists in the map
	if endpoint, exists := endpoints[region]; exists {
		return endpoint, nil
	}

	// Return an error if the region is not recognized
	return "", fmt.Errorf("unknown region: %s", region)
}

func (u *clevertapServiceImpl) getBulkApi(destConfig DestinationConfig) *clevertapServiceImpl {
	endpoint, err := u.getCleverTapEndpoint(destConfig.region)
	if err != nil {
		return nil
	}
	return &clevertapServiceImpl{
		BulkApi:   fmt.Sprintf("https://%s/get_custom_list_segment_url", endpoint),
		NotifyApi: fmt.Sprintf("https://%s/upload_custom_list_segment_completed", endpoint),
	}
}

func (*ClevertapBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (u *clevertapServiceImpl) MakeHTTPRequest(data *HttpRequestData) ([]byte, int, error) {
	req, err := http.NewRequest(data.Method, data.Endpoint, data.Body)
	if err != nil {
		return nil, 500, err
	}
	req.Header.Add("X-CleverTap-Account-Id", data.appKey)
	req.Header.Add("X-CleverTap-Passcode", data.accessToken)
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

func (u *clevertapServiceImpl) UploadBulkFile(filePath string, presignedURL string) error {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get the file information
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := fileInfo.Size()

	fmt.Printf("Uploading file: %s, Size: %d bytes\n", fileInfo.Name(), fileSize)

	// Create the PUT request
	req, err := http.NewRequest("PUT", presignedURL, file)
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %w", err)
	}
	req.ContentLength = fileSize
	// Execute the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed, status: %s, response: %s", resp.Status, string(body))
	}

	fmt.Println("CSV file uploaded successfully!")
	return nil
}

func (u *clevertapServiceImpl) getPresignedS3URL(appKey string, accessToken string) (string, error) {
	data := &HttpRequestData{
		Method:      http.MethodPost,
		Endpoint:    u.BulkApi,
		ContentType: "application/json",
		appKey:      appKey,
		accessToken: accessToken,
	}

	body, _, err := u.MakeHTTPRequest(data)
	if err != nil {
		return "", err
	}

	// Parse the response
	var result struct {
		PresignedS3URL string `json:"presignedS3URL"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	if result.PresignedS3URL == "" {
		return "", fmt.Errorf("presigned URL is empty after parsing")
	}

	return result.PresignedS3URL, nil
}

// Function to convert *backendconfig.Connection to ConnectionConfig using marshal and unmarshal
func (u *clevertapServiceImpl) convertToConnectionConfig(conn *backendconfig.Connection) (*ConnectionConfig, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	// Marshal the backendconfig.Connection to JSON
	data, err := json.Marshal(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal connection: %w", err)
	}

	// Unmarshal the JSON into ConnectionConfig
	var connConfig ConnectionConfig
	err = json.Unmarshal(data, &connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal to ConnectionConfig: %w", err)
	}

	// Set default SenderName if it is empty
	if connConfig.Config.Destination.SenderName == "" {
		connConfig.Config.Destination.SenderName = "Rudderstack"
	}

	return &connConfig, nil
}

func (u *clevertapServiceImpl) namingSegment(destination *backendconfig.DestinationT, presignedURL, csvFilePath, appKey, accessToken string) error {
	url := u.NotifyApi

	// Construct the request payload
	payload := map[string]interface{}{
		"name":     u.ConnectionConfig.Config.Destination.SegmentName,
		"email":    u.ConnectionConfig.Config.Destination.AdminEmail,
		"filename": csvFilePath,
		"creator":  u.ConnectionConfig.Config.Destination.SenderName,
		"url":      presignedURL,
		"replace":  false,
	}

	payloadBytes, err := json.Marshal(payload)
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
	_, _, err = u.MakeHTTPRequest(data)
	if err != nil {
		return err
	}

	return nil
}

func (u *ClevertapBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	// connection := asyncDestStruct.Connection
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

	presignedURL, urlErr := u.service.getPresignedS3URL(u.appKey, u.accessToken)

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
			AbortReason:   fmt.Sprintf("%s %v", "Error while fetching presigned url", err.Error()),
		}
	}

	startTime := time.Now()
	errorDuringUpload := u.service.UploadBulkFile(actionFiles.CSVFilePath, presignedURL)
	uploadTimeStat.Since(startTime)

	if errorDuringUpload != nil {
		u.logger.Error("error in uploading the bulk file: %v", errorDuringUpload)
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

	errorDuringNaming := u.service.namingSegment(destination, presignedURL, actionFiles.CSVFilePath, u.appKey, u.accessToken)

	if errorDuringNaming != nil {
		// Handle error appropriately, e.g., log it or return it
		u.logger.Error("Error during naming segment: %v", errorDuringNaming)
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, failedJobs...),
			FailedReason:  fmt.Sprintf("Error during naming segment: %v", errorDuringNaming.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(failedJobs),
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
