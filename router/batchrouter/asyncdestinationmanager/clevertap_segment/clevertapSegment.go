package clevertapSegment

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type ClevertapServiceImpl struct {
	BulkApi          string
	NotifyApi        string
	ConnectionConfig *ConnectionConfig
}

func (*ClevertapBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (u *ClevertapServiceImpl) MakeHTTPRequest(data *HttpRequestData) ([]byte, int, error) {
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

func (u *ClevertapServiceImpl) UploadBulkFile(filePath, presignedURL string) error {
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
	return nil
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
	if err := json.Unmarshal(body, &result); err != nil {
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
	if err := json.Unmarshal(body, &result); err != nil {
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

	presignedURL, urlErr := u.getPresignedS3URL(u.appKey, u.accessToken)

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
	errorDuringUpload := u.service.UploadBulkFile(actionFiles.CSVFilePath, presignedURL)
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

	errorDuringNaming := u.namingSegment(presignedURL, actionFiles.CSVFilePath, u.appKey, u.accessToken)

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
