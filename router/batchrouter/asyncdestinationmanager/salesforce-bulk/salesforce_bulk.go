package salesforcebulk

import (
	"encoding/json"
	"fmt"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

// Transform extracts already-transformed Salesforce payload from job
// The Salesforce transformer (/salesforce/transform.js) has already run in Processor
// and performed field mapping via VDM. We just extract the result.
func (s *SalesforceBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	// Extract the Salesforce-formatted payload from body.JSON
	// This was created by the Salesforce transformer with VDM field mapping
	return common.GetMarshalledData(
		gjson.GetBytes(job.EventPayload, "body.JSON").String(),
		job.JobID,
	)
}

// Upload handles the bulk upload to Salesforce Bulk API 2.0
func (s *SalesforceBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	destinationID := destination.ID
	filePath := asyncDestStruct.FileName
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs

	// Read jobs from file
	input, err := readJobsFromFile(filePath)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf("Error reading jobs from file: %v", err),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	// Extract object type and external ID field from first job's context
	// In VDM/RETL flow, these come from context.externalId
	objectInfo, err := extractObjectInfo(input)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf("Error extracting object info: %v", err),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	// Create CSV file from jobs
	csvFilePath, insertedJobIDs, overflowedJobIDs, err := createCSVFile(
		destinationID,
		input,
		s.dataHashToJobID,
	)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf("Error creating CSV file: %v", err),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	importingJobIDs = insertedJobIDs
	failedJobIDs = append(failedJobIDs, overflowedJobIDs...)

	s.logger.Infof("Created CSV with %d jobs for Salesforce Bulk Upload", len(insertedJobIDs))

	// Create Salesforce Bulk API job
	jobID, apiError := s.apiService.CreateJob(
		objectInfo.ObjectType,
		s.config.Operation,
		objectInfo.ExternalIDField,
	)
	if apiError != nil {
		return s.handleAPIError(apiError, failedJobIDs, importingJobIDs, destinationID)
	}

	// Upload CSV data
	apiError = s.apiService.UploadData(jobID, csvFilePath)
	if apiError != nil {
		// Try to clean up the job
		_ = s.apiService.DeleteJob(jobID)
		return s.handleAPIError(apiError, failedJobIDs, importingJobIDs, destinationID)
	}

	// Close job to trigger processing
	apiError = s.apiService.CloseJob(jobID)
	if apiError != nil {
		return s.handleAPIError(apiError, failedJobIDs, importingJobIDs, destinationID)
	}

	s.logger.Infof("Successfully created and closed Salesforce Bulk job %s", jobID)

	// Return importing status with job ID
	return common.AsyncUploadOutput{
		ImportingJobIDs:     importingJobIDs,
		ImportingParameters: json.RawMessage(`{"jobId":"` + jobID + `"}`),
		FailedJobIDs:        failedJobIDs,
		ImportingCount:      len(importingJobIDs),
		FailedCount:         len(failedJobIDs),
		DestinationID:       destinationID,
	}
}

// Poll checks the status of an ongoing Salesforce Bulk API job
func (s *SalesforceBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	// Extract jobId from parameters
	jobID := pollInput.ImportId

	jobStatus, apiError := s.apiService.GetJobStatus(jobID)
	if apiError != nil {
		return s.handlePollError(apiError)
	}

	switch jobStatus.State {
	case "JobComplete":
		return common.PollStatusResponse{
			StatusCode: 200,
			Complete:   true,
			HasFailed:  jobStatus.NumberRecordsFailed > 0,
			// Store job ID for retrieving results in GetUploadStats
			FailedJobParameters: jobID,
		}
	case "InProgress", "UploadComplete":
		return common.PollStatusResponse{
			StatusCode: 200,
			InProgress: true,
		}
	case "Failed", "Aborted":
		return common.PollStatusResponse{
			StatusCode: 200,
			Complete:   true,
			HasFailed:  true,
			Error:      jobStatus.ErrorMessage,
		}
	default:
		return common.PollStatusResponse{
			StatusCode: 500,
			Complete:   false,
			Error:      fmt.Sprintf("Unknown job state: %s", jobStatus.State),
		}
	}
}

// GetUploadStats retrieves detailed statistics about the upload
func (s *SalesforceBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	// Parse jobId from parameters
	var params struct {
		JobID string `json:"jobId"`
	}
	err := jsonrs.Unmarshal(input.Parameters, &params)
	if err != nil {
		return common.GetUploadStatsResponse{
			StatusCode: 500,
			Error:      fmt.Sprintf("Failed to parse parameters: %v", err),
		}
	}

	// Fetch failed records
	failedRecords, apiError := s.apiService.GetFailedRecords(params.JobID)
	if apiError != nil {
		return common.GetUploadStatsResponse{
			StatusCode: apiError.StatusCode,
			Error:      fmt.Sprintf("Failed to fetch failed records: %s", apiError.Message),
		}
	}

	// Fetch successful records
	successRecords, apiError := s.apiService.GetSuccessfulRecords(params.JobID)
	if apiError != nil {
		return common.GetUploadStatsResponse{
			StatusCode: apiError.StatusCode,
			Error:      fmt.Sprintf("Failed to fetch successful records: %s", apiError.Message),
		}
	}

	// Match records to job IDs using hash tracking
	metadata := s.matchRecordsToJobs(input.ImportingList, failedRecords, successRecords)

	// Clear hash map after processing
	s.clearHashToJobID()

	return common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata:   metadata,
	}
}

// handleAPIError handles API errors and returns appropriate AsyncUploadOutput
func (s *SalesforceBulkUploader) handleAPIError(
	apiError *APIError,
	failedJobIDs, importingJobIDs []int64,
	destinationID string,
) common.AsyncUploadOutput {
	allFailedJobs := append(failedJobIDs, importingJobIDs...)

	switch apiError.Category {
	case "RefreshToken":
		// Token expired - jobs will be retried
		return common.AsyncUploadOutput{
			FailedJobIDs:  allFailedJobs,
			FailedReason:  fmt.Sprintf("OAuth token expired: %s", apiError.Message),
			FailedCount:   len(allFailedJobs),
			DestinationID: destinationID,
		}
	case "RateLimit":
		// Rate limited - jobs will be retried
		return common.AsyncUploadOutput{
			FailedJobIDs:  allFailedJobs,
			FailedReason:  fmt.Sprintf("Salesforce API rate limit: %s", apiError.Message),
			FailedCount:   len(allFailedJobs),
			DestinationID: destinationID,
		}
	case "BadRequest":
		// Invalid request - abort jobs
		return common.AsyncUploadOutput{
			AbortJobIDs:   allFailedJobs,
			AbortReason:   fmt.Sprintf("Invalid request: %s", apiError.Message),
			AbortCount:    len(allFailedJobs),
			DestinationID: destinationID,
		}
	default:
		// Server error - retry
		return common.AsyncUploadOutput{
			FailedJobIDs:  allFailedJobs,
			FailedReason:  apiError.Message,
			FailedCount:   len(allFailedJobs),
			DestinationID: destinationID,
		}
	}
}

// handlePollError handles errors during polling
func (s *SalesforceBulkUploader) handlePollError(apiError *APIError) common.PollStatusResponse {
	if apiError.Category == "RefreshToken" {
		return common.PollStatusResponse{
			StatusCode: 500,
			Error:      "OAuth token expired during poll",
			Complete:   false,
		}
	}

	switch apiError.StatusCode {
	case 429:
		return common.PollStatusResponse{
			StatusCode: 429,
			Error:      "Rate limit exceeded during poll",
			Complete:   false,
		}
	case 400, 404:
		return common.PollStatusResponse{
			StatusCode: apiError.StatusCode,
			Error:      apiError.Message,
			Complete:   true,
			HasFailed:  true,
		}
	default:
		return common.PollStatusResponse{
			StatusCode: 500,
			Error:      apiError.Message,
			Complete:   false,
		}
	}
}

// matchRecordsToJobs matches Salesforce results back to original job IDs
func (s *SalesforceBulkUploader) matchRecordsToJobs(
	importingList []*jobsdb.JobT,
	failedRecords, successRecords []map[string]string,
) common.EventStatMeta {
	metadata := common.EventStatMeta{
		FailedKeys:     make([]int64, 0),
		AbortedKeys:    make([]int64, 0),
		WarningKeys:    make([]int64, 0),
		SucceededKeys:  make([]int64, 0),
		FailedReasons:  make(map[int64]string),
		AbortedReasons: make(map[int64]string),
		WarningReasons: make(map[int64]string),
	}

	// Track failed records
	for _, failedRecord := range failedRecords {
		hash := calculateHashFromRecord(failedRecord)
		if jobID, exists := s.dataHashToJobID[hash]; exists && jobID != 0 {
			metadata.AbortedKeys = append(metadata.AbortedKeys, jobID)
			if errorMsg, ok := failedRecord["sf__Error"]; ok {
				metadata.AbortedReasons[jobID] = errorMsg
			}
		}
	}

	// Mark all non-failed jobs as succeeded
	failedJobIDSet := make(map[int64]bool)
	for _, jobID := range metadata.AbortedKeys {
		failedJobIDSet[jobID] = true
	}

	for _, job := range importingList {
		if !failedJobIDSet[job.JobID] {
			metadata.SucceededKeys = append(metadata.SucceededKeys, job.JobID)
		}
	}

	return metadata
}

// clearHashToJobID clears the hash tracking map
func (s *SalesforceBulkUploader) clearHashToJobID() {
	for k := range s.dataHashToJobID {
		delete(s.dataHashToJobID, k)
	}
}

