package salesforcebulk

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func (s *SalesforceBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(
		gjson.GetBytes(job.EventPayload, "body.JSON").String(),
		job.JobID,
	)
}

func (s *SalesforceBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	destinationID := destination.ID
	filePath := asyncDestStruct.FileName
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs

	input, err := readJobsFromFile(filePath)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf("Error reading jobs from file: %v", err),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	objectInfo, err := extractObjectInfo(input, s.config)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf("Error extracting object info: %v", err),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	jobsByOperation := groupJobsByOperation(input, s.config.Operation)

	var allImportingJobIDs []int64
	var allFailedJobIDs []int64
	var sfJobIDs []string

	allFailedJobIDs = append(allFailedJobIDs, failedJobIDs...)

	for operation, jobs := range jobsByOperation {
		s.logger.Infof("Processing %d jobs for operation: %s", len(jobs), operation)

		s.hashMapMutex.Lock()
		csvFilePath, csvHeaders, insertedJobIDs, overflowedJobIDs, err := createCSVFile(
			destinationID,
			jobs,
			s.dataHashToJobID,
		)
		s.csvHeaders = csvHeaders
		s.hashMapMutex.Unlock()

		if err != nil {
			s.logger.Errorf("Error creating CSV for operation %s: %v", operation, err)
			for _, job := range jobs {
				if jobID, ok := job.Metadata["job_id"].(float64); ok {
					allFailedJobIDs = append(allFailedJobIDs, int64(jobID))
				}
			}
			continue
		}

		defer func(path string) {
			if err := os.Remove(path); err != nil {
				s.logger.Debugf("Failed to remove CSV file %s: %v", path, err)
			}
		}(csvFilePath)

		allFailedJobIDs = append(allFailedJobIDs, overflowedJobIDs...)

		s.logger.Infof("Created CSV with %d jobs for operation: %s", len(insertedJobIDs), operation)

		sfJobID, apiError := s.apiService.CreateJob(
			objectInfo.ObjectType,
			operation,
			objectInfo.ExternalIDField,
		)
		if apiError != nil {
			s.logger.Errorf("Error creating Salesforce job for operation %s: %v", operation, apiError)
			allFailedJobIDs = append(allFailedJobIDs, insertedJobIDs...)
			continue
		}

		apiError = s.apiService.UploadData(sfJobID, csvFilePath)
		if apiError != nil {
			s.logger.Errorf("Error uploading data for operation %s: %v", operation, apiError)
			_ = s.apiService.DeleteJob(sfJobID)
			allFailedJobIDs = append(allFailedJobIDs, insertedJobIDs...)
			continue
		}

		apiError = s.apiService.CloseJob(sfJobID)
		if apiError != nil {
			s.logger.Errorf("Error closing job for operation %s: %v", operation, apiError)
			allFailedJobIDs = append(allFailedJobIDs, insertedJobIDs...)
			continue
		}

		s.logger.Infof("Successfully created and closed Salesforce Bulk job %s for operation: %s", sfJobID, operation)

		allImportingJobIDs = append(allImportingJobIDs, insertedJobIDs...)
		sfJobIDs = append(sfJobIDs, sfJobID)
	}

	if len(allImportingJobIDs) == 0 {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(allFailedJobIDs, importingJobIDs...),
			FailedReason:  "All operations failed to create Salesforce Bulk jobs",
			FailedCount:   len(allFailedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	jobIDsJSON := fmt.Sprintf(`{"jobIds":["%s"]}`, strings.Join(sfJobIDs, `","`))

	return common.AsyncUploadOutput{
		ImportingJobIDs:     allImportingJobIDs,
		ImportingParameters: json.RawMessage(jobIDsJSON),
		FailedJobIDs:        allFailedJobIDs,
		ImportingCount:      len(allImportingJobIDs),
		FailedCount:         len(allFailedJobIDs),
		DestinationID:       destinationID,
	}
}

func (s *SalesforceBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	var params struct {
		JobIDs []string `json:"jobIds"`
	}

	err := jsonrs.Unmarshal([]byte(pollInput.ImportId), &params)
	var jobIDs []string

	if err != nil || len(params.JobIDs) == 0 {
		jobIDs = []string{pollInput.ImportId}
	} else {
		jobIDs = params.JobIDs
	}

	allComplete := true
	anyInProgress := false
	anyFailed := false
	var errors []string

	for _, jobID := range jobIDs {
		jobStatus, apiError := s.apiService.GetJobStatus(jobID)
		if apiError != nil {
			return s.handlePollError(apiError)
		}

		switch jobStatus.State {
		case "JobComplete":
			if jobStatus.NumberRecordsFailed > 0 {
				anyFailed = true
			}
		case "InProgress", "UploadComplete":
			allComplete = false
			anyInProgress = true
		case "Failed", "Aborted":
			anyFailed = true
			if jobStatus.ErrorMessage != "" {
				errors = append(errors, fmt.Sprintf("Job %s: %s", jobID, jobStatus.ErrorMessage))
			}
		default:
			return common.PollStatusResponse{
				StatusCode: 500,
				Complete:   false,
				Error:      fmt.Sprintf("Unknown job state for %s: %s", jobID, jobStatus.State),
			}
		}
	}

	if allComplete {
		errorMsg := ""
		if len(errors) > 0 {
			errorMsg = strings.Join(errors, "; ")
		}

		return common.PollStatusResponse{
			StatusCode:          200,
			Complete:            true,
			HasFailed:           anyFailed,
			FailedJobParameters: pollInput.ImportId,
			Error:               errorMsg,
		}
	}

	if anyInProgress {
		return common.PollStatusResponse{
			StatusCode: 200,
			InProgress: true,
		}
	}

	return common.PollStatusResponse{
		StatusCode: 200,
		InProgress: true,
	}
}

func (s *SalesforceBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	var paramsMulti struct {
		JobIDs []string `json:"jobIds"`
	}

	err := jsonrs.Unmarshal(input.Parameters, &paramsMulti)
	var jobIDs []string

	if err != nil || len(paramsMulti.JobIDs) == 0 {
		var paramsSingle struct {
			JobID string `json:"jobId"`
		}
		err = jsonrs.Unmarshal(input.Parameters, &paramsSingle)
		if err != nil || paramsSingle.JobID == "" {
			return common.GetUploadStatsResponse{
				StatusCode: 500,
				Error:      fmt.Sprintf("Failed to parse parameters: %v", err),
			}
		}
		jobIDs = []string{paramsSingle.JobID}
	} else {
		jobIDs = paramsMulti.JobIDs
	}

	var allFailedRecords []map[string]string
	var allSuccessRecords []map[string]string

	for _, jobID := range jobIDs {
		failedRecords, apiError := s.apiService.GetFailedRecords(jobID)
		if apiError != nil {
			s.logger.Errorf("Failed to fetch failed records for job %s: %s", jobID, apiError.Message)
			continue
		}

		successRecords, apiError := s.apiService.GetSuccessfulRecords(jobID)
		if apiError != nil {
			s.logger.Errorf("Failed to fetch successful records for job %s: %s", jobID, apiError.Message)
			continue
		}

		allFailedRecords = append(allFailedRecords, failedRecords...)
		allSuccessRecords = append(allSuccessRecords, successRecords...)
	}

	metadata := s.matchRecordsToJobs(input.ImportingList, allFailedRecords, allSuccessRecords)
	s.clearHashToJobID()

	return common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata:   metadata,
	}
}

func (s *SalesforceBulkUploader) handleAPIError(
	apiError *APIError,
	failedJobIDs, importingJobIDs []int64,
	destinationID string,
) common.AsyncUploadOutput {
	allFailedJobs := append(failedJobIDs, importingJobIDs...)

	switch apiError.Category {
	case "RefreshToken":
		return common.AsyncUploadOutput{
			FailedJobIDs:  allFailedJobs,
			FailedReason:  fmt.Sprintf("OAuth token expired: %s", apiError.Message),
			FailedCount:   len(allFailedJobs),
			DestinationID: destinationID,
		}
	case "RateLimit":
		return common.AsyncUploadOutput{
			FailedJobIDs:  allFailedJobs,
			FailedReason:  fmt.Sprintf("Salesforce API rate limit: %s", apiError.Message),
			FailedCount:   len(allFailedJobs),
			DestinationID: destinationID,
		}
	case "BadRequest":
		return common.AsyncUploadOutput{
			AbortJobIDs:   allFailedJobs,
			AbortReason:   fmt.Sprintf("Invalid request: %s", apiError.Message),
			AbortCount:    len(allFailedJobs),
			DestinationID: destinationID,
		}
	default:
		return common.AsyncUploadOutput{
			FailedJobIDs:  allFailedJobs,
			FailedReason:  apiError.Message,
			FailedCount:   len(allFailedJobs),
			DestinationID: destinationID,
		}
	}
}

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

	s.hashMapMutex.RLock()
	csvHeaders := s.csvHeaders

	for _, failedRecord := range failedRecords {
		hash := calculateHashFromRecord(failedRecord, csvHeaders)
		if jobID, exists := s.dataHashToJobID[hash]; exists && jobID != 0 {
			metadata.AbortedKeys = append(metadata.AbortedKeys, jobID)
			if errorMsg, ok := failedRecord["sf__Error"]; ok && errorMsg != "" {
				metadata.AbortedReasons[jobID] = errorMsg
			} else if errorMsg, ok := failedRecord["Error"]; ok && errorMsg != "" {
				metadata.AbortedReasons[jobID] = errorMsg
			}
		}
	}

	s.hashMapMutex.RUnlock()

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

func (s *SalesforceBulkUploader) clearHashToJobID() {
	s.hashMapMutex.Lock()
	defer s.hashMapMutex.Unlock()

	for k := range s.dataHashToJobID {
		delete(s.dataHashToJobID, k)
	}
}
