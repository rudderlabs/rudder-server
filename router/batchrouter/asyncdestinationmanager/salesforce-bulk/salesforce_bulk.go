package salesforcebulk

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func (s *SalesforceBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	asyncJob, err := prepareAsyncJob(job.EventPayload, job.JobID, s.config.Operation)
	if err != nil {
		return "", err
	}

	responsePayload, err := jsonrs.Marshal(asyncJob)
	if err != nil {
		return "", err
	}

	return string(responsePayload), nil
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

	jobsByOperation := groupJobsByOperation(input, s.config.Operation)

	var allImportingJobIDs []int64
	var allFailedJobIDs []int64
	var sfJobs []SalesforceJobInfo

	allFailedJobIDs = append(allFailedJobIDs, failedJobIDs...)

	for operation, jobs := range jobsByOperation {
		s.logger.Infof("Processing %d jobs for operation: %s", len(jobs), operation)

		remainingJobs := jobs

		for len(remainingJobs) > 0 {
			var (
				currentObjectInfo *ObjectInfo
				matchingJobs      []common.AsyncJob
				nonMatchingJobs   []common.AsyncJob
			)

			for _, job := range remainingJobs {
				info, err := extractObjectInfoFromJob(job, s.config)
				if err != nil {
					if jobID, ok := job.Metadata["job_id"].(float64); ok {
						s.logger.Errorf("Error extracting object info for job %d: %v", int64(jobID), err)
						allFailedJobIDs = append(allFailedJobIDs, int64(jobID))
					} else {
						s.logger.Errorf("Error extracting object info for job: %v", err)
					}
					continue
				}

				if currentObjectInfo == nil {
					currentObjectInfo = info
				}

				if info.ObjectType == currentObjectInfo.ObjectType && info.ExternalIDField == currentObjectInfo.ExternalIDField {
					matchingJobs = append(matchingJobs, job)
				} else {
					nonMatchingJobs = append(nonMatchingJobs, job)
				}
			}

			if currentObjectInfo == nil || len(matchingJobs) == 0 {
				break
			}

			s.hashMapMutex.Lock()
			csvFilePath, csvHeaders, insertedJobIDs, overflowedJobs, err := createCSVFile(
				destinationID,
				matchingJobs,
				s.dataHashToJobID,
				operation,
			)
			s.hashMapMutex.Unlock()

			if err != nil {
				s.logger.Errorf("Error creating CSV for operation %s: %v", operation, err)
				for _, job := range matchingJobs {
					if jobID, ok := job.Metadata["job_id"].(float64); ok {
						allFailedJobIDs = append(allFailedJobIDs, int64(jobID))
					}
				}
				break
			}

			if len(insertedJobIDs) == 0 {
				if err := os.Remove(csvFilePath); err != nil {
					s.logger.Debugf("Failed to remove empty CSV file %s: %v", csvFilePath, err)
				}
				s.logger.Errorf("No jobs fit in CSV for operation %s, marking as failed", operation)
				for _, job := range matchingJobs {
					if jobID, ok := job.Metadata["job_id"].(float64); ok {
						allFailedJobIDs = append(allFailedJobIDs, int64(jobID))
					}
				}
				break
			}

			s.logger.Infof("Created CSV with %d jobs for operation %s (batch %d of %d total)",
				len(insertedJobIDs), operation, len(allImportingJobIDs)/100+1, len(jobs))

			sfJobID, apiError := s.apiService.CreateJob(
				currentObjectInfo.ObjectType,
				operation,
				currentObjectInfo.ExternalIDField,
			)
			if apiError != nil {
				s.logger.Errorf("Error creating Salesforce job for operation %s: %v", operation, apiError)
				allFailedJobIDs = append(allFailedJobIDs, insertedJobIDs...)
				for _, job := range overflowedJobs {
					allFailedJobIDs = append(allFailedJobIDs, int64(job.Metadata["job_id"].(float64)))
				}
				break
			}

			apiError = s.apiService.UploadData(sfJobID, csvFilePath)
			if apiError != nil {
				s.logger.Errorf("Error uploading data for operation %s: %v", operation, apiError)
				_ = s.apiService.DeleteJob(sfJobID)
				allFailedJobIDs = append(allFailedJobIDs, insertedJobIDs...)
				for _, job := range overflowedJobs {
					allFailedJobIDs = append(allFailedJobIDs, int64(job.Metadata["job_id"].(float64)))
				}
				break
			}

			apiError = s.apiService.CloseJob(sfJobID)
			if apiError != nil {
				s.logger.Errorf("Error closing job for operation %s: %v", operation, apiError)
				allFailedJobIDs = append(allFailedJobIDs, insertedJobIDs...)
				for _, job := range overflowedJobs {
					allFailedJobIDs = append(allFailedJobIDs, int64(job.Metadata["job_id"].(float64)))
				}
				break
			}

			s.logger.Infof("Successfully created and closed Salesforce Bulk job %s for operation: %s", sfJobID, operation)

			allImportingJobIDs = append(allImportingJobIDs, insertedJobIDs...)
			sfJobs = append(sfJobs, SalesforceJobInfo{
				ID:        sfJobID,
				Operation: operation,
				Headers:   csvHeaders,
			})

			if err := os.Remove(csvFilePath); err != nil {
				s.logger.Debugf("Failed to remove CSV file %s: %v", csvFilePath, err)
			}

			remainingJobs = append(overflowedJobs, nonMatchingJobs...)
		}
	}

	if len(allImportingJobIDs) == 0 {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(allFailedJobIDs, importingJobIDs...),
			FailedReason:  "All operations failed to create Salesforce Bulk jobs",
			FailedCount:   len(allFailedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	importData := map[string]interface{}{"jobs": sfJobs}
	importID, _ := jsonrs.Marshal(importData)

	paramsWithImportID := map[string]interface{}{
		"importId": string(importID),
		"jobs":     sfJobs,
	}
	jobsJSON, _ := jsonrs.Marshal(paramsWithImportID)

	return common.AsyncUploadOutput{
		ImportingJobIDs:     allImportingJobIDs,
		ImportingParameters: json.RawMessage(jobsJSON),
		FailedJobIDs:        allFailedJobIDs,
		ImportingCount:      len(allImportingJobIDs),
		FailedCount:         len(allFailedJobIDs),
		DestinationID:       destinationID,
	}
}

func (s *SalesforceBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	var params struct {
		Jobs []SalesforceJobInfo `json:"jobs"`
	}

	err := jsonrs.Unmarshal([]byte(pollInput.ImportId), &params)
	if err != nil || len(params.Jobs) == 0 {
		return common.PollStatusResponse{
			StatusCode: 500,
			Complete:   false,
			Error:      fmt.Sprintf("Failed to parse poll parameters: %v", err),
		}
	}

	jobIDs := make([]string, len(params.Jobs))
	for i, job := range params.Jobs {
		jobIDs[i] = job.ID
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
	var params struct {
		Jobs []SalesforceJobInfo `json:"jobs"`
	}

	err := jsonrs.Unmarshal(input.Parameters, &params)
	if err != nil || len(params.Jobs) == 0 {
		return common.GetUploadStatsResponse{
			StatusCode: 500,
			Error:      fmt.Sprintf("Failed to parse parameters: %v", err),
		}
	}

	var allFailedRecords []map[string]string
	var allSuccessRecords []map[string]string
	var fetchErrors int

	for _, job := range params.Jobs {
		failedRecords, apiError := s.apiService.GetFailedRecords(job.ID)
		if apiError != nil {
			s.logger.Errorf("Failed to fetch failed records for job %s: %s", job.ID, apiError.Message)
			fetchErrors++
			continue
		}

		for i := range failedRecords {
			failedRecords[i]["_operation"] = job.Operation
			failedRecords[i]["_headers"] = strings.Join(job.Headers, ",")
		}

		successRecords, apiError := s.apiService.GetSuccessfulRecords(job.ID)
		if apiError != nil {
			s.logger.Errorf("Failed to fetch successful records for job %s: %s", job.ID, apiError.Message)
			fetchErrors++
			continue
		}

		for i := range successRecords {
			successRecords[i]["_operation"] = job.Operation
			successRecords[i]["_headers"] = strings.Join(job.Headers, ",")
		}

		allFailedRecords = append(allFailedRecords, failedRecords...)
		allSuccessRecords = append(allSuccessRecords, successRecords...)
	}

	metadata := s.matchRecordsToJobs(input.ImportingList, allFailedRecords, allSuccessRecords)

	return common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata:   metadata,
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
	defer s.hashMapMutex.RUnlock()

	for _, failedRecord := range failedRecords {
		operation := failedRecord["_operation"]
		headersStr := failedRecord["_headers"]
		headers := strings.Split(headersStr, ",")
		hash := calculateHashFromRecord(failedRecord, headers, operation)
		if jobIDs, exists := s.dataHashToJobID[hash]; exists {
			for _, jobID := range jobIDs {
				if jobID != 0 {
					metadata.AbortedKeys = append(metadata.AbortedKeys, jobID)
					if errorMsg, ok := failedRecord["sf__Error"]; ok && errorMsg != "" {
						metadata.AbortedReasons[jobID] = errorMsg
					} else if errorMsg, ok := failedRecord["Error"]; ok && errorMsg != "" {
						metadata.AbortedReasons[jobID] = errorMsg
					}
				}
			}
		}
	}

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
