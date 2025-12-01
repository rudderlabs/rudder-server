package salesforcebulkupload

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/tidwall/gjson"

	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	augmenter "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/salesforce-bulk-upload/augmenter"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	oauthv2common "github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	oauthv2httpclient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
)

func NewManager(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
	backendConfig backendconfig.BackendConfig,
) (common.AsyncDestinationManager, error) {
	destinationInfo := &oauthv2.DestinationInfo{
		Config:           destination.Config,
		DefinitionConfig: destination.DestinationDefinition.Config,
		WorkspaceID:      destination.WorkspaceID,
		DestType:         destination.DestinationDefinition.Name,
		ID:               destination.ID,
	}
	httpClientTimeout := conf.GetDurationVar(30, time.Second, "SalesforceBulkUpload.httpClientTimeout")
	cache := oauthv2.NewOauthTokenCache()
	optionalArgs := &oauthv2httpclient.HttpClientOptionalArgs{
		Logger:              logger.Withn(obskit.DestinationID(destination.ID), obskit.WorkspaceID(destination.WorkspaceID)),
		Augmenter:           augmenter.NewRequestAugmenter(),
		OAuthBreakerOptions: oauthv2.ConfigToOauthBreakerOptions("BatchRouter.SALESFORCE_BULK_UPLOAD", conf),
	}
	originalHttpClient := &http.Client{Transport: &http.Transport{}, Timeout: httpClientTimeout}
	client := oauthv2httpclient.NewOAuthHttpClient(originalHttpClient, oauthv2common.RudderFlowDelivery, &cache, backendConfig, augmenter.GetAuthErrorCategoryForSalesforce, optionalArgs)
	apiService := NewAPIService(logger, destinationInfo, client)
	u := NewUploader(conf, logger, statsFactory, apiService, destinationInfo)
	return u, nil
}

func NewUploader(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	apiService APIServiceInterface,
	destinationInfo *oauthv2.DestinationInfo,
) *Uploader {
	u := &Uploader{
		logger:          logger,
		apiService:      apiService,
		dataHashToJobID: make(map[string][]int64),
		destinationInfo: destinationInfo,
		statsFactory:    statsFactory,
		destName:        destName,
	}
	u.config.maxBufferCapacity = conf.GetReloadableInt64Var(512*bytesize.KB, bytesize.B, "SalesforceBulkUpload.maxBufferCapacity")
	return u
}

func (s *Uploader) Transform(job *jobsdb.JobT) (string, error) {
	// Parse the event payload directly
	if !gjson.ValidBytes(job.EventPayload) {
		return "", fmt.Errorf("invalid JSON in event payload")
	}

	// Extract required fields from the input
	traits := gjson.GetBytes(job.EventPayload, "traits")
	externalID := gjson.GetBytes(job.EventPayload, "context.externalId")

	// Build the metadata object
	metadata := make(map[string]interface{})
	metadata["job_id"] = float64(job.JobID)

	// We are supporting only upsert operation
	metadata["rudderOperation"] = "upsert"

	// Add externalId to metadata if it exists
	var externalIdArray []interface{}
	if externalID.Exists() {
		if err := jsonrs.Unmarshal([]byte(externalID.Raw), &externalIdArray); err == nil {
			metadata["externalId"] = externalIdArray
		}
	}

	// Build the message object
	message := make(map[string]interface{})

	// Add all traits to message
	if traits.Exists() && traits.IsObject() {
		traits.ForEach(func(key, value gjson.Result) bool {
			message[key.String()] = value.Value()
			return true
		})
	}

	externalIdObject, err := extractFromVDM(externalIdArray)
	if err != nil {
		return "", fmt.Errorf("failed to extract externalId: %w", err)
	}
	// Add externalId field to message
	message[externalIdObject.ExternalIDField] = externalIdObject.ExternalIDValue

	// Create the final AsyncJob structure
	asyncJob := common.AsyncJob{
		Message:  message,
		Metadata: metadata,
	}

	// Marshal and return
	responsePayload, err := jsonrs.Marshal(asyncJob)
	if err != nil {
		return "", fmt.Errorf("failed to marshal async job: %w", err)
	}

	return string(responsePayload), nil
}

func (s *Uploader) readJobsFromFile(filePath string) ([]common.AsyncJob, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	var jobs []common.AsyncJob
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, int(s.config.maxBufferCapacity.Load()))

	for scanner.Scan() {
		var job common.AsyncJob
		if err := jsonrs.Unmarshal(scanner.Bytes(), &job); err != nil {
			return nil, fmt.Errorf("unmarshalling job: %w", err)
		}
		jobs = append(jobs, job)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning file: %w", err)
	}

	return jobs, nil
}

func (s *Uploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	destinationID := destination.ID
	filePath := asyncDestStruct.FileName
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs

	input, err := s.readJobsFromFile(filePath)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf("Error reading jobs from file: %v", err),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	objectInfo, err := extractObjectInfo(input)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf("Error extracting object info: %v", err),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	var allImportingJobIDs []int64
	var allFailedJobIDs []int64
	var sfJobs []SalesforceJobInfo

	for len(input) > 0 {
		csvFilePath, csvHeaders, insertedJobIDs, overflowedJobs, err := createCSVFile(
			destinationID,
			input,
			s.dataHashToJobID,
		)

		if err != nil {
			s.logger.Errorn("Error creating CSV: %v", obskit.Error(err))
			for _, job := range input {
				if jobID, ok := job.Metadata["job_id"].(float64); ok {
					allFailedJobIDs = append(allFailedJobIDs, int64(jobID))
				}
			}
			break
		}

		if len(insertedJobIDs) == 0 {
			if err := os.Remove(csvFilePath); err != nil {
				s.logger.Debugn("Failed to remove empty CSV file %s: %v", logger.NewStringField("csvFilePath", csvFilePath), obskit.Error(err))
			}
			s.logger.Errorn("No jobs fit in CSV, marking as failed")
			for _, job := range input {
				if jobID, ok := job.Metadata["job_id"].(float64); ok {
					allFailedJobIDs = append(allFailedJobIDs, int64(jobID))
				}
			}
			break
		}

		s.logger.Infon("Created CSV with %d jobs (batch %d of %d total)",
			logger.NewIntField("jobs", int64(len(insertedJobIDs))),
			logger.NewIntField("batch", int64(len(allImportingJobIDs)/100+1)),
			logger.NewIntField("total", int64(len(input))),
		)
		sfJobID, apiError := s.apiService.CreateJob(
			objectInfo.ObjectType,
			"upsert",
			objectInfo.ExternalIDField,
		)
		if apiError != nil {
			s.logger.Errorn("Error creating Salesforce job for operation upsert: %v", logger.NewStringField("apiErrorMessage", apiError.Message))
			allFailedJobIDs = append(allFailedJobIDs, insertedJobIDs...)
			for _, job := range overflowedJobs {
				allFailedJobIDs = append(allFailedJobIDs, int64(job.Metadata["job_id"].(float64)))
			}
			break
		}

		apiError = s.apiService.UploadData(sfJobID, csvFilePath)
		if apiError != nil {
			s.logger.Errorn("Error uploading data for operation upsert: %v", logger.NewStringField("apiErrorMessage", apiError.Message))
			_ = s.apiService.DeleteJob(sfJobID)
			allFailedJobIDs = append(allFailedJobIDs, insertedJobIDs...)
			for _, job := range overflowedJobs {
				allFailedJobIDs = append(allFailedJobIDs, int64(job.Metadata["job_id"].(float64)))
			}
			break
		}

		apiError = s.apiService.CloseJob(sfJobID)
		if apiError != nil {
			s.logger.Errorn("Error closing job for operation upsert: %v", logger.NewStringField("apiErrorMessage", apiError.Message))
			allFailedJobIDs = append(allFailedJobIDs, insertedJobIDs...)
			for _, job := range overflowedJobs {
				allFailedJobIDs = append(allFailedJobIDs, int64(job.Metadata["job_id"].(float64)))
			}
			break
		}

		s.logger.Infon("Successfully created and closed Salesforce Bulk job %s for operation upsert", logger.NewStringField("jobID", sfJobID))

		allImportingJobIDs = append(allImportingJobIDs, insertedJobIDs...)
		sfJobs = append(sfJobs, SalesforceJobInfo{
			ID:      sfJobID,
			Headers: csvHeaders,
		})

		if err := os.Remove(csvFilePath); err != nil {
			s.logger.Debugn("Failed to remove CSV file %s: %v", logger.NewStringField("csvFilePath", csvFilePath), obskit.Error(err))
		}

		input = overflowedJobs
	}

	if len(allImportingJobIDs) == 0 {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(allFailedJobIDs, importingJobIDs...),
			FailedReason:  "Unable to upload data to Salesforce Bulk jobs, retrying in next iteration",
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

func (s *Uploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
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

func (s *Uploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
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

	for _, job := range params.Jobs {
		failedRecords, apiError := s.apiService.GetFailedRecords(job.ID)
		if apiError != nil {
			s.logger.Errorn("Failed to fetch failed records for job %s: %s", logger.NewStringField("jobID", job.ID), logger.NewStringField("apiError", apiError.Message))
			return common.GetUploadStatsResponse{
				StatusCode: 500,
				Error:      fmt.Sprintf("Failed to fetch failed records for job %s: %s", job.ID, apiError.Message),
			}
		}

		for i := range failedRecords {
			failedRecords[i]["_headers"] = strings.Join(job.Headers, ",")
		}

		successRecords, apiError := s.apiService.GetSuccessfulRecords(job.ID)
		if apiError != nil {
			s.logger.Errorn("Failed to fetch successful records for job %s: %s", logger.NewStringField("jobID", job.ID), logger.NewStringField("apiError", apiError.Message))
			return common.GetUploadStatsResponse{
				StatusCode: 500,
				Error:      fmt.Sprintf("Failed to fetch successful records for job %s: %s", job.ID, apiError.Message),
			}
		}

		for i := range successRecords {
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

func (s *Uploader) handlePollError(apiError *APIError) common.PollStatusResponse {
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

func (s *Uploader) matchRecordsToJobs(
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

	for _, failedRecord := range failedRecords {
		headersStr := failedRecord["_headers"]
		headers := strings.Split(headersStr, ",")
		hash := calculateHashFromRecord(failedRecord, headers)
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

	for _, successRecord := range successRecords {
		headersStr := successRecord["_headers"]
		headers := strings.Split(headersStr, ",")
		hash := calculateHashFromRecord(successRecord, headers)
		if jobIDs, exists := s.dataHashToJobID[hash]; exists {
			for _, jobID := range jobIDs {
				if jobID != 0 {
					metadata.SucceededKeys = append(metadata.SucceededKeys, jobID)
				}
			}
		}
	}

	if len(metadata.SucceededKeys)+len(metadata.AbortedKeys) != len(importingList) {
		s.logger.Errorn(
			"Number of succeeded and aborted keys do not match the number of importing jobs, %d + %d != %d",
			logger.NewIntField("succeeded_keys", int64(len(metadata.SucceededKeys))),
			logger.NewIntField("aborted_keys", int64(len(metadata.AbortedKeys))),
			logger.NewIntField("importing_jobs", int64(len(importingList))),
		)

		successJobIDSet := make(map[int64]bool)
		for _, jobID := range metadata.SucceededKeys {
			successJobIDSet[jobID] = true
		}

		for _, job := range importingList {
			if !successJobIDSet[job.JobID] {
				metadata.FailedKeys = append(metadata.FailedKeys, job.JobID)
				metadata.FailedReasons[job.JobID] = "Input hash is not found in the data hash to job ID map or the job is not found in the successRespons or failedResponse"
			}
		}
	}
	return metadata
}
