package salesforcebulkupload

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
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
	httpClientTimeout := conf.GetDurationVar(30, time.Second, "SalesforceBulkUpload.httpClientTimeout")
	cache := oauthv2.NewOauthTokenCache()
	childLogger := logger.Child("salesforcebulkupload").Withn(obskit.DestinationID(destination.ID), obskit.WorkspaceID(destination.WorkspaceID))

	optionalArgs := &oauthv2httpclient.HttpClientOptionalArgs{
		Logger:              childLogger,
		Augmenter:           augmenter.NewRequestAugmenter(),
		OAuthBreakerOptions: oauthv2.ConfigToOauthBreakerOptions("BatchRouter.SALESFORCE_BULK_UPLOAD", conf),
	}
	originalHttpClient := &http.Client{Transport: &http.Transport{}, Timeout: httpClientTimeout}
	client := oauthv2httpclient.NewOAuthHttpClient(originalHttpClient, oauthv2common.RudderFlowDelivery, &cache, backendConfig, func(responseBody []byte) (string, error) {
		return augmenter.GetAuthErrorCategoryForSalesforce(responseBody), nil
	}, optionalArgs)
	apiService := newAPIService(childLogger, destination, client)
	u := NewUploader(conf, childLogger, statsFactory, apiService, destination)
	return u, nil
}

func NewUploader(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	apiService APIServiceInterface,
	destination *backendconfig.DestinationT,
) *Uploader {
	u := &Uploader{
		logger:            logger,
		apiService:        apiService,
		externalIDToJobID: make(map[string][]int64),
		destination:       destination,
		statsFactory:      statsFactory,
		destName:          destName,
	}
	u.config.maxBufferCapacity = conf.GetReloadableInt64Var(512*bytesize.KB, bytesize.B, "SalesforceBulkUpload.maxBufferCapacity")
	return u
}

func (s *Uploader) Transform(job *jobsdb.JobT) (string, error) {
	// Extract required fields from the input
	traits := gjson.GetBytes(job.EventPayload, "traits")
	externalID := gjson.GetBytes(job.EventPayload, "context.externalId")

	// Build the metadata object
	metadata := make(map[string]any)
	metadata["job_id"] = float64(job.JobID)

	// We are supporting only upsert operation
	metadata["rudderOperation"] = "upsert"

	// Add externalId to metadata if it exists
	var externalIdArray []any
	if externalID.Exists() {
		if err := jsonrs.Unmarshal([]byte(externalID.Raw), &externalIdArray); err == nil {
			metadata["externalId"] = externalIdArray
		}
	}

	// Build the message object
	message := make(map[string]any)

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

func (s *Uploader) Upload(_ context.Context, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	destinationID := destination.ID
	filePath := asyncDestStruct.FileName
	importingJobIDs := asyncDestStruct.ImportingJobIDs

	input, err := s.readJobsFromFile(filePath)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  importingJobIDs,
			FailedReason:  fmt.Sprintf("Error reading jobs from file: %v", err),
			FailedCount:   len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	objectInfo, err := extractObjectInfo(input)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  importingJobIDs,
			FailedReason:  fmt.Sprintf("Error extracting object info: %v", err),
			FailedCount:   len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	if objectInfo.ExternalIDField == "" {
		return common.AsyncUploadOutput{
			FailedJobIDs:  importingJobIDs,
			FailedReason:  "externalId field is empty; cannot correlate poll results back to jobs",
			FailedCount:   len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	// Drop events without an externalId value up front: with no upsert key they
	// cannot be sent to Salesforce, so abort them with an error rather than
	// emitting an uncorrelatable CSV row. Valid events continue to the upload.
	validJobs := make([]common.AsyncJob, 0, len(input))
	validJobIDs := make([]int64, 0, len(input))
	var abortedJobIDs []int64
	for _, job := range input {
		jobID := int64(job.Metadata["job_id"].(float64))
		if externalIDValue, _ := common.FormatCSVValue(job.Message[objectInfo.ExternalIDField]); externalIDValue == "" {
			abortedJobIDs = append(abortedJobIDs, jobID)
			continue
		}
		validJobs = append(validJobs, job)
		validJobIDs = append(validJobIDs, jobID)
	}
	if len(abortedJobIDs) > 0 {
		s.logger.Infon("Aborting events with missing externalId", logger.NewIntField("abortedJobs", int64(len(abortedJobIDs))))
	}

	csvFilePath, externalIDToJobID, err := createCSVFile(
		destinationID,
		objectInfo.ExternalIDField,
		validJobs,
	)
	defer func() {
		if err := os.Remove(csvFilePath); err != nil {
			s.logger.Debugn("Failed to remove CSV file.", logger.NewStringField("csvFilePath", csvFilePath), obskit.Error(err))
		}
	}()
	if err != nil {
		s.logger.Errorn("Error creating CSV", obskit.Error(err))
		return common.AsyncUploadOutput{
			FailedJobIDs:  importingJobIDs,
			FailedReason:  fmt.Sprintf("Error creating CSV: %v", err),
			FailedCount:   len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	s.externalIDToJobID = externalIDToJobID

	// The source is expected to send unique externalIds per batch. Fewer keys
	// than valid jobs means duplicates collided onto the same key, which makes
	// per-job status attribution ambiguous after polling.
	if len(externalIDToJobID) < len(validJobs) {
		s.logger.Warnn("Duplicate externalId values detected in batch; per-job status attribution may be ambiguous",
			logger.NewIntField("uniqueExternalIds", int64(len(externalIDToJobID))),
			logger.NewIntField("jobs", int64(len(validJobs))),
		)
	}

	s.logger.Infon("Created CSV file",
		logger.NewStringField("csvFilePath", csvFilePath),
		logger.NewIntField("jobs", int64(len(validJobs))),
	)
	sfJobID, apiError := s.apiService.CreateJob(
		objectInfo.ObjectType,
		"upsert",
		objectInfo.ExternalIDField,
	)
	if apiError != nil {
		s.logger.Errorn("Error creating Salesforce job for operation upsert.", logger.NewStringField("apiErrorMessage", apiError.Message), logger.NewStringField("category", apiError.Category), logger.NewIntField("statusCode", int64(apiError.StatusCode)))
		return common.AsyncUploadOutput{
			FailedJobIDs:  importingJobIDs,
			FailedReason:  fmt.Sprintf("Error creating Salesforce job: %v", apiError.Message),
			FailedCount:   len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	apiError = s.apiService.UploadData(sfJobID, csvFilePath)
	if apiError != nil {
		s.logger.Errorn("Error uploading data for operation upsert", logger.NewStringField("apiErrorMessage", apiError.Message))
		if err := s.apiService.DeleteJob(sfJobID); err != nil {
			s.logger.Errorn("Error deleting Salesforce job.", logger.NewStringField("apiErrorMessage", err.Message))
		}
		return common.AsyncUploadOutput{
			FailedJobIDs:  importingJobIDs,
			FailedReason:  fmt.Sprintf("Error uploading data: %v", apiError.Message),
			FailedCount:   len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	apiError = s.apiService.CloseJob(sfJobID)
	if apiError != nil {
		s.logger.Errorn("Error closing job for operation upsert.", logger.NewStringField("apiErrorMessage", apiError.Message))
		if err := s.apiService.DeleteJob(sfJobID); err != nil {
			s.logger.Errorn("Error deleting Salesforce job.", logger.NewStringField("apiErrorMessage", err.Message))
		}
		return common.AsyncUploadOutput{
			FailedJobIDs:  importingJobIDs,
			FailedReason:  fmt.Sprintf("Error closing job: %v", apiError.Message),
			FailedCount:   len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	s.logger.Infon("Successfully created and closed Salesforce Bulk job", logger.NewStringField("jobID", sfJobID))

	importParameters, err := jsonrs.Marshal(common.ImportParameters{
		ImportId: SalesforceJobInfo{
			ID:              sfJobID,
			ExternalIDField: objectInfo.ExternalIDField,
		},
		ImportCount: len(validJobIDs),
	})
	if err != nil {
		s.logger.Errorn("marshalling parameters", obskit.Error(err))
	}
	return common.AsyncUploadOutput{
		ImportingJobIDs:     validJobIDs,
		ImportingParameters: importParameters,
		ImportingCount:      len(validJobIDs),
		AbortJobIDs:         abortedJobIDs,
		AbortReason:         "externalId is missing for the event; cannot upsert to Salesforce",
		AbortCount:          len(abortedJobIDs),
		DestinationID:       destinationID,
	}
}

func (s *Uploader) Poll(_ context.Context, pollInput common.AsyncPoll) common.PollStatusResponse {
	var saleforceJobInfo SalesforceJobInfo

	err := jsonrs.Unmarshal([]byte(pollInput.ImportId), &saleforceJobInfo)
	if err != nil {
		return common.PollStatusResponse{
			StatusCode: 500,
			Error:      fmt.Sprintf("Failed to parse poll parameters: %v", err.Error()),
		}
	}

	jobStatus, apiError := s.apiService.GetJobStatus(saleforceJobInfo.ID)
	if apiError != nil {
		return s.handlePollError(apiError)
	}

	switch jobStatus.State {
	case "JobComplete":
		hasFailed := jobStatus.NumberRecordsFailed > 0
		if !hasFailed {
			s.clearExternalIDToJobID()
		}
		return common.PollStatusResponse{
			StatusCode: http.StatusOK,
			Complete:   true,
			HasFailed:  hasFailed,
			InProgress: false,
		}
	case "InProgress", "UploadComplete":
		return common.PollStatusResponse{
			StatusCode: http.StatusOK,
			InProgress: true,
		}
	case "Failed", "Aborted":
		return common.PollStatusResponse{
			StatusCode: http.StatusOK,
			Complete:   true,
			HasFailed:  true,
			InProgress: false,
			Error:      fmt.Sprintf("Job %s: %s", saleforceJobInfo.ID, jobStatus.ErrorMessage),
		}
	default:
		return common.PollStatusResponse{
			StatusCode: 500,
			Complete:   false,
			Error:      fmt.Sprintf("Unknown job state for %s: %s", saleforceJobInfo.ID, jobStatus.State),
		}
	}
}

func (s *Uploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	defer func() {
		s.clearExternalIDToJobID()
	}()
	var saleforceJobInfo SalesforceJobInfo
	importId := gjson.GetBytes(input.Parameters, "importId").String()
	err := jsonrs.Unmarshal([]byte(importId), &saleforceJobInfo)
	if err != nil {
		return common.GetUploadStatsResponse{
			StatusCode: http.StatusInternalServerError,
			Error:      fmt.Sprintf("Failed to parse poll parameters: %v", err.Error()),
		}
	}

	failedRecords, apiError := s.apiService.GetFailedRecords(saleforceJobInfo.ID)
	if apiError != nil {
		s.logger.Errorn("Failed to fetch failed records for job", logger.NewStringField("jobID", saleforceJobInfo.ID), logger.NewStringField("apiError", apiError.Message))
		return common.GetUploadStatsResponse{
			StatusCode: http.StatusInternalServerError,
			Error:      fmt.Sprintf("Failed to fetch failed records for job: %s, %s", saleforceJobInfo.ID, apiError.Message),
		}
	}
	successRecords, apiError := s.apiService.GetSuccessfulRecords(saleforceJobInfo.ID)
	if apiError != nil {
		s.logger.Errorn("Failed to fetch successful records for job", logger.NewStringField("jobID", saleforceJobInfo.ID), logger.NewStringField("apiError", apiError.Message))
		return common.GetUploadStatsResponse{
			StatusCode: http.StatusInternalServerError,
			Error:      fmt.Sprintf("Failed to fetch successful records for job: %s, %s", saleforceJobInfo.ID, apiError.Message),
		}
	}

	metadata := s.matchRecordsToJobs(input.ImportingList, failedRecords, successRecords, saleforceJobInfo.ExternalIDField)
	return common.GetUploadStatsResponse{
		StatusCode: http.StatusOK,
		Metadata:   metadata,
	}
}

func (s *Uploader) handlePollError(apiError *APIError) common.PollStatusResponse {
	if apiError.Category == "RefreshToken" {
		return common.PollStatusResponse{
			StatusCode: http.StatusInternalServerError,
			Error:      "OAuth token expired during poll",
		}
	}

	switch apiError.StatusCode {
	case 429:
		return common.PollStatusResponse{
			StatusCode: http.StatusTooManyRequests,
			Error:      "Rate limit exceeded during poll",
		}
	case 400, 404:
		return common.PollStatusResponse{
			StatusCode: apiError.StatusCode,
			Error:      apiError.Message,
		}
	default:
		return common.PollStatusResponse{
			StatusCode: http.StatusInternalServerError,
			Error:      apiError.Message,
		}
	}
}

func (s *Uploader) matchRecordsToJobs(
	importingList []*jobsdb.JobT,
	failedRecords, successRecords []map[string]string,
	externalIDField string,
) common.EventStatMeta {
	metadata := common.EventStatMeta{
		FailedKeys:     make([]int64, 0),
		AbortedKeys:    make([]int64, 0),
		SucceededKeys:  make([]int64, 0),
		FailedReasons:  make(map[int64]string),
		AbortedReasons: make(map[int64]string),
	}

	for _, failedRecord := range failedRecords {
		key := failedRecord[externalIDField]
		if jobIDs, exists := s.externalIDToJobID[key]; exists {
			for _, jobID := range jobIDs {
				metadata.AbortedKeys = append(metadata.AbortedKeys, jobID)
				if errorMsg, ok := failedRecord["sf__Error"]; ok && errorMsg != "" {
					metadata.AbortedReasons[jobID] = errorMsg
				} else if errorMsg, ok := failedRecord["Error"]; ok && errorMsg != "" {
					metadata.AbortedReasons[jobID] = errorMsg
				}
			}
		}
	}

	for _, successRecord := range successRecords {
		key := successRecord[externalIDField]
		if jobIDs, exists := s.externalIDToJobID[key]; exists {
			metadata.SucceededKeys = append(metadata.SucceededKeys, jobIDs...)
		}
	}

	if len(metadata.SucceededKeys)+len(metadata.AbortedKeys) != len(importingList) {
		s.logger.Errorn(
			"Number of succeeded and aborted keys do not match the number of importing jobs",
			logger.NewIntField("succeeded_keys", int64(len(metadata.SucceededKeys))),
			logger.NewIntField("aborted_keys", int64(len(metadata.AbortedKeys))),
			logger.NewIntField("importing_jobs", int64(len(importingList))),
		)

		importingJobIDs := lo.Map(importingList, func(job *jobsdb.JobT, _ int) int64 {
			return job.JobID
		})
		matchedJobIDs := append(append([]int64{}, metadata.SucceededKeys...), metadata.AbortedKeys...)
		missingJobIDs, _ := lo.Difference(importingJobIDs, matchedJobIDs)

		if len(s.externalIDToJobID) == 0 {
			// No correlation data at all (e.g. the process restarted between
			// Upload and GetUploadStats and lost the in-memory map). This is a
			// systemic loss, not a per-record mismatch, so keep these jobs
			// retryable rather than permanently dropping a batch that may have
			// succeeded in Salesforce.
			metadata.FailedKeys = missingJobIDs
			metadata.FailedReasons = lo.SliceToMap(missingJobIDs, func(jobID int64) (int64, string) {
				return jobID, "Correlation map is empty (likely a restart between upload and stats); retrying"
			})
		} else {
			// The map was populated but these records could not be matched back
			// to a job, most likely because Salesforce reformatted the
			// externalId value on store. Retrying would re-upload the same value
			// and miss again, so abort instead of looping through retries.
			metadata.AbortedKeys = append(metadata.AbortedKeys, missingJobIDs...)
			for _, jobID := range missingJobIDs {
				metadata.AbortedReasons[jobID] = "Could not correlate Salesforce result back to the job: externalId not found in success/failed records (possibly reformatted by Salesforce on store)"
			}
		}
	}
	return metadata
}

func (s *Uploader) clearExternalIDToJobID() {
	s.externalIDToJobID = make(map[string][]int64)
}
