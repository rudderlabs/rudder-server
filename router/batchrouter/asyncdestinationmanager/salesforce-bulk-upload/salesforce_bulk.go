package salesforcebulkupload

import (
	"bufio"
	"context"
	stdjson "encoding/json"
	"errors"
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

var errExternalIDRequired = errors.New("externalId is required but not present in the event")

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
		logger:       logger,
		apiService:   apiService,
		destination:  destination,
		statsFactory: statsFactory,
		destName:     destName,
	}
	statTags := stats.Tags{
		"destType":      destName,
		"destinationId": destination.ID,
		"workspaceId":   destination.WorkspaceID,
	}
	u.payloadSizeStat = statsFactory.NewTaggedStat("brt_async_dest_payload_size", stats.HistogramType, statTags)
	u.eventsPerFileStat = statsFactory.NewTaggedStat("brt_async_dest_events_per_file", stats.HistogramType, statTags)
	u.asyncUploadTimeStat = statsFactory.NewTaggedStat("brt_async_dest_async_upload_time", stats.TimerType, statTags)
	u.config.maxBufferCapacity = conf.GetReloadableInt64Var(512*bytesize.KB, bytesize.B, "SalesforceBulkUpload.maxBufferCapacity")
	return u
}

func (s *Uploader) Transform(job *jobsdb.JobT) (string, error) {
	// Extract required fields from the input
	traits := gjson.GetBytes(job.EventPayload, "traits")
	externalID := gjson.GetBytes(job.EventPayload, "context.externalId")

	// externalId is mandatory — it is the upsert key. Fail the event up front if
	// it is absent rather than letting it flow downstream.
	if !externalID.Exists() {
		return "", errExternalIDRequired
	}
	var externalIDs []SalesforceExternalID
	if err := jsonrs.Unmarshal([]byte(externalID.Raw), &externalIDs); err != nil {
		return "", fmt.Errorf("failed to unmarshal externalId: %w", err)
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

	externalIdObject, err := extractFromVDM(externalIDs)
	if err != nil {
		return "", fmt.Errorf("failed to extract externalId: %w", err)
	}
	// Add externalId field to message
	message[externalIdObject.ExternalIDField] = externalIdObject.ExternalIDValue

	asyncJob := SalesforceAsyncJob{
		Message: message,
		Metadata: SalesforceJobMetadata{
			JobID:           job.JobID,
			RudderOperation: "upsert", // we only support upsert
			ExternalIDs:     externalIDs,
		},
	}

	// Marshal and return
	responsePayload, err := jsonrs.Marshal(asyncJob)
	if err != nil {
		return "", fmt.Errorf("failed to marshal async job: %w", err)
	}

	return string(responsePayload), nil
}

func (s *Uploader) readJobsFromFile(filePath string) ([]SalesforceAsyncJob, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	var jobs []SalesforceAsyncJob
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, int(s.config.maxBufferCapacity.Load()))

	for scanner.Scan() {
		var job SalesforceAsyncJob
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

func (s *Uploader) failedJobs(asyncDestStruct *common.AsyncDestinationStruct, failedReason string) common.AsyncUploadOutput {
	return common.AsyncUploadOutput{
		FailedJobIDs:  asyncDestStruct.ImportingJobIDs,
		FailedCount:   len(asyncDestStruct.ImportingJobIDs),
		FailedReason:  failedReason,
		DestinationID: asyncDestStruct.Destination.ID,
	}
}

func (s *Uploader) Upload(_ context.Context, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destinationID := asyncDestStruct.Destination.ID
	filePath := asyncDestStruct.FileName

	jobs, err := s.readJobsFromFile(filePath)
	if err != nil {
		return s.failedJobs(asyncDestStruct, fmt.Sprintf("reading jobs from file: %v", err))
	}

	objectInfo, err := extractObjectInfo(jobs)
	if err != nil {
		return s.failedJobs(asyncDestStruct, fmt.Sprintf("extracting object info: %v", err))
	}

	// Every job reaching Upload was already validated at the Transform boundary
	// (externalId id, type and identifierType are guaranteed present), so we can
	// upload them all without re-checking the upsert key here.
	jobIDs := lo.Map(jobs, func(job SalesforceAsyncJob, _ int) int64 {
		return job.Metadata.JobID
	})

	csvFilePath, fileSize, err := createCSVFile(
		destinationID,
		objectInfo.ExternalIDField,
		jobs,
	)
	defer func() {
		if err := os.Remove(csvFilePath); err != nil {
			s.logger.Debugn("Failed to remove CSV file.", logger.NewStringField("csvFilePath", csvFilePath), obskit.Error(err))
		}
	}()
	if err != nil {
		s.logger.Errorn("Error creating CSV", obskit.Error(err))
		return s.failedJobs(asyncDestStruct, fmt.Sprintf("creating CSV: %v", err))
	}

	// Track the size of the CSV we send to Salesforce and how many events it packs.
	s.eventsPerFileStat.Observe(float64(len(jobs)))
	s.payloadSizeStat.Observe(float64(fileSize))

	s.logger.Infon("Created CSV file",
		logger.NewStringField("csvFilePath", csvFilePath),
		logger.NewIntField("jobs", int64(len(jobs))),
	)
	sfJobID, apiError := s.apiService.CreateJob(
		objectInfo.ObjectType,
		"upsert",
		objectInfo.ExternalIDField,
	)
	if apiError != nil {
		s.logger.Errorn("Error creating Salesforce job for operation upsert.", logger.NewStringField("apiErrorMessage", apiError.Message), logger.NewStringField("category", apiError.Category), logger.NewIntField("statusCode", int64(apiError.StatusCode)))
		return s.failedJobs(asyncDestStruct, fmt.Sprintf("creating Salesforce job: %v", apiError.Message))
	}

	uploadStartTime := time.Now()
	apiError = s.apiService.UploadData(sfJobID, csvFilePath)
	s.asyncUploadTimeStat.Since(uploadStartTime)
	if apiError != nil {
		s.logger.Errorn("Error uploading data for operation upsert", logger.NewStringField("apiErrorMessage", apiError.Message))
		if err := s.apiService.DeleteJob(sfJobID); err != nil {
			s.logger.Errorn("Error deleting Salesforce job.", logger.NewStringField("apiErrorMessage", err.Message))
		}
		return s.failedJobs(asyncDestStruct, fmt.Sprintf("uploading data: %v", apiError.Message))
	}

	apiError = s.apiService.CloseJob(sfJobID)
	if apiError != nil {
		s.logger.Errorn("Error closing job for operation upsert.", logger.NewStringField("apiErrorMessage", apiError.Message))
		if err := s.apiService.DeleteJob(sfJobID); err != nil {
			s.logger.Errorn("Error deleting Salesforce job.", logger.NewStringField("apiErrorMessage", err.Message))
		}
		return s.failedJobs(asyncDestStruct, fmt.Sprintf("closing job: %v", apiError.Message))
	}

	s.logger.Infon("Successfully created and closed Salesforce Bulk job", logger.NewStringField("jobID", sfJobID))

	// Persist each job's externalId (hashed) in its importing status params, so the
	// correlation can be rebuilt from the DB at poll time (restart-safe, no
	// in-memory state, no payload reads). Built only after the Salesforce job is
	// created and closed, so we don't do this work for a failed upload.
	jobImportingParameters := make(map[int64]stdjson.RawMessage, len(jobs))
	for _, job := range jobs {
		md, err := jsonrs.Marshal(importingMetadata{ExternalIDHash: HashExternalID(job.Metadata.ExternalIDs[0].ID)})
		if err != nil {
			s.logger.Errorn("marshalling importing metadata", logger.NewIntField("jobID", job.Metadata.JobID), obskit.Error(err))
			continue
		}
		jobImportingParameters[job.Metadata.JobID] = md
	}

	importParameters, err := jsonrs.Marshal(common.ImportParameters{
		ImportId: SalesforceJobInfo{
			ID:              sfJobID,
			ExternalIDField: objectInfo.ExternalIDField,
		},
		ImportCount: len(jobIDs),
	})
	if err != nil {
		s.logger.Errorn("marshalling parameters", obskit.Error(err))
	}
	return common.AsyncUploadOutput{
		ImportingJobIDs:        jobIDs,
		ImportingParameters:    importParameters,
		ImportingCount:         len(jobIDs),
		JobImportingParameters: jobImportingParameters,
		DestinationID:          destinationID,
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
		return common.PollStatusResponse{
			StatusCode: http.StatusOK,
			Complete:   true,
			HasFailed:  jobStatus.NumberRecordsFailed > 0,
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

	// Rebuild externalIdHash -> jobIDs from the per-job metadata persisted on the
	// importing job statuses. This is durable across restarts and needs no
	// in-memory state or job payloads. We key on the hash because the raw
	// externalId (often PII) is never stored; the Salesforce-returned externalId
	// is re-hashed below to match.
	externalIDHashToJobID := make(map[string][]int64)
	for _, job := range importingList {
		externalIDHash := gjson.GetBytes(job.LastJobStatus.Parameters, "metadata.externalIdHash").String()
		if externalIDHash == "" {
			continue
		}
		externalIDHashToJobID[externalIDHash] = append(externalIDHashToJobID[externalIDHash], job.JobID)
	}

	// No correlation metadata on any importing job: not a single Salesforce result
	// can be matched back, so abort the whole batch up front (the records may
	// already be in Salesforce, so retrying would only re-upsert). This is an
	// abnormal state (pre-change in-flight jobs or a metadata-persistence bug), so
	// log loudly for alerting.
	if len(externalIDHashToJobID) == 0 {
		s.logger.Errorn(
			"No externalId correlation metadata found on importing jobs; aborting batch (records may already be in Salesforce)",
			logger.NewIntField("importing_jobs", int64(len(importingList))),
		)
		for _, job := range importingList {
			metadata.AbortedKeys = append(metadata.AbortedKeys, job.JobID)
			metadata.AbortedReasons[job.JobID] = noCorrelationMetadataReason
		}
		return metadata
	}

	for _, failedRecord := range failedRecords {
		key := HashExternalID(failedRecord[externalIDField])
		if jobIDs, exists := externalIDHashToJobID[key]; exists {
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
		key := HashExternalID(successRecord[externalIDField])
		if jobIDs, exists := externalIDHashToJobID[key]; exists {
			metadata.SucceededKeys = append(metadata.SucceededKeys, jobIDs...)
		}
	}

	if len(metadata.SucceededKeys)+len(metadata.AbortedKeys) != len(importingList) {
		importingJobIDs := lo.Map(importingList, func(job *jobsdb.JobT, _ int) int64 {
			return job.JobID
		})
		matchedJobIDs := lo.Concat(metadata.SucceededKeys, metadata.AbortedKeys)
		missingJobIDs, _ := lo.Difference(importingJobIDs, matchedJobIDs)

		// Some results couldn't be correlated back (e.g. Salesforce reformatted the
		// externalId on store). Abort those jobs — re-uploading would only re-upsert
		// records already in Salesforce, so retrying is pointless here.
		s.logger.Errorn(
			"Some Salesforce results could not be correlated back to jobs; aborting them",
			logger.NewIntField("succeeded_keys", int64(len(metadata.SucceededKeys))),
			logger.NewIntField("aborted_keys", int64(len(metadata.AbortedKeys))),
			logger.NewIntField("importing_jobs", int64(len(importingList))),
		)
		metadata.AbortedKeys = append(metadata.AbortedKeys, missingJobIDs...)
		for _, jobID := range missingJobIDs {
			metadata.AbortedReasons[jobID] = resultCorrelationFailedReason
		}
	}
	return metadata
}
