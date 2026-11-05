package marketobulkupload

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"os"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type MarketoBulkUploader struct {
	destName          string
	destinationConfig MarketoConfig
	logger            logger.Logger
	statsFactory      stats.Stats
	apiService        MarketoAPIServiceInterface
}

type MarketoAsyncFailedInput struct {
	Message  map[string]any
	Metadata struct {
		JobID int64
	}
}

type MarketoAsyncFailedPayload struct {
	Config   map[string]any
	Input    []MarketoAsyncFailedInput
	DestType string
	ImportId string
	MetaData common.MetaDataT
}

const (
	MARKETO_WARNING_HEADER = "Import Warning Reason"
	MARKETO_FAILED_HEADER  = "Import Failure Reason"
	ambiguousResultReason  = "Marketo bulk upload result could not be correlated; records may already have been imported"
)

func (b *MarketoBulkUploader) Upload(_ context.Context, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	destinationID := destination.ID
	filePath := asyncDestStruct.FileName
	destType := destination.DestinationDefinition.Name
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs

	input, err := readJobsFromFile(filePath)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	payloadSizeStat := b.statsFactory.NewTaggedStat("payload_size", stats.GaugeType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
		"destID":   destinationID,
	})
	jobHashes := make(map[int64]string)
	csvFilePath, _, insertedJobIDs, overflowedJobIDs, err := createCSVFile(destinationID, b.destinationConfig, input, jobHashes)

	b.logger.Infon("Number of jobs in the batch",
		logger.NewIntField("jobCount", int64(len(insertedJobIDs))))

	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf("BRT: Error in Creating CSV File: %v", err),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	importingJobIDs = insertedJobIDs
	failedJobIDs = append(failedJobIDs, overflowedJobIDs...)

	defer func() { _ = os.Remove(csvFilePath) }() // Clean up the temporary file

	// Check file size
	fileInfo, err := os.Stat(csvFilePath)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf("BRT: Error in Getting File Info: %v", err),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	payloadSizeStat.Gauge(float64(fileInfo.Size()))

	importID, apiError := b.apiService.ImportLeads(csvFilePath, b.destinationConfig.DeduplicationField)

	b.logger.Debugn("[Async Destination Manager] File Upload Finished for Dest Type",
		obskit.DestinationType(destType))

	if apiError != nil {

		if apiError.Category == "RefreshToken" {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
				FailedReason:  fmt.Sprintf("BRT: Error in Uploading File: Token Expired: %v", apiError.Message),
				FailedCount:   len(failedJobIDs) + len(importingJobIDs),
				DestinationID: destinationID,
			}
		}

		switch apiError.StatusCode {
		case 429, 500:
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
				FailedReason:  fmt.Sprintf("BRT: Error in Uploading File: %v", apiError.Message),
				FailedCount:   len(failedJobIDs) + len(importingJobIDs),
				DestinationID: destinationID,
			}
		case 400:
			return common.AsyncUploadOutput{
				AbortJobIDs:   append(failedJobIDs, importingJobIDs...),
				AbortReason:   fmt.Sprintf("BRT: Error in Uploading File (Aborted): %v", apiError.Message),
				AbortCount:    len(failedJobIDs) + len(importingJobIDs),
				DestinationID: destinationID,
			}
		}

	}

	importParameters, err := jsonrs.Marshal(common.ImportParameters{
		ImportId:    importID,
		ImportCount: len(importingJobIDs),
	})
	if err != nil {
		b.logger.Errorn("marshalling parameters", obskit.Error(err))
		return common.AsyncUploadOutput{FailedJobIDs: append(failedJobIDs, importingJobIDs...), FailedCount: len(failedJobIDs) + len(importingJobIDs), FailedReason: fmt.Sprintf("marshalling parameters: %v", err), DestinationID: destinationID}
	}

	// The batch router persists these on each importing job's status parameters.
	// Only accepted rows have hashes: overflowed rows were never sent to Marketo.
	jobImportingParameters := make(map[int64]stdjson.RawMessage, len(importingJobIDs))
	for _, jobID := range importingJobIDs {
		metadata, err := jsonrs.Marshal(struct {
			RowHash string `json:"rowHash"`
		}{RowHash: jobHashes[jobID]})
		if err != nil {
			b.logger.Errorn("marshalling importing metadata", logger.NewIntField("jobID", jobID), obskit.Error(err))
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
				FailedCount:   len(failedJobIDs) + len(importingJobIDs),
				FailedReason:  fmt.Sprintf("marshalling importing metadata: %v", err),
				DestinationID: destinationID,
			}
		}
		jobImportingParameters[jobID] = metadata
	}

	return common.AsyncUploadOutput{
		ImportingJobIDs:        importingJobIDs,
		ImportingParameters:    importParameters,
		JobImportingParameters: jobImportingParameters,
		FailedJobIDs:           failedJobIDs,
		ImportingCount:         len(importingJobIDs),
		FailedCount:            len(failedJobIDs),
		DestinationID:          destinationID,
	}
}

func (b *MarketoBulkUploader) Poll(_ context.Context, pollInput common.AsyncPoll) common.PollStatusResponse {
	importId := pollInput.ImportId

	marketoResponse, apiError := b.apiService.PollImportStatus(importId)

	if b.logger.IsDebugLevel() { // reflection on marketoResponse might not be a cheap operation
		b.logger.Debugn("[Async Destination Manager] Marketo Poll Response",
			logger.NewStringField("marketoResponse", fmt.Sprintf("%+v", marketoResponse))) // nolint:forbidigo
	}

	if apiError != nil {
		if apiError.Category == "RefreshToken" {
			return common.PollStatusResponse{StatusCode: 500, Error: apiError.Message, Complete: false}
		}
		switch apiError.StatusCode {
		case 500:
			return common.PollStatusResponse{StatusCode: apiError.StatusCode, Error: apiError.Message, Complete: false}
		case 400:
			return common.PollStatusResponse{StatusCode: apiError.StatusCode, Error: apiError.Message, Complete: true, HasFailed: true}
		case 429:
			return common.PollStatusResponse{StatusCode: apiError.StatusCode, Error: apiError.Message, Complete: false}
		}
	}

	// Check if the response is empty
	if len(marketoResponse.Result) == 0 {
		return common.PollStatusResponse{
			StatusCode: 500,
			Complete:   false,
			HasFailed:  true,
			Error:      "No result found in the API response",
		}
	}

	batchStatus := marketoResponse.Result[0].Status
	hasWarning := marketoResponse.Result[0].NumOfRowsWithWarning > 0

	coreURl := fmt.Sprintf("https://%s.mktorest.com/bulk/v1/leads/batch/%s", b.destinationConfig.MunchkinId, importId)
	failedJobURLs := fmt.Sprintf("%s/failures.json", coreURl)
	warningJobURLs := fmt.Sprintf("%s/warnings.json", coreURl)

	pollStatus := common.PollStatusResponse{}

	switch batchStatus {
	case "Complete":
		pollStatus.Complete = true
		pollStatus.StatusCode = 200
		pollStatus.FailedJobParameters = failedJobURLs
		pollStatus.WarningJobParameters = warningJobURLs
		// The router otherwise marks all jobs successful without calling GetUploadStats.
		// Even with the legacy count policy, missing metadata must not pass as success.
		pollStatus.HasFailed = true
		pollStatus.HasWarning = hasWarning

	case "Importing", "Queued":
		pollStatus.InProgress = true
		pollStatus.StatusCode = 500
	case "Failed":
		pollStatus.HasFailed = true
		// A terminal batch failure can still have row-level outcomes. Route it
		// through GetUploadStats rather than discarding those results.
		pollStatus.StatusCode = 200
		pollStatus.FailedJobParameters = failedJobURLs
		pollStatus.WarningJobParameters = warningJobURLs
		pollStatus.HasWarning = hasWarning
		pollStatus.Complete = true
		pollStatus.Error = fmt.Sprintf("Marketo Bulk Upload Failed: %s", marketoResponse.Result[0].Message)
	default:
		pollStatus.StatusCode = 500
		pollStatus.Complete = false
		pollStatus.Error = fmt.Sprintf("Unknown status: %s", batchStatus)
	}

	return pollStatus
}

func (b *MarketoBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	// Extract importId from parameters
	var params struct {
		ImportId string `json:"importId"`
	}
	err := jsonrs.Unmarshal(input.Parameters, &params)
	if err != nil {
		return common.GetUploadStatsResponse{
			StatusCode: 500,
			Error:      "Failed to parse parameters: " + err.Error(),
		}
	}

	if params.ImportId == "" {
		return common.GetUploadStatsResponse{StatusCode: 500, Error: "Missing Marketo importId"}
	}
	// Jobs are fetched by destination, not import ID. Do not consume another
	// import's jobs when the destination-wide query returns a mixed list.
	for _, job := range input.ImportingList {
		if jobImportID := gjson.GetBytes(job.LastJobStatus.Parameters, "importId").String(); jobImportID != "" && jobImportID != params.ImportId {
			return common.GetUploadStatsResponse{StatusCode: 500, Error: "mixed Marketo importing IDs; retrying poll"}
		}
	}

	if count := gjson.GetBytes(input.Parameters, "importCount"); count.Exists() && count.Int() != int64(len(input.ImportingList)) {
		return common.GetUploadStatsResponse{StatusCode: 500, Error: "incomplete Marketo importing job list"}
	}

	// Poll again: the preceding Poll may have run in another process, or for a
	// different import. Never infer counts from uploader fields or URL presence.
	status, apiError := b.apiService.PollImportStatus(params.ImportId)
	if apiError != nil {
		return common.GetUploadStatsResponse{StatusCode: 500, Error: fmt.Sprintf("Failed to poll Marketo import: %s", apiError.Message)}
	}
	if status == nil || len(status.Result) == 0 {
		return common.GetUploadStatsResponse{StatusCode: 500, Error: "Missing Marketo import status"}
	}
	result := status.Result[0]
	if result.Status != "Complete" && result.Status != "Failed" {
		return common.GetUploadStatsResponse{StatusCode: 500, Error: "Marketo import is not terminal: " + result.Status}
	}
	if result.NumOfRowsFailed < 0 || result.NumOfRowsWithWarning < 0 {
		return b.ambiguousStats(input.ImportingList, "negative Marketo result counts")
	}

	var failedJobs, warningJobs []map[string]string
	if result.NumOfRowsFailed > 0 {
		if input.FailedJobParameters == "" {
			return common.GetUploadStatsResponse{StatusCode: 500, Error: "Missing Marketo failures URL"}
		}
		failedJobs, apiError = b.apiService.GetLeadStatus(input.FailedJobParameters)
		if apiError != nil {
			return common.GetUploadStatsResponse{StatusCode: 500, Error: fmt.Sprintf("Failed to fetch failed jobs: %s", apiError.Message)}
		}
	}
	if result.NumOfRowsWithWarning > 0 {
		if input.WarningJobParameters == "" {
			return common.GetUploadStatsResponse{StatusCode: 500, Error: "Missing Marketo warnings URL"}
		}
		warningJobs, apiError = b.apiService.GetLeadStatus(input.WarningJobParameters)
		if apiError != nil {
			return common.GetUploadStatsResponse{StatusCode: 500, Error: fmt.Sprintf("Failed to fetch warning jobs: %s", apiError.Message)}
		}
	}

	return b.reconcileJobs(input.ImportingList, params.ImportId, result, failedJobs, warningJobs)
}

func (*MarketoBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
}

// ambiguousStats aborts all importing jobs rather than guessing which records
// Marketo has already accepted. A retry could duplicate an already imported lead.
func (b *MarketoBulkUploader) ambiguousStats(jobs []*jobsdb.JobT, detail string) common.GetUploadStatsResponse {
	b.logger.Errorn("Ambiguous Marketo bulk upload result", logger.NewStringField("detail", detail), logger.NewIntField("importing_jobs", int64(len(jobs))))
	b.statsFactory.NewTaggedStat("marketo_bulk_upload_ambiguous_reconciliation", stats.CountType, map[string]string{
		"module": "batch_router", "destType": "MARKETO_BULK_UPLOAD",
	}).Count(1)
	metadata := common.EventStatMeta{AbortedReasons: make(map[int64]string)}
	for _, job := range jobs {
		metadata.AbortedKeys = append(metadata.AbortedKeys, job.JobID)
		metadata.AbortedReasons[job.JobID] = ambiguousResultReason + ": " + detail
	}
	return common.GetUploadStatsResponse{StatusCode: 200, Metadata: metadata}
}

func (b *MarketoBulkUploader) reconcileJobs(jobs []*jobsdb.JobT, importID string, result Result, failedJobs, warningJobs []map[string]string) common.GetUploadStatsResponse {
	byHash := make(map[string][]int64)
	for _, job := range jobs {
		hash := gjson.GetBytes(job.LastJobStatus.Parameters, "metadata.rowHash").String()
		if hash == "" {
			return b.ambiguousStats(jobs, fmt.Sprintf("missing correlation metadata for job %d", job.JobID))
		}
		count := gjson.GetBytes(job.LastJobStatus.Parameters, "importCount")
		if gjson.GetBytes(job.LastJobStatus.Parameters, "importId").String() != importID || !count.Exists() || count.Int() != int64(len(jobs)) {
			return b.ambiguousStats(jobs, "missing or mixed Marketo import ID or count")
		}
		byHash[hash] = append(byHash[hash], job.JobID)
	}

	if len(failedJobs) != result.NumOfRowsFailed || len(warningJobs) != result.NumOfRowsWithWarning {
		return b.ambiguousStats(jobs, fmt.Sprintf("result row count mismatch: failures %d/%d, warnings %d/%d",
			len(failedJobs), result.NumOfRowsFailed, len(warningJobs), result.NumOfRowsWithWarning))
	}

	metadata := common.EventStatMeta{AbortedReasons: make(map[int64]string)}
	seen := make(map[int64]bool)
	match := func(rows []map[string]string, reasonColumn string) string {
		for _, row := range rows {
			ids := byHash[hashResultRow(row)]
			if len(ids) == 0 {
				return "unmatched Marketo result row"
			}
			if len(ids) != 1 {
				return "multiple importing jobs share one Marketo result row"
			}
			id := ids[0]
			if seen[id] {
				return fmt.Sprintf("duplicate or overlapping Marketo result for job %d", id)
			}
			seen[id] = true
			metadata.AbortedKeys = append(metadata.AbortedKeys, id)
			metadata.AbortedReasons[id] = row[reasonColumn]
		}
		return ""
	}
	if detail := match(failedJobs, MARKETO_FAILED_HEADER); detail != "" {
		return b.ambiguousStats(jobs, detail)
	}
	if detail := match(warningJobs, MARKETO_WARNING_HEADER); detail != "" {
		return b.ambiguousStats(jobs, detail)
	}

	// Marketo does not provide a success file. The remainder can succeed only
	// when every reported failure/warning has been uniquely attributed. With
	// strict mode on, even identical clean rows cannot be individually proved.
	strict := config.GetBoolVar(true, "BatchRouter.MARKETO_BULK_UPLOAD.strictResultReconciliation")
	if strict {
		for _, ids := range byHash {
			if len(ids) > 1 {
				return b.ambiguousStats(jobs, "duplicate mapped rows cannot be uniquely correlated")
			}
		}
	}
	if result.Status == "Failed" && len(seen) != len(jobs) {
		return b.ambiguousStats(jobs, "failed import left rows without a result")
	}
	for _, job := range jobs {
		if !seen[job.JobID] {
			metadata.SucceededKeys = append(metadata.SucceededKeys, job.JobID)
		}
	}
	return common.GetUploadStatsResponse{StatusCode: 200, Metadata: metadata}
}
