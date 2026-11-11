package marketobulkupload

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/tidwall/gjson"

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

type importingMetadata struct {
	RowHash string `json:"rowHash"`
}

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
	}

	// The batch router persists these on each importing job's status parameters.
	// Only accepted rows have hashes: overflowed rows were never sent to Marketo.
	jobImportingParameters := make(map[int64]stdjson.RawMessage, len(importingJobIDs))
	for _, jobID := range importingJobIDs {
		metadata, err := jsonrs.Marshal(importingMetadata{RowHash: jobHashes[jobID]})
		if err != nil {
			b.logger.Errorn("marshalling importing metadata", logger.NewIntField("jobID", jobID), obskit.Error(err))
			continue
		}
		jobImportingParameters[jobID] = metadata
	}

	// return the response
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
	hasFailed := marketoResponse.Result[0].NumOfRowsFailed > 0
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
		pollStatus.HasFailed = hasFailed
		pollStatus.HasWarning = hasWarning

	case "Importing", "Queued":
		pollStatus.InProgress = true
		pollStatus.StatusCode = 500
	case "Failed":
		pollStatus.HasFailed = true
		pollStatus.StatusCode = 400
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

	// Poll again: the preceding Poll may have run in another process, or for a
	// different import. Never infer counts from uploader fields or URL presence.
	status, apiError := b.apiService.PollImportStatus(params.ImportId)
	if apiError != nil {
		return common.GetUploadStatsResponse{StatusCode: 500, Error: fmt.Sprintf("Failed to poll Marketo import: %s", apiError.Message)}
	}
	if status == nil || len(status.Result) == 0 {
		return common.GetUploadStatsResponse{StatusCode: 500, Error: "No result found in the API response"}
	}
	result := status.Result[0]

	var failedJobs, warningJobs []map[string]string
	if result.NumOfRowsFailed > 0 {
		failedJobs, apiError = b.apiService.GetLeadStatus(input.FailedJobParameters)
		if apiError != nil {
			return common.GetUploadStatsResponse{
				StatusCode: 500,
				Error:      fmt.Sprintf("Failed to fetch failed jobs: %s", apiError.Message),
			}
		}
	}
	if result.NumOfRowsWithWarning > 0 {
		warningJobs, apiError = b.apiService.GetLeadStatus(input.WarningJobParameters)
		if apiError != nil {
			return common.GetUploadStatsResponse{
				StatusCode: 500,
				Error:      fmt.Sprintf("Failed to fetch warning jobs: %s", apiError.Message),
			}
		}
	}

	metadata := b.updateJobStatus(input.ImportingList, result, failedJobs, warningJobs)

	return common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata:   metadata,
	}
}

func (*MarketoBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
}

func (b *MarketoBulkUploader) updateJobStatus(importingList []*jobsdb.JobT, result Result, failedJobs, warningJobs []map[string]string) common.EventStatMeta {
	byHash := make(map[string][]int64)
	missing := make(map[int64]bool)
	for _, job := range importingList {
		hash := gjson.GetBytes(job.LastJobStatus.Parameters, "metadata.rowHash").String()
		if hash == "" {
			missing[job.JobID] = true
			continue
		}
		byHash[hash] = append(byHash[hash], job.JobID)
	}
	metadata := common.EventStatMeta{AbortedReasons: make(map[int64]string)}
	if len(byHash) == 0 {
		const detail = "no correlation metadata"
		b.logger.Errorn("Ambiguous Marketo bulk upload result", logger.NewStringField("detail", detail), logger.NewIntField("importing_jobs", int64(len(importingList))))
		for _, job := range importingList {
			metadata.AbortedKeys = append(metadata.AbortedKeys, job.JobID)
			metadata.AbortedReasons[job.JobID] = ambiguousResultReason + ": " + detail
		}
		return metadata
	}

	seen := make(map[int64]bool)
	matchedRows := 0
	match := func(rows []map[string]string, reasonColumn string) {
		for _, row := range rows {
			columns := make([]string, 0, len(row))
			for name := range row {
				if name != MARKETO_FAILED_HEADER && name != MARKETO_WARNING_HEADER {
					columns = append(columns, name)
				}
			}
			sort.Strings(columns)
			values := make([]string, 0, len(columns))
			for _, name := range columns {
				values = append(values, row[name])
			}
			ids := byHash[calculateHashCode(values)]
			if len(ids) == 0 {
				continue
			}
			matchedRows++
			for _, id := range ids {
				if !seen[id] {
					seen[id] = true
					metadata.AbortedKeys = append(metadata.AbortedKeys, id)
					metadata.AbortedReasons[id] = row[reasonColumn]
				}
			}
		}
	}
	match(failedJobs, MARKETO_FAILED_HEADER)
	match(warningJobs, MARKETO_WARNING_HEADER)

	complete := matchedRows == result.NumOfRowsFailed+result.NumOfRowsWithWarning && len(failedJobs) == result.NumOfRowsFailed && len(warningJobs) == result.NumOfRowsWithWarning
	ambiguousCount := 0
	for _, job := range importingList {
		if seen[job.JobID] {
			continue
		}
		if missing[job.JobID] || !complete {
			ambiguousCount++
			metadata.AbortedKeys = append(metadata.AbortedKeys, job.JobID)
			metadata.AbortedReasons[job.JobID] = ambiguousResultReason
		} else {
			metadata.SucceededKeys = append(metadata.SucceededKeys, job.JobID)
		}
	}
	if !complete || ambiguousCount > 0 {
		b.logger.Errorn("Ambiguous Marketo bulk upload result", logger.NewIntField("jobs", int64(ambiguousCount)), logger.NewIntField("matchedRows", int64(matchedRows)))
	}
	return metadata
}
