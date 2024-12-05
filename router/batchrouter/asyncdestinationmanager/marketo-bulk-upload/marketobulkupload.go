package marketobulkupload

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type MarketoBulkUploader struct {
	destName          string
	destinationConfig MarketoConfig
	logger            logger.Logger
	statsFactory      stats.Stats
	csvHeaders        []string
	dataHashToJobId   map[string]int64
	hasFailures       bool
	hasWarning        bool
	apiService        MarketoAPIServiceInterface
}

type MarketoAsyncFailedInput struct {
	Message  map[string]interface{}
	Metadata struct {
		JobID int64
	}
}

type MarketoAsyncFailedPayload struct {
	Config   map[string]interface{}
	Input    []MarketoAsyncFailedInput
	DestType string
	ImportId string
	MetaData common.MetaDataT
}

const (
	MARKETO_WARNING_HEADER = "Import Warning Reason"
	MARKETO_FAILED_HEADER  = "Import Failure Reason"
)

func getImportingParameters(importID string) json.RawMessage {
	return json.RawMessage(`{"importId": "` + importID + `"}`)
}

func (b *MarketoBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
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
	csvFilePath, headerRowOrder, insertedJobIDs, overflowedJobIDs, err := createCSVFile(destinationID, b.destinationConfig, input, b.dataHashToJobId)

	b.logger.Infof("Number of jobs in the batch: %d", len(insertedJobIDs))

	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf("BRT: Error in Creating CSV File: %v", err),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	b.csvHeaders = headerRowOrder
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

	b.logger.Debugf("[Async Destination Manager] File Upload Finished for Dest Type %v", destType)

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

	// return the response
	return common.AsyncUploadOutput{
		ImportingJobIDs:     importingJobIDs,
		ImportingParameters: getImportingParameters(importID),
		FailedJobIDs:        failedJobIDs,
		ImportingCount:      len(importingJobIDs),
		FailedCount:         len(failedJobIDs),
		DestinationID:       destinationID,
	}
}

func (b *MarketoBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	importId := pollInput.ImportId

	marketoResponse, apiError := b.apiService.PollImportStatus(importId)

	b.logger.Debugf("[Async Destination Manager] Marketo Poll Response: %v", marketoResponse)

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

	// Set State
	b.hasFailures = hasFailed
	b.hasWarning = hasWarning

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

	// in case of success, clear the hashToJobId map
	if !hasFailed && !hasWarning {
		b.clearHashToJobId()
	}

	return pollStatus
}

func (b *MarketoBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	// Extract importId from parameters
	var params struct {
		ImportId string `json:"importId"`
	}
	err := jsonfast.Unmarshal(input.Parameters, &params)
	if err != nil {
		return common.GetUploadStatsResponse{
			StatusCode: 500,
			Error:      "Failed to parse parameters: " + err.Error(),
		}
	}

	var failedJobs []map[string]string
	var apiError *APIError
	// Fetch and parse failed jobs
	if b.hasFailures {
		failedJobs, apiError = b.apiService.GetLeadStatus(input.FailedJobParameters)

		if apiError != nil {
			return common.GetUploadStatsResponse{
				StatusCode: 500,
				Error:      fmt.Sprintf("Failed to fetch failed jobs: %s", apiError.Message),
			}
		}
	}

	var warningJobs []map[string]string
	// Fetch and parse warning jobs
	if b.hasWarning {
		warningJobs, apiError = b.apiService.GetLeadStatus(input.WarningJobParameters)

		if apiError != nil {
			return common.GetUploadStatsResponse{
				StatusCode: 500,
				Error:      fmt.Sprintf("Failed to fetch warning jobs: %s", apiError.Message),
			}
		}
	}

	metadata := b.updateJobStatus(input.ImportingList, failedJobs, warningJobs)

	return common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata:   metadata,
	}
}

func (*MarketoBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
}

func (b *MarketoBulkUploader) updateJobStatus(importingList []*jobsdb.JobT, failedJobs, warningJobs []map[string]string) common.EventStatMeta {
	metadata := common.EventStatMeta{
		AbortedKeys:    make([]int64, 0),
		WarningKeys:    make([]int64, 0),
		SucceededKeys:  make([]int64, 0),
		AbortedReasons: make(map[int64]string),
		WarningReasons: make(map[int64]string),
	}

	for _, failedJob := range failedJobs {
		// get failedJob data
		var failedJobRow []string
		for _, col := range b.csvHeaders {
			if val, ok := failedJob[col]; ok {
				failedJobRow = append(failedJobRow, val)
			} else {
				failedJobRow = append(failedJobRow, "")
			}
		}
		// get jobID from jobToDataHash
		hash := calculateHashCode(failedJobRow)
		abortedJobID := b.dataHashToJobId[hash]
		if abortedJobID != 0 {
			metadata.AbortedKeys = append(metadata.AbortedKeys, abortedJobID)
		}
		abortedJobReason := failedJob[MARKETO_FAILED_HEADER]
		if abortedJobReason != "" {
			metadata.AbortedReasons[abortedJobID] = abortedJobReason
		}

	}

	for _, warningJob := range warningJobs {
		// get warningJob data
		var warningJobRow []string
		for _, col := range b.csvHeaders {
			if val, ok := warningJob[col]; ok {
				warningJobRow = append(warningJobRow, val)
			} else {
				warningJobRow = append(warningJobRow, "")
			}
		}
		// get jobID from jobToDataHash
		hash := calculateHashCode(warningJobRow)
		warningJobId := b.dataHashToJobId[hash]
		if warningJobId != 0 {
			// Even if a job has warning, it is considered as a failure
			metadata.AbortedKeys = append(metadata.AbortedKeys, warningJobId)
		}
		warningJobReason := warningJob[MARKETO_WARNING_HEADER]
		if warningJobReason != "" {
			metadata.AbortedReasons[warningJobId] = warningJobReason
		}
	}

	// calculate succeeded keys
	for _, job := range importingList {
		if !lo.Contains(metadata.AbortedKeys, job.JobID) && !lo.Contains(metadata.WarningKeys, job.JobID) {
			metadata.SucceededKeys = append(metadata.SucceededKeys, job.JobID)
		}
	}

	b.clearHashToJobId()

	return metadata
}

func (b *MarketoBulkUploader) clearHashToJobId() {
	for k := range b.dataHashToJobId {
		delete(b.dataHashToJobId, k)
	}
}
