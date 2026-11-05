package eloqua

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func (b *EloquaBulkUploader) createAsyncUploadErrorOutput(errorString string, err error, destinationId string, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	return common.AsyncUploadOutput{
		FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
		FailedReason:  fmt.Sprintf(errorString+"%v", err),
		FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
		DestinationID: destinationId,
	}
}

func (*EloquaBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
}

func (b *EloquaBulkUploader) Upload(_ context.Context, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	uploadRetryableStat := b.statsFactory.NewTaggedStat("events_over_prescribed_limit", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	file, err := os.Open(asyncDestStruct.FileName)
	if err != nil {
		return b.createAsyncUploadErrorOutput("got error while opening the file. ", err, destination.ID, asyncDestStruct)
	}
	defer file.Close()
	eventDetails, err := getEventDetails(file)
	if err != nil {
		return b.createAsyncUploadErrorOutput("got error while checking the event type. ", err, destination.ID, asyncDestStruct)
	}

	customObjectData := HttpRequestData{
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
	}
	if eventDetails.Type == "track" {
		customObjectData.DynamicPart = eventDetails.CustomObjectId
	}
	eloquaFields, err := b.service.FetchFields(&customObjectData)
	if err != nil {
		return b.createAsyncUploadErrorOutput("got error while fetching fields. ", err, destination.ID, asyncDestStruct)
	}

	uniqueKeys := getUniqueKeys(eloquaFields)
	uploadJobInfo := JobInfo{
		fileSizeLimit: b.fileSizeLimit,
		importingJobs: asyncDestStruct.ImportingJobIDs,
	}

	importDefinitionBody, err := createBodyForImportDefinition(eventDetails, eloquaFields)
	if err != nil {
		return b.createAsyncUploadErrorOutput("got error while creating body for import definition. ", err, destination.ID, asyncDestStruct)
	}
	marshalledData, err := jsonrs.Marshal(importDefinitionBody)
	if err != nil {
		return b.createAsyncUploadErrorOutput("unable marshal importDefinitionBody. ", err, destination.ID, asyncDestStruct)
	}

	importDefinitionData := HttpRequestData{
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
		Body:          strings.NewReader(string(marshalledData)),
		DynamicPart:   eventDetails.CustomObjectId,
	}

	importDefinition, err := b.service.CreateImportDefinition(&importDefinitionData, eventDetails.Type)
	if err != nil {
		return b.createAsyncUploadErrorOutput("unable to create importdefinition. ", err, destination.ID, asyncDestStruct)
	}

	uploadDataData := HttpRequestData{
		BaseEndpoint:  b.baseEndpoint,
		DynamicPart:   importDefinition.URI,
		Authorization: b.authorization,
	}
	rowToJob := make(map[int64]int64)
	filePAth, fileSize, err := createCSVFile(eventDetails.Fields, file, &uploadJobInfo, rowToJob)
	if err != nil {
		return b.createAsyncUploadErrorOutput("unable to create csv file. ", err, destination.ID, asyncDestStruct)
	}
	CSVFileSizeStat := b.statsFactory.NewTaggedStat("csv_file_size", stats.HistogramType,
		map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})
	CSVFileSizeStat.Observe(float64(fileSize))
	defer os.Remove(filePAth)
	uploadTimeStat := b.statsFactory.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	startTime := time.Now()
	err = b.service.UploadData(&uploadDataData, filePAth)
	uploadTimeStat.Since(startTime)
	if err != nil {
		return b.createAsyncUploadErrorOutput("unable to upload the data. ", err, destination.ID, asyncDestStruct)
	}
	runSyncBody := map[string]any{
		"syncedInstanceUri": importDefinition.URI,
	}
	marshalledData, err = jsonrs.Marshal(runSyncBody)
	if err != nil {
		return b.createAsyncUploadErrorOutput("unable marshal importDefinitionBody. ", err, destination.ID, asyncDestStruct)
	}
	runSyncData := HttpRequestData{
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
		Body:          strings.NewReader(string(marshalledData)),
	}
	syncURI, err := b.service.RunSync(&runSyncData)
	if err != nil {
		return b.createAsyncUploadErrorOutput("unable to run the sync after uploading the file. ", err, destination.ID, asyncDestStruct)
	}
	importParameters, err := jsonrs.Marshal(struct {
		common.ImportParameters
		UniqueKeys []string `json:"uniqueKeys"`
	}{
		ImportId:    syncURI + ":" + importDefinition.URI,
		ImportCount: len(uploadJobInfo.succeededJobs),
		UniqueKeys:  uniqueKeys,
	})
	if err != nil {
		return b.createAsyncUploadErrorOutput("error while marshaling parameters. ", err, destination.ID, asyncDestStruct)
	}
	jobParameters := make(map[int64]stdjson.RawMessage, len(rowToJob))
	for row, jobID := range rowToJob {
		metadata, err := jsonrs.Marshal(struct {
			CSVRowIndex int64 `json:"csvRowIndex"`
		}{CSVRowIndex: row})
		if err != nil {
			return b.createAsyncUploadErrorOutput("error while marshaling row metadata. ", err, destination.ID, asyncDestStruct)
		}
		jobParameters[jobID] = metadata
	}
	uploadRetryableStat.Count(len(uploadJobInfo.failedJobs))
	return common.AsyncUploadOutput{
		ImportingJobIDs:        uploadJobInfo.succeededJobs,
		FailedJobIDs:           append(asyncDestStruct.FailedJobIDs, uploadJobInfo.failedJobs...),
		FailedReason:           "failed as the fileSizeLimit has over",
		ImportingParameters:    importParameters,
		JobImportingParameters: jobParameters,
		ImportingCount:         len(uploadJobInfo.succeededJobs),
		FailedCount:            len(asyncDestStruct.FailedJobIDs) + len(uploadJobInfo.failedJobs),
		DestinationID:          asyncDestStruct.Destination.ID,
	}
}

func (b *EloquaBulkUploader) Poll(_ context.Context, pollInput common.AsyncPoll) common.PollStatusResponse {
	importIds := strings.SplitN(pollInput.ImportId, ":", 2)
	if len(importIds) != 2 {
		return common.PollStatusResponse{StatusCode: 500, Error: "invalid Eloqua import ID"}
	}
	checkSyncStatusData := HttpRequestData{
		DynamicPart:   importIds[0],
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
	}
	uploadStatus, err := b.service.CheckSyncStatus(&checkSyncStatusData)
	if err != nil {
		return common.PollStatusResponse{
			Complete:   false,
			InProgress: false,
			StatusCode: 500,
			HasFailed:  false,
			HasWarning: false,
		}
	}
	switch uploadStatus {
	case "success":
		// Even a clean sync must go through stats to validate the persisted row
		// metadata and check rejects before declaring any job successful.
		return common.PollStatusResponse{
			Complete:             true,
			StatusCode:           200,
			HasWarning:           true,
			WarningJobParameters: importIds[0],
		}
	case "error":
		return common.PollStatusResponse{
			Complete:            true,
			InProgress:          false,
			StatusCode:          200,
			HasFailed:           true,
			HasWarning:          false,
			FailedJobParameters: importIds[0],
		}
	case "warning":
		return common.PollStatusResponse{
			Complete:             true,
			InProgress:           false,
			StatusCode:           200,
			HasFailed:            true,
			HasWarning:           true,
			WarningJobParameters: importIds[0],
		}
	case "pending", "active":
		return common.PollStatusResponse{
			InProgress: true,
		}
	default:
		return common.PollStatusResponse{
			Complete:   false,
			InProgress: false,
			StatusCode: 500,
			HasFailed:  false,
			HasWarning: false,
		}
	}
}

func (b *EloquaBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	var uploadStatusResponse common.GetUploadStatsResponse
	defer func() {
		eventsAbortedStat := b.statsFactory.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})
		eventsAbortedStat.Count(len(uploadStatusResponse.Metadata.AbortedKeys))

		eventsSuccessStat := b.statsFactory.NewTaggedStat("success_job_count", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})
		eventsSuccessStat.Count(len(uploadStatusResponse.Metadata.SucceededKeys))
	}()

	// The batch router passes the first importing job's parameters here. The
	// shared policy and import URI are also present on every importing status.
	parameters := input.Parameters
	if len(parameters) == 0 && len(input.ImportingList) > 0 {
		parameters = input.ImportingList[0].LastJobStatus.Parameters
	}
	// The router fetches by destination, not sync. A foreign import in this
	// list must remain importing rather than being classified under this sync.
	importID := gjson.GetBytes(parameters, "importId").String()
	for _, job := range input.ImportingList {
		if jobImportID := gjson.GetBytes(job.LastJobStatus.Parameters, "importId").String(); jobImportID != "" && jobImportID != importID {
			return common.GetUploadStatsResponse{StatusCode: 500, Error: "mixed Eloqua importing IDs; retrying poll"}
		}
	}
	if input.WarningJobParameters != "" {
		checkRejectedData := HttpRequestData{
			BaseEndpoint:  b.baseEndpoint,
			DynamicPart:   input.WarningJobParameters,
			Authorization: b.authorization,
		}
		metadata, err := parseRejectedData(&checkRejectedData, input.ImportingList, b, parameters)
		if err != nil {
			b.logger.Errorn("Error while fetching Eloqua rejects; retrying stats", obskit.Error(err))
			return common.GetUploadStatsResponse{StatusCode: 500, Error: err.Error()}
		}
		uploadStatusResponse.Metadata = *metadata
	} else if input.FailedJobParameters != "" {
		expected := gjson.GetBytes(parameters, "importCount")
		if !expected.Exists() || expected.Type != gjson.Number || expected.Int() < 0 || expected.Raw != fmt.Sprint(expected.Int()) || expected.Int() != int64(len(input.ImportingList)) {
			return common.GetUploadStatsResponse{StatusCode: 500, Error: "eloqua importing list incomplete or invalid import count"}
		}
		if !strings.HasPrefix(gjson.GetBytes(parameters, "importId").String(), input.FailedJobParameters+":") {
			uploadStatusResponse.Metadata = *abortAmbiguousJobs(input.ImportingList, b, "missing or mismatched sync ID")
		} else {
			for _, job := range input.ImportingList {
				if gjson.GetBytes(job.LastJobStatus.Parameters, "importId").String() != gjson.GetBytes(parameters, "importId").String() {
					uploadStatusResponse.Metadata = *abortAmbiguousJobs(input.ImportingList, b, "mixed importing sync IDs")
					break
				}
			}
			if len(uploadStatusResponse.Metadata.AbortedKeys) == 0 {
				uploadStatusResponse.Metadata = *parseFailedData(input.FailedJobParameters, input.ImportingList)
			}
		}
	} else {
		return common.GetUploadStatsResponse{StatusCode: 500, Error: "missing Eloqua sync status"}
	}
	// Retain the import definition until the framework has committed job statuses.
	// There is no post-commit callback today: even successful stats may be retried
	// if the subsequent database update fails.
	uploadStatusResponse.StatusCode = 200
	return uploadStatusResponse
}
