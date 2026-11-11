package eloqua

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/samber/lo"
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
		metadata, err := jsonrs.Marshal(importingMetadata{CSVRowIndex: row})
		if err != nil {
			b.logger.Errorn("marshalling importing metadata", obskit.Error(err))
			continue
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
	importIds := strings.Split(pollInput.ImportId, ":")
	checkSyncStatusData := HttpRequestData{
		DynamicPart:   importIds[0],
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
	}
	uploadStatus, err := b.service.CheckSyncStatus(&checkSyncStatusData)
	defer func() {
		if lo.Contains([]string{"success", "error", "warning"}, uploadStatus) {
			b.deleteImportDef(importIds[1])
		}
	}()
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
		return common.PollStatusResponse{
			Complete:   true,
			InProgress: false,
			StatusCode: 200,
			HasFailed:  false,
			HasWarning: false,
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

func (b *EloquaBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
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

	if UploadStatsInput.WarningJobParameters != "" {
		checkRejectedData := HttpRequestData{
			BaseEndpoint:  b.baseEndpoint,
			DynamicPart:   UploadStatsInput.WarningJobParameters,
			Authorization: b.authorization,
		}
		metadata, err := parseRejectedData(&checkRejectedData, UploadStatsInput.ImportingList, b, UploadStatsInput.Parameters)
		if err != nil {
			b.logger.Errorn("Error while parsing rejected data", obskit.Error(err))
			return common.GetUploadStatsResponse{StatusCode: 500}
		}
		uploadStatusResponse.StatusCode = 200
		uploadStatusResponse.Metadata = *metadata
		return uploadStatusResponse
	}

	uploadStatusResponse.Metadata = *parseFailedData(UploadStatsInput.FailedJobParameters, UploadStatsInput.ImportingList)
	uploadStatusResponse.StatusCode = 200
	return uploadStatusResponse
}

// Deletes import definition from Eloqua itself
func (b *EloquaBulkUploader) deleteImportDef(importDefId string) {
	deleteImportDefinitionData := HttpRequestData{
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
		DynamicPart:   importDefId,
	}
	err := b.service.DeleteImportDefinition(&deleteImportDefinitionData)
	if err != nil {
		b.logger.Errorn("Error while deleting import definition", obskit.Error(err))
	}
}
