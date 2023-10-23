package eloqua

import (
	stdjson "encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func (b *EloquaBulkUploader) createAsyncUploadErrorOutput(errorString string, err error, destinationId string, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	b.clearJobToCsvMap()
	return common.AsyncUploadOutput{
		FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
		FailedReason:  fmt.Sprintf(errorString+"%v", err),
		FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
		DestinationID: destinationId,
	}
}

func (b *EloquaBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	uploadRetryableStat := stats.Default.NewTaggedStat("events_over_prescribed_limit", stats.CountType, map[string]string{
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
	b.uniqueKeys = uniqueKeys
	uploadJobInfo := JobInfo{
		fileSizeLimit: b.fileSizeLimit,
		importingJobs: asyncDestStruct.ImportingJobIDs,
	}

	importDefinitionBody, err := createBodyForImportDefinition(eventDetails, eloquaFields)
	if err != nil {
		return b.createAsyncUploadErrorOutput("got error while creating body for import definition. ", err, destination.ID, asyncDestStruct)
	}
	marshalledData, err := stdjson.Marshal(importDefinitionBody)
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
	filePAth, fileSize, err := createCSVFile(eventDetails.Fields, file, &uploadJobInfo, b.jobToCSVMap)
	if err != nil {
		return b.createAsyncUploadErrorOutput("unable to create csv file. ", err, destination.ID, asyncDestStruct)
	}
	CSVFileSizeStat := stats.Default.NewTaggedStat("csv_file_size", stats.HistogramType,
		map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})
	CSVFileSizeStat.Observe(float64(fileSize))
	defer os.Remove(filePAth)
	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	startTime := time.Now()
	err = b.service.UploadData(&uploadDataData, filePAth)
	uploadTimeStat.Since(startTime)
	if err != nil {
		return b.createAsyncUploadErrorOutput("unable to upload the data. ", err, destination.ID, asyncDestStruct)
	}
	runSyncBody := map[string]interface{}{
		"syncedInstanceUri": importDefinition.URI,
	}
	marshalledData, err = stdjson.Marshal(runSyncBody)
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
	var parameters common.ImportParameters
	parameters.ImportId = syncURI + ":" + importDefinition.URI
	importParameters, err := stdjson.Marshal(parameters)
	if err != nil {
		return b.createAsyncUploadErrorOutput("error while marshaling parameters. ", err, destination.ID, asyncDestStruct)
	}
	uploadRetryableStat.Count(len(uploadJobInfo.failedJobs))
	return common.AsyncUploadOutput{
		ImportingJobIDs:     uploadJobInfo.succeededJobs,
		FailedJobIDs:        append(asyncDestStruct.FailedJobIDs, uploadJobInfo.failedJobs...),
		FailedReason:        "failed as the fileSizeLimit has over",
		ImportingParameters: importParameters,
		ImportingCount:      len(uploadJobInfo.succeededJobs),
		FailedCount:         len(asyncDestStruct.FailedJobIDs) + len(uploadJobInfo.failedJobs),
		DestinationID:       asyncDestStruct.Destination.ID,
	}
}

func (b *EloquaBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
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
		if err != nil || uploadStatus == "success" {
			b.clearJobToCsvMap()
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
			Complete:      true,
			InProgress:    false,
			StatusCode:    200,
			HasFailed:     true,
			HasWarning:    false,
			FailedJobURLs: importIds[0],
		}
	case "warning":
		return common.PollStatusResponse{
			Complete:       true,
			InProgress:     false,
			StatusCode:     200,
			HasFailed:      true,
			HasWarning:     true,
			WarningJobURLs: importIds[0],
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
		b.clearJobToCsvMap()
		eventsAbortedStat := stats.Default.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})
		eventsAbortedStat.Count(len(uploadStatusResponse.Metadata.FailedKeys))

		eventsSuccessStat := stats.Default.NewTaggedStat("success_job_count", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})
		eventsSuccessStat.Count(len(uploadStatusResponse.Metadata.SucceededKeys))
	}()

	if UploadStatsInput.WarningJobURLs != "" {
		checkRejectedData := HttpRequestData{
			BaseEndpoint:  b.baseEndpoint,
			DynamicPart:   UploadStatsInput.WarningJobURLs,
			Authorization: b.authorization,
		}
		eventStatMetaWithRejectedSucceededJobs, err := parseRejectedData(&checkRejectedData, UploadStatsInput.ImportingList, b)
		if err != nil {
			b.logger.Error("Error while parsing rejected data", err)
			return common.GetUploadStatsResponse{
				StatusCode: 500,
			}
		}
		uploadStatusResponse.StatusCode = 200
		uploadStatusResponse.Metadata = *eventStatMetaWithRejectedSucceededJobs
		return uploadStatusResponse
	}

	eventStatMetaWithFailedJobs := parseFailedData(UploadStatsInput.FailedJobURLs, UploadStatsInput.ImportingList)
	uploadStatusResponse.StatusCode = 200
	uploadStatusResponse.Metadata = *eventStatMetaWithFailedJobs
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
		b.logger.Error("Error while deleting import definition", err)
	}
}

func (b *EloquaBulkUploader) clearJobToCsvMap() {
	for k := range b.jobToCSVMap {
		delete(b.jobToCSVMap, k)
	}
}
