package eloqua

import (
	"encoding/json"
	stdjson "encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func createAsyncUploadOutput(errorString string, err error, destinationId string, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	return common.AsyncUploadOutput{
		FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
		FailedReason:  fmt.Sprintf(errorString+"%v", err),
		FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
		DestinationID: destinationId,
	}
}

func (b *EloquaBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	uploadDataThroughCSV := b.uploadDataThroughCSV
	file, err := os.Open(asyncDestStruct.FileName)
	if err != nil {
		return createAsyncUploadOutput("got error while opening the file. ", err, destination.ID, asyncDestStruct)
	}
	defer file.Close()
	eventType, customObjectId, fields, identifierFieldName, err := getEventDetails(file)
	if err != nil {
		return createAsyncUploadOutput("got error while checking the event type. ", err, destination.ID, asyncDestStruct)
	}

	var eloquaFields *Fields

	customObjectData := HttpRequestData{
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
	}
	if eventType == "track" {
		customObjectData.DynamicPart = customObjectId
	}
	eloquaFields, err = b.service.FetchFields(&customObjectData)
	if err != nil {
		return createAsyncUploadOutput("got error while fetching fields. ", err, destination.ID, asyncDestStruct)
	}

	uploadJobInfo := JobInfo{
		fileSizeLimit: b.fileSizeLimit,
		importingJobs: asyncDestStruct.ImportingJobIDs,
	}

	importDefinitionBody, err := createBodyForImportDefinition(eventType, fields, eloquaFields, identifierFieldName)
	if err != nil {
		return createAsyncUploadOutput("got error while creating body for import definition. ", err, destination.ID, asyncDestStruct)
	}
	marshalledData, err := json.Marshal(importDefinitionBody)
	if err != nil {
		return createAsyncUploadOutput("unable marshal importDefinitionBody. ", err, destination.ID, asyncDestStruct)
	}
	importDefinitionData := HttpRequestData{
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
		Body:          strings.NewReader(string(marshalledData)),
		DynamicPart:   customObjectId,
	}

	importDefinition, err := b.service.CreateImportDefinition(&importDefinitionData, eventType)
	if err != nil {
		return createAsyncUploadOutput("unable to create importdefinition. ", err, destination.ID, asyncDestStruct)
	}

	defer func() {
		deleteImportDefinitionData := HttpRequestData{
			BaseEndpoint:  b.baseEndpoint,
			Authorization: b.authorization,
			DynamicPart:   importDefinition.URI,
		}
		err := b.service.DeleteImportDefinition(&deleteImportDefinitionData)
		if err != nil {
			b.logger.Error("Error while deleting import definition", err)
		}
	}()
	uploadDataData := HttpRequestData{
		BaseEndpoint:  b.baseEndpoint,
		DynamicPart:   importDefinition.URI,
		Authorization: b.authorization,
	}
	var (
		filePAth   string
		uploadData []map[string]interface{}
	)
	if uploadDataThroughCSV {
		filePAth, _ = createCSVFile(fields, file, &uploadJobInfo)
		defer os.Remove(filePAth)
		err = b.service.UploadData(&uploadDataData, filePAth)
	} else {
		uploadData = createUploadData(file, &uploadJobInfo)
		err = b.service.UploadDataWithoutCSV(&uploadDataData, uploadData)
	}
	if err != nil {
		return createAsyncUploadOutput("unable to upload the data. ", err, destination.ID, asyncDestStruct)
	}
	runSyncBody := map[string]interface{}{
		"syncedInstanceUri": importDefinition.URI,
	}
	marshalledData, err = json.Marshal(runSyncBody)
	if err != nil {
		return createAsyncUploadOutput("unable marshal importDefinitionBody. ", err, destination.ID, asyncDestStruct)
	}
	runSyncData := HttpRequestData{
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
		Body:          strings.NewReader(string(marshalledData)),
	}
	syncURI, err := b.service.RunSync(&runSyncData)
	if err != nil {
		return createAsyncUploadOutput("unable to run the sync after uploading the file. ", err, destination.ID, asyncDestStruct)
	}

	var parameters common.ImportParameters
	parameters.ImportId = syncURI
	importParameters, err := stdjson.Marshal(parameters)
	if err != nil {
		return createAsyncUploadOutput("error while marshaling parameters. ", err, destination.ID, asyncDestStruct)
	}
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
	checkSyncStatusData := HttpRequestData{
		DynamicPart:   pollInput.ImportId,
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
		return common.PollStatusResponse{
			Complete:      true,
			InProgress:    false,
			StatusCode:    200,
			HasFailed:     false,
			HasWarning:    false,
			FailedJobURLs: pollInput.ImportId,
		}
	case "error":
		return common.PollStatusResponse{
			Complete:      true,
			InProgress:    false,
			StatusCode:    200,
			HasFailed:     true,
			HasWarning:    false,
			FailedJobURLs: "error",
		}
	case "warning":
		return common.PollStatusResponse{
			Complete:      true,
			InProgress:    false,
			StatusCode:    200,
			HasFailed:     true,
			HasWarning:    true,
			FailedJobURLs: pollInput.ImportId,
		}
	case "pending":
		return common.PollStatusResponse{
			InProgress: true,
		}
	case "active":
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
	jobIDs := []int64{}
	failedReasons := map[int64]string{}
	for _, job := range UploadStatsInput.ImportingList {
		jobIDs = append(jobIDs, job.JobID)
		failedReasons[job.JobID] = "error due to unknown reason"
	}
	eventStatMeta := common.EventStatMeta{}
	if UploadStatsInput.FailedJobURLs == "error" {
		eventStatMeta.FailedKeys = jobIDs
		eventStatMeta.SucceededKeys = []int64{}
		eventStatMeta.FailedReasons = failedReasons
		return common.GetUploadStatsResponse{
			StatusCode: 200,
			Metadata:   eventStatMeta,
		}
	}
	checkRejectedData := HttpRequestData{
		BaseEndpoint:  b.baseEndpoint,
		DynamicPart:   UploadStatsInput.FailedJobURLs,
		Authorization: b.authorization,
	}
	eventStatMetaWithFailedJobs, err := parseRejectedData(&checkRejectedData, UploadStatsInput.ImportingList, b.service)
	if err != nil {
		b.logger.Error("Error while parsing rejected data", err)
		return common.GetUploadStatsResponse{
			StatusCode: 500,
		}
	}
	uploadStatusResponse := common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata:   *eventStatMetaWithFailedJobs,
	}
	return uploadStatusResponse
}
