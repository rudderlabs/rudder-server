package eloqua

import (
	stdjson "encoding/json"
	"fmt"
	"os"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func (b *EloquaBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination

	file, err := os.Open(asyncDestStruct.FileName)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while opening the file. %v", err),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	defer file.Close()
	eventType, customObjectId, err := checkEventType(file)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while checking the event type. %v", err),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	fields := getFields(file)

	var eloquaFields *Fields

	if eventType == "track" {
		customObjectData := Data{
			BaseEndpoint:  b.baseEndpoint,
			Authorization: b.authorization,
			DynamicPart:   customObjectId,
		}
		eloquaFields, err = FetchFields(&customObjectData)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
				FailedReason:  fmt.Sprintf("got error while fetching fields. %v", err),
				FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
				DestinationID: destination.ID,
			}
		}
	} else {
		customObjectData := Data{
			BaseEndpoint:  b.baseEndpoint,
			Authorization: b.authorization,
		}
		eloquaFields, err = FetchFields(&customObjectData)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
				FailedReason:  fmt.Sprintf("got error while fetching fields. %v", err),
				FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
				DestinationID: destination.ID,
			}
		}
	}

	filePAth, _ := createCSVFile(fields, file)

	importDefinitionBody, err := createBodyForImportDefinition(eventType, fields, eloquaFields, file)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("%v", err),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	importDefinitionData := Data{
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
		Body:          importDefinitionBody,
	}

	importDefinition, err := CreateImportDefinition(&importDefinitionData)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("unable to create importdefinition", err),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	defer func() {
		deleteImportDefinitionData := Data{
			BaseEndpoint:  b.baseEndpoint,
			Authorization: b.authorization,
			DynamicPart:   importDefinition.URI,
		}
		err := DeleteImportDefinition(&deleteImportDefinitionData)
		if err != nil {
			b.logger.Error("Error while deleting import definition", err)
		}
	}()
	uploadDataData := Data{
		BaseEndpoint:  b.baseEndpoint,
		DynamicPart:   importDefinition.URI,
		Authorization: b.authorization,
	}

	err = UploadData(&uploadDataData, filePAth)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("unable to upload the data", err),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	runSyncBody := map[string]interface{}{
		"syncedInstanceUri": importDefinition.URI,
	}
	runSyncData := Data{
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
		Body:          runSyncBody,
	}
	syncURI, err := RunSync(&runSyncData)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("unable to run the sync after uploading the file", err),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}

	var parameters common.ImportParameters
	parameters.ImportId = syncURI
	importParameters, err := stdjson.Marshal(parameters)
	return common.AsyncUploadOutput{
		ImportingJobIDs:     asyncDestStruct.ImportingJobIDs,
		FailedReason:        "failed due to some unknown reason",
		ImportingCount:      len(asyncDestStruct.ImportingJobIDs),
		FailedCount:         len(asyncDestStruct.FailedJobIDs),
		DestinationID:       asyncDestStruct.Destination.ID,
		ImportingParameters: importParameters,
	}
}

func (b *EloquaBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	checkSyncStatusData := Data{
		DynamicPart:   pollInput.ImportId,
		BaseEndpoint:  b.baseEndpoint,
		Authorization: b.authorization,
	}

	uploadStatus, err := CheckSyncStatus(&checkSyncStatusData)
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
			FailedJobURLs: pollInput.ImportId,
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
	checkRejectedData := Data{
		BaseEndpoint:  b.baseEndpoint,
		DynamicPart:   UploadStatsInput.FailedJobURLs,
		Authorization: b.authorization,
	}
	eventStatMeta, err := parseRejectedData(&checkRejectedData, UploadStatsInput.ImportingList)
	if err != nil {
		b.logger.Error("Error while parsing rejected data", err)
		return common.GetUploadStatsResponse{
			StatusCode: 500,
		}
	}
	uploadStatusResponse := common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata:   *eventStatMeta,
	}
	return uploadStatusResponse
}
