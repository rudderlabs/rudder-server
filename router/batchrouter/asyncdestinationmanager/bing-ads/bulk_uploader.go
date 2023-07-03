package bingads

import (
	stdjson "encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	bingads "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func NewBingAdsBulkUploader(name string, service bingads.BulkServiceI, client *Client) *BingAdsBulkUploader {
	return &BingAdsBulkUploader{
		destName: name,
		service:  service,
		logger:   logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("BingAds").Child("BingAdsBulkUploader"),
		client:   *client,
	}
}

/*
This function create at most 3 zip files from the text file created by the batchrouter
It takes the text file path as input and returns the zip file path
The maximum size of the zip file is 100MB, if the size of the zip file exceeds 100MB then the job is marked as failed
*/
func (b *BingAdsBulkUploader) Upload(destination *backendconfig.DestinationT, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destConfig := DestinationConfig{}
	// TODO: Need to handle errors here
	jsonConfig, err := stdjson.Marshal(destination.Config)
	err = stdjson.Unmarshal(jsonConfig, &destConfig)
	var failedJobs []int64
	var successJobs []int64
	var importIds []string
	var errors []string
	actionFiles, err := b.CreateZipFile(asyncDestStruct.FileName, destConfig.AudienceId)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while transforming the file. %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	uploadRetryableStat := stats.Default.NewTaggedStat("events_over_prescribed_limit", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	uploadRetryableStat.Count(len(actionFiles[0].FailedJobIDs) + len(actionFiles[1].FailedJobIDs) + len(actionFiles[2].FailedJobIDs))
	for _, actionFile := range actionFiles {
		_, err := os.Stat(actionFile.ZipFilePath)
		if err != nil || actionFile.EventCount == 0 {
			continue
		}
		urlResp, err := b.service.GetBulkUploadUrl()
		if err != nil {
			b.logger.Error("Error in getting bulk upload url: %w", err)
			failedJobs = append(append(failedJobs, actionFile.SuccessfulJobIDs...), actionFile.FailedJobIDs...)
			errors = append(errors, fmt.Sprintf("%s:error in getting bulk upload url: %s", actionFile.Action, err.Error()))
			continue
		}

		if urlResp.UploadUrl == "" || urlResp.RequestId == "" {
			b.logger.Error(`{"error" : "getting empty string in upload url or request id"}`)
			failedJobs = append(append(failedJobs, actionFile.SuccessfulJobIDs...), actionFile.FailedJobIDs...)
			errors = append(errors, fmt.Sprintf("%s:getting empty string in upload url or request id", actionFile.Action))
			continue
		}

		uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})

		startTime := time.Now()
		uploadBulkFileResp, errorDuringUpload := b.service.UploadBulkFile(urlResp.UploadUrl, actionFile.ZipFilePath)
		uploadTimeStat.Since(startTime)

		err = os.Remove(actionFile.ZipFilePath)
		if err != nil {
			b.logger.Error("Error in removing zip file: %v", err)
			// To do add an alert here
		}
		if errorDuringUpload != nil {
			b.logger.Error("error in uploading the bulk file: %v", errorDuringUpload)
			failedJobs = append(append(failedJobs, actionFile.SuccessfulJobIDs...), actionFile.FailedJobIDs...)
			errors = append(errors, fmt.Sprintf("%s:error in uploading the bulk file: %v", actionFile.Action, errorDuringUpload))
			continue
		}

		if uploadBulkFileResp.RequestId == "" || uploadBulkFileResp.TrackingId == "" {
			failedJobs = append(append(failedJobs, actionFile.SuccessfulJobIDs...), actionFile.FailedJobIDs...)
			errors = append(errors, fmt.Sprintf("%s:getting empty string in upload url or request id, ", actionFile.Action))
			continue
		}
		importIds = append(importIds, uploadBulkFileResp.RequestId)
		failedJobs = append(failedJobs, actionFile.FailedJobIDs...)
		successJobs = append(successJobs, actionFile.SuccessfulJobIDs...)
	}

	var parameters common.ImportParameters
	parameters.ImportId = strings.Join(importIds, comma)
	importParameters, err := stdjson.Marshal(parameters)
	if err != nil {
		b.logger.Error("Errored in Marshalling parameters" + err.Error())
	}
	allErrors := `{"error":"` + strings.Join(errors, comma) + `"}`
	return common.AsyncUploadOutput{
		ImportingJobIDs:     successJobs,
		FailedJobIDs:        append(asyncDestStruct.FailedJobIDs, failedJobs...),
		FailedReason:        allErrors,
		ImportingParameters: stdjson.RawMessage(importParameters),
		ImportingCount:      len(successJobs),
		FailedCount:         len(asyncDestStruct.FailedJobIDs) + len(failedJobs),
		DestinationID:       destination.ID,
	}
}

func (b *BingAdsBulkUploader) PollSingleImport(requestId string) common.PollStatusResponse {
	uploadStatusResp, err := b.service.GetBulkUploadStatus(requestId)
	if err != nil {
		return common.PollStatusResponse{
			StatusCode: 500,
			HasFailed:  true,
		}
	}
	switch uploadStatusResp.RequestStatus {
	case "Completed":
		return common.PollStatusResponse{
			Complete:   true,
			StatusCode: 200,
		}
	case "CompletedWithErrors":
		return common.PollStatusResponse{
			Complete:   true,
			StatusCode: 200,
			HasFailed:  true,
		}
	case "FileUploaded", "InProgress", "PendingFileUpload":
		return common.PollStatusResponse{
			InProgress: true,
			StatusCode: 200,
		}
	default:
		return common.PollStatusResponse{
			HasFailed:  true,
			StatusCode: 400,
		}
	}
}

func (b *BingAdsBulkUploader) Poll(pollInput common.AsyncPoll) (common.PollStatusResponse, int) {
	fmt.Println("Polling Bing Ads")
	var cumulativeResp common.PollStatusResponse
	var statusCode int
	requestIdsArray := common.GenerateArrayOfStrings(pollInput.ImportId)
	for _, requestId := range requestIdsArray {
		resp := b.PollSingleImport(requestId)
		if status != 200 {
			cumulativeResp = resp
			statusCode = status
			break
		}
		cumulativeResp = common.PollStatusResponse{
			Complete:       resp.Success,
			StatusCode:     200,
			HasFailed:      cumulativeResp.HasFailed || resp.HasFailed,
			HasWarning:     cumulativeResp.HasWarning || resp.HasWarning,
			FailedJobsURL:  "",
			WarningJobsURL: "",
			OutputFilePath: cumulativeResp.OutputFilePath + resp.OutputFilePath + ",",
		}
		statusCode = status
	}

	return cumulativeResp, statusCode
}

func (b *BingAdsBulkUploader) GetUploadStatsOfSingleImport(filePath string) (common.GetUploadStatsResponse, int) {
	records := ReadPollResults(filePath)
	status := "200"
	clientIDErrors := ProcessPollStatusData(records)
	eventStatsResponse := common.GetUploadStatsResponse{
		Status: status,
		Metadata: common.EventStatMeta{
			FailedKeys:    GetFailedKeys(clientIDErrors),
			ErrFailed:     nil,
			WarningKeys:   []int64{},
			ErrWarning:    nil,
			SucceededKeys: []int64{},
			ErrSuccess:    nil,
			FailedReasons: GetFailedReasons(clientIDErrors),
		},
	}

	eventsAbortedStat := stats.Default.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	eventsAbortedStat.Count(len(eventStatsResponse.Metadata.FailedKeys))

	eventsSuccessStat := stats.Default.NewTaggedStat("success_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	eventsSuccessStat.Count(len(eventStatsResponse.Metadata.SucceededKeys))

	return eventStatsResponse, 200
}

func (b *BingAdsBulkUploader) GetUploadStats(UploadStatsInput common.FetchUploadJobStatus) (common.GetUploadStatsResponse, int) {
	// considering importing jobs are the primary list of jobs sent
	// making an array of those jobIds
	importList := UploadStatsInput.ImportingList
	var initialEventList []int64
	for _, job := range importList {
		initialEventList = append(initialEventList, job.JobID)
	}
	var eventStatsResponse common.GetUploadStatsResponse
	filePaths := common.GenerateArrayOfStrings(UploadStatsInput.OutputFilePath)
	for _, filePath := range filePaths {
		response, _ := b.GetUploadStatsOfSingleImport(filePath)
		eventStatsResponse = common.GetUploadStatsResponse{
			Status: response.Status,
			Metadata: common.EventStatMeta{
				FailedKeys:    append(eventStatsResponse.Metadata.FailedKeys, response.Metadata.FailedKeys...),
				ErrFailed:     nil,
				WarningKeys:   []int64{},
				ErrWarning:    nil,
				SucceededKeys: []int64{},
				ErrSuccess:    nil,
				FailedReasons: common.MergeMaps(eventStatsResponse.Metadata.FailedReasons, response.Metadata.FailedReasons),
			},
		}
	}

	// filtering out failed jobIds from the total array of jobIds
	eventStatsResponse.Metadata.SucceededKeys = GetSuccessKeys(eventStatsResponse.Metadata.FailedKeys, initialEventList)

	return eventStatsResponse, 200
}
