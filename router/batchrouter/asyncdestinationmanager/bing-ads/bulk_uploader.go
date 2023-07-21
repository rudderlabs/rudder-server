package bingads

import (
	stdjson "encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/samber/lo"

	bingads "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
)

func NewBingAdsBulkUploader(destName string, service bingads.BulkServiceI, client *Client) *BingAdsBulkUploader {
	return &BingAdsBulkUploader{
		destName:      destName,
		service:       service,
		logger:        logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("BingAds").Child("BingAdsBulkUploader"),
		client:        *client,
		fileSizeLimit: common.GetBatchRouterConfigInt64("MaxUploadLimit", destName, 100*bytesize.MB),
		eventsLimit:   common.GetBatchRouterConfigInt64("MaxEventsLimit", destName, 4000000),
	}
}

/*
This function create at most 3 zip files from the text file created by the batchrouter
It takes the text file path as input and returns the zip file path
The maximum size of the zip file is 100MB, if the size of the zip file exceeds 100MB then the job is marked as failed
*/
func (b *BingAdsBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	destConfig := DestinationConfig{}
	jsonConfig, err := stdjson.Marshal(destination.Config)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while marshalling the destination config %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	err = stdjson.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while unmarshalling the destination config %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
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
	for _, actionFile := range actionFiles {
		uploadRetryableStat.Count(len(actionFile.FailedJobIDs))
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
			b.logger.Error(`{"error" : "getting empty string in upload url or request id. Provide correct configuration credentials."}`)
			failedJobs = append(append(failedJobs, actionFile.SuccessfulJobIDs...), actionFile.FailedJobIDs...)
			errors = append(errors, fmt.Sprintf("%s:getting empty string in upload url or request id. Provide correct configuration credentials.", actionFile.Action))
			continue
		}

		uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
			"module":   "batch_router",
			"destType": b.destName,
		})

		startTime := time.Now()
		uploadBulkFileResp, errorDuringUpload := b.service.UploadBulkFile(urlResp.UploadUrl, actionFile.ZipFilePath)
		uploadTimeStat.Since(startTime)

		if errorDuringUpload != nil {
			b.logger.Error("error in uploading the bulk file: %v", errorDuringUpload)
			failedJobs = append(append(failedJobs, actionFile.SuccessfulJobIDs...), actionFile.FailedJobIDs...)
			errors = append(errors, fmt.Sprintf("%s:error in uploading the bulk file: %v", actionFile.Action, errorDuringUpload))
			continue
		}

		if uploadBulkFileResp.RequestId == "" || uploadBulkFileResp.TrackingId == "" {
			failedJobs = append(append(failedJobs, actionFile.SuccessfulJobIDs...), actionFile.FailedJobIDs...)
			errors = append(errors, fmt.Sprintf("%s:getting empty string in upload url or request id, Provide correct configuration credentials.", actionFile.Action))
			continue
		}
		importIds = append(importIds, uploadBulkFileResp.RequestId)
		failedJobs = append(failedJobs, actionFile.FailedJobIDs...)
		successJobs = append(successJobs, actionFile.SuccessfulJobIDs...)
	}

	var parameters common.ImportParameters
	parameters.ImportId = strings.Join(importIds, commaSeparator)
	importParameters, err := stdjson.Marshal(parameters)
	if err != nil {
		b.logger.Error("Errored in Marshalling parameters" + err.Error())
	}
	allErrors := router_utils.EnhanceJSON([]byte(`{}`), "error", strings.Join(errors, commaSeparator))

	for _, actionFile := range actionFiles {
		err = os.Remove(actionFile.ZipFilePath)
		if err != nil {
			b.logger.Error("Error in removing zip file: %v", err)
		}
	}

	return common.AsyncUploadOutput{
		ImportingJobIDs:     successJobs,
		FailedJobIDs:        append(asyncDestStruct.FailedJobIDs, failedJobs...),
		FailedReason:        string(allErrors),
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
			Complete:      true,
			StatusCode:    200,
			HasFailed:     true,
			FailedJobURLs: uploadStatusResp.ResultFileUrl,
		}
	case "FileUploaded", "InProgress", "PendingFileUpload":
		return common.PollStatusResponse{
			InProgress: true,
			StatusCode: 200,
		}
	default:
		return common.PollStatusResponse{
			HasFailed:  true,
			StatusCode: 500,
		}
	}
}

func (b *BingAdsBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	var cumulativeResp common.PollStatusResponse
	var completionStatus []bool
	var failedJobURLs []string
	var cumulativeCompletionStatus, cumulativeProgressStatus, cumulativeFailureStatus bool
	requestIdsArray := lo.Reject(strings.Split(pollInput.ImportId, commaSeparator), func(url string, _ int) bool {
		return url == ""
	})
	for _, requestId := range requestIdsArray {
		resp := b.PollSingleImport(requestId)
		if resp.StatusCode != 200 {
			// if any of the request fails then the whole request fails
			cumulativeResp = resp
			return cumulativeResp
		}
		/*
			Cumulative Response logic:
			1. If any of the request is in progress then the whole request is in progress and it should retry
			2. if all the requests are completed then the whole request is completed
			3. if any of the requests are completed with errors then the whole request is completed with errors
			4. if any of the requests are failed then the whole request is failed and retried
			5. if all the requests are failed then the whole request is failed
		*/
		completionStatus = append(completionStatus, resp.Complete)
		cumulativeCompletionStatus = !lo.Contains(completionStatus, false)
		failedJobURLs = append(failedJobURLs, resp.FailedJobURLs)
		cumulativeProgressStatus = cumulativeProgressStatus || resp.InProgress
		cumulativeFailureStatus = cumulativeFailureStatus || resp.HasFailed
	}

	cumulativeResp = common.PollStatusResponse{
		Complete:      cumulativeCompletionStatus,
		InProgress:    cumulativeProgressStatus,
		StatusCode:    200,
		HasFailed:     cumulativeFailureStatus,
		FailedJobURLs: strings.Join(failedJobURLs, commaSeparator), // creating a comma separated string of all the result file urls
	}

	return cumulativeResp
}

func (b *BingAdsBulkUploader) GetUploadStatsOfSingleImport(filePath string) (common.GetUploadStatsResponse, error) {
	records, err := b.ReadPollResults(filePath)
	if err != nil {
		return common.GetUploadStatsResponse{}, err
	}
	clientIDErrors, err := ProcessPollStatusData(records)
	if err != nil {
		return common.GetUploadStatsResponse{}, err
	}
	eventStatsResponse := common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata: common.EventStatMeta{
			FailedKeys:    lo.Keys(clientIDErrors),
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
	return eventStatsResponse, nil
}

func (b *BingAdsBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	// considering importing jobs are the primary list of jobs sent
	// making an array of those jobIds
	importList := UploadStatsInput.ImportingList
	var initialEventList []int64
	for _, job := range importList {
		initialEventList = append(initialEventList, job.JobID)
	}
	eventStatsResponse := common.GetUploadStatsResponse{}
	var failedJobIds []int64
	var cumulativeFailedReasons map[int64]string
	fileURLs := lo.Reject(strings.Split(UploadStatsInput.FailedJobURLs, commaSeparator), func(url string, _ int) bool {
		return url == ""
	})
	fmt.Println("UploadStatsInput File URLs", UploadStatsInput.FailedJobURLs)
	fmt.Println("File URLs", fileURLs)
	for _, fileURL := range fileURLs {
		filePaths, err := b.DownloadAndGetUploadStatusFile(fileURL)
		if err != nil {
			b.logger.Error("Error in downloading and unzipping the file: %v", err)
			return common.GetUploadStatsResponse{
				StatusCode: 500,
			}
		}
		response, err := b.GetUploadStatsOfSingleImport(filePaths[0]) // only one file should be there
		if err != nil {
			b.logger.Error("Error in getting upload stats of single import: %v", err)
			return common.GetUploadStatsResponse{
				StatusCode: 500,
			}
		}
		cumulativeFailedReasons = lo.Assign(cumulativeFailedReasons, response.Metadata.FailedReasons)
		failedJobIds = append(failedJobIds, response.Metadata.FailedKeys...)
		eventStatsResponse.StatusCode = response.StatusCode
	}

	eventStatsResponse.Metadata = common.EventStatMeta{
		FailedKeys:    failedJobIds,
		FailedReasons: cumulativeFailedReasons,
		SucceededKeys: GetSuccessJobIDs(failedJobIds, initialEventList),
	}

	return eventStatsResponse
}
