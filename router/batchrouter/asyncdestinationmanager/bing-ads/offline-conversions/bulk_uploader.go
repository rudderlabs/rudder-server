package offline_conversions

import (
	"encoding/json"
	stdjson "encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func NewBingAdsBulkUploader(logger logger.Logger, statsFactory stats.Stats, destName string, service bingads.BulkServiceI, isHashRequired bool) *BingAdsBulkUploader {
	return &BingAdsBulkUploader{
		isHashRequired: isHashRequired,
		destName:       destName,
		service:        service,
		logger:         logger.Child("BingAds").Child("BingAdsBulkUploader"),
		statsFactory:   statsFactory,
		fileSizeLimit:  common.GetBatchRouterConfigInt64("MaxUploadLimit", destName, 100*bytesize.MB),
		eventsLimit:    common.GetBatchRouterConfigInt64("MaxEventsLimit", destName, 1000),
	}
}

/*
	Microsoft Docs: https://learn.microsoft.com/en-us/advertising/bulk-service/offline-conversion

returns: A string of object in form

	{
		message:{
			Action: "insert", "update", "delete",
			fields: {}
		},
		metadata:{
			jobId: "job_id"
		}
	}
*/
func (b *BingAdsBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	// Unmarshal the JSON raw message into the record struct
	payload := string(job.EventPayload)
	var event Record
	var fields map[string]interface{}
	err := json.Unmarshal(job.EventPayload, &event)
	if err != nil {
		return payload, fmt.Errorf("unmarshalling event %w:", err)
	}
	err = json.Unmarshal(event.Fields, &fields)
	if err != nil {
		return payload, fmt.Errorf("unmarshalling event.fields: %w", err)
	}
	// validate for conversion time mscklid and conversion name
	generalRequiredFields := []string{"microsoftClickId", "conversionName", "conversionTime"}
	for _, field := range generalRequiredFields {
		err := validateField(fields, field)
		if err != nil {
			return payload, err
		}
	}
	if event.Action != "insert" {
		// validate for adjusted time
		err := validateField(fields, "adjustedConversionTime")
		if err != nil {
			return payload, err
		}
		if event.Action == "update" {
			// validate for Adjustment Value
			err := validateField(fields, "conversionValue")
			if err != nil {
				return payload, err
			}
		}
	}
	if b.isHashRequired {
		event.Fields, err = hashFields(fields)
		if err != nil {
			return payload, fmt.Errorf("Unable to hash fields.%w", err)
		}
	}
	data := Data{
		Message: Message{
			Fields: event.Fields,
			Action: event.Action,
		},
		Metadata: Metadata{
			JobID: job.JobID,
		},
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return payload, err
	}
	return string(jsonData), nil
}

/*
This function create at most 3 zip files from the text file created by the batchrouter
It takes the text file path as input and returns the zip file path
The maximum size of the zip file is 100MB, if the size of the zip file exceeds 100MB then the job is marked as failed
*/
func (b *BingAdsBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	var failedJobs []int64
	var successJobs []int64
	var importIds []string
	var errors []string
	actionFiles, err := b.createZipFile(asyncDestStruct.FileName)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
			FailedReason:  fmt.Sprintf("got error while transforming the file. %v", err.Error()),
			FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
			DestinationID: destination.ID,
		}
	}
	uploadRetryableStat := b.statsFactory.NewTaggedStat("events_over_prescribed_limit", stats.CountType, map[string]string{
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

		uploadTimeStat := b.statsFactory.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
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

			// remove the file that could not be uploaded
			err = os.Remove(actionFile.ZipFilePath)
			if err != nil {
				b.logger.Error("Error in removing zip file: %v", err)
			}
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
	errorMap := map[string]string{
		"error": strings.Join(errors, commaSeparator),
	}
	allErrors, err := json.Marshal(errorMap)
	if err != nil {
		b.logger.Error("Error while marshalling error")
	}
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
		ImportingParameters: importParameters,
		ImportingCount:      len(successJobs),
		FailedCount:         len(asyncDestStruct.FailedJobIDs) + len(failedJobs),
		DestinationID:       destination.ID,
	}
}

func (b *BingAdsBulkUploader) pollSingleImport(requestId string) common.PollStatusResponse {
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
			Complete:            true,
			StatusCode:          200,
			HasFailed:           true,
			FailedJobParameters: uploadStatusResp.ResultFileUrl,
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
	var pollStatusCode []int
	var cumulativeProgressStatus, cumulativeFailureStatus bool
	var cumulativeStatusCode int
	requestIdsArray := lo.Reject(strings.Split(pollInput.ImportId, commaSeparator), func(url string, _ int) bool {
		return url == ""
	})
	for _, requestId := range requestIdsArray {
		resp := b.pollSingleImport(requestId)
		/*
			Cumulative Response logic:
			1. If any of the request is in progress then the whole request is in progress and it should retry
			2. if all the requests are completed then the whole request is completed
			3. if any of the requests are completed with errors then the whole request is completed with errors
			4. if any of the requests are failed then the whole request is failed and retried
			5. if all the requests are failed then the whole request is failed
		*/
		pollStatusCode = append(pollStatusCode, resp.StatusCode)
		completionStatus = append(completionStatus, resp.Complete)
		failedJobURLs = append(failedJobURLs, resp.FailedJobParameters)
		cumulativeProgressStatus = cumulativeProgressStatus || resp.InProgress
		cumulativeFailureStatus = cumulativeFailureStatus || resp.HasFailed
	}
	if lo.Contains(pollStatusCode, 500) {
		cumulativeStatusCode = 500
	} else {
		cumulativeStatusCode = 200
	}
	cumulativeResp = common.PollStatusResponse{
		Complete:            !lo.Contains(completionStatus, false),
		InProgress:          cumulativeProgressStatus,
		StatusCode:          cumulativeStatusCode,
		HasFailed:           cumulativeFailureStatus,
		FailedJobParameters: strings.Join(failedJobURLs, commaSeparator), // creating a comma separated string of all the result file urls
	}

	return cumulativeResp
}

func (b *BingAdsBulkUploader) getUploadStatsOfSingleImport(filePath string) (common.GetUploadStatsResponse, error) {
	records, err := b.readPollResults(filePath)
	if err != nil {
		return common.GetUploadStatsResponse{}, err
	}
	jobIdErrors, err := processPollStatusData(records)
	if err != nil {
		return common.GetUploadStatsResponse{}, err
	}
	eventStatsResponse := common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata: common.EventStatMeta{
			AbortedKeys:    lo.Keys(jobIdErrors),
			AbortedReasons: getAbortedReasons(jobIdErrors),
		},
	}

	eventsAbortedStat := b.statsFactory.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	eventsAbortedStat.Count(len(eventStatsResponse.Metadata.AbortedKeys))

	eventsSuccessStat := b.statsFactory.NewTaggedStat("success_job_count", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": b.destName,
	})
	eventsSuccessStat.Count(len(eventStatsResponse.Metadata.SucceededKeys))
	return eventStatsResponse, nil
}

func (b *BingAdsBulkUploader) GetUploadStats(uploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	// considering importing jobs are the primary list of jobs sent
	// making an array of those jobIds
	importList := uploadStatsInput.ImportingList
	var initialEventList []int64
	for _, job := range importList {
		initialEventList = append(initialEventList, job.JobID)
	}
	eventStatsResponse := common.GetUploadStatsResponse{}
	var abortedJobIDs []int64
	var cumulativeAbortedReasons map[int64]string
	fileURLs := lo.Reject(strings.Split(uploadStatsInput.FailedJobParameters, commaSeparator), func(url string, _ int) bool {
		return url == ""
	})
	for _, fileURL := range fileURLs {
		filePaths, err := b.downloadAndGetUploadStatusFile(fileURL)
		if err != nil {
			b.logger.Error("Error in downloading and unzipping the file: %v", err)
			return common.GetUploadStatsResponse{
				StatusCode: 500,
				Error:      fmt.Sprint(err),
			}
		}
		response, err := b.getUploadStatsOfSingleImport(filePaths[0]) // only one file should be there
		if err != nil {
			b.logger.Error("Error in getting upload stats of single import: %v", err)
			return common.GetUploadStatsResponse{
				StatusCode: 500,
			}
		}
		cumulativeAbortedReasons = lo.Assign(cumulativeAbortedReasons, response.Metadata.AbortedReasons)
		abortedJobIDs = append(abortedJobIDs, response.Metadata.AbortedKeys...)
		eventStatsResponse.StatusCode = response.StatusCode
	}

	eventStatsResponse.Metadata = common.EventStatMeta{
		AbortedKeys:    abortedJobIDs,
		AbortedReasons: cumulativeAbortedReasons,
		SucceededKeys:  getSuccessJobIDs(abortedJobIDs, initialEventList),
	}

	return eventStatsResponse
}
