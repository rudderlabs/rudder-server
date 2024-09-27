package marketobulkupload

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"
)

type MarketoBulkUploader struct {
	destName          string
	destinationConfig map[string]interface{}
	transformUrl      string
	pollUrl           string
	conf              *config.Config
	logger            logger.Logger
	statsFactory      stats.Stats
	timeout           time.Duration
}

func NewManager(conf *config.Config, logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) (*MarketoBulkUploader, error) {
	marketoBulkUpload := &MarketoBulkUploader{
		destName:          destination.DestinationDefinition.Name,
		destinationConfig: destination.Config,
		pollUrl:           "/pollStatus",
		transformUrl:      conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
		conf:              conf,
		logger:            logger.Child("Marketo").Child("MarketoBulkUploader"),
		statsFactory:      statsFactory,
		timeout:           conf.GetDuration("HttpClient.marketoBulkUpload.timeout", 30, time.Second),
	}
	return marketoBulkUpload, nil
}

type marketoPollInputStruct struct {
	ImportId   string                 `json:"importId"`
	DestType   string                 `json:"destType"`
	DestConfig map[string]interface{} `json:"config"`
}

func (b *MarketoBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	finalPollInput := marketoPollInputStruct{
		ImportId:   pollInput.ImportId,
		DestType:   "MARKETO_BULK_UPLOAD",
		DestConfig: b.destinationConfig,
	}
	payload, err := json.Marshal(finalPollInput)
	if err != nil {
		b.logger.Errorf("Error in Marshalling Poll Input: %v", err)
		return common.PollStatusResponse{
			StatusCode: 500,
			HasFailed:  true,
		}
	}

	pollURL, err := url.JoinPath(b.transformUrl, b.pollUrl)
	if err != nil {
		b.logger.Errorf("Error in preparing poll url: %v", err)
		return common.PollStatusResponse{
			StatusCode: 500,
			HasFailed:  true,
			Error:      err.Error(),
		}
	}

	bodyBytes, transformerConnectionStatus := misc.HTTPCallWithRetryWithTimeout(pollURL, payload, b.timeout)
	if transformerConnectionStatus != 200 {
		return common.PollStatusResponse{
			StatusCode: transformerConnectionStatus,
			HasFailed:  true,
			Error:      string(bodyBytes),
		}
	}
	var asyncResponse common.PollStatusResponse
	err = json.Unmarshal(bodyBytes, &asyncResponse)
	if err != nil {
		// needs to be retried
		b.logger.Errorf("Error in Unmarshalling Poll Response: %v", err)
		return common.PollStatusResponse{
			StatusCode: 500,
			HasFailed:  true,
		}
	}

	if asyncResponse.Error != "" {
		b.logger.Errorw("[Batch Router] Failed to fetch status for",
			lf.DestinationType, "MARKETO_BULK_UPLOAD",
			"body", string(bodyBytes[:512]),
			lf.Error, asyncResponse.Error,
		)
		return common.PollStatusResponse{
			StatusCode: 500,
			HasFailed:  true,
			Error:      asyncResponse.Error,
		}
	}
	return asyncResponse
}

func (b *MarketoBulkUploader) generateFailedPayload(jobs []*jobsdb.JobT, importID, destType, csvHeaders string) []byte {
	var failedPayloadT common.AsyncFailedPayload
	failedPayloadT.Input = make([]map[string]interface{}, len(jobs))
	failedPayloadT.Config = b.destinationConfig
	for index, job := range jobs {
		failedPayloadT.Input[index] = make(map[string]interface{})
		var message map[string]interface{}
		metadata := make(map[string]interface{})
		err := json.Unmarshal([]byte(common.GetTransformedData(job.EventPayload)), &message)
		if err != nil {
			b.logger.Errorf("Error in Unmarshalling GetUploadStats Input : %v", err)
			return nil
		}
		metadata["job_id"] = job.JobID
		failedPayloadT.Input[index]["message"] = message
		failedPayloadT.Input[index]["metadata"] = metadata
	}
	failedPayloadT.DestType = strings.ToLower(destType)
	failedPayloadT.ImportId = importID
	failedPayloadT.MetaData = common.MetaDataT{CSVHeaders: csvHeaders}
	payload, err := json.Marshal(failedPayloadT)
	if err != nil {
		b.logger.Errorf("Error in Marshalling GetUploadStats Input : %v", err)
		return nil
	}
	return payload
}

func (b *MarketoBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	var jobsURL string

	if UploadStatsInput.FailedJobURLs != "" {
		jobsURL = UploadStatsInput.FailedJobURLs
	} else {
		jobsURL = UploadStatsInput.WarningJobURLs
	}

	parameters := UploadStatsInput.Parameters
	importId := gjson.GetBytes(parameters, "importId").String()
	csvHeaders := gjson.GetBytes(parameters, "metadata.csvHeader").String()
	payload := b.generateFailedPayload(UploadStatsInput.ImportingList, importId, b.destName, csvHeaders)
	if payload == nil {
		return common.GetUploadStatsResponse{
			StatusCode: 500,
		}
	}

	failedBodyBytes, statusCode := misc.HTTPCallWithRetryWithTimeout(b.transformUrl+jobsURL, payload, b.timeout)
	if statusCode != 200 {
		return common.GetUploadStatsResponse{
			StatusCode: statusCode,
		}
	}
	var failedJobsResponse common.GetUploadStatsResponse
	err := json.Unmarshal(failedBodyBytes, &failedJobsResponse)
	if err != nil {
		b.logger.Errorf("Error in Unmarshalling Failed Jobs Response: %v", err)
		return common.GetUploadStatsResponse{
			StatusCode: 500,
		}
	}

	if failedJobsResponse.Error != "" {
		b.logger.Errorw("[Batch Router] Failed to fetch status for",
			lf.DestinationType, "MARKETO_BULK_UPLOAD",
			"body", string(failedBodyBytes),
			lf.Error, failedJobsResponse.Error,
		)
		return common.GetUploadStatsResponse{
			StatusCode: 500,
		}
	}

	failedReasons := failedJobsResponse.Metadata.FailedReasons
	warningReasons := failedJobsResponse.Metadata.WarningReasons

	// Build the response body
	return common.GetUploadStatsResponse{
		StatusCode: failedJobsResponse.StatusCode,
		Metadata: common.EventStatMeta{
			FailedKeys:     failedJobsResponse.Metadata.FailedKeys,
			WarningKeys:    failedJobsResponse.Metadata.WarningKeys,
			SucceededKeys:  failedJobsResponse.Metadata.SucceededKeys,
			FailedReasons:  failedReasons,
			WarningReasons: warningReasons,
		},
	}
}

func extractJobStats(keyMap map[string]interface{}, importingJobIDs []int64, statusCode int) ([]int64, []int64) {
	if len(keyMap) == 0 {
		if statusCode == http.StatusOK {
			// putting in failed jobs list
			return []int64{}, importingJobIDs
		} else {
			// putting in aborted jobs list
			return importingJobIDs, []int64{}
		}
	}

	_, ok := keyMap["successfulJobs"].([]interface{})
	var succesfulJobIDs, failedJobIDsTrans []int64
	var err error
	if ok {
		succesfulJobIDs, err = misc.ConvertStringInterfaceToIntArray(keyMap["successfulJobs"])
		if err != nil {
			failedJobIDsTrans = importingJobIDs
		}
	}
	_, ok = keyMap["unsuccessfulJobs"].([]interface{})
	if ok {
		failedJobIDsTrans, err = misc.ConvertStringInterfaceToIntArray(keyMap["unsuccessfulJobs"])
		if err != nil {
			failedJobIDsTrans = importingJobIDs
		}
	}
	return succesfulJobIDs, failedJobIDsTrans
}

func (*MarketoBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
}

func (b *MarketoBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destination := asyncDestStruct.Destination
	destinationID := destination.ID
	filePath := asyncDestStruct.FileName
	destConfig := destination.Config
	destType := destination.DestinationDefinition.Name
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs

	destinationUploadUrl := asyncDestStruct.DestinationUploadURL
	uploadURL, err := url.JoinPath(b.transformUrl, destinationUploadUrl)
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: Failed to prepare upload url " + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	file, err := os.Open(filePath)
	if err != nil {
		panic("BRT: Read File Failed" + err.Error())
	}
	defer file.Close()
	var input []common.AsyncJob
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tempJob common.AsyncJob
		jobBytes := scanner.Bytes()
		err := json.Unmarshal(jobBytes, &tempJob)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
				FailedReason:  "BRT: Error in Unmarshalling Job: " + err.Error(),
				FailedCount:   len(failedJobIDs) + len(importingJobIDs),
				DestinationID: destinationID,
			}
		}
		input = append(input, tempJob)
	}
	payload, err := json.Marshal(common.AsyncUploadT{
		Input:    input,
		Config:   destConfig,
		DestType: strings.ToLower(destType),
	})
	if err != nil {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  "BRT: JSON Marshal Failed" + err.Error(),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	uploadTimeStat := b.statsFactory.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	payloadSizeStat := b.statsFactory.NewTaggedStat("payload_size", stats.HistogramType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	startTime := time.Now()
	payloadSizeStat.Observe(float64(len(payload)))
	b.logger.Debugf("[Async Destination Manager] File Upload Started for Dest Type %v", destType)
	responseBody, statusCodeHTTP := misc.HTTPCallWithRetryWithTimeout(uploadURL, payload, b.conf.GetDuration("HttpClient.marketoBulkUpload.timeout", 10, time.Minute))
	b.logger.Debugf("[Async Destination Manager] File Upload Finished for Dest Type %v", destType)
	uploadTimeStat.Since(startTime)
	var bodyBytes []byte
	var statusCode string
	if statusCodeHTTP != 200 {
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  fmt.Sprintf(`HTTP Call to Transformer Returned Non 200. StatusCode: %d`, statusCodeHTTP),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	bodyBytes = responseBody
	statusCode = gjson.GetBytes(bodyBytes, "statusCode").String()

	var uploadResponse common.AsyncUploadOutput
	switch statusCode {
	case "200":
		var responseStruct common.UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
				FailedReason:  "BRT: Incorrect Response from Transformer:: " + err.Error(),
				FailedCount:   len(failedJobIDs) + len(importingJobIDs),
				DestinationID: destinationID,
			}
		}
		var parameters common.ImportParameters
		parameters.ImportId = responseStruct.ImportId
		metaDataString, _ := responseStruct.Metadata["csvHeader"].(string)
		parameters.MetaData = common.MetaDataT{CSVHeaders: metaDataString}
		importParameters, err := json.Marshal(parameters)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
				FailedReason:  "BRT: JSON Marshal Failed" + err.Error(),
				FailedCount:   len(failedJobIDs) + len(importingJobIDs),
				DestinationID: destinationID,
			}
		}
		successfulJobIDs, failedJobIDsTrans := extractJobStats(responseStruct.Metadata, importingJobIDs, http.StatusOK)

		uploadResponse = common.AsyncUploadOutput{
			ImportingJobIDs:     successfulJobIDs,
			FailedJobIDs:        append(failedJobIDs, failedJobIDsTrans...),
			FailedReason:        `Jobs flowed over the prescribed limit`,
			ImportingParameters: importParameters,
			ImportingCount:      len(importingJobIDs),
			FailedCount:         len(failedJobIDs) + len(failedJobIDsTrans),
			DestinationID:       destinationID,
		}
	case "400":
		var responseStruct common.UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			return common.AsyncUploadOutput{
				FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
				FailedReason:  "BRT: Incorrect Response from Transformer:: " + err.Error(),
				FailedCount:   len(failedJobIDs) + len(importingJobIDs),
				DestinationID: destinationID,
			}
		}
		eventsAbortedStat := b.statsFactory.NewTaggedStat("events_delivery_aborted", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": destType,
		})
		abortedJobIDs, failedJobIDsTrans := extractJobStats(responseStruct.Metadata, importingJobIDs, http.StatusBadRequest)
		errorMessageFromTransformer := gjson.GetBytes(bodyBytes, "error").String()
		eventsAbortedStat.Count(len(abortedJobIDs))
		uploadResponse = common.AsyncUploadOutput{
			AbortJobIDs:   abortedJobIDs,
			FailedJobIDs:  append(failedJobIDs, failedJobIDsTrans...),
			AbortReason:   string(bodyBytes),
			AbortCount:    len(importingJobIDs),
			FailedCount:   len(failedJobIDs) + len(failedJobIDsTrans),
			DestinationID: destinationID,
		}
		if errorMessageFromTransformer != "" {
			uploadResponse.FailedReason = errorMessageFromTransformer
		} else {
			uploadResponse.FailedReason = `Jobs flowed over the prescribed limit`
		}
	default:
		uploadResponse = common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}

	}
	return uploadResponse
}
