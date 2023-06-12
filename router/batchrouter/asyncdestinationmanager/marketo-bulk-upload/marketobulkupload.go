package marketobulkupload

import (
	"bufio"
	"encoding/json"
	stdjson "encoding/json"
	"net/url"
	"os"
	"strings"
	time "time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

type MarketoBulkUploader struct {
	destName          string
	timeout           time.Duration
	destinationConfig map[string]interface{}
	TransformUrl      string
	PollUrl           string
}

func NewManager(destination *backendconfig.DestinationT, HTTPTimeout time.Duration) (*MarketoBulkUploader, error) {
	marketoBulkUpload := &MarketoBulkUploader{destName: "MARKETO_BULK_UPLOAD", timeout: HTTPTimeout, destinationConfig: destination.DestinationDefinition.Config, PollUrl: "/pollStatus", TransformUrl: config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")}
	return marketoBulkUpload, nil

}

var (
	pkgLogger logger.Logger
)

func init() {
	pkgLogger = logger.NewLogger().Child("asyncdestinationmanager").Child("marketobulkupload")
}

func (b *MarketoBulkUploader) Poll(pollStruct common.AsyncPoll) (common.AsyncStatusResponse, int) {
	payload, err := json.Marshal(pollStruct)
	if err != nil {
		panic("JSON Marshal Failed" + err.Error())
	}
	bodyBytes, statusCode := misc.HTTPCallWithRetryWithTimeout(b.TransformUrl+b.PollUrl, payload, b.timeout)
	var asyncResponse common.AsyncStatusResponse
	err = json.Unmarshal(bodyBytes, &asyncResponse)
	if err != nil {
		panic("JSON Unmarshal Failed" + err.Error())
	}
	return asyncResponse, statusCode
}

func (b *MarketoBulkUploader) FetchFailedEvents(failedJobsStatus common.FetchFailedStatus) ([]byte, int) {
	transformUrl := config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	failedJobUrl := failedJobsStatus.FailedJobsURL
	parameters := failedJobsStatus.Parameters
	importId := gjson.GetBytes(parameters, "importId").String()
	csvHeaders := gjson.GetBytes(parameters, "metadata.csvHeader").String()
	payload := common.GenerateFailedPayload(b.destinationConfig, failedJobsStatus.ImportingList, importId, b.destName, csvHeaders)
	failedBodyBytes, statusCode := misc.HTTPCallWithRetryWithTimeout(transformUrl+failedJobUrl, payload, b.timeout)
	return failedBodyBytes, statusCode
}

func (b *MarketoBulkUploader) Upload(destination *backendconfig.DestinationT, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	resolveURL := func(base, relative string) string {
		// var logger logger.Logger
		baseURL, _ := url.Parse(base)
		// if err != nil {
		// 	logger.Fatal(err)
		// }
		relURL, _ := url.Parse(relative)
		// if err != nil {
		// 	logger.Fatal(err)
		// }
		destURL := baseURL.ResolveReference(relURL).String()
		return destURL
	}
	destinationID := destination.ID
	destinationUploadUrl := asyncDestStruct.URL
	url := resolveURL(b.TransformUrl, destinationUploadUrl)
	filePath := asyncDestStruct.FileName
	config := destination.Config
	destType := destination.DestinationDefinition.Name
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs

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
			panic("Unmarshalling a Single Line Failed")
		}
		input = append(input, tempJob)
	}
	payload, err := json.Marshal(common.AsyncUploadT{
		Input:    input,
		Config:   config,
		DestType: strings.ToLower(destType),
	})
	if err != nil {
		panic("BRT: JSON Marshal Failed " + err.Error())
	}

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	startTime := time.Now()
	payloadSizeStat.Observe(float64(len(payload)))
	pkgLogger.Debugf("[Async Destination Maanger] File Upload Started for Dest Type %v", destType)
	responseBody, statusCodeHTTP := misc.HTTPCallWithRetryWithTimeout(url, payload, b.timeout)
	pkgLogger.Debugf("[Async Destination Maanger] File Upload Finished for Dest Type %v", destType)
	uploadTimeStat.Since(startTime)
	var bodyBytes []byte
	var statusCode string
	if statusCodeHTTP != 200 {
		bodyBytes = []byte(`"error" : "HTTP Call to Transformer Returned Non 200"`)
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
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
			panic("Incorrect Response from Transformer: " + err.Error())
		}
		var parameters common.Parameters
		parameters.ImportId = responseStruct.ImportId
		url := responseStruct.PollUrl
		parameters.PollUrl = &url
		metaDataString, ok := responseStruct.Metadata["csvHeader"].(string)
		if !ok {
			parameters.MetaData = common.MetaDataT{CSVHeaders: ""}
		} else {
			parameters.MetaData = common.MetaDataT{CSVHeaders: metaDataString}
		}
		importParameters, err := json.Marshal(parameters)
		if err != nil {
			panic("Errored in Marshalling" + err.Error())
		}
		successfulJobIDs, failedJobIDsTrans := common.CleanUpData(responseStruct.Metadata, importingJobIDs)

		uploadResponse = common.AsyncUploadOutput{
			ImportingJobIDs:     successfulJobIDs,
			FailedJobIDs:        append(failedJobIDs, failedJobIDsTrans...),
			FailedReason:        `{"error":"Jobs flowed over the prescribed limit"}`,
			ImportingParameters: stdjson.RawMessage(importParameters),
			ImportingCount:      len(importingJobIDs),
			FailedCount:         len(failedJobIDs) + len(failedJobIDsTrans),
			DestinationID:       destinationID,
		}
	case "400":
		var responseStruct common.UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			panic("Incorrect Response from Transformer: " + err.Error())
		}
		eventsAbortedStat := stats.Default.NewTaggedStat("events_delivery_aborted", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": destType,
		})
		abortedJobIDs, failedJobIDsTrans := common.CleanUpData(responseStruct.Metadata, importingJobIDs)
		eventsAbortedStat.Count(len(abortedJobIDs))
		uploadResponse = common.AsyncUploadOutput{
			AbortJobIDs:   abortedJobIDs,
			FailedJobIDs:  append(failedJobIDs, failedJobIDsTrans...),
			FailedReason:  `{"error":"Jobs flowed over the prescribed limit"}`,
			AbortReason:   string(bodyBytes),
			AbortCount:    len(importingJobIDs),
			FailedCount:   len(failedJobIDs) + len(failedJobIDsTrans),
			DestinationID: destinationID,
		}
	default:
		uploadResponse = common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}

	}
	// else if statusCode == "400" {
	// 	var responseStruct common.UploadStruct
	// 	err := json.Unmarshal(bodyBytes, &responseStruct)
	// 	if err != nil {
	// 		panic("Incorrect Response from Transformer: " + err.Error())
	// 	}
	// 	eventsAbortedStat := stats.Default.NewTaggedStat("events_delivery_aborted", stats.CountType, map[string]string{
	// 		"module":   "batch_router",
	// 		"destType": destType,
	// 	})
	// 	abortedJobIDs, failedJobIDsTrans := common.CleanUpData(responseStruct.Metadata, importingJobIDs)
	// 	eventsAbortedStat.Count(len(abortedJobIDs))
	// 	uploadResponse = common.AsyncUploadOutput{
	// 		AbortJobIDs:   abortedJobIDs,
	// 		FailedJobIDs:  append(failedJobIDs, failedJobIDsTrans...),
	// 		FailedReason:  `{"error":"Jobs flowed over the prescribed limit"}`,
	// 		AbortReason:   string(bodyBytes),
	// 		AbortCount:    len(importingJobIDs),
	// 		FailedCount:   len(failedJobIDs) + len(failedJobIDsTrans),
	// 		DestinationID: destinationID,
	// 	}
	// } else {
	// 	uploadResponse = common.AsyncUploadOutput{
	// 		FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
	// 		FailedReason:  string(bodyBytes),
	// 		FailedCount:   len(failedJobIDs) + len(importingJobIDs),
	// 		DestinationID: destinationID,
	// 	}
	// }
	return uploadResponse

}

func (b *MarketoBulkUploader) RetrieveImportantKeys(metadata map[string]interface{}, retrieveKeys string) ([]int64, error) {
	retrievedKeys, err := misc.ConvertStringInterfaceToIntArray(metadata[retrieveKeys])
	return retrievedKeys, err
}
