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

type MARKETO_BULK_UPLOAD struct {
	destName string
}

func Manager() *MARKETO_BULK_UPLOAD {
	marketobulkupload := &MARKETO_BULK_UPLOAD{destName: "MARKETO_BULK_UPLOAD"}
	return marketobulkupload
}

var (
	pkgLogger logger.Logger
)

func (b *MARKETO_BULK_UPLOAD) Poll(url string, payload []byte, timeout time.Duration) ([]byte, int) {
	bodyBytes, statusCode := misc.HTTPCallWithRetryWithTimeout(url, payload, timeout)
	// resp := common.AsyncStatusResponse{
	// 	Success:        true,
	// 	StatusCode:     200,
	// 	HasFailed:      false,
	// 	HasWarning:     false,
	// 	FailedJobsURL:  "",
	// 	WarningJobsURL: "",
	// }

	respBytes, err := stdjson.Marshal(bodyBytes)
	if err != nil {
		panic(err)
	}

	return respBytes, statusCode
}

func (b *MARKETO_BULK_UPLOAD) Upload(destination *backendconfig.DestinationT, asyncDestStruct map[string]*common.AsyncDestinationStruct) common.AsyncUploadOutput {
	resolveURL := func(base, relative string) string {
		var logger logger.Logger
		baseURL, err := url.Parse(base)
		if err != nil {
			logger.Fatal(err)
		}
		relURL, err := url.Parse(relative)
		if err != nil {
			logger.Fatal(err)
		}
		destURL := baseURL.ResolveReference(relURL).String()
		return destURL
	}
	transformUrl := config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	destinationID := destination.ID
	destinationUploadUrl := asyncDestStruct[destinationID].URL
	url := resolveURL(transformUrl, destinationUploadUrl)
	filePath := asyncDestStruct[destinationID].FileName
	config := destination.Config
	destType := destination.DestinationDefinition.Name
	failedJobIDs := asyncDestStruct[destinationID].FailedJobIDs
	importingJobIDs := asyncDestStruct[destinationID].ImportingJobIDs

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
	var uploadT common.AsyncUploadT
	uploadT.Input = input
	uploadT.Config = config
	uploadT.DestType = strings.ToLower(destType)
	payload, err := json.Marshal(uploadT)
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
	payloadSizeStat.SendTiming(time.Millisecond * time.Duration(len(payload)))
	pkgLogger.Debugf("[Async Destination Maanger] File Upload Started for Dest Type %v", destType)
	responseBody, statusCodeHTTP := misc.HTTPCallWithRetryWithTimeout(url, payload, common.HTTPTimeout)
	pkgLogger.Debugf("[Async Destination Maanger] File Upload Finished for Dest Type %v", destType)
	uploadTimeStat.Since(startTime)
	var bodyBytes []byte
	var httpFailed bool
	var statusCode string
	if statusCodeHTTP != 200 {
		bodyBytes = []byte(`"error" : "HTTP Call to Transformer Returned Non 200"`)
		httpFailed = true
	} else {
		bodyBytes = responseBody
		statusCode = gjson.GetBytes(bodyBytes, "statusCode").String()
	}

	var uploadResponse common.AsyncUploadOutput
	if httpFailed {
		uploadResponse = common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	} else if statusCode == "200" {
		var responseStruct common.UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			panic("Incorrect Response from Transformer: " + err.Error())
		}
		var parameters common.Parameters
		parameters.ImportId = responseStruct.ImportId
		parameters.PollUrl = responseStruct.PollUrl
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
	} else if statusCode == "400" {
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
	} else {
		uploadResponse = common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	return uploadResponse

}

// func (b *MARKETO_BULK_UPLOAD) Upload(url, filePath string, config map[string]interface{}, destType string, failedJobIDs, importingJobIDs []int64, destinationID string) common.AsyncUploadOutput {
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		panic("BRT: Read File Failed" + err.Error())
// 	}
// 	defer file.Close()
// 	var input []common.AsyncJob
// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		var tempJob common.AsyncJob
// 		jobBytes := scanner.Bytes()
// 		err := json.Unmarshal(jobBytes, &tempJob)
// 		if err != nil {
// 			panic("Unmarshalling a Single Line Failed")
// 		}
// 		input = append(input, tempJob)
// 	}
// 	var uploadT common.AsyncUploadT
// 	uploadT.Input = input
// 	uploadT.Config = config
// 	uploadT.DestType = strings.ToLower(destType)
// 	payload, err := json.Marshal(uploadT)
// 	if err != nil {
// 		panic("BRT: JSON Marshal Failed " + err.Error())
// 	}

// 	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
// 		"module":   "batch_router",
// 		"destType": destType,
// 	})

// 	payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.TimerType, map[string]string{
// 		"module":   "batch_router",
// 		"destType": destType,
// 	})

// 	startTime := time.Now()
// 	payloadSizeStat.SendTiming(time.Millisecond * time.Duration(len(payload)))
// 	pkgLogger.Debugf("[Async Destination Maanger] File Upload Started for Dest Type %v", destType)
// 	responseBody, statusCodeHTTP := misc.HTTPCallWithRetryWithTimeout(url, payload, common.HTTPTimeout)
// 	pkgLogger.Debugf("[Async Destination Maanger] File Upload Finished for Dest Type %v", destType)
// 	uploadTimeStat.Since(startTime)
// 	var bodyBytes []byte
// 	var httpFailed bool
// 	var statusCode string
// 	if statusCodeHTTP != 200 {
// 		bodyBytes = []byte(`"error" : "HTTP Call to Transformer Returned Non 200"`)
// 		httpFailed = true
// 	} else {
// 		bodyBytes = responseBody
// 		statusCode = gjson.GetBytes(bodyBytes, "statusCode").String()
// 	}

// 	var uploadResponse common.AsyncUploadOutput
// 	if httpFailed {
// 		uploadResponse = common.AsyncUploadOutput{
// 			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
// 			FailedReason:  string(bodyBytes),
// 			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
// 			DestinationID: destinationID,
// 		}
// 	} else if statusCode == "200" {
// 		var responseStruct common.UploadStruct
// 		err := json.Unmarshal(bodyBytes, &responseStruct)
// 		if err != nil {
// 			panic("Incorrect Response from Transformer: " + err.Error())
// 		}
// 		var parameters common.Parameters
// 		parameters.ImportId = responseStruct.ImportId
// 		parameters.PollUrl = responseStruct.PollUrl
// 		metaDataString, ok := responseStruct.Metadata["csvHeader"].(string)
// 		if !ok {
// 			parameters.MetaData = common.MetaDataT{CSVHeaders: ""}
// 		} else {
// 			parameters.MetaData = common.MetaDataT{CSVHeaders: metaDataString}
// 		}
// 		importParameters, err := json.Marshal(parameters)
// 		if err != nil {
// 			panic("Errored in Marshalling" + err.Error())
// 		}
// 		successfulJobIDs, failedJobIDsTrans := common.CleanUpData(responseStruct.Metadata, importingJobIDs)

// 		uploadResponse = common.AsyncUploadOutput{
// 			ImportingJobIDs:     successfulJobIDs,
// 			FailedJobIDs:        append(failedJobIDs, failedJobIDsTrans...),
// 			FailedReason:        `{"error":"Jobs flowed over the prescribed limit"}`,
// 			ImportingParameters: stdjson.RawMessage(importParameters),
// 			ImportingCount:      len(importingJobIDs),
// 			FailedCount:         len(failedJobIDs) + len(failedJobIDsTrans),
// 			DestinationID:       destinationID,
// 		}
// 	} else if statusCode == "400" {
// 		var responseStruct common.UploadStruct
// 		err := json.Unmarshal(bodyBytes, &responseStruct)
// 		if err != nil {
// 			panic("Incorrect Response from Transformer: " + err.Error())
// 		}
// 		eventsAbortedStat := stats.Default.NewTaggedStat("events_delivery_aborted", stats.CountType, map[string]string{
// 			"module":   "batch_router",
// 			"destType": destType,
// 		})
// 		abortedJobIDs, failedJobIDsTrans := common.CleanUpData(responseStruct.Metadata, importingJobIDs)
// 		eventsAbortedStat.Count(len(abortedJobIDs))
// 		uploadResponse = common.AsyncUploadOutput{
// 			AbortJobIDs:   abortedJobIDs,
// 			FailedJobIDs:  append(failedJobIDs, failedJobIDsTrans...),
// 			FailedReason:  `{"error":"Jobs flowed over the prescribed limit"}`,
// 			AbortReason:   string(bodyBytes),
// 			AbortCount:    len(importingJobIDs),
// 			FailedCount:   len(failedJobIDs) + len(failedJobIDsTrans),
// 			DestinationID: destinationID,
// 		}
// 	} else {
// 		uploadResponse = common.AsyncUploadOutput{
// 			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
// 			FailedReason:  string(bodyBytes),
// 			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
// 			DestinationID: destinationID,
// 		}
// 	}
// 	return uploadResponse
// }
