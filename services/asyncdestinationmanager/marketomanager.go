package asyncdestinationmanager

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

type MarketoManager struct {
}

type UploadStruct struct {
	ImportId string                 `json:"importId"`
	PollUrl  string                 `json:"pollURL"`
	Metadata map[string]interface{} `json:"metadata"`
}

func init() {

}

type Parameters struct {
	ImportId string    `json:"importId"`
	PollUrl  string    `json:"pollURL"`
	MetaData MetaDataT `json:"metadata"`
}

func CleanUpData(keyMap map[string]interface{}, importingJobIDs []int64) ([]int64, []int64) {
	if keyMap == nil {
		return []int64{}, importingJobIDs
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

func (manager *MarketoManager) Upload(url string, filePath string, config map[string]interface{}, destType string, failedJobIDs []int64, importingJobIDs []int64, destinationID string) AsyncUploadOutput {
	file, err := os.Open(filePath)
	if err != nil {
		panic("BRT: Read File Failed" + err.Error())
	}
	defer file.Close()
	var input []AsyncJob
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tempJob AsyncJob
		jobBytes := scanner.Bytes()
		err := json.Unmarshal(jobBytes, &tempJob)
		if err != nil {
			panic("Unmarshalling a Single Line Failed")
		}
		input = append(input, tempJob)
	}
	var uploadT AsyncUploadT
	uploadT.Input = input
	uploadT.Config = config
	uploadT.DestType = strings.ToLower(destType)
	payload, err := json.Marshal(uploadT)
	uploadTimeStat := stats.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
		"url":      url,
	})
	if err != nil {
		panic("BRT: JSON Marshal Failed " + err.Error())
	}

	payloadSizeStat := stats.NewTaggedStat("payload_size", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
		"url":      url,
	})
	uploadTimeStat.Start()
	payloadSizeStat.SendTiming(time.Millisecond * time.Duration(len(payload)))
	responseBody, statusCodeHTTP := misc.HTTPCallWithRetry(url, payload)
	uploadTimeStat.End()
	fmt.Println("***********************************************")
	fmt.Println("uploadURL  : ", url)
	fmt.Println("Payload : ", string(payload))
	fmt.Println("Response : ", string(responseBody))
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
	eventsDeliveredStat := stats.NewTaggedStat("events_delivery_success", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
		"url":      url,
	})
	eventsFailedStat := stats.NewTaggedStat("events_delivery_failed", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
		"url":      url,
	})
	eventsAbortedStat := stats.NewTaggedStat("events_delivery_aborted", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
		"url":      url,
	})

	var uploadResponse AsyncUploadOutput
	if httpFailed {
		uploadResponse = AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	} else if statusCode == "200" {
		var responseStruct UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			panic("Incorrect Response from Transformer: " + err.Error())
		}
		var parameters Parameters
		parameters.ImportId = responseStruct.ImportId
		parameters.PollUrl = responseStruct.PollUrl
		metaDataString, ok := responseStruct.Metadata["csvHeader"].(string)
		if !ok {
			parameters.MetaData = MetaDataT{CSVHeaders: ""}
		} else {
			parameters.MetaData = MetaDataT{CSVHeaders: metaDataString}
		}
		importParameters, err := json.Marshal(parameters)
		if err != nil {
			panic("Errored in Marshalling" + err.Error())
		}
		succesfulJobIDs, failedJobIDsTrans := CleanUpData(responseStruct.Metadata, importingJobIDs)
		eventsFailedStat.Count(len(failedJobIDsTrans))
		eventsDeliveredStat.Count(len(succesfulJobIDs))

		uploadResponse = AsyncUploadOutput{
			ImportingJobIDs:     succesfulJobIDs,
			FailedJobIDs:        append(failedJobIDs, failedJobIDsTrans...),
			FailedReason:        `{"error":"Jobs flowed over the prescribed limit"}`,
			ImportingParameters: json.RawMessage(importParameters),
			importingCount:      len(importingJobIDs),
			FailedCount:         len(failedJobIDs) + len(failedJobIDsTrans),
			DestinationID:       destinationID,
		}
	} else if statusCode == "400" {
		var responseStruct UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			panic("Incorrect Response from Transformer: " + err.Error())
		}
		succesfulJobIDs, failedJobIDsTrans := CleanUpData(responseStruct.Metadata, importingJobIDs)
		eventsFailedStat.Count(len(failedJobIDsTrans))
		eventsAbortedStat.Count(len(succesfulJobIDs))
		uploadResponse = AsyncUploadOutput{
			AbortJobIDs:   succesfulJobIDs,
			FailedJobIDs:  append(failedJobIDs, failedJobIDsTrans...),
			FailedReason:  `{"error":"Jobs flowed over the prescribed limit"}`,
			AbortReason:   string(bodyBytes),
			AbortCount:    len(importingJobIDs),
			FailedCount:   len(failedJobIDs) + len(failedJobIDsTrans),
			DestinationID: destinationID,
		}
	} else {
		uploadResponse = AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}
	return uploadResponse
}
func (manager *MarketoManager) GetTransformedData(payload json.RawMessage) string {
	return gjson.Get(string(payload), "body.JSON").String()
}

func (manager *MarketoManager) GetMarshalledData(payload string, jobID int64) string {
	var job AsyncJob
	err := json.Unmarshal([]byte(payload), &job.Message)
	if err != nil {
		panic("Unmarshalling Transformer Response Failed")
	}
	job.Metadata = make(map[string]interface{})
	job.Metadata["job_id"] = jobID
	responsePayload, err := json.Marshal(job)
	if err != nil {
		panic("Marshalling Response Payload Failed")
	}
	return string(responsePayload)
}

func (manager *MarketoManager) GenerateFailedPayload(config map[string]interface{}, jobs []*jobsdb.JobT, importID string, destType string, csvHeaders string) []byte {
	var failedPayloadT AsyncFailedPayload
	failedPayloadT.Input = make([]map[string]interface{}, len(jobs))
	index := 0
	failedPayloadT.Config = config
	for _, job := range jobs {
		failedPayloadT.Input[index] = make(map[string]interface{})
		var message map[string]interface{}
		metadata := make(map[string]interface{})
		err := json.Unmarshal([]byte(manager.GetTransformedData(job.EventPayload)), &message)
		if err != nil {
			panic("Unmarshalling Transformer Data to JSON Failed")
		}
		metadata["job_id"] = job.JobID
		failedPayloadT.Input[index]["message"] = message
		failedPayloadT.Input[index]["metadata"] = metadata
		index++
	}
	failedPayloadT.DestType = strings.ToLower(destType)
	failedPayloadT.ImportId = importID
	failedPayloadT.MetaData = MetaDataT{CSVHeaders: csvHeaders}
	payload, err := json.Marshal(failedPayloadT)
	if err != nil {
		panic("JSON Marshal Failed" + err.Error())
	}
	return payload
}
