package asyncdestinationmanager

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

type MarketoManager struct {
}

type UploadStruct struct {
	ImportId int                    `json:"importId"`
	PollUrl  string                 `json:"pollUrl"`
	Metadata map[string]interface{} `json:"metadata"`
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
	uploadT.DestType = destType
	payload, err := json.Marshal(uploadT)
	if err != nil {
		panic("BRT: JSON Marshal Failed " + err.Error())
	}
	responseBody, statusCodeHTTP := misc.HTTPCallWithRetry(url, payload)
	var bodyBytes []byte
	var httpFailed bool
	var statusCode string
	if statusCodeHTTP != 200 {
		bodyBytes = []byte("HTTP Call to Transformer Returned Non 200")
		httpFailed = true
	} else {
		bodyBytes = responseBody
		statusCode = gjson.GetBytes(bodyBytes, "statusCode").String()
	}
	var uploadResponse AsyncUploadOutput

	if httpFailed {
		uploadResponse = AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	} else if statusCode == "" {
		var responseStruct UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			panic("Incorrect Response from Transformer: " + err.Error())
		}
		successJobsInterface, ok := responseStruct.Metadata["successfulJobs"].([]interface{})
		var succesfulJobIDs, failedJobIDs []int64
		if ok {
			succesfulJobIDs, err = misc.ConvertStringInterfaceToIntArray(successJobsInterface)
			if err != nil {
				failedJobIDs = importingJobIDs
			}
		}
		failedJobsInterface, ok := responseStruct.Metadata["unsuccessfulJob"].([]interface{})
		if ok {
			failedJobIDs, err = misc.ConvertStringInterfaceToIntArray(failedJobsInterface)
			if err != nil {
				failedJobIDs = importingJobIDs
			}
		}
		uploadResponse = AsyncUploadOutput{
			ImportingJobIDs:     succesfulJobIDs,
			FailedJobIDs:        failedJobIDs,
			FailedReason:        `{"error":"Jobs flowed over the prescribed limit"}`,
			ImportingParameters: json.RawMessage(bodyBytes),
			importingCount:      len(importingJobIDs),
			FailedCount:         len(failedJobIDs),
			DestinationID:       destinationID,
		}
	} else if statusCode == "400" {
		uploadResponse = AsyncUploadOutput{
			AbortJobIDs:   importingJobIDs,
			FailedJobIDs:  failedJobIDs,
			FailedReason:  `{"error":"Jobs flowed over the prescribed limit"}`,
			AbortReason:   string(bodyBytes),
			AbortCount:    len(importingJobIDs),
			FailedCount:   len(failedJobIDs),
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

func (manager *MarketoManager) GenerateFailedPayload(config map[string]interface{}, jobs []*jobsdb.JobT, importID string, destType string) []byte {
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
	}
	failedPayloadT.DestType = destType
	failedPayloadT.ImportId = importID
	payload, err := json.Marshal(failedPayloadT)
	if err != nil {
		panic("JSON Marshal Failed" + err.Error())
	}
	return payload
}
