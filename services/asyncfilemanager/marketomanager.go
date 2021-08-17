package asyncfilemanager

import (
	"encoding/json"
	"io/ioutil"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

type MarketoManager struct {
}

func (manager *MarketoManager) Upload(url string, filePath string, config map[string]interface{}, destType string, failedJobIDs []int64, importingJobIDs []int64, destinationID string) AsyncUploadOutput {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic("BRT: Read File Failed" + err.Error())
	}
	var uploadT AsyncUploadT
	uploadT.Data = string(data)
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
		uploadResponse = AsyncUploadOutput{
			ImportingJobIDs:     importingJobIDs,
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
	return gjson.Get(string(payload), "body.CSVRow").String()
}
