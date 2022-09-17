package jobs

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/services/pgnotifier"
)

func convertToPayloadStatusStructWithSingleStatus(payloads []AsyncJobPayloadT, status string, err error) []AsyncJobsStatusMap {
	var asyncJobsStatusMap []AsyncJobsStatusMap
	for _, payload := range payloads {
		asyncjobmap := AsyncJobsStatusMap{
			Id:     payload.Id,
			Status: status,
			Error:  err,
		}
		asyncJobsStatusMap = append(asyncJobsStatusMap, asyncjobmap)
	}
	return asyncJobsStatusMap
}

//convert to pgNotifier Payload and return the array of payloads
func getMessagePayloadsFromAsyncJobPayloads(asyncjobs []AsyncJobPayloadT) ([]pgnotifier.JobPayload, error) {
	var messages []pgnotifier.JobPayload
	for _, job := range asyncjobs {
		message, err := json.Marshal(job)
		if err != nil {
			return messages, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func getSinglePayloadFromBatchPayloadByTableName(asyncJobs []AsyncJobPayloadT, tableName string) int {
	for ind, asyncjob := range asyncJobs {
		if asyncjob.TableName == tableName {
			return ind
		}
	}
	return -1
}

func validatePayload(payload StartJobReqPayload) bool {
	if payload.SourceID == "" || payload.JobRunID == "" || payload.TaskRunID == "" || payload.DestinationID == "" {
		return false
	}
	return true
}

func skipTable(th string) bool {
	if th == "RUDDER_DISCARDS" || th == "rudder_discards" {
		return true
	}
	return false
}
