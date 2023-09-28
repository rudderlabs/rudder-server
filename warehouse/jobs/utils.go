package jobs

import (
	"encoding/json"
)

func convertToPayloadStatusStructWithSingleStatus(payloads []AsyncJobPayload, status string, err error) map[string]AsyncJobStatus {
	asyncJobStatusMap := make(map[string]AsyncJobStatus)
	for _, payload := range payloads {
		asyncJobStatusMap[payload.Id] = AsyncJobStatus{
			Id:     payload.Id,
			Status: status,
			Error:  err,
		}
	}
	return asyncJobStatusMap
}

// convert to notifier Payload and return the array of payloads
func getMessagePayloadsFromAsyncJobPayloads(asyncJobPayloads []AsyncJobPayload) ([]json.RawMessage, error) {
	var messages []json.RawMessage
	for _, job := range asyncJobPayloads {
		message, err := json.Marshal(job)
		if err != nil {
			return messages, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func getAsyncStatusMapFromAsyncPayloads(payloads []AsyncJobPayload) map[string]AsyncJobStatus {
	asyncJobStatusMap := make(map[string]AsyncJobStatus)
	for _, payload := range payloads {
		asyncJobStatusMap[payload.Id] = AsyncJobStatus{
			Id:     payload.Id,
			Status: WhJobFailed,
		}
	}
	return asyncJobStatusMap
}
