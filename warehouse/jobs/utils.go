package jobs

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/pgnotifier"
)

func convertToPayloadStatusStructWithSingleStatus(payloads []AsyncJobPayloadT, status string, err error) map[string]AsyncJobStatus {
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

// convert to pgNotifier Payload and return the array of payloads
func getMessagePayloadsFromAsyncJobPayloads(asyncJobPayloads []AsyncJobPayloadT) ([]pgnotifier.JobPayload, error) {
	var messages []pgnotifier.JobPayload
	for _, job := range asyncJobPayloads {
		message, err := json.Marshal(job)
		if err != nil {
			return messages, err
		}
		messages = append(messages, message)
	}
	return messages, nil
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

func contains(sArray []string, s string) bool {
	for _, s1 := range sArray {
		if s1 == s {
			return true
		}
	}
	return false
}

func getAsyncStatusMapFromAsyncPayloads(payloads []AsyncJobPayloadT) map[string]AsyncJobStatus {
	asyncJobStatusMap := make(map[string]AsyncJobStatus)
	for _, payload := range payloads {
		asyncJobStatusMap[payload.Id] = AsyncJobStatus{
			Id:     payload.Id,
			Status: WhJobFailed,
		}
	}
	return asyncJobStatusMap
}

func updateStatusJobPayloadsFromPgNotifierResponse(r []pgnotifier.ResponseT, m map[string]AsyncJobStatus) error {
	var err error
	for _, resp := range r {
		var pgNotifierOutput PGNotifierOutput
		err = json.Unmarshal(resp.Output, &pgNotifierOutput)
		if err != nil {
			pkgLogger.Errorf("error unmarshalling pgnotifier payload to AsyncJobStatusMa for Id: %s", pgNotifierOutput.Id)
			continue
		}
		pkgLogger.Infof("Successfully unmarshalled pgnotifier payload to AsyncJobStatusMa for Id: %s", pgNotifierOutput.Id)
		if output, ok := m[pgNotifierOutput.Id]; ok {
			output.Status = resp.Status
			if resp.Error != "" {
				output.Error = fmt.Errorf(resp.Error)
			}
			m[pgNotifierOutput.Id] = output
		}
	}
	return err
}
