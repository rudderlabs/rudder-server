package jobs

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/pgnotifier"
)

func convertToPayloadStatusStructWithSingleStatus(payloads []AsyncJobPayloadT, status string, err error) map[string]AsyncJobsStatusMap {
	asyncJobsStatusMap := make(map[string]AsyncJobsStatusMap)
	for _, payload := range payloads {
		asyncjobmap := AsyncJobsStatusMap{
			Id:     payload.Id,
			Status: status,
			Error:  err,
		}
		asyncJobsStatusMap[payload.Id] = asyncjobmap
	}
	return asyncJobsStatusMap
}

// convert to pgNotifier Payload and return the array of payloads
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

func getAsyncStatusMapFromAsyncPayloads(payloads []AsyncJobPayloadT) map[string]AsyncJobsStatusMap {
	asyncjobsstatusmap := make(map[string]AsyncJobsStatusMap)
	for _, payload := range payloads {
		asyncstatus := AsyncJobsStatusMap{
			Id:     payload.Id,
			Status: WhJobFailed,
		}
		asyncjobsstatusmap[payload.Id] = asyncstatus
	}
	return asyncjobsstatusmap
}

func updateStatusJobPayloadsFromPgnotifierResponse(r []pgnotifier.ResponseT, m map[string]AsyncJobsStatusMap) error {
	var err error
	for _, resp := range r {
		var pgoutput PGNotifierOutput
		err = json.Unmarshal(resp.Output, &pgoutput)
		if err != nil {
			pkgLogger.Errorf("error unmarshaling pgnotifier payload to AsyncJobStatusMa for Id: %s", pgoutput.Id)
			continue
		}
		pkgLogger.Infof("Successfully unmarshaled pgnotifier payload to AsyncJobStatusMa for Id: %s", pgoutput.Id)
		if output, ok := m[pgoutput.Id]; ok {
			output.Status = resp.Status
			if resp.Error != "" {
				output.Error = fmt.Errorf(resp.Error)
			}
			m[pgoutput.Id] = output
		}
	}
	return err
}
