package bingads

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager"
)

func Upload(importingJobIDs []int64, destinationID string) asyncdestinationmanager.AsyncUploadOutput {
	return asyncdestinationmanager.AsyncUploadOutput{
		ImportingJobIDs:     importingJobIDs,
		FailedJobIDs:        nil,
		FailedReason:        ``,
		ImportingParameters: []byte("{}"),
		ImportingCount:      len(importingJobIDs),
		FailedCount:         0,
		DestinationID:       destinationID,
	}
}

func Poll() ([]byte, int) {
	resp := asyncdestinationmanager.AsyncStatusResponse{
		Success:        true,
		StatusCode:     200,
		HasFailed:      false,
		HasWarning:     false,
		FailedJobsURL:  "",
		WarningJobsURL: "",
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}

	return respBytes, 200
}
