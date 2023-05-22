package bingads

import (
	stdjson "encoding/json"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type BING_ADS struct {
	destName string
}

func (b *BING_ADS) Upload(importingJobIDs []int64, destinationID string) common.AsyncUploadOutput {
	return common.AsyncUploadOutput{
		ImportingJobIDs:     importingJobIDs,
		FailedJobIDs:        nil,
		FailedReason:        ``,
		ImportingParameters: []byte("{}"),
		ImportingCount:      len(importingJobIDs),
		FailedCount:         0,
		DestinationID:       destinationID,
	}
}

func (b *BING_ADS) Poll() ([]byte, int) {
	resp := common.AsyncStatusResponse{
		Success:        true,
		StatusCode:     200,
		HasFailed:      false,
		HasWarning:     false,
		FailedJobsURL:  "",
		WarningJobsURL: "",
	}

	respBytes, err := stdjson.Marshal(resp)
	if err != nil {
		panic(err)
	}

	return respBytes, 200
}

func Manager() *BING_ADS {
	bingads := &BING_ADS{destName: "BING_ADS"}
	return bingads
}
