package common

import (
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

type InvalidManager struct{}

func (f *InvalidManager) Transform(job *jobsdb.JobT) (string, error) {
	return "", errors.New("invalid job")
}

func (f *InvalidManager) Upload(asyncDestStruct *AsyncDestinationStruct) AsyncUploadOutput {
	abortedJobIDs := append(asyncDestStruct.ImportingJobIDs, asyncDestStruct.FailedJobIDs...)
	return AsyncUploadOutput{
		AbortJobIDs:   abortedJobIDs,
		AbortReason:   `{"error":"` + fmt.Sprintf("%s could not be initialized. Please check account settings.", asyncDestStruct.Destination.Name) + `"}`,
		DestinationID: asyncDestStruct.Destination.ID,
		AbortCount:    len(abortedJobIDs),
	}
}

func (f *InvalidManager) Poll(_ AsyncPoll) PollStatusResponse {
	return PollStatusResponse{
		StatusCode: 400,
	}
}

func (f *InvalidManager) GetUploadStats(_ GetUploadStatsInput) GetUploadStatsResponse {
	return GetUploadStatsResponse{
		StatusCode: 400,
	}
}
