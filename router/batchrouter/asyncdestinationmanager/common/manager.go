package common

import (
	"context"
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

type InvalidManager struct {
	Error error
}

func (*InvalidManager) Transform(job *jobsdb.JobT) (string, error) {
	return "", errors.New("invalid job")
}

func (*InvalidManager) Upload(_ context.Context, asyncDestStruct *AsyncDestinationStruct) AsyncUploadOutput {
	abortedJobIDs := append(asyncDestStruct.ImportingJobIDs, asyncDestStruct.FailedJobIDs...)
	return AsyncUploadOutput{
		AbortJobIDs: abortedJobIDs,
		// AbortReason:   `{"error":"BingAds could not be initialized. Please check account settings."}`,
		AbortReason:   `{"error":"` + fmt.Sprintf("%s could not be initialized. Please check account settings.", asyncDestStruct.Destination.Name) + `"}`,
		DestinationID: asyncDestStruct.Destination.ID,
		AbortCount:    len(abortedJobIDs),
	}
}

func (*InvalidManager) Poll(_ context.Context, _ AsyncPoll) PollStatusResponse {
	return PollStatusResponse{
		StatusCode: 400,
	}
}

func (*InvalidManager) GetUploadStats(_ GetUploadStatsInput) GetUploadStatsResponse {
	return GetUploadStatsResponse{
		StatusCode: 400,
	}
}
