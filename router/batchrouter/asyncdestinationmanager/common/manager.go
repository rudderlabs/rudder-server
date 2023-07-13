package common

type InvalidManager struct{}

func (f *InvalidManager) Upload(asyncDestStruct *AsyncDestinationStruct) AsyncUploadOutput {
	abortedJobIDs := append(asyncDestStruct.ImportingJobIDs, asyncDestStruct.FailedJobIDs...)
	return AsyncUploadOutput{
		AbortJobIDs:   abortedJobIDs,
		AbortReason:   "Manager is not initialized",
		DestinationID: asyncDestStruct.Destination.ID,
		AbortCount:    len(abortedJobIDs),
	}
}

func (f *InvalidManager) Poll(pollInput AsyncPoll) PollStatusResponse {
	return PollStatusResponse{
		StatusCode: 400,
	}
}

func (f *InvalidManager) GetUploadStats(UploadStatsInput GetUploadStatsInput) GetUploadStatsResponse {
	return GetUploadStatsResponse{
		StatusCode: 400,
	}
}
