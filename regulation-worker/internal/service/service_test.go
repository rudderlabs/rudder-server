package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
)

func TestJobSvc(t *testing.T) {
	config := map[string]interface{}{
		"bucketName":  "malani-deletefeature-testdata",
		"prefix":      "regulation",
		"accessKeyID": "xyz",
		"accessKey":   "pqr",
		"enableSSE":   false,
	}
	tests := []struct {
		name                        string
		job                         model.Job
		getErr                      error
		expectedStatus              model.JobStatus
		updateStatusErrBefore       error
		dest                        model.Destination
		deleteJobStatusByDeleter    model.JobStatus
		finalDeleteJobStatus        model.JobStatus
		deleteJobErr                error
		updateStatusErrAfter        error
		expectedFinalErr            error
		getJobCallCount             int
		updateStatusBeforeCallCount int
		updateStatusAfterCallCount  int
		deleteJobCallCount          int
		getDestDetailsCount         int
	}{
		{
			name: "regulation worker returns without err",
			job: model.Job{
				ID:          1,
				WorkspaceID: "1234",
			},
			expectedStatus:           model.JobStatus{Status: model.JobStatusRunning},
			deleteJobStatusByDeleter: model.JobStatus{Status: model.JobStatusComplete},
			finalDeleteJobStatus:     model.JobStatus{Status: model.JobStatusComplete},
			dest: model.Destination{
				Config:        config,
				DestinationID: "1111",
				Name:          "S3",
			},
			getJobCallCount:             1,
			updateStatusBeforeCallCount: 1,
			updateStatusAfterCallCount:  1,
			deleteJobCallCount:          1,
			getDestDetailsCount:         1,
		},
		{
			name:                        "regulation worker returns with get job err",
			getErr:                      model.ErrNoRunnableJob,
			expectedFinalErr:            model.ErrNoRunnableJob,
			getJobCallCount:             1,
			updateStatusBeforeCallCount: 0,
			updateStatusAfterCallCount:  0,
			deleteJobCallCount:          0,
			getDestDetailsCount:         0,
		},
		{
			name: "regulation worker returns aborted status if failedAttempts >=5",
			job: model.Job{
				ID:             1,
				WorkspaceID:    "1234",
				FailedAttempts: 4,
			},
			getJobCallCount:             1,
			updateStatusBeforeCallCount: 1,
			updateStatusAfterCallCount:  1,
			deleteJobCallCount:          1,
			getDestDetailsCount:         1,
			expectedStatus:              model.JobStatus{Status: model.JobStatusRunning},
			deleteJobStatusByDeleter:    model.JobStatus{Status: model.JobStatusFailed},
			finalDeleteJobStatus:        model.JobStatus{Status: model.JobStatusAborted},
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			mockAPIClient := service.NewMockAPIClient(mockCtrl)
			mockAPIClient.EXPECT().Get(ctx).Return(tt.job, tt.getErr).Times(tt.getJobCallCount)

			jobID := tt.job.ID
			mockAPIClient.EXPECT().UpdateStatus(ctx, tt.expectedStatus, jobID).Return(tt.updateStatusErrBefore).Times(tt.updateStatusBeforeCallCount)
			mockAPIClient.EXPECT().UpdateStatus(ctx, tt.finalDeleteJobStatus, jobID).Return(tt.updateStatusErrAfter).Times(tt.updateStatusAfterCallCount)

			mockDeleter := service.NewMockdeleter(mockCtrl)
			mockDeleter.EXPECT().Delete(ctx, tt.job, tt.dest).Return(tt.deleteJobStatusByDeleter).Times(tt.deleteJobCallCount)

			mockDestDetail := service.NewMockdestDetail(mockCtrl)
			mockDestDetail.EXPECT().GetDestDetails(tt.job.DestinationID).Return(tt.dest, nil).Times(tt.getDestDetailsCount)
			svc := service.JobSvc{
				API:        mockAPIClient,
				Deleter:    mockDeleter,
				DestDetail: mockDestDetail,
			}
			err := svc.JobSvc(ctx)
			require.Equal(t, tt.expectedFinalErr, err, "actual error different than expected")
		})
	}
}
