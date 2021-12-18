package service_test

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
	"github.com/stretchr/testify/require"
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
		deleteJobStatus             model.JobStatus
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
			expectedStatus:  model.JobStatusRunning,
			deleteJobStatus: model.JobStatusComplete,
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
			mockAPIClient.EXPECT().UpdateStatus(ctx, tt.deleteJobStatus, jobID).Return(tt.updateStatusErrAfter).Times(tt.updateStatusAfterCallCount)

			mockDeleter := service.NewMockdeleter(mockCtrl)
			mockDeleter.EXPECT().Delete(ctx, tt.job, tt.dest).Return(tt.deleteJobStatus).Times(tt.deleteJobCallCount)

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
