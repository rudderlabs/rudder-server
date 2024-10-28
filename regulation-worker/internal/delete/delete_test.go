package delete_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

func TestDelete(t *testing.T) {
	ctx := context.Background()
	testData := []struct {
		name                              string
		job                               model.Job
		destDetail                        model.Destination
		expectedStatus                    model.JobStatus
		md1CallCount                      int
		getSupportedDestinationsCallCount int
	}{
		{
			name: "destination exists",
			destDetail: model.Destination{
				Name: "d1",
			},
			expectedStatus:                    model.JobStatus{Status: model.JobStatusComplete},
			md1CallCount:                      1,
			getSupportedDestinationsCallCount: 1,
		},
		{
			name: "destination doesn't exists",
			destDetail: model.Destination{
				Name: "d5",
			},
			expectedStatus: model.JobStatus{Status: model.JobStatusAborted, Error: model.ErrDestNotSupported},
		},
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDelete := delete.NewMockdeleteManager(mockCtrl)
	md1 := mockDelete
	md2 := mockDelete

	r := delete.NewRouter(md1, md2)
	for _, tt := range testData {
		md1.EXPECT().GetSupportedDestinations().Return([]string{"d1", "d2"}).Times(tt.getSupportedDestinationsCallCount)
		md2.EXPECT().GetSupportedDestinations().Return([]string{"d3", "d4"}).Times(tt.getSupportedDestinationsCallCount)
		md1.EXPECT().Delete(ctx, tt.job, tt.destDetail).Return(tt.expectedStatus).Times(tt.md1CallCount)
		status := r.Delete(ctx, tt.job, tt.destDetail)
		require.Equal(t, tt.expectedStatus, status, "actual status different than expected")
	}
}
