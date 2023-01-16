package delete_test

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/stretchr/testify/require"
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
			expectedStatus:                    model.JobStatusComplete,
			md1CallCount:                      1,
			getSupportedDestinationsCallCount: 1,
		},
		{
			name: "destination doesn't exists",
			destDetail: model.Destination{
				Name: "d5",
			},
			expectedStatus: model.JobStatusAborted,
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
		md1.EXPECT().Delete(ctx, tt.job, tt.destDetail).Return(model.JobStatusComplete).Times(tt.md1CallCount)
		status := r.Delete(ctx, tt.job, tt.destDetail)
		require.Equal(t, tt.expectedStatus, status, "actual status different than expected")
	}
}
