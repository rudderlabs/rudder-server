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

	var mockJob model.Job
	testData := []struct {
		name              string
		destDetail        model.Destination
		expectedStatus    model.JobStatus
		expectedCallCount int
	}{
		{
			name: "API type deleter",
			destDetail: model.Destination{
				Type: "api",
			},
			expectedStatus:    model.JobStatusComplete,
			expectedCallCount: 1,
		},
		{
			name: "batch type deleter",
			destDetail: model.Destination{
				Type: "batch",
			},
			expectedStatus:    model.JobStatusComplete,
			expectedCallCount: 1,
		},
		{
			name: "custom type deleter",
			destDetail: model.Destination{
				Type: "custom",
			},
			expectedStatus:    model.JobStatusComplete,
			expectedCallCount: 1,
		},
		{
			name: "random type deleter",
			destDetail: model.Destination{
				Type: "random",
			},
			expectedStatus:    model.JobStatusFailed,
			expectedCallCount: 0,
		},
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockdeleter := delete.NewMockdeleter(mockCtrl)

	deleter := delete.DeleteFacade{
		AM: mockdeleter,
		BM: mockdeleter,
		CM: mockdeleter,
	}

	ctx := context.Background()
	for _, tt := range testData {
		t.Run("TestFlow", func(t *testing.T) {
			mockdeleter.EXPECT().Delete(ctx, mockJob, tt.destDetail.Config, tt.destDetail.Name).Return(tt.expectedStatus).Times(tt.expectedCallCount)
			status := deleter.Delete(ctx, mockJob, tt.destDetail)
			require.Equal(t, tt.expectedStatus, status, "actual status different than expected")
		})
	}

}
