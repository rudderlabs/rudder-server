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
		name           string
		job            model.Job
		destDetail     model.Destination
		expectedStatus model.JobStatus
	}{
		{},
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDelete := delete.NewMockdeleteManager(mockCtrl)
	mockDelete.EXPECT().GetSupportedDestination().Return([]string{}).Times(1)

	r := delete.DeleteRouter{
		Managers: {},
	}

	for _, tt := range testData {

		status := r.Delete(ctx, tt.job, tt.destDetail)
		require.Equal(t, tt.expectedStatus, status, "actual status different than expected")
	}

}
