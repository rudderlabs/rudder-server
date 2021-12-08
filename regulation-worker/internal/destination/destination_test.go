package destination_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	destination "github.com/rudderlabs/rudder-server/regulation-worker/internal/destination"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/stretchr/testify/require"
)

func TestGetDestDetails(t *testing.T) {

	config := map[string]interface{}{
		"bucketName":  "malani-deletefeature-testdata",
		"prefix":      "regulation",
		"accessKeyID": "xyz",
		"accessKey":   "pqr",
		"enableSSE":   false,
	}

	testConfig := backendconfig.ConfigT{
		WorkspaceID: "1234",
		Sources: []backendconfig.SourceT{
			{
				Destinations: []backendconfig.DestinationT{
					{
						ID:     "1111",
						Config: config,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							Name: "S3",
						},
					},
					{
						ID: "1112",
					},
				},
			},
			{
				Destinations: []backendconfig.DestinationT{
					{
						ID: "1113",
					},
					{
						ID: "1114",
					},
				},
			},
			{
				Destinations: []backendconfig.DestinationT{
					{
						ID: "1115",
					},
					{
						ID: "1116",
					},
				},
			},
		},
	}
	testDestID := "1111"
	testWorkspaceID := "1234"
	expDest := model.Destination{
		Config:        config,
		DestinationID: "1111",
		Name:          "S3",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDestMiddleware := destination.NewMockdestinationMiddleware(mockCtrl)
	mockDestMiddleware.EXPECT().Get().Return(testConfig, true).Times(1)

	dest := destination.DestMiddleware{
		Dest: mockDestMiddleware,
	}

	destDetail, err := dest.GetDestDetails(testDestID, testWorkspaceID)

	require.NoError(t, err, "expected no err")
	require.Equal(t, expDest, destDetail, "actual dest detail different than expected")
}
