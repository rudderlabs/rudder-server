package destination_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/destination"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func TestDestination(t *testing.T) {
	if testing.Verbose() {
		require.NoError(t, os.Setenv("LOG_LEVEL", "DEBUG"))
	}
	config := map[string]interface{}{
		"bucketName":  "malani-deletefeature-testdata",
		"prefix":      "regulation",
		"accessKeyID": "xyz",
		"accessKey":   "pqr",
		"enableSSE":   false,
	}

	destinationID := "1111"

	testConfig := backendconfig.ConfigT{
		WorkspaceID: "1234",
		Sources: []backendconfig.SourceT{
			{
				Destinations: []backendconfig.DestinationT{
					{
						ID:     destinationID,
						Config: config,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							Config: map[string]interface{}{
								"randomKey": "randomValue",
							},
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
	testBackendConfig := map[string]backendconfig.ConfigT{
		testConfig.WorkspaceID: testConfig,
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDestMiddleware := destination.NewMockdestMiddleware(mockCtrl)
	ch := make(chan pubsub.DataEvent)
	mockDestMiddleware.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Return(ch).Times(1)
	go func() {
		ch <- pubsub.DataEvent{Data: testBackendConfig}
	}()
	dest := destination.DestinationConfig{
		Dest: mockDestMiddleware,
	}

	dest.Start(context.Background())
	require.Eventually(t, func() bool {
		_, err := dest.GetDestDetails(destinationID)
		return err == nil
	}, time.Second, 10*time.Millisecond, "config not updated")

	expectedDestinationDetail := model.Destination{
		Config: config,
		DestDefConfig: map[string]interface{}{
			"randomKey": "randomValue",
		},
		DestinationID: destinationID,
		Name:          "S3",
	}

	destDetail, err := dest.GetDestDetails(destinationID)
	require.NoError(t, err, "expected no err")
	require.Equal(t, expectedDestinationDetail, destDetail, "actual dest detail different than expected")
}
