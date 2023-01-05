package destination_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/destination"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/initialize"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func TestDestination(t *testing.T) {
	if testing.Verbose() {
		require.NoError(t, os.Setenv("LOG_LEVEL", "DEBUG"))
	}
	initialize.Init()
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
	mockIdentity := mockIdentity{
		mockWorkspaceID: testConfig.WorkspaceID,
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDestMiddleware := destination.NewMockdestMiddleware(mockCtrl)
	mockDestMiddleware.EXPECT().Identity().Return(mockIdentity).Times(1)
	ch := make(chan pubsub.DataEvent)
	mockDestMiddleware.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Return(ch).Times(1)
	go func() {
		ch <- pubsub.DataEvent{
			Data: map[string]backendconfig.ConfigT{
				testConfig.WorkspaceID: testConfig,
			},
		}
	}()
	dest := destination.DestinationConfig{
		Dest: mockDestMiddleware,
	}

	go dest.BackendConfigSubscriber(context.Background())
	require.Eventually(t, func() bool {
		return reflect.DeepEqual(testConfig, dest.Config)
	}, time.Second, 10*time.Millisecond, "config not updated")

	expectedDestinationDetail := model.Destination{
		Config: config,
		DestDefConfig: map[string]interface{}{
			"randomKey": "randomValue",
		},
		DestinationID: "1111",
		Name:          "S3",
	}

	destDetail, err := dest.GetDestDetails("1111")
	require.NoError(t, err, "expected no err")
	require.Equal(t, expectedDestinationDetail, destDetail, "actual dest detail different than expected")
}

type mockIdentity struct {
	mockWorkspaceID string
}

func (i mockIdentity) ID() string {
	return i.mockWorkspaceID
}

func (mockIdentity) BasicAuth() (string, string) {
	return "", ""
}

func (mockIdentity) Type() deployment.Type {
	return deployment.Type("regulation")
}
