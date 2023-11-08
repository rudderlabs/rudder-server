package reporting

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestEventStatsReporter(t *testing.T) {
	workspaceID := "test-workspace-id"
	sourceID := "test-source-id"
	destinationID := "test-destination-id"
	reportedBy := "test-reported-by"
	sourceCategory := "test-source-category"
	ctrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
		ch := make(chan pubsub.DataEvent, 1)
		ch <- pubsub.DataEvent{
			Data: map[string]backendconfig.ConfigT{
				workspaceID: {
					WorkspaceID: workspaceID,
					Sources: []backendconfig.SourceT{
						{
							ID:      sourceID,
							Enabled: true,
							SourceDefinition: backendconfig.SourceDefinitionT{
								Category: sourceCategory,
							},
							Destinations: []backendconfig.DestinationT{
								{
									ID:      destinationID,
									Enabled: true,
									DestinationDefinition: backendconfig.DestinationDefinitionT{
										Name: "test-destination-name",
									},
								}, // Added a comma here
							},
						},
					},
				},
			},
			Topic: string(backendconfig.TopicBackendConfig),
		}
		close(ch)
		return ch
	}).AnyTimes()

	statsStore := memstats.New()
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cs := newConfigSubscriber(logger.NOP)

	subscribeDone := make(chan struct{})
	go func() {
		defer close(subscribeDone)

		cs.Subscribe(ctx, mockBackendConfig)
	}()

	testReports := []*types.PUReportedMetric{
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:       sourceID,
				DestinationID:  destinationID,
				SourceCategory: sourceCategory,
			},
			PUDetails: types.PUDetails{
				PU:         reportedBy,
				TerminalPU: true,
			},
			StatusDetail: &types.StatusDetail{
				Count:      10,
				Status:     "succeeded",
				StatusCode: 200,
			},
		},
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:       sourceID,
				DestinationID:  destinationID,
				SourceCategory: sourceCategory,
			},
			PUDetails: types.PUDetails{
				PU:         reportedBy,
				TerminalPU: false,
			},
			StatusDetail: &types.StatusDetail{
				Count:      50,
				Status:     "aborted",
				StatusCode: 500,
			},
		},
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:       "im-not-there",
				DestinationID:  destinationID,
				SourceCategory: sourceCategory,
			},
			PUDetails: types.PUDetails{
				PU:         reportedBy,
				TerminalPU: false,
			},
			StatusDetail: &types.StatusDetail{
				Count:      100,
				Status:     "failed",
				StatusCode: 500,
			},
		},
	}
	esr := NewEventStatsReporter(cs, statsStore)
	esr.Record(testReports)
	require.Equal(t, statsStore.Get(measurementNames["succeeded"], map[string]string{
		"workspaceId":     workspaceID,
		"sourceId":        sourceID,
		"destinationId":   destinationID,
		"reportedBy":      reportedBy,
		"sourceCategory":  sourceCategory,
		"terminal":        "true",
		"status_code":     "200",
		"destinationType": "test-destination-name",
	}).LastValue(), float64(10))
	require.Equal(t, statsStore.Get(measurementNames["aborted"], map[string]string{
		"workspaceId":     workspaceID,
		"sourceId":        sourceID,
		"destinationId":   destinationID,
		"reportedBy":      reportedBy,
		"sourceCategory":  sourceCategory,
		"terminal":        "false",
		"status_code":     "500",
		"destinationType": "test-destination-name",
	}).LastValue(), float64(50))

	t.Cleanup(func() {
		cancel()
		<-subscribeDone
	})
}
