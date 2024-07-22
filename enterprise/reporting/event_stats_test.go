package reporting

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
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
	trackingPlanID := "test-tracking-plan-id"
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
								},
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

	statsStore, err := memstats.New()
	require.NoError(t, err)

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
				TrackingPlanID: "",
			},
			PUDetails: types.PUDetails{
				PU:         reportedBy,
				TerminalPU: true,
			},
			StatusDetail: &types.StatusDetail{
				Count:      10,
				Status:     jobsdb.Succeeded.State,
				StatusCode: 200,
			},
		},
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:       sourceID,
				DestinationID:  destinationID,
				SourceCategory: sourceCategory,
				TrackingPlanID: "",
			},
			PUDetails: types.PUDetails{
				PU:         reportedBy,
				TerminalPU: true,
			},
			StatusDetail: &types.StatusDetail{
				Count:      50,
				Status:     jobsdb.Aborted.State,
				StatusCode: 500,
			},
		},
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:       sourceID,
				DestinationID:  destinationID,
				SourceCategory: sourceCategory,
				TrackingPlanID: trackingPlanID,
			},
			PUDetails: types.PUDetails{
				PU:         reportedBy,
				TerminalPU: true,
			},
			StatusDetail: &types.StatusDetail{
				Count:      150,
				Status:     jobsdb.Migrated.State,
				StatusCode: 500,
			},
		},
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:       sourceID,
				DestinationID:  destinationID,
				SourceCategory: sourceCategory,
				TrackingPlanID: trackingPlanID,
			},
			PUDetails: types.PUDetails{
				PU:         reportedBy,
				TerminalPU: false,
			},
			StatusDetail: &types.StatusDetail{
				Count:      100,
				Status:     "non-terminal",
				StatusCode: 500,
			},
		},
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:       sourceID,
				DestinationID:  destinationID,
				SourceCategory: "",
				TrackingPlanID: "",
			},
			PUDetails: types.PUDetails{
				PU:         reportedBy,
				TerminalPU: true,
			},
			StatusDetail: &types.StatusDetail{
				Count:      20,
				Status:     jobsdb.Succeeded.State,
				StatusCode: 200,
			},
		},
	}
	esr := NewEventStatsReporter(cs, statsStore)
	esr.Record(testReports)
	require.Equal(t, statsStore.Get(EventsProcessedMetricName, map[string]string{
		"workspaceId":     workspaceID,
		"sourceId":        sourceID,
		"destinationId":   destinationID,
		"reportedBy":      reportedBy,
		"sourceCategory":  sourceCategory,
		"statusCode":      "200",
		"destinationType": "test-destination-name",
		"terminal":        "true",
		"status":          jobsdb.Succeeded.State,
		"trackingPlanId":  "",
	}).LastValue(), float64(10))
	require.Equal(t, statsStore.Get(EventsProcessedMetricName, map[string]string{
		"workspaceId":     workspaceID,
		"sourceId":        sourceID,
		"destinationId":   destinationID,
		"reportedBy":      reportedBy,
		"sourceCategory":  sourceCategory,
		"statusCode":      "500",
		"destinationType": "test-destination-name",
		"terminal":        "true",
		"status":          jobsdb.Aborted.State,
		"trackingPlanId":  "",
	}).LastValue(), float64(50))
	require.Equal(t, statsStore.Get(EventsProcessedMetricName, map[string]string{
		"workspaceId":     workspaceID,
		"sourceId":        sourceID,
		"destinationId":   destinationID,
		"reportedBy":      reportedBy,
		"sourceCategory":  sourceCategory,
		"statusCode":      "500",
		"destinationType": "test-destination-name",
		"terminal":        "true",
		"status":          jobsdb.Migrated.State,
		"trackingPlanId":  trackingPlanID,
	}).LastValue(), float64(150))
	require.Equal(t, statsStore.Get(EventsProcessedMetricName, map[string]string{
		"workspaceId":     workspaceID,
		"sourceId":        sourceID,
		"destinationId":   destinationID,
		"reportedBy":      reportedBy,
		"sourceCategory":  sourceCategory,
		"statusCode":      "500",
		"destinationType": "test-destination-name",
		"terminal":        "false",
		"status":          "non-terminal",
		"trackingPlanId":  trackingPlanID,
	}).LastValue(), float64(100))
	require.Equal(t, statsStore.Get(EventsProcessedMetricName, map[string]string{
		"workspaceId":     workspaceID,
		"sourceId":        sourceID,
		"destinationId":   destinationID,
		"reportedBy":      reportedBy,
		"sourceCategory":  EventStream,
		"statusCode":      "200",
		"destinationType": "test-destination-name",
		"terminal":        "true",
		"status":          jobsdb.Succeeded.State,
		"trackingPlanId":  "",
	}).LastValue(), float64(20))
	require.Len(t, statsStore.GetAll(), 5)
	t.Cleanup(func() {
		cancel()
		<-subscribeDone
	})
}
