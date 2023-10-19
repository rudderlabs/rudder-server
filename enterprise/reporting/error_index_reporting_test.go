package reporting

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestErrorIndexReporter(t *testing.T) {
	workspaceID := "test-workspace-id"
	sourceID := "test-source-id"
	destinationID := "test-destination-id"
	transformationID := "test-transformation-id"
	trackingPlanID := "test-tracking-plan-id"
	reportedBy := "test-reported-by"
	destinationDefinitionID := "test-destination-definition-id"
	destType := "test-dest-type"
	eventName := "test-event-name"
	eventType := "test-event-type"
	messageID := "test-message-id"

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	ctx := context.Background()

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
							Destinations: []backendconfig.DestinationT{
								{
									ID:      destinationID,
									Enabled: true,
									DestinationDefinition: backendconfig.DestinationDefinitionT{
										ID:   destinationDefinitionID,
										Name: destType,
									},
								},
							},
						},
					},
					Settings: backendconfig.Settings{
						DataRetention: backendconfig.DataRetention{
							DisableReportingPII: true,
						},
					},
				},
			},
			Topic: string(backendconfig.TopicBackendConfig),
		}
		close(ch)
		return ch
	}).AnyTimes()

	receivedAt := time.Now()

	failedAt := func() time.Time {
		return receivedAt.Add(time.Hour)
	}

	t.Run("reports", func(t *testing.T) {
		testCases := []struct {
			name            string
			reports         []*types.PUReportedMetric
			expectedPayload []payload
		}{
			{
				name:            "empty metrics",
				reports:         []*types.PUReportedMetric{},
				expectedPayload: []payload{},
			},
			{
				name: "without failed messages",
				reports: []*types.PUReportedMetric{
					{
						ConnectionDetails: types.ConnectionDetails{
							SourceID:         sourceID,
							DestinationID:    destinationID,
							TransformationID: transformationID,
							TrackingPlanID:   trackingPlanID,
						},
						PUDetails: types.PUDetails{
							PU: reportedBy,
						},
						StatusDetail: &types.StatusDetail{
							EventName: eventName,
							EventType: eventType,
						},
					},
				},
				expectedPayload: []payload{},
			},
			{
				name: "filter with failed messages",
				reports: []*types.PUReportedMetric{
					{
						ConnectionDetails: types.ConnectionDetails{
							SourceID:         sourceID,
							DestinationID:    destinationID,
							TransformationID: transformationID,
							TrackingPlanID:   trackingPlanID,
						},
						PUDetails: types.PUDetails{
							PU: reportedBy,
						},
						StatusDetail: &types.StatusDetail{
							EventName: eventName,
							EventType: eventType,
							FailedMessages: []*types.FailedMessage{
								{
									MessageID:  messageID + "1",
									ReceivedAt: receivedAt.Add(1 * time.Hour),
								},
								{
									MessageID:  messageID + "2",
									ReceivedAt: receivedAt.Add(2 * time.Hour),
								},
							},
						},
					},
					{
						ConnectionDetails: types.ConnectionDetails{
							SourceID:         sourceID,
							DestinationID:    destinationID,
							TransformationID: transformationID,
							TrackingPlanID:   trackingPlanID,
						},
						PUDetails: types.PUDetails{
							PU: reportedBy,
						},
						StatusDetail: &types.StatusDetail{
							EventName: eventName,
							EventType: eventType,
						},
					},
					{
						ConnectionDetails: types.ConnectionDetails{
							SourceID:         sourceID,
							DestinationID:    destinationID,
							TransformationID: transformationID,
							TrackingPlanID:   trackingPlanID,
						},
						PUDetails: types.PUDetails{
							PU: reportedBy,
						},
						StatusDetail: &types.StatusDetail{
							EventName: eventName,
							EventType: eventType,
							FailedMessages: []*types.FailedMessage{
								{
									MessageID:  messageID + "3",
									ReceivedAt: receivedAt.Add(3 * time.Hour),
								},
								{
									MessageID:  messageID + "4",
									ReceivedAt: receivedAt.Add(4 * time.Hour),
								},
							},
						},
					},
				},
				expectedPayload: []payload{
					{
						MessageID:        messageID + "1",
						ReceivedAt:       receivedAt.Add(1 * time.Hour),
						SourceID:         sourceID,
						DestinationID:    destinationID,
						TransformationID: transformationID,
						TrackingPlanID:   trackingPlanID,
						EventName:        eventName,
						EventType:        eventType,
						FailedStage:      reportedBy,
						FailedAt:         failedAt(),
					},
					{
						MessageID:        messageID + "2",
						ReceivedAt:       receivedAt.Add(2 * time.Hour),
						SourceID:         sourceID,
						DestinationID:    destinationID,
						TransformationID: transformationID,
						TrackingPlanID:   trackingPlanID,
						EventName:        eventName,
						EventType:        eventType,
						FailedStage:      reportedBy,
						FailedAt:         failedAt(),
					},
					{
						MessageID:        messageID + "3",
						ReceivedAt:       receivedAt.Add(3 * time.Hour),
						SourceID:         sourceID,
						DestinationID:    destinationID,
						TransformationID: transformationID,
						TrackingPlanID:   trackingPlanID,
						EventName:        eventName,
						EventType:        eventType,
						FailedStage:      reportedBy,
						FailedAt:         failedAt(),
					},
					{
						MessageID:        messageID + "4",
						ReceivedAt:       receivedAt.Add(4 * time.Hour),
						SourceID:         sourceID,
						DestinationID:    destinationID,
						TransformationID: transformationID,
						TrackingPlanID:   trackingPlanID,
						EventName:        eventName,
						EventType:        eventType,
						FailedStage:      reportedBy,
						FailedAt:         failedAt(),
					},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				postgresContainer, err := resource.SetupPostgres(pool, t)
				require.NoError(t, err)

				c := config.New()
				c.Set("DB.port", postgresContainer.Port)
				c.Set("DB.user", postgresContainer.User)
				c.Set("DB.name", postgresContainer.Database)
				c.Set("DB.password", postgresContainer.Password)

				ctx, cancel := context.WithCancel(ctx)

				cs := newConfigSubscriber(logger.NOP)

				subscribeDone := make(chan struct{})
				go func() {
					defer close(subscribeDone)
					cs.Subscribe(ctx, mockBackendConfig)
				}()

				errIndexDB := jobsdb.NewForReadWrite("err_idx", jobsdb.WithConfig(c))
				require.NoError(t, errIndexDB.Start())
				defer errIndexDB.TearDown()
				eir := NewErrorIndexReporter(ctx, logger.NOP, cs, errIndexDB)

				eir.now = failedAt
				sqltx, err := postgresContainer.DB.Begin()
				require.NoError(t, err)
				tx := &Tx{Tx: sqltx}
				err = eir.Report(tc.reports, tx)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
				jr, err := eir.errIndexDB.GetUnprocessed(ctx, jobsdb.GetQueryParams{
					JobsLimit: 100,
				})
				require.NoError(t, err)
				require.Equal(t, len(tc.expectedPayload), len(jr.Jobs))
				for i, job := range jr.Jobs {
					var eventPayload payload
					err := json.Unmarshal(job.EventPayload, &eventPayload)
					require.NoError(t, err)

					require.Equal(t, eventPayload.MessageID, tc.expectedPayload[i].MessageID)
					require.Equal(t, eventPayload.SourceID, tc.expectedPayload[i].SourceID)
					require.Equal(t, eventPayload.DestinationID, tc.expectedPayload[i].DestinationID)
					require.Equal(t, eventPayload.TransformationID, tc.expectedPayload[i].TransformationID)
					require.Equal(t, eventPayload.TrackingPlanID, tc.expectedPayload[i].TrackingPlanID)
					require.Equal(t, eventPayload.FailedStage, tc.expectedPayload[i].FailedStage)
					require.Equal(t, eventPayload.EventName, tc.expectedPayload[i].EventName)
					require.Equal(t, eventPayload.EventType, tc.expectedPayload[i].EventType)
					require.EqualValues(t, eventPayload.FailedAt.UTC(), failedAt().UTC())
					require.EqualValues(t, eventPayload.ReceivedAt.UTC(), tc.expectedPayload[i].ReceivedAt.UTC())

					var params map[string]interface{}
					err = json.Unmarshal(job.Parameters, &params)
					require.NoError(t, err)

					require.Equal(t, params["source_id"], sourceID)
					require.Equal(t, params["workspaceId"], workspaceID)
				}
				cancel()
				<-subscribeDone
			})
		}
	})
	t.Run("Graceful shutdown", func(t *testing.T) {
		postgresContainer, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		c := config.New()
		c.Set("DB.port", postgresContainer.Port)
		c.Set("DB.user", postgresContainer.User)
		c.Set("DB.name", postgresContainer.Database)
		c.Set("DB.password", postgresContainer.Password)

		ctx, cancel := context.WithCancel(ctx)

		cs := newConfigSubscriber(logger.NOP)

		subscribeDone := make(chan struct{})
		go func() {
			defer close(subscribeDone)

			cs.Subscribe(ctx, mockBackendConfig)
		}()

		errIndexDB := jobsdb.NewForReadWrite("err_idx", jobsdb.WithConfig(c))
		require.NoError(t, errIndexDB.Start())
		defer errIndexDB.TearDown()
		eir := NewErrorIndexReporter(ctx, logger.NOP, cs, errIndexDB)
		sqltx, err := postgresContainer.DB.Begin()
		require.NoError(t, err)
		tx := &Tx{Tx: sqltx}
		err = eir.Report([]*types.PUReportedMetric{}, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		syncerDone := make(chan struct{})
		go func() {
			defer close(syncerDone)

			syncer := eir.DatabaseSyncer(types.SyncerConfig{})
			syncer()
		}()

		cancel()

		<-subscribeDone
		<-syncerDone
	})
}
