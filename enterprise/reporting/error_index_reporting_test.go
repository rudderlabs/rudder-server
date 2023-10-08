package reporting

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
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

	now := func() time.Time {
		return receivedAt.Add(time.Hour)
	}

	t.Run("reports", func(t *testing.T) {
		testCases := []struct {
			name             string
			reports          []*types.PUReportedMetric
			expectedMetadata []metadata
		}{
			{
				name:             "empty metrics",
				reports:          []*types.PUReportedMetric{},
				expectedMetadata: []metadata{},
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
						StatusDetail: &types.StatusDetail{},
					},
				},
				expectedMetadata: []metadata{},
			},
			{
				name: "with failed messages",
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
				},
				expectedMetadata: []metadata{
					{
						MessageID:        messageID + "1",
						ReceivedAt:       receivedAt.Add(1 * time.Hour),
						SourceID:         sourceID,
						DestinationID:    destinationID,
						TransformationID: transformationID,
						TrackingPlanID:   trackingPlanID,
						FailedStage:      reportedBy,
						FailedAt:         now(),
					},
					{
						MessageID:        messageID + "2",
						ReceivedAt:       receivedAt.Add(2 * time.Hour),
						SourceID:         sourceID,
						DestinationID:    destinationID,
						TransformationID: transformationID,
						TrackingPlanID:   trackingPlanID,
						FailedStage:      reportedBy,
						FailedAt:         now(),
					},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Skip() // Check with @Sidddddarth why individual test are succeeding but not the whole suite

				postgresContainer, err := resource.SetupPostgres(pool, t)
				require.NoError(t, err)

				c := config.New()
				c.Set("DB.port", postgresContainer.Port)
				c.Set("DB.user", postgresContainer.User)
				c.Set("DB.name", postgresContainer.Database)
				c.Set("DB.password", postgresContainer.Password)

				txn, err := postgresContainer.DB.BeginTx(ctx, &sql.TxOptions{})
				require.NoError(t, err)
				defer func() { _ = txn.Rollback() }()

				ctx, cancel := context.WithCancel(ctx)

				cs := newConfigSubscriber(logger.NOP)

				subscribeDone := make(chan struct{})
				go func() {
					defer close(subscribeDone)

					cs.Subscribe(ctx, mockBackendConfig)
				}()

				eir := NewErrorIndexReporter(ctx, c, logger.NOP, cs)
				eir.now = now
				eir.Report(tc.reports, txn)

				errIndexDB := jobsdb.NewForRead("error_index", jobsdb.WithConfig(c))
				err = errIndexDB.Start()
				require.NoError(t, err)
				defer func() { errIndexDB.Close() }()

				jr, err := errIndexDB.GetUnprocessed(ctx, jobsdb.GetQueryParams{
					JobsLimit: 100,
				})
				require.NoError(t, err)

				metadataList := lo.Map(jr.Jobs, func(j *jobsdb.JobT, index int) metadata {
					var metadata metadata
					err := json.Unmarshal(j.Parameters, &metadata)
					require.NoError(t, err)

					return metadata
				})

				for i, metadata := range metadataList {
					require.Equal(t, metadata.MessageID, tc.expectedMetadata[i].MessageID)
					require.Equal(t, metadata.SourceID, tc.expectedMetadata[i].SourceID)
					require.Equal(t, metadata.DestinationID, tc.expectedMetadata[i].DestinationID)
					require.Equal(t, metadata.TransformationID, tc.expectedMetadata[i].TransformationID)
					require.Equal(t, metadata.TrackingPlanID, tc.expectedMetadata[i].TrackingPlanID)
					require.Equal(t, metadata.FailedStage, tc.expectedMetadata[i].FailedStage)

					require.EqualValues(t, metadata.FailedAt.UTC(), now().UTC())
					require.EqualValues(t, metadata.ReceivedAt.UTC(), tc.expectedMetadata[i].ReceivedAt.UTC())
				}

				cancel()

				<-subscribeDone
			})
		}
	})
	t.Run("panic in case of not able to start errIndexDB", func(t *testing.T) {
		require.Panics(t, func() {
			NewErrorIndexReporter(ctx, config.New(), logger.NOP, newConfigSubscriber(logger.NOP))
		})
	})
}
