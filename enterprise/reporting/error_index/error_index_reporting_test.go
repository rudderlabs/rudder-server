package error_index

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

func newMockConfigSubscriber() *mockConfigSubscriber {
	return &mockConfigSubscriber{
		workspaceIDForSourceIDMap: make(map[string]string),
	}
}

type mockConfigSubscriber struct {
	workspaceIDForSourceIDMap map[string]string
}

func (m *mockConfigSubscriber) WorkspaceIDFromSource(sourceID string) string {
	return m.workspaceIDForSourceIDMap[sourceID]
}

func (m *mockConfigSubscriber) addWorkspaceIDForSourceID(sourceID, workspaceID string) {
	m.workspaceIDForSourceIDMap[sourceID] = workspaceID
}

func TestErrorIndexReporter(t *testing.T) {
	workspaceID := "test-workspace-id"
	sourceID := "test-source-id"
	destinationID := "test-destination-id"
	transformationID := "test-transformation-id"
	trackingPlanID := "test-tracking-plan-id"
	reportedBy := "test-reported-by"
	eventName := "test-event-name"
	eventType := "test-event-type"
	messageID := "test-message-id"

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	ctx := context.Background()

	receivedAt := time.Now().UTC()
	failedAt := receivedAt.Add(time.Hour)

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
						ReceivedAt:       receivedAt.Add(1 * time.Hour).UnixMicro(),
						SourceID:         sourceID,
						DestinationID:    destinationID,
						TransformationID: transformationID,
						TrackingPlanID:   trackingPlanID,
						EventName:        eventName,
						EventType:        eventType,
						FailedStage:      reportedBy,
						FailedAt:         failedAt.UnixMicro(),
					},
					{
						MessageID:        messageID + "2",
						ReceivedAt:       receivedAt.Add(2 * time.Hour).UnixMicro(),
						SourceID:         sourceID,
						DestinationID:    destinationID,
						TransformationID: transformationID,
						TrackingPlanID:   trackingPlanID,
						EventName:        eventName,
						EventType:        eventType,
						FailedStage:      reportedBy,
						FailedAt:         failedAt.UnixMicro(),
					},
					{
						MessageID:        messageID + "3",
						ReceivedAt:       receivedAt.Add(3 * time.Hour).UnixMicro(),
						SourceID:         sourceID,
						DestinationID:    destinationID,
						TransformationID: transformationID,
						TrackingPlanID:   trackingPlanID,
						EventName:        eventName,
						EventType:        eventType,
						FailedStage:      reportedBy,
						FailedAt:         failedAt.UnixMicro(),
					},
					{
						MessageID:        messageID + "4",
						ReceivedAt:       receivedAt.Add(4 * time.Hour).UnixMicro(),
						SourceID:         sourceID,
						DestinationID:    destinationID,
						TransformationID: transformationID,
						TrackingPlanID:   trackingPlanID,
						EventName:        eventName,
						EventType:        eventType,
						FailedStage:      reportedBy,
						FailedAt:         failedAt.UnixMicro(),
					},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				postgresContainer, err := postgres.Setup(pool, t)
				require.NoError(t, err)

				c := config.New()

				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				cs := newMockConfigSubscriber()
				cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

				eir := NewErrorIndexReporter(ctx, logger.NOP, cs, c, stats.NOP)
				defer eir.Stop()

				syncer := eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: postgresContainer.DBDsn})
				syncerDone := make(chan struct{})
				go func() {
					defer close(syncerDone)
					syncer()
				}()

				eir.now = func() time.Time {
					return failedAt
				}
				sqlTx, err := postgresContainer.DB.Begin()
				require.NoError(t, err)
				tx := &Tx{Tx: sqlTx}
				err = eir.Report(context.Background(), tc.reports, tx)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
				db, err := eir.resolveJobsDB(tx)
				require.NoError(t, err)
				jr, err := db.GetUnprocessed(ctx, jobsdb.GetQueryParams{
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
					require.Equal(t, eventPayload.FailedAt, tc.expectedPayload[i].FailedAt)
					require.Equal(t, eventPayload.ReceivedAt, tc.expectedPayload[i].ReceivedAt)

					var params map[string]interface{}
					err = json.Unmarshal(job.Parameters, &params)
					require.NoError(t, err)

					require.Equal(t, params["source_id"], sourceID)
					require.Equal(t, params["workspaceId"], workspaceID)

					<-syncerDone
				}
			})
		}
	})

	t.Run("graceful shutdown", func(t *testing.T) {
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		c := config.New()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		cf := newMockConfigSubscriber()
		cf.addWorkspaceIDForSourceID(sourceID, workspaceID)

		eir := NewErrorIndexReporter(ctx, logger.NOP, cf, c, stats.NOP)
		defer eir.Stop()

		syncer := eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: postgresContainer.DBDsn})
		syncerDone := make(chan struct{})
		go func() {
			defer close(syncerDone)
			syncer()
		}()

		sqlTx, err := postgresContainer.DB.Begin()
		require.NoError(t, err)
		tx := &Tx{Tx: sqlTx}
		err = eir.Report(context.Background(), []*types.PUReportedMetric{}, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		cancel()
		<-syncerDone
	})

	t.Run("using 1 syncer", func(t *testing.T) {
		t.Run("wrong transaction", func(t *testing.T) {
			pg1, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			pg2, err := postgres.Setup(pool, t)
			require.NoError(t, err)

			c := config.New()

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			cf := newMockConfigSubscriber()
			cf.addWorkspaceIDForSourceID(sourceID, workspaceID)

			eir := NewErrorIndexReporter(ctx, logger.NOP, cf, c, stats.NOP)
			defer eir.Stop()

			syncer := eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: pg1.DBDsn})
			syncerDone := make(chan struct{})
			go func() {
				defer close(syncerDone)
				syncer()
			}()

			sqlTx, err := pg2.DB.Begin()
			require.NoError(t, err)
			tx := &Tx{Tx: sqlTx}
			err = eir.Report(context.Background(), []*types.PUReportedMetric{
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
			}, tx)
			require.Error(t, err)
			require.Error(t, tx.Commit())

			<-syncerDone
		})
	})

	t.Run("using 2 syncers", func(t *testing.T) {
		pg1, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		pg2, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		pg3, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		c := config.New()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		cs := newMockConfigSubscriber()
		cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

		eir := NewErrorIndexReporter(ctx, logger.NOP, cs, c, stats.NOP)
		defer eir.Stop()

		syncer1 := eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: pg1.DBDsn})
		syncer2 := eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: pg2.DBDsn})

		syncersDone := make(chan struct{})
		go func() {
			defer close(syncersDone)
			syncer1()
			syncer2()
		}()

		t.Run("correct transaction", func(t *testing.T) {
			sqlTx, err := pg1.DB.Begin()
			require.NoError(t, err)
			tx := &Tx{Tx: sqlTx}
			err = eir.Report(context.Background(), []*types.PUReportedMetric{
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
			}, tx)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
		})
		t.Run("wrong transaction", func(t *testing.T) {
			sqlTx, err := pg3.DB.Begin()
			require.NoError(t, err)
			tx := &Tx{Tx: sqlTx}
			err = eir.Report(context.Background(), []*types.PUReportedMetric{
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
			}, tx)
			require.Error(t, err)
			require.NoError(t, tx.Commit())
		})

		<-syncersDone
	})

	t.Run("sync data", func(t *testing.T) {
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		minioResource, err := minio.Setup(pool, t)
		require.NoError(t, err)

		reports := []*types.PUReportedMetric{
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
		}

		c := config.New()
		c.Set("ErrorIndex.storage.Bucket", minioResource.BucketName)
		c.Set("ErrorIndex.storage.Endpoint", minioResource.Endpoint)
		c.Set("ErrorIndex.storage.AccessKey", minioResource.AccessKeyID)
		c.Set("ErrorIndex.storage.SecretAccessKey", minioResource.AccessKeySecret)
		c.Set("ErrorIndex.storage.S3ForcePathStyle", true)
		c.Set("ErrorIndex.storage.DisableSSL", true)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		cs := newMockConfigSubscriber()
		cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

		eir := NewErrorIndexReporter(ctx, logger.NOP, cs, c, stats.NOP)
		eir.now = func() time.Time {
			return failedAt
		}
		eir.trigger = func() <-chan time.Time {
			return time.After(time.Duration(0))
		}
		defer eir.Stop()

		syncer := eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: postgresContainer.DBDsn})

		syncerDone := make(chan struct{})
		go func() {
			defer close(syncerDone)
			syncer()
		}()

		sqlTx, err := postgresContainer.DB.Begin()
		require.NoError(t, err)

		tx := &Tx{Tx: sqlTx}
		err = eir.Report(context.Background(), reports, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		db, err := eir.resolveJobsDB(tx)
		require.NoError(t, err)

		failedJobs := lo.Flatten(lo.Map(reports, func(item *types.PUReportedMetric, index int) []*types.FailedMessage {
			return item.StatusDetail.FailedMessages
		}))

		require.Eventually(t, func() bool {
			jr, err := db.GetSucceeded(ctx, jobsdb.GetQueryParams{
				JobsLimit: 100,
			})
			require.NoError(t, err)

			return len(jr.Jobs) == len(failedJobs)
		},
			time.Second*30,
			time.Millisecond*100,
		)

		cancel()

		<-syncerDone
	})
}
