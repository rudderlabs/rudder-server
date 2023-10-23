package error_index

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/testhelper/destination"

	"github.com/ory/dockertest/v3"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/jobsdb"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

func newMockConfigFetcher() *mockConfigFetcher {
	return &mockConfigFetcher{
		workspaceIDForSourceIDMap: make(map[string]string),
		piiReportingSettings:      make(map[string]bool),
	}
}

type mockConfigFetcher struct {
	workspaceIDForSourceIDMap map[string]string
	piiReportingSettings      map[string]bool
}

func (m *mockConfigFetcher) WorkspaceIDFromSource(sourceID string) string {
	return m.workspaceIDForSourceIDMap[sourceID]
}

func (m *mockConfigFetcher) IsPIIReportingDisabled(workspaceID string) bool {
	return m.piiReportingSettings[workspaceID]
}

func (m *mockConfigFetcher) addWorkspaceIDForSourceID(sourceID, workspaceID string) {
	m.workspaceIDForSourceIDMap[sourceID] = workspaceID
}

func (m *mockConfigFetcher) addPIIReportingSettings(workspaceID string, disabled bool) {
	m.piiReportingSettings[workspaceID] = disabled
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

				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				cf := newMockConfigFetcher()
				cf.addWorkspaceIDForSourceID(sourceID, workspaceID)

				eir := NewErrorIndexReporter(ctx, logger.NOP, cf, c, stats.Default)
				_ = eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: postgresContainer.DBDsn})
				defer eir.Stop()

				eir.now = failedAt
				sqltx, err := postgresContainer.DB.Begin()
				require.NoError(t, err)
				tx := &Tx{Tx: sqltx}
				err = eir.Report(tc.reports, tx)
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
					require.EqualValues(t, eventPayload.FailedAt.UTC(), failedAt().UTC())
					require.EqualValues(t, eventPayload.ReceivedAt.UTC(), tc.expectedPayload[i].ReceivedAt.UTC())

					var params map[string]interface{}
					err = json.Unmarshal(job.Parameters, &params)
					require.NoError(t, err)

					require.Equal(t, params["source_id"], sourceID)
					require.Equal(t, params["workspaceId"], workspaceID)
				}
			})
		}
	})

	t.Run("syncers", func(t *testing.T) {
		postgresContainer, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := destination.SetupMINIO(pool, t)
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
		c.Set("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
		c.Set("ErrorIndex.Storage.Bucket", minioResource.BucketName)
		c.Set("MINIO_ENDPOINT", minioResource.Endpoint)
		c.Set("MINIO_ACCESS_KEY_ID", minioResource.AccessKey)
		c.Set("MINIO_SECRET_ACCESS_KEY", minioResource.SecretKey)
		c.Set("MINIO_SSL", "false")

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		cf := newMockConfigFetcher()
		cf.addWorkspaceIDForSourceID(sourceID, workspaceID)

		eir := NewErrorIndexReporter(ctx, logger.NOP, cf, c, stats.Default)
		eir.now = failedAt
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

		sqltx, err := postgresContainer.DB.Begin()
		require.NoError(t, err)

		tx := &Tx{Tx: sqltx}
		err = eir.Report(reports, tx)
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

	t.Run("graceful shutdown", func(t *testing.T) {
		postgresContainer, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		c := config.New()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		cf := newMockConfigFetcher()
		cf.addWorkspaceIDForSourceID(sourceID, workspaceID)

		eir := NewErrorIndexReporter(ctx, logger.NOP, cf, c, stats.Default)
		defer eir.Stop()
		syncer := eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: postgresContainer.DBDsn})

		sqltx, err := postgresContainer.DB.Begin()
		require.NoError(t, err)
		tx := &Tx{Tx: sqltx}
		err = eir.Report([]*types.PUReportedMetric{}, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		syncerDone := make(chan struct{})
		go func() {
			defer close(syncerDone)
			syncer()
		}()

		cancel()

		<-syncerDone
	})

	t.Run("using 1 syncer", func(t *testing.T) {
		t.Run("wrong transaction", func(t *testing.T) {
			pg1, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			pg2, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)

			c := config.New()

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			cf := newMockConfigFetcher()
			cf.addWorkspaceIDForSourceID(sourceID, workspaceID)

			eir := NewErrorIndexReporter(ctx, logger.NOP, cf, c, stats.Default)
			defer eir.Stop()
			_ = eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: pg1.DBDsn})

			sqltx, err := pg2.DB.Begin()
			require.NoError(t, err)
			tx := &Tx{Tx: sqltx}
			err = eir.Report([]*types.PUReportedMetric{
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
		})
	})

	t.Run("using 2 syncers", func(t *testing.T) {
		pg1, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		pg2, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		pg3, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		c := config.New()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		cf := newMockConfigFetcher()
		cf.addWorkspaceIDForSourceID(sourceID, workspaceID)

		eir := NewErrorIndexReporter(ctx, logger.NOP, cf, c, stats.Default)
		defer eir.Stop()

		_ = eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: pg1.DBDsn})
		_ = eir.DatabaseSyncer(types.SyncerConfig{ConnInfo: pg2.DBDsn})

		t.Run("correct transaction", func(t *testing.T) {
			sqltx, err := pg1.DB.Begin()
			require.NoError(t, err)
			tx := &Tx{Tx: sqltx}
			err = eir.Report([]*types.PUReportedMetric{
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
			sqltx, err := pg3.DB.Begin()
			require.NoError(t, err)
			tx := &Tx{Tx: sqltx}
			err = eir.Report([]*types.PUReportedMetric{
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
	})
}
