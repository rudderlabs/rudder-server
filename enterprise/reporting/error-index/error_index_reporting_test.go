package error_index

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/jobsdb"
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
				c.Set("DB.port", postgresContainer.Port)
				c.Set("DB.user", postgresContainer.User)
				c.Set("DB.name", postgresContainer.Database)
				c.Set("DB.password", postgresContainer.Password)

				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				cs := newMockConfigFetcher()
				cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

				eir := NewErrorIndexReporter(ctx, c, logger.NOP, cs)
				eir.now = failedAt
				defer func() {
					eir.errIndexDB.TearDown()
				}()

				err = eir.Report(tc.reports, nil)
				require.NoError(t, err)

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
			})
		}
	})
	t.Run("panic in case of not able to start errIndexDB", func(t *testing.T) {
		require.Panics(t, func() {
			_ = NewErrorIndexReporter(ctx, config.New(), logger.NOP, newMockConfigFetcher())
		})
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

		cs := newMockConfigFetcher()
		cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

		eir := NewErrorIndexReporter(ctx, c, logger.NOP, cs)
		defer eir.errIndexDB.TearDown()

		err = eir.Report([]*types.PUReportedMetric{}, nil)
		require.NoError(t, err)

		syncerDone := make(chan struct{})
		go func() {
			defer close(syncerDone)

			syncer := eir.DatabaseSyncer(types.SyncerConfig{})
			syncer()
		}()

		cancel()

		<-syncerDone
	})
	t.Run("txn rollback", func(t *testing.T) {})
}

func TestFileFormatComparisonEncoding(t *testing.T) {
	now := time.Now()

	t.Run("CSV", func(t *testing.T) {
		var records [][]string
		var record []string

		for i := 0; i < 1000000; i++ {
			record = append(record, "messageId"+strconv.Itoa(i))
			record = append(record, "sourceId")
			record = append(record, "destinationId"+strconv.Itoa(i%10))
			record = append(record, "transformationId"+strconv.Itoa(i%10))
			record = append(record, "trackingPlanId"+strconv.Itoa(i%10))
			record = append(record, "failedStage"+strconv.Itoa(i%10))
			record = append(record, "eventType"+strconv.Itoa(i%10))
			record = append(record, "eventName"+strconv.Itoa(i%10))
			record = append(record, now.Add(time.Duration(i)*time.Second).Format(time.RFC3339))
			record = append(record, now.Add(time.Duration(i)*time.Second).Format(time.RFC3339))
			records = append(records, record)
			record = nil
		}

		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		c := csv.NewWriter(buf)

		err := c.WriteAll(records)
		require.NoError(t, err)

		t.Log("csv size:", buf.Len()) // csv size: 160MB
	})
	t.Run("JSON", func(t *testing.T) {
		var records []payload

		for i := 0; i < 1000000; i++ {
			records = append(records, payload{
				MessageID:        "messageId" + strconv.Itoa(i),
				SourceID:         "sourceId",
				DestinationID:    "destinationId" + strconv.Itoa(i%10),
				TransformationID: "transformationId" + strconv.Itoa(i%10),
				TrackingPlanID:   "trackingPlanId" + strconv.Itoa(i%10),
				FailedStage:      "failedStage" + strconv.Itoa(i%10),
				EventType:        "eventType" + strconv.Itoa(i%10),
				EventName:        "eventName" + strconv.Itoa(i%10),
				ReceivedAt:       now.Add(time.Duration(i) * time.Second),
				FailedAt:         now.Add(time.Duration(i) * time.Second),
			})
		}

		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		e := json.NewEncoder(buf)

		for _, record := range records {
			err := e.Encode(record)
			require.NoError(t, err)
		}

		t.Log("json size:", buf.Len()) // json size: 333MB
	})
	t.Run("Parquet", func(t *testing.T) {
		var records []payload

		for i := 0; i < 1000000; i++ {
			records = append(records, payload{
				MessageID:        "messageId" + strconv.Itoa(i),
				SourceID:         "sourceId",
				DestinationID:    "destinationId" + strconv.Itoa(i%10),
				TransformationID: "transformationId" + strconv.Itoa(i%10),
				TrackingPlanID:   "trackingPlanId" + strconv.Itoa(i%10),
				FailedStage:      "failedStage" + strconv.Itoa(i%10),
				EventType:        "eventType" + strconv.Itoa(i%10),
				EventName:        "eventName" + strconv.Itoa(i%10),
				ReceivedAt:       now.Add(time.Duration(i) * time.Second),
				FailedAt:         now.Add(time.Duration(i) * time.Second),
			})
		}

		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		wp := newWriterParquet(config.New())

		err := wp.Write(buf, records)
		require.NoError(t, err)
		t.Log("parquet size:", buf.Len())
		// parquet size: 13.74MB (rowGroupSizeInMB=512, pageSizeInKB=8, compression=snappy, encoding=[RLE_DICTIONARY,DELTA_BINARY_PACKED], No sorting) // Check with @atzoum
		// parquet size: 21.71MB (rowGroupSizeInMB=512, pageSizeInKB=8, compression=snappy, encoding=[RLE_DICTIONARY,DELTA_BINARY_PACKED])
		// parquet size: 21.71MB (rowGroupSizeInMB=1024, pageSizeInKB=8, compression=snappy, encoding=[RLE_DICTIONARY,DELTA_BINARY_PACKED])
		// parquet size: 21.71MB (rowGroupSizeInMB=128, pageSizeInKB=8, compression=snappy, encoding=[RLE_DICTIONARY,DELTA_BINARY_PACKED])
		// parquet size: 23.52MB (rowGroupSizeInMB=512, pageSizeInKB=8, compression=snappy, encoding=[RLE_DICTIONARY])
		// parquet size: 25.98MB (rowGroupSizeInMB=512, pageSizeInKB=8, compression=snappy, encoding=[])
	})
}
