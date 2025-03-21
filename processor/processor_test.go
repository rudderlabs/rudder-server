package processor

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"
)

var _gatewayCustomVal = []string{"GW"}

func TestProcessor(t *testing.T) {
	initProcessor()

	t.Run("Processor with ArchivalV2 enabled", func(t *testing.T) {
		t.Run("should process events and write to archival DB", func(t *testing.T) {
			c, cleanup := setupTest(t)
			defer cleanup()

			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
				// this message should not be delivered to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-a-definition-display-name": false},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     _gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
					WorkspaceId:   sampleWorkspaceID,
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: _gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT),
					WorkspaceId:   sampleWorkspaceID,
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     _gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
					WorkspaceId:   sampleWorkspaceID,
				},
			}

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			c.mockArchivalDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1).
				Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
					_ = f(jobsdb.EmptyStoreSafeTx())
				}).Return(nil)

			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					require.Len(t, jobs, 2)
				})

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))
			processor.archivalDB = c.mockArchivalDB
			processor.config.archivalEnabled = config.SingleValueLoader(true)

			Setup(processor, c, false, false)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.NoError(t, processor.config.asyncInit.WaitContext(ctx))
			t.Log("Processor setup and init done")
			preTransMessage, err := processor.processJobsForDest(
				"",
				subJob{
					subJobs: unprocessedJobsList,
				},
			)
			require.NoError(t, err)
			_, _ = processor.generateTransformationMessage(preTransMessage)

			require.Len(t, c.MockObserver.calls, 1)
		})
	})
}

func TestProcessorWithTrackedUsers(t *testing.T) {
	initProcessor()

	t.Run("should track Users from unprocessed jobs", func(t *testing.T) {
		_, cleanup := setupTest(t)
		defer cleanup()

		// Test implementation here...
	})

	t.Run("should track Users from unprocessed jobs with parallelScan", func(t *testing.T) {
		_, cleanup := setupTest(t)
		defer cleanup()

		// Test implementation here...
	})
}

// ... rest of the test cases ...
