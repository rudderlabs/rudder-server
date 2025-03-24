package processor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"
	mockDedup "github.com/rudderlabs/rudder-server/mocks/services/dedup"
	mockreportingtypes "github.com/rudderlabs/rudder-server/mocks/utils/types"
	"github.com/rudderlabs/rudder-server/processor/isolation"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func setupTrackingPlanTest(t *testing.T) (*testContext, func()) {
	c := &testContext{}
	c.mockCtrl = gomock.NewController(t)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockGatewayJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockReadProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockWriteProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockEventSchemasDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockArchivalDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.MockRsourcesService = rsources.NewMockJobService(c.mockCtrl)
	c.MockReportingI = mockreportingtypes.NewMockReporting(c.mockCtrl)
	c.MockDedup = mockDedup.NewMockDedup(c.mockCtrl)
	c.MockObserver = &mockObserver{}
	c.mockTrackedUsersReporter = &mockTrackedUsersReporter{}

	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{
				Data: map[string]backendconfig.ConfigT{
					sampleWorkspaceID: sampleBackendConfig,
				},
				Topic: string(topic),
			}
			close(ch)
			return ch
		})

	c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1) // crash recovery check

	return c, func() {
		c.mockCtrl.Finish()
	}
}

func TestTrackingPlanValidation(t *testing.T) {
	initProcessor()

	t.Run("RudderTyper", func(t *testing.T) {
		t.Run("Tracking plan id and version from DgSourceTrackingPlanConfig", func(t *testing.T) {
			c, cleanup := setupTrackingPlanTest(t)
			defer cleanup()

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Validate(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(types.Response{})

			isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
			require.NoError(t, err)

			processor := NewHandle(config.Default, mockTransformer)
			processor.isolationStrategy = isolationStrategy
			processor.config.archivalEnabled = config.SingleValueLoader(false)
			Setup(processor, c, false, false)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			require.NoError(t, processor.config.asyncInit.WaitContext(ctx))
			t.Log("Processor setup and init done")

			preTransMessage, err := processor.processJobsForDest(
				"",
				subJob{
					subJobs: []*jobsdb.JobT{
						{
							UUID:      uuid.New(),
							JobID:     1,
							CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
							ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
							CustomVal: gatewayCustomVal[0],
							EventPayload: createBatchPayload(
								WriteKeyEnabledTp,
								"2001-01-02T02:23:45.000Z",
								[]mockEventData{
									{
										id:                        "1",
										jobid:                     1,
										originalTimestamp:         "2000-01-02T01:23:45",
										expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
										sentAt:                    "2000-01-02 01:23",
										expectedSentAt:            "2000-01-02T01:23:00.000Z",
										expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
									},
								},
								func(e mockEventData) string {
									return fmt.Sprintf(`
										{
										  "rudderId": "some-rudder-id",
										  "messageId": "message-%[1]s",
										  "some-property": "property-%[1]s",
										  "originalTimestamp": %[2]q,
										  "sentAt": %[3]q
										}
									`,
										e.id,
										e.originalTimestamp,
										e.sentAt,
									)
								},
							),
							EventCount:    1,
							LastJobStatus: jobsdb.JobStatusT{},
							Parameters:    createBatchParameters(SourceIDEnabledTp),
							WorkspaceId:   sampleWorkspaceID,
						},
					},
				},
			)
			require.NoError(t, err)
			_, _ = processor.generateTransformationMessage(preTransMessage)

			require.Len(t, c.MockObserver.calls, 1)
			for _, v := range c.MockObserver.calls {
				for _, e := range v.events {
					require.Equal(t, "tracking-plan-id", e.Metadata.TrackingPlanID)
					require.Equal(t, int64(100), e.Metadata.TrackingPlanVersion) // from DgSourceTrackingPlanConfig
				}
			}
		})

		t.Run("Tracking plan version override from context.ruddertyper", func(t *testing.T) {
			c, cleanup := setupTrackingPlanTest(t)
			defer cleanup()

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Validate(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(types.Response{})

			isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
			require.NoError(t, err)

			processor := NewHandle(config.Default, mockTransformer)
			processor.isolationStrategy = isolationStrategy
			processor.config.archivalEnabled = config.SingleValueLoader(false)
			Setup(processor, c, false, false)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			require.NoError(t, processor.config.asyncInit.WaitContext(ctx))
			t.Log("Processor setup and init done")

			preTransMessage, err := processor.processJobsForDest(
				"",
				subJob{
					subJobs: []*jobsdb.JobT{
						{
							UUID:      uuid.New(),
							JobID:     1,
							CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
							ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
							CustomVal: gatewayCustomVal[0],
							EventPayload: createBatchPayload(
								WriteKeyEnabledTp,
								"2001-01-02T02:23:45.000Z",
								[]mockEventData{
									{
										id:                        "1",
										jobid:                     1,
										originalTimestamp:         "2000-01-02T01:23:45",
										expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
										sentAt:                    "2000-01-02 01:23",
										expectedSentAt:            "2000-01-02T01:23:00.000Z",
										expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
									},
								},
								func(e mockEventData) string {
									return fmt.Sprintf(`
										{
										  "rudderId": "some-rudder-id",
										  "messageId": "message-%[1]s",
										  "some-property": "property-%[1]s",
										  "originalTimestamp": %[2]q,
										  "sentAt": %[3]q,
										  "context": {
											"ruddertyper": {
                                              "trackingPlanId": "tracking-plan-id",
											  "trackingPlanVersion": 123
											}
										  }
										}
									`,
										e.id,
										e.originalTimestamp,
										e.sentAt,
									)
								},
							),
							EventCount:    1,
							LastJobStatus: jobsdb.JobStatusT{},
							Parameters:    createBatchParameters(SourceIDEnabledTp),
							WorkspaceId:   sampleWorkspaceID,
						},
					},
				},
			)
			require.NoError(t, err)
			_, _ = processor.generateTransformationMessage(preTransMessage)

			require.Len(t, c.MockObserver.calls, 1)
			for _, v := range c.MockObserver.calls {
				for _, e := range v.events {
					require.Equal(t, "tracking-plan-id", e.Metadata.TrackingPlanID)
					require.Equal(t, int64(123), e.Metadata.TrackingPlanVersion) // Overridden happens when tracking plan id is same in context.ruddertyper and DgSourceTrackingPlanConfig
				}
			}
		})
	})
}
