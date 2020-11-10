package processor

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"

	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"
	mocksStats "github.com/rudderlabs/rudder-server/mocks/stats"

	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

var testTimeout = 5 * time.Second

type context struct {
	asyncHelper       testutils.AsyncTestHelper
	configInitialised bool

	minDBReadBatchSize int
	maxDBReadBatchSize int

	mockCtrl              *gomock.Controller
	mockBackendConfig     *mocksBackendConfig.MockBackendConfig
	mockGatewayJobsDB     *mocksJobsDB.MockJobsDB
	mockRouterJobsDB      *mocksJobsDB.MockJobsDB
	mockBatchRouterJobsDB *mocksJobsDB.MockJobsDB
	mockProcErrorsDB      *mocksJobsDB.MockJobsDB
	mockStats             *mocksStats.MockStats

	mockStatGatewayDBRead            *mocksStats.MockRudderStats
	mockStatGatewayDBWrite           *mocksStats.MockRudderStats
	mockStatRouterDBWrite            *mocksStats.MockRudderStats
	mockStatBatchRouterDBWrite       *mocksStats.MockRudderStats
	mockStatActiveUsers              *mocksStats.MockRudderStats
	mockStatGatewayDBReadTime        *mocksStats.MockRudderStats
	mockStatGatewayDBWriteTime       *mocksStats.MockRudderStats
	mockStatLoopTime                 *mocksStats.MockRudderStats
	mockStatSessionTransformTime     *mocksStats.MockRudderStats
	mockStatUserTransformTime        *mocksStats.MockRudderStats
	mockStatDestTransformTime        *mocksStats.MockRudderStats
	mockStatJobListSort              *mocksStats.MockRudderStats
	mockStatMarshalSingularEvents    *mocksStats.MockRudderStats
	mockStatDestProcessing           *mocksStats.MockRudderStats
	mockStatProcErrDBWrite           *mocksStats.MockRudderStats
	mockStatNumRequests              *mocksStats.MockRudderStats
	mockStatNumEvents                *mocksStats.MockRudderStats
	mockStatDestNumOuputEvents       *mocksStats.MockRudderStats
	mockStatBatchDestNumOutputEvents *mocksStats.MockRudderStats

	mockEnabledADestStats *DestStatT
	mockEnabledBDestStats *DestStatT
}

func (c *context) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockGatewayJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockStats = mocksStats.NewMockStats(c.mockCtrl)

	c.configInitialised = false
	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		Do(func(channel chan utils.DataEvent, topic backendconfig.Topic) {
			// on Subscribe, emulate a backend configuration event
			go func() {
				channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
				c.configInitialised = true
			}()
		}).
		Do(c.asyncHelper.ExpectAndNotifyCallback()).
		Return().Times(1)

	c.minDBReadBatchSize = 100
	c.maxDBReadBatchSize = 10000

	registerStatMocks := func(name string, statType string) *mocksStats.MockRudderStats {
		stat := mocksStats.NewMockRudderStats(c.mockCtrl)
		c.mockStats.EXPECT().NewStat(name, statType).Return(stat).Times(1)
		return stat
	}

	registerTaggedStatMocks := func(name string, statType string) *mocksStats.MockRudderStats {
		stat := mocksStats.NewMockRudderStats(c.mockCtrl)
		c.mockStats.EXPECT().NewTaggedStat(name, statType, gomock.Any()).Return(stat).Times(1)
		return stat
	}

	registerDestStatMocks := func(destID string) *DestStatT {
		numEvents := mocksStats.NewMockRudderStats(c.mockCtrl)
		numOutputEvents := mocksStats.NewMockRudderStats(c.mockCtrl)
		sessionTransform := mocksStats.NewMockRudderStats(c.mockCtrl)
		userTransform := mocksStats.NewMockRudderStats(c.mockCtrl)
		destTransform := mocksStats.NewMockRudderStats(c.mockCtrl)

		// These are registered asynchronously when a new backend config is received
		c.asyncHelper.RegisterCalls(
			c.mockStats.EXPECT().NewTaggedStat("proc_num_events", stats.CountType, map[string]string{"destID": destID,}).Return(numEvents).Times(1),
			c.mockStats.EXPECT().NewTaggedStat("proc_num_output_events", stats.CountType, map[string]string{"destID": destID,}).Return(numOutputEvents).Times(1),
			c.mockStats.EXPECT().NewTaggedStat("proc_session_transform", stats.TimerType, map[string]string{"destID": destID,}).Return(sessionTransform).Times(1),
			c.mockStats.EXPECT().NewTaggedStat("proc_user_transform", stats.TimerType, map[string]string{"destID": destID,}).Return(userTransform).Times(1),
			c.mockStats.EXPECT().NewTaggedStat("proc_dest_transform", stats.TimerType, map[string]string{"destID": destID,}).Return(destTransform).Times(1),
		)

		return &DestStatT{
			id:               destID,
			numEvents:        numEvents,
			numOutputEvents:  numOutputEvents,
			sessionTransform: sessionTransform,
			userTransform:    userTransform,
			destTransform:    destTransform,
		}
	}

	c.mockStatGatewayDBRead = registerStatMocks("processor.gateway_db_read", stats.CountType)
	c.mockStatGatewayDBWrite = registerStatMocks("processor.gateway_db_write", stats.CountType)
	c.mockStatRouterDBWrite = registerStatMocks("processor.router_db_write", stats.CountType)
	c.mockStatBatchRouterDBWrite = registerStatMocks("processor.batch_router_db_write", stats.CountType)
	c.mockStatActiveUsers = registerStatMocks("processor.active_users", stats.GaugeType)
	c.mockStatGatewayDBReadTime = registerStatMocks("processor.gateway_db_read_time", stats.TimerType)
	c.mockStatGatewayDBWriteTime = registerStatMocks("processor.gateway_db_write_time", stats.TimerType)
	c.mockStatLoopTime = registerStatMocks("processor.loop_time", stats.TimerType)
	c.mockStatSessionTransformTime = registerStatMocks("processor.session_transform_time", stats.TimerType)
	c.mockStatUserTransformTime = registerStatMocks("processor.user_transform_time", stats.TimerType)
	c.mockStatDestTransformTime = registerStatMocks("processor.dest_transform_time", stats.TimerType)
	c.mockStatJobListSort = registerStatMocks("processor.job_list_sort", stats.TimerType)
	c.mockStatMarshalSingularEvents = registerStatMocks("processor.marshal_singular_events", stats.TimerType)
	c.mockStatDestProcessing = registerStatMocks("processor.dest_processing", stats.TimerType)
	c.mockStatProcErrDBWrite = registerStatMocks("processor.proc_err_db_write", stats.CountType)
	c.mockStatNumRequests = registerStatMocks("processor.num_requests", stats.CountType)
	c.mockStatNumEvents = registerStatMocks("processor.num_events", stats.CountType)
	c.mockStatDestNumOuputEvents = registerTaggedStatMocks("processor.num_output_events", stats.CountType)
	c.mockStatBatchDestNumOutputEvents = registerTaggedStatMocks("processor.num_output_events", stats.CountType)

	c.mockEnabledADestStats = registerDestStatMocks(DestinationIDEnabledA)
	c.mockEnabledBDestStats = registerDestStatMocks(DestinationIDEnabledB)
	registerDestStatMocks(DestinationIDDisabled)
}

func (c *context) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

const (
	WriteKeyEnabled       = "enabled-write-key"
	SourceIDEnabled       = "enabled-source"
	SourceIDDisabled      = "disabled-source"
	DestinationIDEnabledA = "enabled-destination-a" // test destination router
	DestinationIDEnabledB = "enabled-destination-b" // test destination batch router
	DestinationIDDisabled = "disabled-destination"
)

var (
	gatewayCustomVal []string = []string{"GW"}
	emptyJobsList    []*jobsdb.JobT
)

//SetEnableEventSchemasFeature overrides enableEventSchemasFeature configuration and returns previous value
func SetEnableEventSchemasFeature(b bool) bool {
	prev := enableEventSchemasFeature
	enableEventSchemasFeature = b
	return prev
}

// This configuration is assumed by all processor tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = backendconfig.SourcesT{
	Sources: []backendconfig.SourceT{
		{
			ID:       SourceIDDisabled,
			WriteKey: WriteKeyEnabled,
			Enabled:  false,
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabled,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledA,
					Name:               "A",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 DestinationIDEnabledB,
					Name:               "B",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-b-definition-id",
						Name:        "MINIO",
						DisplayName: "enabled-destination-b-definition-display-name",
						Config:      map[string]interface{}{},
					},
					Transformations: []backendconfig.TransformationT{
						{
							ID:          "transformation-id",
							Name:        "transformation-name",
							Description: "transformation-description",
							VersionID:   "transformation-version-id",
						},
					},
				},
				// This destination should receive no events
				{
					ID:                 DestinationIDDisabled,
					Name:               "C",
					Enabled:            false,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-disabled",
						Name:        "destination-definition-name-disabled",
						DisplayName: "destination-definition-display-name-disabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
	},
}

var _ = Describe("Processor", func() {
	var c *context

	BeforeEach(func() {
		c = &context{}
		c.Setup()

		// setup static requirements of dependencies
		stats.Setup()

		SetEnableEventSchemasFeature(false)
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {
		It("should initialize (no jobs to recover)", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, c.maxDBReadBatchSize, nil).Return(emptyJobsList).Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, c.mockStats)
			processor.ResetDBReadBatchSize()
		})

		It("should recover after crash", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			var executingJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:         uuid.NewV4(),
					JobID:        1010,
					CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: nil,
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 1,
					},
					Parameters: nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         1003,
					CreatedAt:     time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
			}

			// GetExecuting is called in a loop until all executing jobs have been updated. Each executing job should be updated to failed status
			var executingCall1 = c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, c.maxDBReadBatchSize, nil).Return(executingJobsList[0:2]).Times(1)
			var updateCall1 = c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(2), gatewayCustomVal, nil).After(executingCall1).Times(1).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(executingJobsList[0], statuses[0], jobsdb.Failed.State, "", "{}", 2)
					assertJobStatus(executingJobsList[1], statuses[1], jobsdb.Failed.State, "", "{}", 1)
				})

			// second loop iteration
			var executingCall2 = c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, c.maxDBReadBatchSize, nil).Return(emptyJobsList).After(updateCall1).Return(executingJobsList[2:]).Times(1)
			var updateCall2 = c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(1), gatewayCustomVal, nil).After(executingCall2).Times(1).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(executingJobsList[2], statuses[0], jobsdb.Failed.State, "", "{}", 1)
				})

			c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, c.maxDBReadBatchSize, nil).After(updateCall2).Return(emptyJobsList).Times(1) // returning empty job list should end crash recover loop

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, c.mockStats)
			processor.ResetDBReadBatchSize()
		})
	})

	Context("normal operation", func() {
		BeforeEach(func() {
			// crash recovery check
			c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, c.maxDBReadBatchSize, nil).Return(emptyJobsList).Times(1)
		})

		It("should only send proper stats, if not pending jobs are returned", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, c.mockStats)
			processor.ResetDBReadBatchSize()

			callLoopTime := c.mockStatLoopTime.EXPECT().Start().Times(1)
			callDBRTime := c.mockStatGatewayDBReadTime.EXPECT().Start().Times(1).After(callLoopTime)

			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(gatewayCustomVal, c.minDBReadBatchSize, nil).Return(emptyJobsList).Times(1).After(callDBRTime)
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gatewayCustomVal, c.minDBReadBatchSize, nil).Return(emptyJobsList).Times(1).After(callRetry)

			c.mockStatGatewayDBReadTime.EXPECT().End().Times(1).After(callUnprocessed)

			var didWork = processor.handlePendingGatewayJobs()
			Expect(didWork).To(Equal(false))
		})

		It("should process ToRetry and Unprocessed jobs", func() {
			var messages map[string]mockEventData = map[string]mockEventData{
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
				// this message should be delivered only to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-b-definition-display-name": false},
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

			var unprocessedJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:          uuid.NewV4(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  createBatchPayload(WriteKeyEnabled, "2001-01-02T02:23:45.000Z", []mockEventData{messages["message-1"], messages["message-2"]}),
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 04, 28, 23, 27, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 27, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
			}

			var toRetryJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:         uuid.NewV4(),
					JobID:        2010,
					CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: createBatchPayload(WriteKeyEnabled, "2002-01-02T02:23:45.000Z", []mockEventData{messages["message-3"], messages["message-4"], messages["message-5"]}),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 1,
					},
					Parameters: nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
			}

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			callLoopTime := c.mockStatLoopTime.EXPECT().Start().Times(1)
			callDBRTime := c.mockStatGatewayDBReadTime.EXPECT().Start().Times(1).After(callLoopTime)
			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(gatewayCustomVal, c.minDBReadBatchSize, nil).Return(toRetryJobsList).Times(1).After(callDBRTime)
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gatewayCustomVal, c.minDBReadBatchSize-len(toRetryJobsList), nil).Return(unprocessedJobsList).Times(1).After(callRetry)
			callDBRTimeEnd := c.mockStatGatewayDBReadTime.EXPECT().End().Times(1).After(callUnprocessed)
			callListSort := c.mockStatJobListSort.EXPECT().Start().Times(1).After(callDBRTimeEnd)
			c.mockStatGatewayDBRead.EXPECT().Count(len(toRetryJobsList) + len(unprocessedJobsList)).Times(1).After(callListSort)
			callListSortEnd := c.mockStatJobListSort.EXPECT().End().Times(1).After(callListSort)
			callNumRequests := c.mockStatNumRequests.EXPECT().Count(5).Times(1).After(callListSortEnd)
			callMarshallSingularEvents := c.mockStatMarshalSingularEvents.EXPECT().Start().Times(1).After(callNumRequests)
			callMarshallSingularEventsEnd := c.mockStatMarshalSingularEvents.EXPECT().End().Times(1).After(callMarshallSingularEvents)
			c.mockStatNumEvents.EXPECT().Count(5).Times(1).After(callMarshallSingularEvents)
			callDestProcessing := c.mockStatDestProcessing.EXPECT().Start().Times(1).After(callMarshallSingularEventsEnd)
			callDestANumEvents := c.mockEnabledADestStats.numEvents.(*mocksStats.MockRudderStats).EXPECT().Count(4).Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.numOutputEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.numEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.numOutputEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callDestProcessing)
			callUserTransformStart := c.mockEnabledBDestStats.userTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)

			// We expect one call to user transform for destination B
			callUserTransform := mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).After(callUserTransformStart).
				DoAndReturn(func(clientEvents []transformer.TransformerEventT, url string, batchSize int, breakIntoBatchWhenUserChanges bool) transformer.ResponseT {
					Expect(breakIntoBatchWhenUserChanges).To(BeFalse())
					Expect(url).To(Equal("http://localhost:9090/customTransform"))

					outputEvents := make([]transformer.TransformerResponseT, 0)

					for _, event := range clientEvents {
						event.Message["user-transform"] = "value"
						outputEvents = append(outputEvents, transformer.TransformerResponseT{
							Output: map[string]interface{}(event.Message),
						})
					}

					return transformer.ResponseT{
						Events: outputEvents,
					}
				})

			c.mockEnabledBDestStats.userTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callUserTransform)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledA: {
					events:                    4,
					messageIds:                "message-1,message-2,message-3,message-4",
					receiveMetadata:           true,
					destinationDefinitionName: "enabled-destination-a-definition-name",
				},
				DestinationIDEnabledB: {
					events:                    2,
					messageIds:                "message-3,message-4",
					destinationDefinitionName: "minio",
				},
			}

			// We expect one transform call to destination A, after sending numEvents stat.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				After(callDestANumEvents).DoAndReturn(assertDestinationTransform(messages, DestinationIDEnabledA, transformExpectations[DestinationIDEnabledA]))

			// We expect one transform call to destination B, after user transform for destination B.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				After(callUserTransform).DoAndReturn(assertDestinationTransform(messages, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB]))

			c.mockStatDestNumOuputEvents.EXPECT().Count(2).Times(1)
			c.mockStatBatchDestNumOutputEvents.EXPECT().Count(2).Times(1)

			callDestProcessingEnd := c.mockStatDestProcessing.EXPECT().End().Times(1).After(callDestProcessing)
			callDBWrite := c.mockStatGatewayDBWriteTime.EXPECT().Start().Times(1).After(callDestProcessingEnd)

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				// Expect(job.CustomVal).To(Equal("destination-definition-name-a"))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":"%s"}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				Expect(string(job.Parameters)).To(Equal(`{"source_id": "source-from-transformer", "destination_id": "destination-from-transformer", "received_at": ""}`))
			}

			// One Store call is expected for all events
			callStoreRouter := c.mockRouterJobsDB.EXPECT().Store(gomock.Any()).Times(1).After(callDBWrite).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-a")
					}
				})

			callStoreBatchRouter := c.mockBatchRouterJobsDB.EXPECT().Store(gomock.Any()).Times(1).After(callDBWrite).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-b")
					}
				})

			callUpdateJobs := c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreBatchRouter).After(callStoreRouter).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different than order of jobs
					assertJobStatus(unprocessedJobsList[1], statuses[0], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1) // id 1002
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1) // id 1010
					assertJobStatus(toRetryJobsList[1], statuses[2], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2002
					assertJobStatus(toRetryJobsList[2], statuses[3], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2003
					assertJobStatus(toRetryJobsList[0], statuses[4], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2010
				})

			c.mockStatGatewayDBWriteTime.EXPECT().End().Times(1).After(callUpdateJobs)
			c.mockStatGatewayDBWrite.EXPECT().Count(len(toRetryJobsList) + len(unprocessedJobsList)).Times(1).After(callUpdateJobs)
			c.mockStatProcErrDBWrite.EXPECT().Count(0).Times(1).After(callUpdateJobs)
			c.mockStatRouterDBWrite.EXPECT().Count(2).Times(1).After(callUpdateJobs)
			c.mockStatBatchRouterDBWrite.EXPECT().Count(2).Times(1).After(callUpdateJobs)

			c.mockStatLoopTime.EXPECT().End().Times(1).After(callUpdateJobs)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			processorSetupAndAssertJobHandling(processor, c)
		})
	})

	/*Context("sessions", func() {
		BeforeEach(func() {
			// crash recovery check
			c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, c.maxDBReadBatchSize, nil).Return(emptyJobsList).Times(1)
		})

		It("should process ToRetry and Unprocessed jobs, when total events are less than sessionThreshold", func() {
			var messages map[string]mockEventData = map[string]mockEventData{
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
				// this message should be delivered only to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-b-definition-display-name": false},
				},
				// The following messages are part of job 2010, but merged in 1010 due to sessions.
				// The jobid and received at values are overwritten by 1010.
				"message-3": {
					id:                 "3",
					jobid:              1010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2001-01-02T02:23:45.000Z", // received at is taken from job 1010
					integrations:       map[string]bool{"All": true},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     1010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z", // received at is taken from job 1010
					integrations:              map[string]bool{},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              1010,
					expectedReceivedAt: "2001-01-02T02:23:45.000Z", // received at is taken from job 1010
					integrations:       map[string]bool{"All": false},
				},
			}

			var unprocessedJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:          uuid.NewV4(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  createBatchPayload(WriteKeyEnabled, "2001-01-02T02:23:45.000Z", []mockEventData{messages["message-1"], messages["message-2"]}),
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 04, 28, 23, 27, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 27, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
			}

			var toRetryJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:         uuid.NewV4(),
					JobID:        2010,
					CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: createBatchPayload(WriteKeyEnabled, "2002-01-02T02:23:45.000Z", []mockEventData{messages["message-3"], messages["message-4"], messages["message-5"]}),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 1,
					},
					Parameters: nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
			}

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			callLoopTime := c.mockStatLoopTime.EXPECT().Start().Times(1)
			callDBRTime := c.mockStatGatewayDBReadTime.EXPECT().Start().Times(1).After(callLoopTime)
			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(gatewayCustomVal, c.minDBReadBatchSize, nil).Return(toRetryJobsList).Times(1).After(callDBRTime)
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gatewayCustomVal, c.minDBReadBatchSize-len(toRetryJobsList), nil).Return(unprocessedJobsList).Times(1).After(callRetry)
			callDBRTimeEnd := c.mockStatGatewayDBReadTime.EXPECT().End().Times(1).After(callUnprocessed)
			callListSort := c.mockStatJobListSort.EXPECT().Start().Times(1).After(callDBRTimeEnd)
			c.mockStatGatewayDBRead.EXPECT().Count(len(toRetryJobsList) + len(unprocessedJobsList)).Times(1).After(callListSort)
			callListSortEnd := c.mockStatJobListSort.EXPECT().End().Times(1).After(callListSort)

			callUpdateJobs := c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callListSortEnd).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different than order of jobs
					assertJobStatus(unprocessedJobsList[1], statuses[0], jobsdb.Executing.State, "200", `{"success":"OK"}`, 1) // id 1002
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "200", `{"success":"OK"}`, 1) // id 1010
					assertJobStatus(toRetryJobsList[1], statuses[2], jobsdb.Executing.State, "200", `{"success":"OK"}`, 1)     // id 2002
					assertJobStatus(toRetryJobsList[2], statuses[3], jobsdb.Executing.State, "200", `{"success":"OK"}`, 1)     // id 2003
					assertJobStatus(toRetryJobsList[0], statuses[4], jobsdb.Executing.State, "200", `{"success":"OK"}`, 1)     // id 2010
				})

			callNumRequests := c.mockStatNumRequests.EXPECT().Count(gomock.Any()).AnyTimes().After(callUpdateJobs)
			callMarshallSingularEvents := c.mockStatMarshalSingularEvents.EXPECT().Start().Times(1).After(callNumRequests)
			callMarshallSingularEventsEnd := c.mockStatMarshalSingularEvents.EXPECT().End().Times(1).After(callMarshallSingularEvents)
			c.mockStatNumEvents.EXPECT().Count(gomock.Any()).AnyTimes().After(callMarshallSingularEvents)
			callDestProcessing := c.mockStatDestProcessing.EXPECT().Start().Times(1).After(callMarshallSingularEventsEnd)
			callDestANumEvents := c.mockEnabledADestStats.numEvents.(*mocksStats.MockRudderStats).EXPECT().Count(4).Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.numOutputEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.numEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.numOutputEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callDestProcessing)
			callUserTransformStart := c.mockEnabledBDestStats.sessionTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)

			c.mockStatLoopTime.EXPECT().End().Times(1).After(callUpdateJobs)

			// We expect one call to user transform for destination B
			callUserTransform := mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).After(callUserTransformStart).
				DoAndReturn(func(clientEvents []transformer.TransformerEventT, url string, batchSize int, breakIntoBatchWhenUserChanges bool) transformer.ResponseT {
					Expect(breakIntoBatchWhenUserChanges).To(BeTrue())
					Expect(url).To(Equal("http://localhost:9090/customTransform?processSessions=true"))

					outputEvents := make([]transformer.TransformerResponseT, 0)

					for _, event := range clientEvents {
						event.Message["user-transform"] = "value"
						outputEvents = append(outputEvents, transformer.TransformerResponseT{
							Output: map[string]interface{}(event.Message),
						})
					}

					return transformer.ResponseT{
						Events: outputEvents,
					}
				})

			c.mockEnabledBDestStats.sessionTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callUserTransform)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledA: {
					events:                    4,
					messageIds:                "message-1,message-2,message-3,message-4",
					receiveMetadata:           true,
					destinationDefinitionName: "enabled-destination-a-definition-name",
				},
				DestinationIDEnabledB: {
					events:                    2,
					messageIds:                "message-3,message-4",
					destinationDefinitionName: "minio",
				},
			}

			// We expect one transform call to destination A, after sending numEvents stat.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				After(callDestANumEvents).DoAndReturn(assertDestinationTransform(messages, DestinationIDEnabledA, transformExpectations[DestinationIDEnabledA]))

			// We expect one transform call to destination B, after user transform for destination B.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				After(callUserTransform).DoAndReturn(assertDestinationTransform(messages, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB]))

			c.mockStatDestNumOuputEvents.EXPECT().Count(gomock.Any()).AnyTimes()
			c.mockStatBatchDestNumOutputEvents.EXPECT().Count(gomock.Any()).AnyTimes()

			callDestProcessingEnd := c.mockStatDestProcessing.EXPECT().End().Times(1).After(callDestProcessing)
			callDBWrite := c.mockStatGatewayDBWriteTime.EXPECT().Start().Times(1).After(callDestProcessingEnd)

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				// Expect(job.CustomVal).To(Equal("destination-definition-name-a"))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":"%s"}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				Expect(string(job.Parameters)).To(Equal(`{"source_id": "source-from-transformer", "destination_id": "destination-from-transformer", "received_at": ""}`))
			}

			// One Store call is expected for all events
			callStoreRouter := c.mockRouterJobsDB.EXPECT().Store(gomock.Any()).Times(1).After(callDBWrite).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-a")
					}
				})

			callStoreBatchRouter := c.mockBatchRouterJobsDB.EXPECT().Store(gomock.Any()).Times(1).After(callDBWrite).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-b")
					}
				})

			callUpdateJobsSuccess := c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(2), gatewayCustomVal, nil).Times(1).After(callStoreBatchRouter).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1) // id 1002
					assertJobStatus(toRetryJobsList[0], statuses[1], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2002
				})

			c.mockStatGatewayDBWriteTime.EXPECT().End().Times(1).After(callStoreRouter).After(callUpdateJobsSuccess)
			c.mockStatGatewayDBWrite.EXPECT().Count(2).Times(1)
			c.mockStatRouterDBWrite.EXPECT().Count(2).Times(1)
			c.mockStatBatchRouterDBWrite.EXPECT().Count(2).Times(1)
			c.mockStatProcErrDBWrite.EXPECT().Count(0).Times(1)

			// expecting any number of empty gauge stats, from createSessions loop
			c.mockStatActiveUsers.EXPECT().Gauge(0).AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatMarshalSingularEvents.EXPECT().Start().AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatMarshalSingularEvents.EXPECT().End().AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatDestProcessing.EXPECT().Start().AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatDestProcessing.EXPECT().End().AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatGatewayDBWriteTime.EXPECT().Start().AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatGatewayDBWriteTime.EXPECT().End().AnyTimes().After(callUpdateJobsSuccess)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(0), gatewayCustomVal, nil).AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatGatewayDBWrite.EXPECT().Count(0).AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatRouterDBWrite.EXPECT().Count(0).AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatBatchRouterDBWrite.EXPECT().Count(0).AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatProcErrDBWrite.EXPECT().Count(0).AnyTimes().After(callUpdateJobsSuccess)

			var processor *HandleT = &HandleT{
				transformer:            mockTransformer,
				processSessions:        true,
				sessionThresholdEvents: 3, // this test will send 5 events
			}

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("should process ToRetry and Unprocessed jobs, when total events are more than sessionThreshold", func() {
			var messages map[string]mockEventData = map[string]mockEventData{
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
				// this message should be delivered only to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-b-definition-display-name": false},
				},
				// The following messages are part of job 2010, but merged in 1010 due to sessions.
				// The jobid and received at values are overwritten by 1010.
				"message-3": {
					id:                 "3",
					jobid:              1010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2001-01-02T02:23:45.000Z", // received at is taken from job 1010
					integrations:       map[string]bool{"All": true},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     1010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z", // received at is taken from job 1010
					integrations:              map[string]bool{},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              1010,
					expectedReceivedAt: "2001-01-02T02:23:45.000Z", // received at is taken from job 1010
					integrations:       map[string]bool{"All": false},
				},
			}

			var unprocessedJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:          uuid.NewV4(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  createBatchPayload(WriteKeyEnabled, "2001-01-02T02:23:45.000Z", []mockEventData{messages["message-1"], messages["message-2"]}),
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 04, 28, 23, 27, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 27, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
			}

			var toRetryJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:         uuid.NewV4(),
					JobID:        2010,
					CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: createBatchPayload(WriteKeyEnabled, "2002-01-02T02:23:45.000Z", []mockEventData{messages["message-3"], messages["message-4"], messages["message-5"]}),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 1,
					},
					Parameters: nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
			}

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			callLoopTime := c.mockStatLoopTime.EXPECT().Start().Times(1)
			callDBRTime := c.mockStatGatewayDBReadTime.EXPECT().Start().Times(1).After(callLoopTime)
			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(gatewayCustomVal, c.minDBReadBatchSize, nil).Return(toRetryJobsList).Times(1).After(callDBRTime)
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gatewayCustomVal, c.minDBReadBatchSize-len(toRetryJobsList), nil).Return(unprocessedJobsList).Times(1).After(callRetry)
			callDBRTimeEnd := c.mockStatGatewayDBReadTime.EXPECT().End().Times(1).After(callUnprocessed)
			callListSort := c.mockStatJobListSort.EXPECT().Start().Times(1).After(callDBRTimeEnd)
			c.mockStatGatewayDBRead.EXPECT().Count(len(toRetryJobsList) + len(unprocessedJobsList)).Times(1).After(callListSort)
			callListSortEnd := c.mockStatJobListSort.EXPECT().End().Times(1).After(callListSort)

			c.mockStatActiveUsers.EXPECT().Gauge(1).Times(1).After(callListSortEnd).
				Do(c.asyncHelper.ExpectAndNotifyCallback())

			callUpdateJobs := c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callListSortEnd).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different than order of jobs
					assertJobStatus(unprocessedJobsList[1], statuses[0], jobsdb.Executing.State, "200", `{"success":"OK"}`, 1) // id 1002
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "200", `{"success":"OK"}`, 1) // id 1010
					assertJobStatus(toRetryJobsList[1], statuses[2], jobsdb.Executing.State, "200", `{"success":"OK"}`, 1)     // id 2002
					assertJobStatus(toRetryJobsList[2], statuses[3], jobsdb.Executing.State, "200", `{"success":"OK"}`, 1)     // id 2003
					assertJobStatus(toRetryJobsList[0], statuses[4], jobsdb.Executing.State, "200", `{"success":"OK"}`, 1)     // id 2010
				})

			callNumRequests := c.mockStatNumRequests.EXPECT().Count(gomock.Any()).AnyTimes().After(callUpdateJobs)
			callMarshallSingularEvents := c.mockStatMarshalSingularEvents.EXPECT().Start().Times(1).After(callNumRequests)
			callMarshallSingularEventsEnd := c.mockStatMarshalSingularEvents.EXPECT().End().Times(1).After(callMarshallSingularEvents)
			c.mockStatNumEvents.EXPECT().Count(gomock.Any()).AnyTimes().After(callMarshallSingularEvents)
			callDestProcessing := c.mockStatDestProcessing.EXPECT().Start().Times(1).After(callMarshallSingularEventsEnd)
			callDestANumEvents := c.mockEnabledADestStats.numEvents.(*mocksStats.MockRudderStats).EXPECT().Count(4).Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.numOutputEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.numEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.numOutputEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callDestProcessing)
			callUserTransformStart := c.mockEnabledBDestStats.sessionTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)

			c.mockStatLoopTime.EXPECT().End().Times(1).After(callUpdateJobs)

			// We expect one call to user transform for destination B
			callUserTransform := mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).After(callUserTransformStart).
				DoAndReturn(func(clientEvents []transformer.TransformerEventT, url string, batchSize int, breakIntoBatchWhenUserChanges bool) transformer.ResponseT {
					Expect(breakIntoBatchWhenUserChanges).To(BeTrue())
					Expect(url).To(Equal("http://localhost:9090/customTransform?processSessions=true"))

					outputEvents := make([]transformer.TransformerResponseT, 0)

					for _, event := range clientEvents {
						event.Message["user-transform"] = "value"
						outputEvents = append(outputEvents, transformer.TransformerResponseT{
							Output: map[string]interface{}(event.Message),
						})
					}

					return transformer.ResponseT{
						Events: outputEvents,
					}
				})

			c.mockEnabledBDestStats.sessionTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callUserTransform)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledA: {
					events:                    4,
					messageIds:                "message-1,message-2,message-3,message-4",
					receiveMetadata:           true,
					destinationDefinitionName: "enabled-destination-a-definition-name",
				},
				DestinationIDEnabledB: {
					events:                    2,
					messageIds:                "message-3,message-4",
					destinationDefinitionName: "minio",
				},
			}

			// We expect one transform call to destination A, after sending numEvents stat.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				After(callDestANumEvents).DoAndReturn(assertDestinationTransform(messages, DestinationIDEnabledA, transformExpectations[DestinationIDEnabledA]))

			// We expect one transform call to destination B, after user transform for destination B.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				After(callUserTransform).DoAndReturn(assertDestinationTransform(messages, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB]))

			c.mockStatDestNumOuputEvents.EXPECT().Count(gomock.Any()).AnyTimes()
			c.mockStatBatchDestNumOutputEvents.EXPECT().Count(gomock.Any()).AnyTimes()

			callDestProcessingEnd := c.mockStatDestProcessing.EXPECT().End().Times(1).After(callDestProcessing)
			callDBWrite := c.mockStatGatewayDBWriteTime.EXPECT().Start().Times(1).After(callDestProcessingEnd)

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				// Expect(job.CustomVal).To(Equal("destination-definition-name-a"))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":"%s"}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				Expect(string(job.Parameters)).To(Equal(`{"source_id": "source-from-transformer", "destination_id": "destination-from-transformer", "received_at": ""}`))
			}

			// One Store call is expected for all events
			callStoreRouter := c.mockRouterJobsDB.EXPECT().Store(gomock.Any()).Times(1).After(callDBWrite).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-a")
					}
				})

			callStoreBatchRouter := c.mockBatchRouterJobsDB.EXPECT().Store(gomock.Any()).Times(1).After(callDBWrite).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-b")
					}
				})

			callUpdateJobsSuccess := c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(2), gatewayCustomVal, nil).Times(1).After(callStoreBatchRouter).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1) // id 1002
					assertJobStatus(toRetryJobsList[0], statuses[1], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2002
				})

			c.asyncHelper.RegisterCalls(
				c.mockStatGatewayDBWriteTime.EXPECT().End().Times(1).After(callStoreRouter).After(callUpdateJobsSuccess),
				c.mockStatGatewayDBWrite.EXPECT().Count(2).Times(1),
				c.mockStatRouterDBWrite.EXPECT().Count(2).Times(1),
				// c.mockStatProcErrDBWrite.EXPECT().Count(0).Times(1),
				c.mockStatBatchRouterDBWrite.EXPECT().Count(2).Times(1),
			)

			// expecting any number of empty gauge stats, from createSessions loop
			c.mockStatActiveUsers.EXPECT().Gauge(0).AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatMarshalSingularEvents.EXPECT().Start().AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatMarshalSingularEvents.EXPECT().End().AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatDestProcessing.EXPECT().Start().AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatDestProcessing.EXPECT().End().AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatGatewayDBWriteTime.EXPECT().Start().AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatGatewayDBWriteTime.EXPECT().End().AnyTimes().After(callUpdateJobsSuccess)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(0), gatewayCustomVal, nil).AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatGatewayDBWrite.EXPECT().Count(0).AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatRouterDBWrite.EXPECT().Count(0).AnyTimes().After(callUpdateJobsSuccess)
			c.mockStatProcErrDBWrite.EXPECT().Count(0).AnyTimes()
			c.mockStatBatchRouterDBWrite.EXPECT().Count(0).AnyTimes().After(callUpdateJobsSuccess)

			var processor *HandleT = &HandleT{
				transformer:            mockTransformer,
				processSessions:        true,
				sessionThresholdEvents: 20, // this test will send 5 events
			}

			processorSetupAndAssertJobHandling(processor, c)
		})
	})*/

	Context("transformations", func() {
		It("messages should be skipped on destination transform failures, without failing the job", func() {
			var messages map[string]mockEventData = map[string]mockEventData{
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
				"message-2": {
					id:                        "2",
					jobid:                     1011,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
			}

			var unprocessedJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:          uuid.NewV4(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  createBatchPayload(WriteKeyEnabled, "2001-01-02T02:23:45.000Z", []mockEventData{messages["message-1"], messages["message-2"]}),
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    []byte(`{"source_id": "source-from-transformer"}`),
				},
			}

			transformerResponses := []transformer.TransformerResponseT{
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-1",
					},
					StatusCode: 400,
					Error:      "error-1",
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					StatusCode: 400,
					Error:      "error-2",
				},
			}

			assertErrStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				Expect(job.CustomVal).To(Equal("enabled-destination-a-definition-name"))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				Expect(string(job.Parameters)).To(Equal(fmt.Sprintf(`{"source_id": "source-from-transformer", "destination_id": "enabled-destination-a", "error": "error-%v", "status_code": "400", "stage": "dest_transformer"}`, i+1)))

				// compare payloads
				var payload []map[string]interface{}
				err := json.Unmarshal(job.EventPayload, &payload)
				Expect(err).To(BeNil())
				Expect(len(payload)).To(Equal(1))
				message := messages[fmt.Sprintf(`message-%v`, i+1)]
				Expect(fmt.Sprintf(`message-%s`, message.id)).To(Equal(payload[0]["messageId"]))
				Expect(payload[0]["some-property"]).To(Equal(fmt.Sprintf(`property-%s`, message.id)))
				Expect(message.expectedOriginalTimestamp).To(Equal(payload[0]["originalTimestamp"]))
			}

			var toRetryJobsList []*jobsdb.JobT = []*jobsdb.JobT{}

			c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, c.maxDBReadBatchSize, nil).Return(emptyJobsList).Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			callLoopTime := c.mockStatLoopTime.EXPECT().Start().Times(1)
			callDBRTime := c.mockStatGatewayDBReadTime.EXPECT().Start().Times(1).After(callLoopTime)
			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(gatewayCustomVal, c.minDBReadBatchSize, nil).Return(toRetryJobsList).Times(1).After(callDBRTime)
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gatewayCustomVal, c.minDBReadBatchSize-len(toRetryJobsList), nil).Return(unprocessedJobsList).Times(1).After(callRetry)
			callDBRTimeEnd := c.mockStatGatewayDBReadTime.EXPECT().End().Times(1).After(callUnprocessed)
			callListSort := c.mockStatJobListSort.EXPECT().Start().Times(1).After(callDBRTimeEnd)
			c.mockStatGatewayDBRead.EXPECT().Count(len(toRetryJobsList) + len(unprocessedJobsList)).Times(1).After(callListSort)
			callListSortEnd := c.mockStatJobListSort.EXPECT().End().Times(1).After(callListSort)
			callNumRequests := c.mockStatNumRequests.EXPECT().Count(1).Times(1).After(callListSortEnd)
			callMarshallSingularEvents := c.mockStatMarshalSingularEvents.EXPECT().Start().Times(1).After(callNumRequests)
			callMarshallSingularEventsEnd := c.mockStatMarshalSingularEvents.EXPECT().End().Times(1).After(callMarshallSingularEvents)
			c.mockStatNumEvents.EXPECT().Count(2).Times(1).After(callMarshallSingularEvents)
			callDestProcessing := c.mockStatDestProcessing.EXPECT().Start().Times(1).After(callMarshallSingularEventsEnd)
			c.mockEnabledADestStats.numEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.numOutputEvents.(*mocksStats.MockRudderStats).EXPECT().Count(0).Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)
			c.mockEnabledADestStats.destTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callDestProcessing)

			// Test transformer failure
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Return(transformer.ResponseT{
					Events:       []transformer.TransformerResponseT{},
					FailedEvents: transformerResponses,
				})

			callDestProcessingEnd := c.mockStatDestProcessing.EXPECT().End().Times(1).After(callDestProcessing)
			c.mockStatGatewayDBWriteTime.EXPECT().Start().Times(1).After(callDestProcessingEnd)

			callUpdateJobs := c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)
				})

			// One Store call is expected for all events
			c.mockProcErrorsDB.EXPECT().Store(gomock.Any()).Times(1).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertErrStoreJob(job, i, "value-enabled-destination-a")
					}
				})

			c.mockStatGatewayDBWriteTime.EXPECT().End().Times(1).After(callUpdateJobs)
			c.mockStatGatewayDBWrite.EXPECT().Count(len(toRetryJobsList) + len(unprocessedJobsList)).Times(1)
			c.mockStatRouterDBWrite.EXPECT().Count(0).Times(1)
			c.mockStatBatchRouterDBWrite.EXPECT().Count(0).Times(1)
			c.mockStatLoopTime.EXPECT().End().Times(1)
			c.mockStatProcErrDBWrite.EXPECT().Count(2).Times(1).After(callUpdateJobs)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("messages should be skipped on user transform failures, without failing the job", func() {
			var messages map[string]mockEventData = map[string]mockEventData{
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-b-definition-display-name": true},
				},
				"message-2": {
					id:                        "2",
					jobid:                     1011,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-b-definition-display-name": true},
				},
			}

			var unprocessedJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:          uuid.NewV4(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  createBatchPayload(WriteKeyEnabled, "2001-01-02T02:23:45.000Z", []mockEventData{messages["message-1"], messages["message-2"]}),
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    []byte(`{"source_id": "source-from-transformer"}`),
				},
			}

			transformerResponses := []transformer.TransformerResponseT{
				{
					Metadata: transformer.MetadataT{
						MessageIDs: []string{"message-1", "message-2"},
					},
					StatusCode: 400,
					Error:      "error-combined",
				},
			}

			assertErrStoreJob := func(job *jobsdb.JobT) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				Expect(job.CustomVal).To(Equal("MINIO"))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				Expect(string(job.Parameters)).To(Equal(`{"source_id": "source-from-transformer", "destination_id": "enabled-destination-b", "error": "error-combined", "status_code": "400", "stage": "user_transformer"}`))

				// compare payloads
				var payload []map[string]interface{}
				err := json.Unmarshal(job.EventPayload, &payload)
				Expect(err).To(BeNil())
				Expect(len(payload)).To(Equal(2))
				message1 := messages[fmt.Sprintf(`message-%v`, 1)]
				Expect(fmt.Sprintf(`message-%s`, message1.id)).To(Equal(payload[0]["messageId"]))
				Expect(payload[0]["some-property"]).To(Equal(fmt.Sprintf(`property-%s`, message1.id)))
				Expect(message1.expectedOriginalTimestamp).To(Equal(payload[0]["originalTimestamp"]))
				message2 := messages[fmt.Sprintf(`message-%v`, 2)]
				Expect(fmt.Sprintf(`message-%s`, message2.id)).To(Equal(payload[1]["messageId"]))
				Expect(payload[1]["some-property"]).To(Equal(fmt.Sprintf(`property-%s`, message2.id)))
				Expect(message2.expectedOriginalTimestamp).To(Equal(payload[1]["originalTimestamp"]))
			}

			var toRetryJobsList []*jobsdb.JobT = []*jobsdb.JobT{}

			c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, c.maxDBReadBatchSize, nil).Return(emptyJobsList).Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			callLoopTime := c.mockStatLoopTime.EXPECT().Start().Times(1)
			callDBRTime := c.mockStatGatewayDBReadTime.EXPECT().Start().Times(1).After(callLoopTime)
			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(gatewayCustomVal, c.minDBReadBatchSize, nil).Return(toRetryJobsList).Times(1).After(callDBRTime)
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gatewayCustomVal, c.minDBReadBatchSize-len(toRetryJobsList), nil).Return(unprocessedJobsList).Times(1).After(callRetry)
			callDBRTimeEnd := c.mockStatGatewayDBReadTime.EXPECT().End().Times(1).After(callUnprocessed)
			callListSort := c.mockStatJobListSort.EXPECT().Start().Times(1).After(callDBRTimeEnd)
			c.mockStatGatewayDBRead.EXPECT().Count(len(toRetryJobsList) + len(unprocessedJobsList)).Times(1).After(callListSort)
			callListSortEnd := c.mockStatJobListSort.EXPECT().End().Times(1).After(callListSort)
			callNumRequests := c.mockStatNumRequests.EXPECT().Count(1).Times(1).After(callListSortEnd)
			callMarshallSingularEvents := c.mockStatMarshalSingularEvents.EXPECT().Start().Times(1).After(callNumRequests)
			callMarshallSingularEventsEnd := c.mockStatMarshalSingularEvents.EXPECT().End().Times(1).After(callMarshallSingularEvents)
			c.mockStatNumEvents.EXPECT().Count(2).Times(1).After(callMarshallSingularEvents)
			callDestProcessing := c.mockStatDestProcessing.EXPECT().Start().Times(1).After(callMarshallSingularEventsEnd)
			c.mockEnabledBDestStats.numEvents.(*mocksStats.MockRudderStats).EXPECT().Count(2).Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.userTransform.(*mocksStats.MockRudderStats).EXPECT().Start().Times(1).After(callDestProcessing)
			c.mockEnabledBDestStats.userTransform.(*mocksStats.MockRudderStats).EXPECT().End().Times(1).After(callDestProcessing)

			// Test transformer failure
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Return(transformer.ResponseT{
					Events:       []transformer.TransformerResponseT{},
					FailedEvents: transformerResponses,
				})

			callDestProcessingEnd := c.mockStatDestProcessing.EXPECT().End().Times(1).After(callDestProcessing)
			c.mockStatGatewayDBWriteTime.EXPECT().Start().Times(1).After(callDestProcessingEnd)

			callUpdateJobs := c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)
				})

			// One Store call is expected for all events
			c.mockProcErrorsDB.EXPECT().Store(gomock.Any()).Times(1).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(1))
					for _, job := range jobs {
						assertErrStoreJob(job)
					}
				})

			c.mockStatGatewayDBWriteTime.EXPECT().End().Times(1).After(callUpdateJobs)
			c.mockStatGatewayDBWrite.EXPECT().Count(len(toRetryJobsList) + len(unprocessedJobsList)).Times(1)
			c.mockStatRouterDBWrite.EXPECT().Count(0).Times(1)
			c.mockStatBatchRouterDBWrite.EXPECT().Count(0).Times(1)
			c.mockStatLoopTime.EXPECT().End().Times(1)
			c.mockStatProcErrDBWrite.EXPECT().Count(1).Times(1).After(callUpdateJobs)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			processorSetupAndAssertJobHandling(processor, c)
		})
	})
})

type mockEventData struct {
	id                        string
	jobid                     int64
	originalTimestamp         string
	expectedOriginalTimestamp string
	sentAt                    string
	expectedSentAt            string
	expectedReceivedAt        string
	integrations              map[string]bool
}

type mockTransformerData struct {
	id string
}

type transformExpectation struct {
	events                    int
	messageIds                string
	receiveMetadata           bool
	destinationDefinitionName string
}

func createMessagePayload(e mockEventData) string {
	integrations, _ := json.Marshal(e.integrations)
	return fmt.Sprintf(`{"rudderId": "some-rudder-id", "messageId":"message-%s","integrations":%s,"some-property":"property-%s","originalTimestamp":"%s","sentAt":"%s"}`, e.id, integrations, e.id, e.originalTimestamp, e.sentAt)
}

func createBatchPayload(writeKey string, receivedAt string, events []mockEventData) []byte {
	payloads := make([]string, 0)
	for _, event := range events {
		payloads = append(payloads, createMessagePayload(event))
	}
	batch := strings.Join(payloads, ",")
	return []byte(fmt.Sprintf(`{"writeKey": "%s", "batch": [%s], "requestIP": "1.2.3.4", "receivedAt": "%s"}`, writeKey, batch, receivedAt))
}

func assertJobStatus(job *jobsdb.JobT, status *jobsdb.JobStatusT, expectedState string, errorCode string, errorResponse string, attemptNum int) {
	Expect(status.JobID).To(Equal(job.JobID))
	Expect(status.JobState).To(Equal(expectedState))
	Expect(status.ErrorCode).To(Equal(errorCode))
	Expect(status.ErrorResponse).To(MatchJSON(errorResponse))
	Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
	Expect(status.AttemptNum).To(Equal(attemptNum))
}

func assertDestinationTransform(messages map[string]mockEventData, destinationID string, expectations transformExpectation) func(clientEvents []transformer.TransformerEventT, url string, batchSize int, breakIntoBatchWhenUserChanges bool) transformer.ResponseT {
	return func(clientEvents []transformer.TransformerEventT, url string, batchSize int, breakIntoBatchWhenUserChanges bool) transformer.ResponseT {
		destinationDefinitionName := expectations.destinationDefinitionName

		Expect(url).To(Equal(fmt.Sprintf("http://localhost:9090/v0/%s", destinationDefinitionName)))
		Expect(breakIntoBatchWhenUserChanges).To(BeFalse())

		Expect(clientEvents).To(HaveLen(expectations.events))

		messageIDs := make([]string, 0)
		for i := range clientEvents {
			event := clientEvents[i]
			messageID := event.Message["messageId"].(string)
			messageIDs = append(messageIDs, messageID)

			// Expect all messages belong to same destination
			Expect(event.Destination.ID).To(Equal(destinationID))

			// Expect metadata
			Expect(event.Metadata.DestinationID).To(Equal(destinationID))

			// Metadata are stripped from destination transform, is a user transform occurred before.
			if expectations.receiveMetadata {
				Expect(event.Metadata.DestinationType).To(Equal(fmt.Sprintf("%s-definition-name", destinationID)))
				Expect(event.Metadata.JobID).To(Equal(messages[messageID].jobid))
				Expect(event.Metadata.MessageID).To(Equal(messageID))
				Expect(event.Metadata.SourceID).To(Equal("")) // ???
			} else {
				// Expect(event.Metadata.DestinationType).To(Equal(""))
				Expect(event.Metadata.JobID).To(Equal(int64(0)))
				Expect(event.Metadata.MessageID).To(Equal(""))
				Expect(event.Metadata.SourceID).To(Equal("")) // ???
			}

			// Expect timestamp fields
			expectTimestamp := func(input string, expected time.Time) {
				inputTime, _ := time.Parse(misc.RFC3339Milli, input)
				Expect(inputTime).To(BeTemporally("~", expected, time.Second))
			}

			parseTimestamp := func(timestamp string) time.Time {
				parsed, ok := time.Parse(misc.RFC3339Milli, timestamp)
				if ok != nil {
					return time.Now()
				} else {
					return parsed
				}
			}

			receivedAt := parseTimestamp(messages[messageID].expectedReceivedAt)
			sentAt := parseTimestamp(messages[messageID].expectedSentAt)
			originalTimestamp := parseTimestamp(messages[messageID].expectedOriginalTimestamp)

			expectTimestamp(event.Message["receivedAt"].(string), receivedAt)
			expectTimestamp(event.Message["sentAt"].(string), sentAt)
			expectTimestamp(event.Message["originalTimestamp"].(string), originalTimestamp)
			expectTimestamp(event.Message["timestamp"].(string), misc.GetChronologicalTimeStamp(receivedAt, sentAt, originalTimestamp))

			// Expect message properties
			Expect(event.Message["some-property"].(string)).To(Equal(fmt.Sprintf("property-%s", messages[messageID].id)))

			if destinationID == "enabled-destination-b" {
				Expect(event.Message["user-transform"].(string)).To(Equal("value"))
			}
		}

		Expect(strings.Join(messageIDs, ",")).To(Equal(expectations.messageIds))

		return transformer.ResponseT{
			Events: []transformer.TransformerResponseT{
				{
					Output: map[string]interface{}{
						"int-value":    0,
						"string-value": fmt.Sprintf("value-%s", destinationID),
					},
					Metadata: transformer.MetadataT{
						SourceID:      "source-from-transformer",      // transformer should replay source id
						DestinationID: "destination-from-transformer", // transformer should replay destination id
					},
				},
				{
					Output: map[string]interface{}{
						"int-value":    1,
						"string-value": fmt.Sprintf("value-%s", destinationID),
					},
					Metadata: transformer.MetadataT{
						SourceID:      "source-from-transformer",      // transformer should replay source id
						DestinationID: "destination-from-transformer", // transformer should replay destination id
					},
				},
			},
		}
	}
}

func processorSetupAndAssertJobHandling(processor *HandleT, c *context) {
	processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, c.mockStats)
	processor.ResetDBReadBatchSize()

	// make sure the mock backend config has sent the configuration
	testutils.RunTestWithTimeout(func() {
		for !c.configInitialised {
			// a minimal sleep is required, to free this thread and allow scheduler to run other goroutines.
			time.Sleep(time.Nanosecond)
		}
	}, time.Second)

	didWork := processor.handlePendingGatewayJobs()
	Expect(didWork).To(Equal(true))
}
