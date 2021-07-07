package processor

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
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
	"github.com/rudderlabs/rudder-server/utils/types"

	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"

	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

var testTimeout = 10 * time.Second

type context struct {
	asyncHelper       testutils.AsyncTestHelper
	configInitialised bool
	dbReadBatchSize   int

	mockCtrl              *gomock.Controller
	mockBackendConfig     *mocksBackendConfig.MockBackendConfig
	mockGatewayJobsDB     *mocksJobsDB.MockJobsDB
	mockRouterJobsDB      *mocksJobsDB.MockJobsDB
	mockBatchRouterJobsDB *mocksJobsDB.MockJobsDB
	mockProcErrorsDB      *mocksJobsDB.MockJobsDB
}

func (c *context) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockGatewayJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)

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

	c.dbReadBatchSize = 10000

}

func (c *context) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

const (
	WriteKeyEnabled       = "enabled-write-key"
	WriteKeyEnabledNoUT   = "enabled-write-key-no-ut"
	WriteKeyEnabledOnlyUT = "enabled-write-key-only-ut"
	WorkspaceID           = "some-workspace-id"
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
var sampleBackendConfig = backendconfig.ConfigT{
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
							VersionID: "transformation-version-id",
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
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabledNoUT,
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
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabledOnlyUT,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
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
							VersionID: "transformation-version-id",
						},
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
		var clearDB = false
		It("should initialize (no jobs to recover)", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil)
		})

		It("should recover after crash", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil)
		})
	})

	Context("normal operation", func() {
		var clearDB = false
		BeforeEach(func() {
			// crash recovery check
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)
		})

		It("should only send proper stats, if not pending jobs are returned", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil)

			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: c.dbReadBatchSize}).Return(emptyJobsList).Times(1)
			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: c.dbReadBatchSize}).Return(emptyJobsList).Times(1).After(callRetry)

			var didWork = processor.handlePendingGatewayJobs()
			Expect(didWork).To(Equal(false))
		})

		It("should process ToRetry and Unprocessed jobs to destination without user transformation", func() {
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

			var unprocessedJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:          uuid.NewV4(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  createBatchPayload(WriteKeyEnabledNoUT, "2001-01-02T02:23:45.000Z", []mockEventData{messages["message-1"], messages["message-2"]}),
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
					EventPayload: createBatchPayload(WriteKeyEnabledNoUT, "2002-01-02T02:23:45.000Z", []mockEventData{messages["message-3"], messages["message-4"], messages["message-5"]}),
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

			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: c.dbReadBatchSize}).Return(toRetryJobsList).Times(1)
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: c.dbReadBatchSize - len(toRetryJobsList)}).Return(unprocessedJobsList).Times(1).After(callRetry)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledA: {
					events:                    3,
					messageIds:                "message-1,message-3,message-4",
					receiveMetadata:           true,
					destinationDefinitionName: "enabled-destination-a-definition-name",
				},
			}

			// We expect one transform call to destination A, after callUnprocessed.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).After(callUnprocessed).
				DoAndReturn(assertDestinationTransform(messages, DestinationIDEnabledA, transformExpectations[DestinationIDEnabledA]))

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":"%s"}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				Expect(string(job.Parameters)).To(Equal(`{"source_id":"source-from-transformer","destination_id":"destination-from-transformer","received_at":"","transform_at":"processor","message_id":"","gateway_job_id":0,"source_batch_id":"","source_task_id":"","source_task_run_id":"","source_job_id":"","source_job_run_id":""}`))
			}

			// One Store call is expected for all events
			callStoreRouter := c.mockRouterJobsDB.EXPECT().Store(gomock.Any()).Times(1).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-a")
					}
				})

			c.mockGatewayJobsDB.EXPECT().BeginGlobalTransaction().Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().AcquireUpdateJobStatusLocks()
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTxn(nil, gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreRouter).
				Do(func(txn *sql.Tx, statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different than order of jobs
					assertJobStatus(unprocessedJobsList[1], statuses[0], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1) // id 1002
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1) // id 1010
					assertJobStatus(toRetryJobsList[1], statuses[2], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2002
					assertJobStatus(toRetryJobsList[2], statuses[3], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2003
					assertJobStatus(toRetryJobsList[0], statuses[4], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2010
				})
			c.mockGatewayJobsDB.EXPECT().CommitTransaction(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().ReleaseUpdateJobStatusLocks().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}
			c.mockBackendConfig.EXPECT().GetWorkspaceIDForWriteKey(WriteKeyEnabledNoUT).Return(WorkspaceID).AnyTimes()
			c.mockBackendConfig.EXPECT().GetWorkspaceLibrariesForWorkspaceID(WorkspaceID).Return(backendconfig.LibrariesT{}).AnyTimes()
			processorSetupAndAssertJobHandling(processor, c)
		})

		It("should process ToRetry and Unprocessed jobs to destination with only user transformation", func() {
			var messages map[string]mockEventData = map[string]mockEventData{
				// this message should only be delivered to destination B
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
				// this message should not be delivered to destination B
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
					EventPayload:  createBatchPayload(WriteKeyEnabledOnlyUT, "2001-01-02T02:23:45.000Z", []mockEventData{messages["message-1"], messages["message-2"]}),
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
					EventPayload: createBatchPayload(WriteKeyEnabledOnlyUT, "2002-01-02T02:23:45.000Z", []mockEventData{messages["message-3"], messages["message-4"], messages["message-5"]}),
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

			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: c.dbReadBatchSize}).Return(toRetryJobsList).Times(1)
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: c.dbReadBatchSize - len(toRetryJobsList)}).Return(unprocessedJobsList).Times(1).After(callRetry)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledB: {
					events:                    3,
					messageIds:                "message-1,message-3,message-4",
					destinationDefinitionName: "minio",
				},
			}

			// We expect one call to user transform for destination B
			callUserTransform := mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).After(callUnprocessed).
				DoAndReturn(func(clientEvents []transformer.TransformerEventT, url string, batchSize int) transformer.ResponseT {
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

			// We expect one transform call to destination B, after user transform for destination B.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				After(callUserTransform).DoAndReturn(assertDestinationTransform(messages, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB]))

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				// Expect(job.CustomVal).To(Equal("destination-definition-name-a"))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":"%s"}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				Expect(string(job.Parameters)).To(Equal(`{"source_id":"source-from-transformer","destination_id":"destination-from-transformer","received_at":"","transform_at":"processor","message_id":"","gateway_job_id":0,"source_batch_id":"","source_task_id":"","source_task_run_id":"","source_job_id":"","source_job_run_id":""}`))
			}

			callStoreBatchRouter := c.mockBatchRouterJobsDB.EXPECT().Store(gomock.Any()).Times(1).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-b")
					}
				})

			c.mockGatewayJobsDB.EXPECT().BeginGlobalTransaction().Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().AcquireUpdateJobStatusLocks()
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTxn(nil, gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreBatchRouter).
				Do(func(txn *sql.Tx, statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different than order of jobs
					assertJobStatus(unprocessedJobsList[1], statuses[0], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1) // id 1002
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1) // id 1010
					assertJobStatus(toRetryJobsList[1], statuses[2], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2002
					assertJobStatus(toRetryJobsList[2], statuses[3], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2003
					assertJobStatus(toRetryJobsList[0], statuses[4], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)     // id 2010
				})
			c.mockGatewayJobsDB.EXPECT().CommitTransaction(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().ReleaseUpdateJobStatusLocks().Times(1)
			c.mockBackendConfig.EXPECT().GetWorkspaceIDForWriteKey(WriteKeyEnabledOnlyUT).Return(WorkspaceID).AnyTimes()
			c.mockBackendConfig.EXPECT().GetWorkspaceLibrariesForWorkspaceID(WorkspaceID).Return(backendconfig.LibrariesT{}).AnyTimes()
			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			processorSetupAndAssertJobHandling(processor, c)
		})
	})

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
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.CustomVal).To(Equal("enabled-destination-a-definition-name"))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))

				var paramsMap, expectedParamsMap map[string]interface{}
				json.Unmarshal(job.Parameters, &paramsMap)
				expectedStr := []byte(fmt.Sprintf(`{"source_id": "source-from-transformer", "destination_id": "enabled-destination-a", "source_job_run_id": "", "error": "error-%v", "status_code": 400, "stage": "dest_transformer"}`, i+1))
				json.Unmarshal(expectedStr, &expectedParamsMap)
				equals := reflect.DeepEqual(paramsMap, expectedParamsMap)
				Expect(equals).To(Equal(true))

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

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: c.dbReadBatchSize}).Return(toRetryJobsList).Times(1)
			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: c.dbReadBatchSize - len(toRetryJobsList)}).Return(unprocessedJobsList).Times(1).After(callRetry)
			// Test transformer failure
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Return(transformer.ResponseT{
					Events:       []transformer.TransformerResponseT{},
					FailedEvents: transformerResponses,
				})

			c.mockGatewayJobsDB.EXPECT().BeginGlobalTransaction().Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().AcquireUpdateJobStatusLocks()
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTxn(nil, gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(txn *sql.Tx, statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)
				})
			c.mockGatewayJobsDB.EXPECT().CommitTransaction(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().ReleaseUpdateJobStatusLocks().Times(1)

			// One Store call is expected for all events
			c.mockProcErrorsDB.EXPECT().Store(gomock.Any()).Times(1).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertErrStoreJob(job, i, "value-enabled-destination-a")
					}
				})
			c.mockBackendConfig.EXPECT().GetWorkspaceIDForWriteKey(WriteKeyEnabled).Return(WorkspaceID).AnyTimes()
			c.mockBackendConfig.EXPECT().GetWorkspaceLibrariesForWorkspaceID(WorkspaceID).Return(backendconfig.LibrariesT{}).AnyTimes()

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
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.CustomVal).To(Equal("MINIO"))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))

				var paramsMap, expectedParamsMap map[string]interface{}
				json.Unmarshal(job.Parameters, &paramsMap)
				expectedStr := []byte(`{"source_id": "source-from-transformer", "destination_id": "enabled-destination-b", "source_job_run_id": "", "error": "error-combined", "status_code": 400, "stage": "user_transformer"}`)
				json.Unmarshal(expectedStr, &expectedParamsMap)
				equals := reflect.DeepEqual(paramsMap, expectedParamsMap)
				Expect(equals).To(Equal(true))

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

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: c.dbReadBatchSize}).Return(toRetryJobsList).Times(1)
			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: c.dbReadBatchSize - len(toRetryJobsList)}).Return(unprocessedJobsList).Times(1).After(callRetry)

			// Test transformer failure
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Return(transformer.ResponseT{
					Events:       []transformer.TransformerResponseT{},
					FailedEvents: transformerResponses,
				})

			c.mockGatewayJobsDB.EXPECT().BeginGlobalTransaction().Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().AcquireUpdateJobStatusLocks()
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTxn(nil, gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(txn *sql.Tx, statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", `{"success":"OK"}`, 1)
				})
			c.mockGatewayJobsDB.EXPECT().CommitTransaction(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().ReleaseUpdateJobStatusLocks().Times(1)

			// One Store call is expected for all events
			c.mockProcErrorsDB.EXPECT().Store(gomock.Any()).Times(1).
				Do(func(jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(1))
					for _, job := range jobs {
						assertErrStoreJob(job)
					}
				})
			c.mockBackendConfig.EXPECT().GetWorkspaceIDForWriteKey(WriteKeyEnabled).Return(WorkspaceID).AnyTimes()
			c.mockBackendConfig.EXPECT().GetWorkspaceLibrariesForWorkspaceID(WorkspaceID).Return(backendconfig.LibrariesT{}).AnyTimes()
			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			processorSetupAndAssertJobHandling(processor, c)
		})
	})

	Context("Pause and Resume Function Tests", func() {
		var clearDB = false
		It("Should Recieve Something on Pause when Processor Is Not Paused", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil)

			go processor.Pause()
			setProcessorPausedVariable(processor, false)
			Eventually(processor.pauseChannel).Should(Receive())
		})

		It("Should Not Recieve Something on Pause when Processor Is Paused", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil)

			go processor.Pause()
			setProcessorPausedVariable(processor, true)
			Eventually(processor.pauseChannel).ShouldNot(Receive())
		})

		It("Should Recieve Something on Resume when Processor is Paused", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil)

			go processor.Resume()
			setProcessorPausedVariable(processor, true)
			Eventually(processor.resumeChannel).Should(Receive())
		})

		It("Should Not Recieve Something on Resume when Processor is Not Paused", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil)

			go processor.Resume()
			setProcessorPausedVariable(processor, false)
			Eventually(processor.resumeChannel).ShouldNot(Receive())
		})
	})

	Context("MainLoop Tests", func() {
		var clearDB = false
		It("Should be paused when recieved something on Pause Channel", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil)
			SetMainLoopTimeout(1 * time.Second)
			go pauseMainLoop(processor)
			go processor.mainLoop()
			go resumeMainLoop(processor)
			time.Sleep(1 * time.Second)
			Expect(processor.paused).To(BeTrue())
		})

		It("Should sleep when Locked and recieved nothing on Pause Channel", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			var processor *HandleT = &HandleT{
				transformer: mockTransformer,
			}

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, Count: -1}).Times(1)
			SetFeaturesRetryAttempts(0)
			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil)
			SetMainLoopTimeout(1 * time.Second)
			go processor.mainLoop()
			time.Sleep(3 * time.Second)
			Expect(processor.paused).To(BeFalse())
		})
	})

})

var _ = Describe("Static Function Tests", func() {

	Context("TransformerFormatResponse Tests", func() {
		It("Should match ConvertToTransformerResponse ", func() {
			var events = []transformer.TransformerEventT{
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"some-key-1": "some-value-1",
					},
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
					},
				},
			}
			var expectedResponses = transformer.ResponseT{
				Events: []transformer.TransformerResponseT{
					{
						Output: map[string]interface{}{
							"some-key-1": "some-value-1",
						},
						StatusCode: 200,
						Metadata: transformer.MetadataT{
							MessageID: "message-1",
						},
					},
					{
						Output: map[string]interface{}{
							"some-key-2": "some-value-2",
						},
						StatusCode: 200,
						Metadata: transformer.MetadataT{
							MessageID: "message-2",
						},
					},
				},
			}
			response := ConvertToTransformerResponse(events)
			Expect(response.Events[0].StatusCode).To(Equal(expectedResponses.Events[0].StatusCode))
			Expect(response.Events[0].Metadata.MessageID).To(Equal(expectedResponses.Events[0].Metadata.MessageID))
			Expect(response.Events[0].Output["some-key-1"]).To(Equal(expectedResponses.Events[0].Output["some-key-1"]))
			Expect(response.Events[1].StatusCode).To(Equal(expectedResponses.Events[1].StatusCode))
			Expect(response.Events[1].Metadata.MessageID).To(Equal(expectedResponses.Events[1].Metadata.MessageID))
			Expect(response.Events[1].Output["some-key-2"]).To(Equal(expectedResponses.Events[1].Output["some-key-2"]))
		})
	})

	Context("getDiffMetrics Tests", func() {
		It("Should match diffMetrics response for Empty Inputs", func() {
			inCountMetadataMap := make(map[string]MetricMetadata, 0)
			inCountMap := make(map[string]int64, 0)
			successCountMap := make(map[string]int64, 0)
			failedCountMap := make(map[string]int64, 0)
			response := getDiffMetrics("some-string-1", "some-string-2", inCountMetadataMap, inCountMap, successCountMap, failedCountMap)
			Expect(len(response)).To(Equal(0))
		})

		It("Should match diffMetrics response for Valid Inputs", func() {
			inCountMetadataMap := map[string]MetricMetadata{
				"some-key-1": {
					sourceID:        "some-source-id-1",
					destinationID:   "some-destination-id-1",
					sourceJobRunID:  "some-source-job-run-id-1",
					sourceJobID:     "some-source-job-id-1",
					sourceTaskID:    "some-source-task-id-1",
					sourceTaskRunID: "some-source-task-run-id-1",
					sourceBatchID:   "some-source-batch-id-1",
				},
				"some-key-2": {
					sourceID:        "some-source-id-2",
					destinationID:   "some-destination-id-2",
					sourceJobRunID:  "some-source-job-run-id-2",
					sourceJobID:     "some-source-job-id-2",
					sourceTaskID:    "some-source-task-id-2",
					sourceTaskRunID: "some-source-task-run-id-2",
					sourceBatchID:   "some-source-batch-id-2",
				},
			}

			inCountMap := map[string]int64{
				"some-key-1": 3,
				"some-key-2": 4,
			}
			successCountMap := map[string]int64{
				"some-key-1": 5,
				"some-key-2": 6,
			}
			failedCountMap := map[string]int64{
				"some-key-1": 1,
				"some-key-2": 2,
			}

			expectedResponse := []types.PUReportedMetric{
				{
					ConnectionDetails: types.ConnectionDetails{
						SourceID:        "some-source-id-1",
						DestinationID:   "some-destination-id-1",
						SourceBatchID:   "some-source-batch-id-1",
						SourceTaskID:    "some-source-task-id-1",
						SourceTaskRunID: "some-source-task-run-id-1",
						SourceJobID:     "some-source-job-id-1",
						SourceJobRunID:  "some-source-job-run-id-1",
					},
					PUDetails: types.PUDetails{
						InPU:       "some-string-1",
						PU:         "some-string-2",
						TerminalPU: false,
						InitialPU:  false,
					},
					StatusDetail: &types.StatusDetail{
						Status:         "diff",
						Count:          3,
						StatusCode:     0,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
					},
				},
				{
					ConnectionDetails: types.ConnectionDetails{
						SourceID:        "some-source-id-2",
						DestinationID:   "some-destination-id-2",
						SourceBatchID:   "some-source-batch-id-2",
						SourceTaskID:    "some-source-task-id-2",
						SourceTaskRunID: "some-source-task-run-id-2",
						SourceJobID:     "some-source-job-id-2",
						SourceJobRunID:  "some-source-job-run-id-2",
					},
					PUDetails: types.PUDetails{
						InPU:       "some-string-1",
						PU:         "some-string-2",
						TerminalPU: false,
						InitialPU:  false,
					},
					StatusDetail: &types.StatusDetail{
						Status:         "diff",
						Count:          4,
						StatusCode:     0,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
					},
				},
			}

			response := getDiffMetrics("some-string-1", "some-string-2", inCountMetadataMap, inCountMap, successCountMap, failedCountMap)
			Expect(len(response)).To(Equal(2))
			Expect(response[0].ConnectionDetails.SourceID).To(Equal(expectedResponse[0].ConnectionDetails.SourceID))
			Expect(response[0].ConnectionDetails.DestinationID).To(Equal(expectedResponse[0].ConnectionDetails.DestinationID))
			Expect(response[0].ConnectionDetails.SourceBatchID).To(Equal(expectedResponse[0].ConnectionDetails.SourceBatchID))
			Expect(response[0].ConnectionDetails.SourceTaskID).To(Equal(expectedResponse[0].ConnectionDetails.SourceTaskID))
			Expect(response[0].ConnectionDetails.SourceTaskRunID).To(Equal(expectedResponse[0].ConnectionDetails.SourceTaskRunID))
			Expect(response[0].ConnectionDetails.SourceJobID).To(Equal(expectedResponse[0].ConnectionDetails.SourceJobID))
			Expect(response[0].ConnectionDetails.SourceJobRunID).To(Equal(expectedResponse[0].ConnectionDetails.SourceJobRunID))

			Expect(response[1].ConnectionDetails.SourceID).To(Equal(expectedResponse[1].ConnectionDetails.SourceID))
			Expect(response[1].ConnectionDetails.DestinationID).To(Equal(expectedResponse[1].ConnectionDetails.DestinationID))
			Expect(response[1].ConnectionDetails.SourceBatchID).To(Equal(expectedResponse[1].ConnectionDetails.SourceBatchID))
			Expect(response[1].ConnectionDetails.SourceTaskID).To(Equal(expectedResponse[1].ConnectionDetails.SourceTaskID))
			Expect(response[1].ConnectionDetails.SourceTaskRunID).To(Equal(expectedResponse[1].ConnectionDetails.SourceTaskRunID))
			Expect(response[1].ConnectionDetails.SourceJobID).To(Equal(expectedResponse[1].ConnectionDetails.SourceJobID))
			Expect(response[1].ConnectionDetails.SourceJobRunID).To(Equal(expectedResponse[1].ConnectionDetails.SourceJobRunID))

			Expect(response[0].PUDetails.InPU).To(Equal(expectedResponse[0].PUDetails.InPU))
			Expect(response[0].PUDetails.PU).To(Equal(expectedResponse[0].PUDetails.PU))
			Expect(response[0].PUDetails.TerminalPU).To(Equal(expectedResponse[0].PUDetails.TerminalPU))
			Expect(response[0].PUDetails.InitialPU).To(Equal(expectedResponse[0].PUDetails.InitialPU))

			Expect(response[1].PUDetails.InPU).To(Equal(expectedResponse[1].PUDetails.InPU))
			Expect(response[1].PUDetails.PU).To(Equal(expectedResponse[1].PUDetails.PU))
			Expect(response[1].PUDetails.TerminalPU).To(Equal(expectedResponse[1].PUDetails.TerminalPU))
			Expect(response[1].PUDetails.InitialPU).To(Equal(expectedResponse[1].PUDetails.InitialPU))

			Expect(response[0].StatusDetail.Status).To(Equal(expectedResponse[0].StatusDetail.Status))
			Expect(response[0].StatusDetail.Count).To(Equal(expectedResponse[0].StatusDetail.Count))
			Expect(response[0].StatusDetail.StatusCode).To(Equal(expectedResponse[0].StatusDetail.StatusCode))

			Expect(response[1].StatusDetail.Status).To(Equal(expectedResponse[1].StatusDetail.Status))
			Expect(response[1].StatusDetail.Count).To(Equal(expectedResponse[1].StatusDetail.Count))
			Expect(response[1].StatusDetail.StatusCode).To(Equal(expectedResponse[1].StatusDetail.StatusCode))

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

func setProcessorPausedVariable(processor *HandleT, setValue bool) {
	processor.paused = setValue
}

func pauseMainLoop(processor *HandleT) {
	processor.pauseChannel <- &PauseT{respChannel: make(chan bool)}
}

func resumeMainLoop(processor *HandleT) {
	processor.resumeChannel <- true
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
	Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
	Expect(status.AttemptNum).To(Equal(attemptNum))
}

func assertDestinationTransform(messages map[string]mockEventData, destinationID string, expectations transformExpectation) func(clientEvents []transformer.TransformerEventT, url string, batchSize int) transformer.ResponseT {
	return func(clientEvents []transformer.TransformerEventT, url string, batchSize int) transformer.ResponseT {
		destinationDefinitionName := expectations.destinationDefinitionName

		Expect(url).To(Equal(fmt.Sprintf("http://localhost:9090/v0/%s", destinationDefinitionName)))

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
	var clearDB = false
	SetDisableDedupFeature(false)
	processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil)

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
