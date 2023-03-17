package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"

	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"
	mockDedup "github.com/rudderlabs/rudder-server/mocks/services/dedup"
	mocksMultitenant "github.com/rudderlabs/rudder-server/mocks/services/multitenant"
	mockReportingTypes "github.com/rudderlabs/rudder-server/mocks/utils/types"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/isolation"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type testContext struct {
	dbReadBatchSize  int
	processEventSize int

	mockCtrl              *gomock.Controller
	mockBackendConfig     *mocksBackendConfig.MockBackendConfig
	mockGatewayJobsDB     *mocksJobsDB.MockJobsDB
	mockRouterJobsDB      *mocksJobsDB.MockJobsDB
	mockBatchRouterJobsDB *mocksJobsDB.MockJobsDB
	mockProcErrorsDB      *mocksJobsDB.MockJobsDB
	MockReportingI        *mockReportingTypes.MockReportingI
	MockDedup             *mockDedup.MockDedupI
	MockMultitenantHandle *mocksMultitenant.MockMultiTenantI
	MockRsourcesService   *rsources.MockJobService
}

func (c *testContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockGatewayJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.MockRsourcesService = rsources.NewMockJobService(c.mockCtrl)

	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{sampleWorkspaceID: sampleBackendConfig}, Topic: string(topic)}
			close(ch)
			return ch
		})
	c.dbReadBatchSize = 10000
	c.processEventSize = 10000
	c.MockReportingI = mockReportingTypes.NewMockReportingI(c.mockCtrl)
	c.MockDedup = mockDedup.NewMockDedupI(c.mockCtrl)
	c.MockMultitenantHandle = mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)
}

func (c *testContext) Finish() {
	c.mockCtrl.Finish()
}

const (
	WriteKeyEnabled       = "enabled-write-key"
	WriteKeyEnabledNoUT   = "enabled-write-key-no-ut"
	WriteKeyEnabledNoUT2  = "enabled-write-key-no-ut2"
	WriteKeyEnabledOnlyUT = "enabled-write-key-only-ut"
	SourceIDEnabled       = "enabled-source"
	SourceIDEnabledNoUT   = "enabled-source-no-ut"
	SourceIDEnabledOnlyUT = "enabled-source-only-ut"
	SourceIDEnabledNoUT2  = "enabled-source-no-ut2"
	SourceIDDisabled      = "disabled-source"
	DestinationIDEnabledA = "enabled-destination-a" // test destination router
	DestinationIDEnabledB = "enabled-destination-b" // test destination batch router
	DestinationIDEnabledC = "enabled-destination-c"
	DestinationIDDisabled = "disabled-destination"

	SourceID3 = "source-id-3"
	WriteKey3 = "write-key-3"
	DestID1   = "dest-id-1"
	DestID2   = "dest-id-2"
	DestID3   = "dest-id-3"
	DestID4   = "dest-id-4"
)

var (
	gatewayCustomVal = []string{"GW"}
	emptyJobsList    []*jobsdb.JobT
)

// setEnableEventSchemasFeature overrides enableEventSchemasFeature configuration and returns previous value
func setEnableEventSchemasFeature(proc *Handle, b bool) bool {
	prev := proc.config.enableEventSchemasFeature
	proc.config.enableEventSchemasFeature = b
	return prev
}

// SetDisableDedupFeature overrides SetDisableDedupFeature configuration and returns previous value
func setDisableDedupFeature(proc *Handle, b bool) bool {
	prev := proc.config.enableDedup
	proc.config.enableDedup = b
	return prev
}

func setMainLoopTimeout(proc *Handle, timeout time.Duration) {
	proc.config.mainLoopTimeout = timeout
}

var sampleWorkspaceID = "some-workspace-id"

// This configuration is assumed by all processor tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = backendconfig.ConfigT{
	WorkspaceID: sampleWorkspaceID,
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
				{
					ID:                 DestinationIDEnabledC,
					Name:               "C",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-c-definition-id",
						Name:        "WEBHOOK",
						DisplayName: "enabled-destination-c-definition-display-name",
						Config:      map[string]interface{}{"transformAtV1": "none"},
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
			ID:       SourceIDEnabledNoUT,
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
			ID:       SourceIDEnabledOnlyUT,
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
		{
			ID:       SourceIDEnabledNoUT2,
			WriteKey: WriteKeyEnabledNoUT2,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledA,
					Name:               "A",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
						Config: map[string]interface{}{
							"supportedMessageTypes": []interface{}{"identify", "track"},
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
			ID:          SourceID3,
			WriteKey:    WriteKey3,
			WorkspaceID: sampleWorkspaceID,
			Enabled:     true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestID1,
					Name:               "D1",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": "category1"},
							map[string]interface{}{"oneTrustCookieCategory": "category2"},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 DestID2,
					Name:               "D2",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": ""},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 DestID3,
					Name:               "D3",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 DestID4,
					Name:               "D4",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": "category2"},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
	},
}

func initProcessor() {
	config.Reset()
	logger.Reset()
	stash.Init()
	admin.Init()
	dedup.Init()
	misc.Init()
	integrations.Init()
}

var _ = Describe("Processor", Ordered, func() {
	initProcessor()

	var c *testContext
	transformerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"routerTransform": {}}`))
		w.WriteHeader(http.StatusOK)
	}))

	prepareHandle := func(proc *Handle) *Handle {
		proc.config.transformerURL = transformerServer.URL
		setEnableEventSchemasFeature(proc, false)
		isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
		Expect(err).To(BeNil())
		proc.isolationStrategy = isolationStrategy
		return proc
	}
	BeforeEach(func() {
		c = &testContext{}
		c.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	AfterAll(func() {
		transformerServer.Close()
	})

	Context("Initialization", func() {
		clearDB := false
		It("should initialize (no jobs to recover)", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			processor := prepareHandle(NewHandle(mockTransformer))

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil, c.MockMultitenantHandle, transientsource.NewEmptyService(), fileuploader.NewDefaultProvider(), c.MockRsourcesService, destinationdebugger.NewNoOpService(), transformationdebugger.NewNoOpService())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
		})

		It("should recover after crash", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			processor := prepareHandle(NewHandle(mockTransformer))

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil, c.MockMultitenantHandle, transientsource.NewEmptyService(), fileuploader.NewDefaultProvider(), c.MockRsourcesService, destinationdebugger.NewNoOpService(), transformationdebugger.NewNoOpService())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
		})
	})

	Context("normal operation", func() {
		clearDB := false
		BeforeEach(func() {
			// crash recovery check
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)
		})

		It("should only send proper stats, if not pending jobs are returned", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			processor := prepareHandle(NewHandle(mockTransformer))

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, c.MockReportingI, c.MockMultitenantHandle, transientsource.NewEmptyService(), fileuploader.NewDefaultProvider(), c.MockRsourcesService, destinationdebugger.NewNoOpService(), transformationdebugger.NewNoOpService())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())

			payloadLimit := processor.payloadLimit
			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParamsT{CustomValFilters: gatewayCustomVal, JobsLimit: c.dbReadBatchSize, EventsLimit: c.processEventSize, PayloadSizeLimit: payloadLimit}).Return(jobsdb.JobsResult{Jobs: emptyJobsList}, nil).Times(1)

			didWork := processor.handlePendingGatewayJobs("")
			Expect(didWork).To(Equal(false))
		})

		It("should process unprocessed jobs to destination without user transformation", func() {
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
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(WriteKeyEnabledNoUT, "2001-01-02T02:23:45.000Z", []mockEventData{
						messages["message-1"],
						messages["message-2"],
					}),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT),
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.New(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(WriteKeyEnabledNoUT, "2002-01-02T02:23:45.000Z", []mockEventData{
						messages["message-3"],
						messages["message-4"],
						messages["message-5"],
					}),
					EventCount: 3,
					Parameters: createBatchParameters(SourceIDEnabledNoUT),
				},
			}
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			payloadLimit := 100 * bytesize.MB
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParamsT{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        c.dbReadBatchSize,
				EventsLimit:      c.processEventSize,
				PayloadSizeLimit: payloadLimit,
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledA: {
					events:                    3,
					messageIds:                "message-1,message-3,message-4",
					receiveMetadata:           true,
					destinationDefinitionName: "enabled-destination-a-definition-name",
				},
			}

			// We expect one transform call to destination A, after callUnprocessed.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).After(callUnprocessed).
				DoAndReturn(assertDestinationTransform(messages, SourceIDEnabledNoUT, DestinationIDEnabledA, transformExpectations[DestinationIDEnabledA]))

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":%q}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				Expect(string(job.Parameters)).To(Equal(`{"source_id":"source-from-transformer","destination_id":"destination-from-transformer","received_at":"","transform_at":"processor","message_id":"","gateway_job_id":0,"source_task_run_id":"","source_job_id":"","source_job_run_id":"","event_name":"","event_type":"","source_definition_id":"","destination_definition_id":"","source_category":"","record_id":null,"workspaceId":""}`))
			}
			// One Store call is expected for all events
			c.mockRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			callStoreRouter := c.mockRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-a")
					}
				})

			c.MockMultitenantHandle.EXPECT().ReportProcLoopAddStats(gomock.Any(), gomock.Any()).Times(1)

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreRouter).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different from order of jobs
					for i := range unprocessedJobsList {
						assertJobStatus(unprocessedJobsList[i], statuses[i], jobsdb.Succeeded.State)
					}
				})
			processor := prepareHandle(NewHandle(mockTransformer))

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("should process unprocessed jobs to destination with only user transformation", func() {
			messages := map[string]mockEventData{
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

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(WriteKeyEnabledOnlyUT, "2001-01-02T02:23:45.000Z", []mockEventData{
						messages["message-1"],
						messages["message-2"],
					}),
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledOnlyUT),
				},
				{
					UUID:         uuid.New(),
					JobID:        2002,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: nil,
					EventCount:   1,
					Parameters:   nil,
				},
				{
					UUID:         uuid.New(),
					JobID:        2003,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: nil,
					EventCount:   1,
					Parameters:   nil,
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(WriteKeyEnabledOnlyUT, "2002-01-02T02:23:45.000Z", []mockEventData{
						messages["message-3"],
						messages["message-4"],
						messages["message-5"],
					}),
					EventCount: 1,
					Parameters: createBatchParameters(SourceIDEnabledOnlyUT),
				},
			}

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			payloadLimit := 100 * bytesize.MB
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParamsT{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        c.dbReadBatchSize,
				EventsLimit:      c.processEventSize,
				PayloadSizeLimit: payloadLimit,
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledB: {
					events:                    3,
					messageIds:                "message-1,message-3,message-4",
					destinationDefinitionName: "minio",
				},
			}

			// We expect one call to user transform for destination B
			callUserTransform := mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).After(callUnprocessed).
				DoAndReturn(func(ctx context.Context, clientEvents []transformer.TransformerEventT, url string, batchSize int) transformer.ResponseT {
					defer GinkgoRecover()

					Expect(url).To(Equal("http://localhost:9090/customTransform"))

					outputEvents := make([]transformer.TransformerResponseT, 0)

					for _, event := range clientEvents {
						event.Message["user-transform"] = "value"
						outputEvents = append(outputEvents, transformer.TransformerResponseT{
							Output: event.Message,
						})
					}

					return transformer.ResponseT{
						Events: outputEvents,
					}
				})

			// We expect one transform call to destination B, after user transform for destination B.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				After(callUserTransform).DoAndReturn(assertDestinationTransform(messages, SourceIDEnabledOnlyUT, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB]))

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				// Expect(job.CustomVal).To(Equal("destination-definition-name-a"))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":%q}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				Expect(string(job.Parameters)).To(Equal(`{"source_id":"source-from-transformer","destination_id":"destination-from-transformer","received_at":"","transform_at":"processor","message_id":"","gateway_job_id":0,"source_task_run_id":"","source_job_id":"","source_job_run_id":"","event_name":"","event_type":"","source_definition_id":"","destination_definition_id":"","source_category":"","record_id":null,"workspaceId":""}`))
			}

			c.mockBatchRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			callStoreBatchRouter := c.mockBatchRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-b")
					}
				})

			c.MockMultitenantHandle.EXPECT().ReportProcLoopAddStats(gomock.Any(), gomock.Any()).Times(1)

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreBatchRouter).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					for i := range unprocessedJobsList {
						assertJobStatus(unprocessedJobsList[i], statuses[i], jobsdb.Succeeded.State)
					}
				})

			processor := prepareHandle(NewHandle(mockTransformer))

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("should process unprocessed jobs to destination without user transformation with enabled Dedup", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-some-id-1": {
					id:                        "some-id",
					jobid:                     2010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-03-02T01:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-c-definition-display-name": true},
				},
				// this message should not be delivered to destination A
				"message-some-id-2": {
					id:                        "some-id",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-c-definition-display-name": true},
				},
				"message-some-id-3": {
					id:                        "some-id",
					jobid:                     3010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-03-02T01:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-c-definition-display-name": true},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayloadWithSameMessageId(WriteKeyEnabled, "2001-01-02T02:23:45.000Z", []mockEventData{
						messages["message-some-id-2"],
						messages["message-some-id-1"],
					}),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayloadWithSameMessageId(WriteKeyEnabled, "2002-01-02T02:23:45.000Z", []mockEventData{
						messages["message-some-id-3"],
					}),
					EventCount: 1,
					Parameters: createBatchParameters(SourceIDEnabled),
				},
			}

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), gomock.Any()).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)
			c.MockDedup.EXPECT().FindDuplicates(gomock.Any(), gomock.Any()).Return([]int{1}).After(callUnprocessed).Times(2)
			c.MockDedup.EXPECT().MarkProcessed(gomock.Any()).Times(1)

			// We expect one transform call to destination A, after callUnprocessed.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0).After(callUnprocessed)
			// One Store call is expected for all events
			c.mockRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil).Times(1)
			callStoreRouter := c.mockRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Len(2)).Times(1)

			c.MockMultitenantHandle.EXPECT().ReportProcLoopAddStats(gomock.Any(), gomock.Any()).Times(1)

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreRouter)
			processor := prepareHandle(NewHandle(mockTransformer))

			Setup(processor, c, true, false)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())

			processor.dedupHandler = c.MockDedup
			processor.multitenantI = c.MockMultitenantHandle
			handlePendingGatewayJobs(processor)
		})
	})

	Context("transformations", func() {
		It("messages should be skipped on destination transform failures, without failing the job", func() {
			messages := map[string]mockEventData{
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

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  createBatchPayload(WriteKeyEnabled, "2001-01-02T02:23:45.000Z", []mockEventData{messages["message-1"], messages["message-2"]}),
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
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

			assertErrStoreJob := func(job *jobsdb.JobT, i int) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.CustomVal).To(Equal("enabled-destination-a-definition-name"))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))

				var paramsMap, expectedParamsMap map[string]interface{}
				err := json.Unmarshal(job.Parameters, &paramsMap)
				Expect(err).To(BeNil())
				expectedStr := []byte(fmt.Sprintf(`{"source_id": "%v", "destination_id": "enabled-destination-a", "source_job_run_id": "", "error": "error-%v", "status_code": 400, "stage": "dest_transformer", "source_task_run_id": "", "record_id": null}`, SourceIDEnabled, i+1))
				err = json.Unmarshal(expectedStr, &expectedParamsMap)
				Expect(err).To(BeNil())
				equals := reflect.DeepEqual(paramsMap, expectedParamsMap)
				Expect(equals).To(Equal(true))

				// compare payloads
				var payload []map[string]interface{}
				err = json.Unmarshal(job.EventPayload, &payload)
				Expect(err).To(BeNil())
				Expect(len(payload)).To(Equal(1))
				message := messages[fmt.Sprintf(`message-%v`, i+1)]
				Expect(fmt.Sprintf(`message-%s`, message.id)).To(Equal(payload[0]["messageId"]))
				Expect(payload[0]["some-property"]).To(Equal(fmt.Sprintf(`property-%s`, message.id)))
				Expect(message.expectedOriginalTimestamp).To(Equal(payload[0]["originalTimestamp"]))
			}

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			payloadLimit := 100 * bytesize.MB
			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParamsT{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        c.dbReadBatchSize,
				EventsLimit:      c.processEventSize,
				PayloadSizeLimit: payloadLimit,
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)
			// Test transformer failure
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Return(transformer.ResponseT{
					Events:       []transformer.TransformerResponseT{},
					FailedEvents: transformerResponses,
				})

			c.MockMultitenantHandle.EXPECT().ReportProcLoopAddStats(gomock.Any(), gomock.Any()).Times(0)

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State)
				})

			// will be used to save failed events to failed keys table
			c.mockProcErrorsDB.EXPECT().WithTx(gomock.Any()).Do(func(f func(tx *jobsdb.Tx) error) {
				_ = f(&jobsdb.Tx{})
			}).Times(1)

			// One Store call is expected for all events
			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertErrStoreJob(job, i)
					}
				})

			processor := prepareHandle(NewHandle(mockTransformer))

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("messages should be skipped on user transform failures, without failing the job", func() {
			messages := map[string]mockEventData{
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

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  createBatchPayload(WriteKeyEnabled, "2001-01-02T02:23:45.000Z", []mockEventData{messages["message-1"], messages["message-2"]}),
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
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
				err := json.Unmarshal(job.Parameters, &paramsMap)
				Expect(err).To(BeNil())
				expectedStr := []byte(fmt.Sprintf(`{"source_id": "%v", "destination_id": "enabled-destination-b", "source_job_run_id": "", "error": "error-combined", "status_code": 400, "stage": "user_transformer", "source_task_run_id":"", "record_id": null}`, SourceIDEnabled))
				err = json.Unmarshal(expectedStr, &expectedParamsMap)
				Expect(err).To(BeNil())
				equals := reflect.DeepEqual(paramsMap, expectedParamsMap)
				Expect(equals).To(Equal(true))

				// compare payloads
				var payload []map[string]interface{}
				err = json.Unmarshal(job.EventPayload, &payload)
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

			var toRetryJobsList []*jobsdb.JobT

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			payloadLimit := 100 * bytesize.MB
			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParamsT{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        c.dbReadBatchSize,
				EventsLimit:      c.processEventSize,
				PayloadSizeLimit: payloadLimit,
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			// Test transformer failure
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Return(transformer.ResponseT{
					Events:       []transformer.TransformerResponseT{},
					FailedEvents: transformerResponses,
				})

			c.MockMultitenantHandle.EXPECT().ReportProcLoopAddStats(gomock.Any(), gomock.Any()).Times(0)

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State)
				})

			c.mockProcErrorsDB.EXPECT().WithTx(gomock.Any()).Do(func(f func(tx *jobsdb.Tx) error) {
				_ = f(&jobsdb.Tx{})
			}).Return(nil).Times(1)

			// One Store call is expected for all events
			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(1))
					for _, job := range jobs {
						assertErrStoreJob(job)
					}
				})

			processor := prepareHandle(NewHandle(mockTransformer))

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("should drop messages that the destination doesn't support", func() {
			messages := map[string]mockEventData{
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
			}
			payload := createBatchPayload(WriteKeyEnabledNoUT2, "2001-01-02T02:23:45.000Z", []mockEventData{messages["message-1"]})
			payload, _ = sjson.SetBytes(payload, "batch.0.type", "identify")

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  payload,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT2),
				},
			}

			var toRetryJobsList []*jobsdb.JobT

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			payloadLimit := 100 * bytesize.MB
			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParamsT{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        c.dbReadBatchSize,
				EventsLimit:      c.processEventSize,
				PayloadSizeLimit: payloadLimit,
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			// Test transformer failure
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Len(0), gomock.Any(), gomock.Any()).Times(0)

			c.MockMultitenantHandle.EXPECT().ReportProcLoopAddStats(gomock.Any(), gomock.Any()).Times(0)

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State)
				})

			c.mockProcErrorsDB.EXPECT().WithTx(gomock.Any()).Do(func(f func(tx *jobsdb.Tx) error) {
				_ = f(&jobsdb.Tx{})
			}).Return(nil).Times(0)

			// One Store call is expected for all events
			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(0).
				Do(func(ctx context.Context, jobs []*jobsdb.JobT) {})

			processor := prepareHandle(NewHandle(mockTransformer))

			processorSetupAndAssertJobHandling(processor, c)
		})
	})

	Context("MainLoop Tests", func() {
		clearDB := false
		It("Should not handle jobs when transformer features are not set", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			processor := prepareHandle(NewHandle(mockTransformer))

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)
			processor.config.featuresRetryMaxAttempts = 0
			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, nil, c.MockMultitenantHandle, transientsource.NewEmptyService(), fileuploader.NewDefaultProvider(), c.MockRsourcesService, destinationdebugger.NewNoOpService(), transformationdebugger.NewNoOpService())

			setMainLoopTimeout(processor, 1*time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			c.mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)
			c.mockProcErrorsDB.EXPECT().FailExecuting().Times(1)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				err := processor.Start(ctx)
				Expect(err).To(BeNil())
				wg.Done()
			}()

			Consistently(func() bool {
				select {
				case <-processor.config.asyncInit.Wait():
					return true
				default:
					return false
				}
			}, 2*time.Second, 10*time.Millisecond).Should(BeFalse())
			wg.Wait()
		})
	})

	Context("ProcessorLoop Tests", func() {
		clearDB := false
		It("Should not handle jobs when transformer features are not set", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			processor := prepareHandle(NewHandle(mockTransformer))

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)
			processor.config.featuresRetryMaxAttempts = 0
			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, c.MockReportingI, c.MockMultitenantHandle, transientsource.NewEmptyService(), fileuploader.NewDefaultProvider(), c.MockRsourcesService, destinationdebugger.NewNoOpService(), transformationdebugger.NewNoOpService())
			defer processor.Shutdown()
			c.MockReportingI.EXPECT().WaitForSetup(gomock.Any(), gomock.Any()).Times(1)

			processor.config.readLoopSleep = time.Millisecond

			c.mockProcErrorsDB.EXPECT().FailExecuting()
			c.mockProcErrorsDB.EXPECT().GetToRetry(gomock.Any(), gomock.Any()).Return(jobsdb.JobsResult{}, nil).AnyTimes()
			c.mockProcErrorsDB.EXPECT().GetUnprocessed(gomock.Any(), gomock.Any()).Return(jobsdb.JobsResult{}, nil).AnyTimes()
			c.mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)

			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), gomock.Any()).Times(0)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			Expect(processor.Start(ctx)).To(BeNil())
		})
	})
	Context("isDestinationEnabled", func() {
		It("should filter based on consent management preferences", func() {
			event := types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []interface{}{"category1", "someOtherCategory"},
					},
				},
				"type":      "track",
				"channel":   "android-srk",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"lbael":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": float64(4.51),
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			}
			_, err := json.Marshal(event)
			Expect(err).To(BeNil())

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Setup().Times(1)

			processor := prepareHandle(NewHandle(mockTransformer))

			Setup(processor, c, false, false)
			processor.multitenantI = c.MockMultitenantHandle
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())

			Expect(
				len(processor.filterDestinations(
					event,
					processor.getEnabledDestinations(
						WriteKey3,
						"destination-definition-name-enabled",
					),
				)),
			).To(Equal(3)) // all except dest-1
			Expect(processor.isDestinationAvailable(event, WriteKey3)).To(BeTrue())
		})
	})
})

var _ = Describe("Static Function Tests", func() {
	initProcessor()

	Context("TransformerFormatResponse Tests", func() {
		It("Should match ConvertToTransformerResponse without filtering", func() {
			events := []transformer.TransformerEventT{
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
			expectedResponses := transformer.ResponseT{
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
			response := ConvertToFilteredTransformerResponse(events, false)
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
			response := getDiffMetrics("some-string-1", "some-string-2", map[string]MetricMetadata{}, map[string]int64{}, map[string]int64{}, map[string]int64{})
			Expect(len(response)).To(Equal(0))
		})

		It("Should match diffMetrics response for Valid Inputs", func() {
			inCountMetadataMap := map[string]MetricMetadata{
				"some-key-1": {
					sourceID:        "some-source-id-1",
					destinationID:   "some-destination-id-1",
					sourceJobRunID:  "some-source-job-run-id-1",
					sourceJobID:     "some-source-job-id-1",
					sourceTaskRunID: "some-source-task-run-id-1",
				},
				"some-key-2": {
					sourceID:        "some-source-id-2",
					destinationID:   "some-destination-id-2",
					sourceJobRunID:  "some-source-job-run-id-2",
					sourceJobID:     "some-source-job-id-2",
					sourceTaskRunID: "some-source-task-run-id-2",
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

			expectedResponse := []*types.PUReportedMetric{
				{
					ConnectionDetails: types.ConnectionDetails{
						SourceID:        "some-source-id-1",
						DestinationID:   "some-destination-id-1",
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
			assertReportMetric(expectedResponse, response)
		})
	})

	Context("ConvertToTransformerResponse Tests", func() {
		It("Should filter out unsupported message types", func() {
			destinationConfig := backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"identify"},
					},
				},
			}

			events := []transformer.TransformerEventT{
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"some-key-1": "some-value-1",
						"type":       "  IDENTIFY ",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       "track",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       123,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.ResponseT{
				Events: []transformer.TransformerResponseT{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponseT{
					{
						Output:     events[2].Message,
						StatusCode: 400,
						Metadata:   events[2].Metadata,
						Error:      "Invalid message type. Type assertion failed",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true)
			Expect(response).To(Equal(expectedResponse))
		})

		It("Should filter out identify events when it is supported but serverSideIdentify is disabled", func() {
			destinationConfig := backendconfig.DestinationT{
				ID: "some-destination-id",
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"identify", "track"},
					},
				},
			}

			events := []transformer.TransformerEventT{
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"some-key-1": "some-value-1",
						"type":       "  IDENTIFY ",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       "track",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       123,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.ResponseT{
				Events: []transformer.TransformerResponseT{
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponseT{
					{
						Output:     events[2].Message,
						StatusCode: 400,
						Metadata:   events[2].Metadata,
						Error:      "Invalid message type. Type assertion failed",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true)
			Expect(response).To(Equal(expectedResponse))
		})

		It("Should allow all events when no supportedMessageTypes key is present in config", func() {
			destinationConfig := backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{},
				},
			}

			events := []transformer.TransformerEventT{
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"some-key-1": "some-value-1",
						"type":       "  IDENTIFY ",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       "track",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       123,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.ResponseT{
				Events: []transformer.TransformerResponseT{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
					{
						Output:     events[2].Message,
						StatusCode: 200,
						Metadata:   events[2].Metadata,
					},
				},
				FailedEvents: nil,
			}
			response := ConvertToFilteredTransformerResponse(events, true)
			Expect(response).To(Equal(expectedResponse))
		})
		It("Should allow all events when supportedMessageTypes is not an array", func() {
			destinationConfig := backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": "identify",
					},
				},
			}

			events := []transformer.TransformerEventT{
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"some-key-1": "some-value-1",
						"type":       "  IDENTIFY ",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       "track",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       123,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.ResponseT{
				Events: []transformer.TransformerResponseT{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
					{
						Output:     events[2].Message,
						StatusCode: 200,
						Metadata:   events[2].Metadata,
					},
				},
				FailedEvents: nil,
			}
			response := ConvertToFilteredTransformerResponse(events, true)
			Expect(response).To(Equal(expectedResponse))
		})

		It("Should filter out messages containing unsupported events", func() {
			destinationConfig := backendconfig.DestinationT{
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
					"listOfConversions": []interface{}{
						map[string]interface{}{"conversions": "Credit Card Added"},
						map[string]interface{}{"conversions": "Credit Card Removed"},
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{},
			}

			events := []transformer.TransformerEventT{
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Cart Cleared",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Credit Card Added",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": 2,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.ResponseT{
				Events: []transformer.TransformerResponseT{
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponseT{
					{
						Output:     events[2].Message,
						StatusCode: 400,
						Metadata:   events[2].Metadata,
						Error:      "Invalid message event. Type assertion failed",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true)
			Expect(response).To(Equal(expectedResponse))
		})

		It("Shouldn;t filter out messages if listOfConversions config is corrupted", func() {
			destinationConfig := backendconfig.DestinationT{
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
					"listOfConversions": []interface{}{
						map[string]interface{}{"conversions": "Credit Card Added"},
						map[string]interface{}{"conversions": 1},
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{},
				},
			}

			events := []transformer.TransformerEventT{
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Cart Cleared",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Credit Card Added",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.MetadataT{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": 2,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.ResponseT{
				Events: []transformer.TransformerResponseT{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
					{
						Output:     events[2].Message,
						StatusCode: 200,
						Metadata:   events[2].Metadata,
					},
				},
				FailedEvents: nil,
			}
			response := ConvertToFilteredTransformerResponse(events, true)
			Expect(response).To(Equal(expectedResponse))
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

type transformExpectation struct {
	events                    int
	messageIds                string
	receiveMetadata           bool
	destinationDefinitionName string
}

func createMessagePayload(e mockEventData) string {
	integrationsBytes, _ := json.Marshal(e.integrations)
	return fmt.Sprintf(
		`{"rudderId":"some-rudder-id","messageId":"message-%s","integrations":%s,"some-property":"property-%s",`+
			`"originalTimestamp":%q,"sentAt":%q,"recordId":{"id":"record_id_1"},"context":{"sources":`+
			`{"task_run_id":"task_run_id_1","batch_id":"batch_id_1","job_run_id":"job_run_id_1",`+
			`"task_id":"task_id_1","job_id":"job_id_1"}}}`,
		e.id, integrationsBytes, e.id, e.originalTimestamp, e.sentAt,
	)
}

func createMessagePayloadWithSameMessageId(e mockEventData) string {
	integrationsBytes, _ := json.Marshal(e.integrations)
	return fmt.Sprintf(
		`{"rudderId":"some-rudder-id","messageId":"message-%s","integrations":%s,"some-property":"property-%s",`+
			`"originalTimestamp":%q,"sentAt":%q}`, "some-id", integrationsBytes, e.id, e.originalTimestamp, e.sentAt,
	)
}

func createBatchPayloadWithSameMessageId(writeKey, receivedAt string, events []mockEventData) []byte {
	payloads := make([]string, 0)
	for _, event := range events {
		payloads = append(payloads, createMessagePayloadWithSameMessageId(event))
	}
	batch := strings.Join(payloads, ",")
	return []byte(fmt.Sprintf(
		`{"writeKey":%q,"batch":[%s],"requestIP":"1.2.3.4","receivedAt":%q}`, writeKey, batch, receivedAt,
	))
}

func createBatchPayload(writeKey, receivedAt string, events []mockEventData) []byte {
	payloads := make([]string, 0)
	for _, event := range events {
		payloads = append(payloads, createMessagePayload(event))
	}
	batch := strings.Join(payloads, ",")
	return []byte(fmt.Sprintf(
		`{"writeKey":%q,"batch":[%s],"requestIP":"1.2.3.4","receivedAt":%q}`, writeKey, batch, receivedAt,
	))
}

func createBatchParameters(sourceId string) []byte {
	return []byte(fmt.Sprintf(`{"source_id":%q}`, sourceId))
}

func assertJobStatus(job *jobsdb.JobT, status *jobsdb.JobStatusT, expectedState string) {
	Expect(status.JobID).To(Equal(job.JobID))
	Expect(status.JobState).To(Equal(expectedState))
	Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
}

func assertReportMetric(expectedMetric, actualMetric []*types.PUReportedMetric) {
	sort.Slice(expectedMetric, func(i, j int) bool {
		return expectedMetric[i].ConnectionDetails.SourceID < expectedMetric[j].ConnectionDetails.SourceID
	})
	sort.Slice(actualMetric, func(i, j int) bool {
		return actualMetric[i].ConnectionDetails.SourceID < actualMetric[j].ConnectionDetails.SourceID
	})
	Expect(len(expectedMetric)).To(Equal(len(actualMetric)))
	for index, value := range expectedMetric {
		Expect(value.ConnectionDetails.SourceID).To(Equal(actualMetric[index].ConnectionDetails.SourceID))
		Expect(value.ConnectionDetails.DestinationID).To(Equal(actualMetric[index].ConnectionDetails.DestinationID))
		Expect(value.ConnectionDetails.SourceJobID).To(Equal(actualMetric[index].ConnectionDetails.SourceJobID))
		Expect(value.ConnectionDetails.SourceJobRunID).To(Equal(actualMetric[index].ConnectionDetails.SourceJobRunID))
		Expect(value.ConnectionDetails.SourceTaskRunID).To(Equal(actualMetric[index].ConnectionDetails.SourceTaskRunID))
		Expect(value.PUDetails.InPU).To(Equal(actualMetric[index].PUDetails.InPU))
		Expect(value.PUDetails.PU).To(Equal(actualMetric[index].PUDetails.PU))
		Expect(value.PUDetails.TerminalPU).To(Equal(actualMetric[index].PUDetails.TerminalPU))
		Expect(value.PUDetails.InitialPU).To(Equal(actualMetric[index].PUDetails.InitialPU))
		Expect(value.StatusDetail.Status).To(Equal(actualMetric[index].StatusDetail.Status))
		Expect(value.StatusDetail.StatusCode).To(Equal(actualMetric[index].StatusDetail.StatusCode))
		Expect(value.StatusDetail.Count).To(Equal(actualMetric[index].StatusDetail.Count))
		Expect(value.StatusDetail.SampleResponse).To(Equal(actualMetric[index].StatusDetail.SampleResponse))
		Expect(value.StatusDetail.SampleEvent).To(Equal(actualMetric[index].StatusDetail.SampleEvent))

	}
}

func assertDestinationTransform(messages map[string]mockEventData, sourceId, destinationID string, expectations transformExpectation) func(ctx context.Context, clientEvents []transformer.TransformerEventT, url string, batchSize int) transformer.ResponseT {
	return func(ctx context.Context, clientEvents []transformer.TransformerEventT, url string, batchSize int) transformer.ResponseT {
		defer GinkgoRecover()
		destinationDefinitionName := expectations.destinationDefinitionName

		fmt.Println("url", url)
		fmt.Println("destinationDefinitionName", destinationDefinitionName)

		Expect(url).To(Equal(fmt.Sprintf("http://localhost:9090/v0/destinations/%s", destinationDefinitionName)))

		fmt.Println("clientEvents:", len(clientEvents))
		fmt.Println("expect:", expectations.events)

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
				Expect(event.Metadata.SourceID).To(Equal(sourceId)) // ???
				rawEvent, err := json.Marshal(event)
				Expect(err).ToNot(HaveOccurred())
				recordID := gjson.GetBytes(rawEvent, "message.recordId").Value()
				Expect(event.Metadata.RecordID).To(Equal(recordID))
				jobRunID := gjson.GetBytes(rawEvent, "message.context.sources.job_run_id").String()
				Expect(event.Metadata.SourceJobRunID).To(Equal(jobRunID))
				taskRunID := gjson.GetBytes(rawEvent, "message.context.sources.task_run_id").String()
				Expect(event.Metadata.SourceTaskRunID).To(Equal(taskRunID))
				sourcesJobID := gjson.GetBytes(rawEvent, "message.context.sources.job_id").String()
				Expect(event.Metadata.SourceJobID).To(Equal(sourcesJobID))
			} else {
				// Expect(event.Metadata.DestinationType).To(Equal(""))
				Expect(event.Metadata.JobID).To(Equal(int64(0)))
				Expect(event.Metadata.MessageID).To(Equal(""))
				Expect(event.Metadata.SourceID).To(Equal(sourceId)) // ???
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

func processorSetupAndAssertJobHandling(processor *Handle, c *testContext) {
	Setup(processor, c, false, false)
	processor.multitenantI = c.MockMultitenantHandle
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
	GinkgoT().Log("Processor setup and init done")
	handlePendingGatewayJobs(processor)
}

func Setup(processor *Handle, c *testContext, enableDedup, enableReporting bool) {
	clearDB := false
	setDisableDedupFeature(processor, enableDedup)
	processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, &clearDB, c.MockReportingI, c.MockMultitenantHandle, transientsource.NewEmptyService(), fileuploader.NewDefaultProvider(), c.MockRsourcesService, destinationdebugger.NewNoOpService(), transformationdebugger.NewNoOpService())
	processor.reportingEnabled = enableReporting
}

func handlePendingGatewayJobs(processor *Handle) {
	didWork := processor.handlePendingGatewayJobs("")
	Expect(didWork).To(Equal(true))
}

var _ = Describe("TestJobSplitter", func() {
	jobs := []*jobsdb.JobT{
		{
			JobID: 1,
		},
		{
			JobID: 2,
		},
		{
			JobID: 3,
		},
		{
			JobID: 4,
		},
		{
			JobID: 5,
		},
	}
	Context("testing jobs splitter, which split jobs into some sub-jobs", func() {
		It("default subJobSize: 2k", func() {
			proc := NewHandle(nil)
			expectedSubJobs := []subJob{
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
						{
							JobID: 2,
						},
						{
							JobID: 3,
						},
						{
							JobID: 4,
						},
						{
							JobID: 5,
						},
					},
					hasMore: false,
				},
			}
			Expect(len(proc.jobSplitter(jobs, nil))).To(Equal(len(expectedSubJobs)))
			Expect(proc.jobSplitter(jobs, nil)).To(Equal(expectedSubJobs))
		})
		It("subJobSize: 1, i.e. dividing read jobs into batch of 1", func() {
			proc := NewHandle(nil)
			proc.config.subJobSize = 1
			expectedSubJobs := []subJob{
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 2,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 3,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 4,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 5,
						},
					},
					hasMore: false,
				},
			}
			Expect(proc.jobSplitter(jobs, nil)).To(Equal(expectedSubJobs))
		})
		It("subJobSize: 2, i.e. dividing read jobs into batch of 2", func() {
			proc := NewHandle(nil)
			proc.config.subJobSize = 2
			expectedSubJobs := []subJob{
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
						{
							JobID: 2,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 3,
						},
						{
							JobID: 4,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 5,
						},
					},
					hasMore: false,
				},
			}
			Expect(proc.jobSplitter(jobs, nil)).To(Equal(expectedSubJobs))
		})
	})
})

var _ = Describe("TestSubJobMerger", func() {
	expectedMergedJob := storeMessage{
		statusList: []*jobsdb.JobStatusT{
			{
				JobID: 1,
			},
			{
				JobID: 2,
			},
		},
		destJobs: []*jobsdb.JobT{
			{
				JobID: 1,
			},
			{
				JobID: 2,
			},
		},
		batchDestJobs: []*jobsdb.JobT{
			{
				JobID: 1,
			},
			{
				JobID: 2,
			},
		},

		procErrorJobsByDestID: map[string][]*jobsdb.JobT{
			"jobError1": {&jobsdb.JobT{}},
			"jobError2": {&jobsdb.JobT{}},
		},
		procErrorJobs: []*jobsdb.JobT{
			{
				JobID: 1,
			},
			{
				JobID: 2,
			},
		},

		reportMetrics: []*types.PUReportedMetric{{}, {}},
		sourceDupStats: map[string]int{
			"stat-1": 1,
			"stat-2": 2,
		},
		uniqueMessageIds: map[string]struct{}{
			"messageId-1": {},
			"messageId-2": {},
		},

		totalEvents: 2,
		start:       time.Date(2022, time.March, 10, 10, 10, 10, 10, time.UTC),
	}
	Context("testing jobs merger, which merge sub-jobs into final job", func() {
		It("subJobSize: 1", func() {
			mergedJob := storeMessage{}
			mergedJob.uniqueMessageIds = make(map[string]struct{})
			mergedJob.procErrorJobsByDestID = make(map[string][]*jobsdb.JobT)
			mergedJob.sourceDupStats = make(map[string]int)

			subJobs := []storeMessage{
				{
					statusList: []*jobsdb.JobStatusT{
						{
							JobID: 1,
						},
					},
					destJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
					},
					batchDestJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
					},

					procErrorJobsByDestID: map[string][]*jobsdb.JobT{
						"jobError1": {
							&jobsdb.JobT{},
						},
					},
					procErrorJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
					},
					reportMetrics: []*types.PUReportedMetric{
						{},
					},
					sourceDupStats: map[string]int{
						"stat-1": 1,
					},
					uniqueMessageIds: map[string]struct{}{
						"messageId-1": {},
					},

					totalEvents: 1,
					start:       time.Date(2022, time.March, 10, 10, 10, 10, 10, time.UTC),
				},
				{
					statusList: []*jobsdb.JobStatusT{
						{
							JobID: 2,
						},
					},
					destJobs: []*jobsdb.JobT{
						{
							JobID: 2,
						},
					},
					batchDestJobs: []*jobsdb.JobT{
						{
							JobID: 2,
						},
					},

					procErrorJobsByDestID: map[string][]*jobsdb.JobT{
						"jobError2": {
							&jobsdb.JobT{},
						},
					},
					procErrorJobs: []*jobsdb.JobT{
						{
							JobID: 2,
						},
					},

					reportMetrics: []*types.PUReportedMetric{{}},
					sourceDupStats: map[string]int{
						"stat-2": 2,
					},
					uniqueMessageIds: map[string]struct{}{
						"messageId-2": {},
					},
					totalEvents: 1,
					start:       time.Date(2022, time.March, 10, 10, 10, 10, 12, time.UTC),
				},
			}
			mergedJobPtr := &mergedJob
			for _, subJob := range subJobs {
				mergedJobPtr = subJobMerger(mergedJobPtr, &subJob)
			}
			Expect(mergedJob.statusList).To(Equal(expectedMergedJob.statusList))
			Expect(mergedJob.destJobs).To(Equal(expectedMergedJob.destJobs))
			Expect(mergedJob.batchDestJobs).To(Equal(expectedMergedJob.batchDestJobs))
			Expect(mergedJob.procErrorJobsByDestID).To(Equal(expectedMergedJob.procErrorJobsByDestID))
			Expect(mergedJob.procErrorJobs).To(Equal(expectedMergedJob.procErrorJobs))
			Expect(mergedJob.reportMetrics).To(Equal(expectedMergedJob.reportMetrics))
			Expect(mergedJob.sourceDupStats).To(Equal(expectedMergedJob.sourceDupStats))
			Expect(mergedJob.uniqueMessageIds).To(Equal(expectedMergedJob.uniqueMessageIds))
			Expect(mergedJob.totalEvents).To(Equal(expectedMergedJob.totalEvents))
		})
	})
	Context("testing jobs merger, which merge sub-jobs into final job", func() {
		It("subJobSize: 2", func() {
			mergedJob := storeMessage{}
			mergedJob.uniqueMessageIds = make(map[string]struct{})
			mergedJob.procErrorJobsByDestID = make(map[string][]*jobsdb.JobT)
			mergedJob.sourceDupStats = make(map[string]int)

			subJobs := []storeMessage{
				{
					statusList: []*jobsdb.JobStatusT{
						{
							JobID: 1,
						},
						{
							JobID: 2,
						},
					},
					destJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
						{
							JobID: 2,
						},
					},
					batchDestJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
						{
							JobID: 2,
						},
					},

					procErrorJobsByDestID: map[string][]*jobsdb.JobT{
						"jobError1": {&jobsdb.JobT{}},
						"jobError2": {&jobsdb.JobT{}},
					},
					procErrorJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
						{
							JobID: 2,
						},
					},

					reportMetrics: []*types.PUReportedMetric{{}, {}},
					sourceDupStats: map[string]int{
						"stat-1": 1,
						"stat-2": 2,
					},
					uniqueMessageIds: map[string]struct{}{
						"messageId-1": {},
						"messageId-2": {},
					},

					totalEvents: 2,
					start:       time.Date(2022, time.March, 10, 10, 10, 10, 10, time.UTC),
				},
			}
			mergedJobPtr := &mergedJob
			for _, subJob := range subJobs {
				mergedJobPtr = subJobMerger(mergedJobPtr, &subJob)
			}
			Expect(mergedJob.statusList).To(Equal(expectedMergedJob.statusList))
			Expect(mergedJob.destJobs).To(Equal(expectedMergedJob.destJobs))
			Expect(mergedJob.batchDestJobs).To(Equal(expectedMergedJob.batchDestJobs))
			Expect(mergedJob.procErrorJobsByDestID).To(Equal(expectedMergedJob.procErrorJobsByDestID))
			Expect(mergedJob.procErrorJobs).To(Equal(expectedMergedJob.procErrorJobs))
			Expect(mergedJob.reportMetrics).To(Equal(expectedMergedJob.reportMetrics))
			Expect(mergedJob.sourceDupStats).To(Equal(expectedMergedJob.sourceDupStats))
			Expect(mergedJob.uniqueMessageIds).To(Equal(expectedMergedJob.uniqueMessageIds))
			Expect(mergedJob.totalEvents).To(Equal(expectedMergedJob.totalEvents))
		})
	})
})

var _ = Describe("TestConfigFilter", func() {
	Context("testing config filter", func() {
		It("success-full test", func() {
			intgConfig := backendconfig.DestinationT{
				ID:   "1",
				Name: "test",
				Config: map[string]interface{}{
					"config_key":   "config_value",
					"long_config1": "long_config1_value..................................",
					"long_config2": map[string]interface{}{"hello": "world"},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"configFilters": []interface{}{"long_config1", "long_config2"},
					},
				},
				Enabled:            true,
				IsProcessorEnabled: true,
			}
			expectedEvent := transformer.TransformerEventT{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: backendconfig.DestinationT{
					ID:   "1",
					Name: "test",
					Config: map[string]interface{}{
						"config_key": "config_value",
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Config: map[string]interface{}{
							"configFilters": []interface{}{"long_config1", "long_config2"},
						},
					},
					Enabled:            true,
					IsProcessorEnabled: true,
				},
			}
			event := transformer.TransformerEventT{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: intgConfig,
			}
			filterConfig(&event)
			Expect(event).To(Equal(expectedEvent))
		})

		It("success-full test with marshalling", func() {
			var intgConfig backendconfig.DestinationT
			var destDef backendconfig.DestinationDefinitionT
			intgConfigStr := `{
				"id": "1",
				"name": "test",
				"config": {
					"config_key":"config_value",
					"long_config1": "long_config1_value..................................",
					"long_config2": {"hello": "world"}
				},
				"enabled": true,
				"isProcessorEnabled": true
			}`
			destDefStr := `{
				"config": {
					"configFilters": ["long_config1", "long_config2"]
				}
			}`
			Expect(json.Unmarshal([]byte(intgConfigStr), &intgConfig)).To(BeNil())
			Expect(json.Unmarshal([]byte(destDefStr), &destDef)).To(BeNil())
			intgConfig.DestinationDefinition = destDef
			expectedEvent := transformer.TransformerEventT{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: backendconfig.DestinationT{
					ID:   "1",
					Name: "test",
					Config: map[string]interface{}{
						"config_key": "config_value",
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Config: map[string]interface{}{
							"configFilters": []interface{}{"long_config1", "long_config2"},
						},
					},
					Enabled:            true,
					IsProcessorEnabled: true,
				},
			}
			event := transformer.TransformerEventT{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: intgConfig,
			}
			filterConfig(&event)
			Expect(event).To(Equal(expectedEvent))
		})

		It("failure test", func() {
			intgConfig := backendconfig.DestinationT{
				ID:   "1",
				Name: "test",
				Config: map[string]interface{}{
					"config_key":   "config_value",
					"long_config1": "long_config1_value..................................",
					"long_config2": map[string]interface{}{"hello": "world"},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"configFilters": nil,
					},
				},
				Enabled:            true,
				IsProcessorEnabled: true,
			}
			expectedEvent := transformer.TransformerEventT{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: backendconfig.DestinationT{
					ID:   "1",
					Name: "test",
					Config: map[string]interface{}{
						"config_key":   "config_value",
						"long_config1": "long_config1_value..................................",
						"long_config2": map[string]interface{}{"hello": "world"},
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Config: map[string]interface{}{
							"configFilters": nil,
						},
					},
					Enabled:            true,
					IsProcessorEnabled: true,
				},
			}
			event := transformer.TransformerEventT{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: intgConfig,
			}
			filterConfig(&event)
			Expect(event).To(Equal(expectedEvent))
		})
	})
})
