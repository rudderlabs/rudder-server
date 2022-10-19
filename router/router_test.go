package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/testhelper/health"

	trand "github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"

	"github.com/gorilla/mux"

	"github.com/rudderlabs/rudder-server/testhelper"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/testhelper/destination"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	jsoniter "github.com/json-iterator/go"

	"github.com/tidwall/sjson"

	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksRouter "github.com/rudderlabs/rudder-server/mocks/router"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/router/transformer"
	mocksMultitenant "github.com/rudderlabs/rudder-server/mocks/services/multitenant"
	"github.com/rudderlabs/rudder-server/router/types"
	routerUtils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

const (
	writeKeyEnabled = "enabled-write-key"
	// writeKeyDisabled          = "disabled-write-key"
	// writeKeyInvalid           = "invalid-write-key"
	// writeKeyEmpty             = ""
	sourceIDEnabled = "enabled-source"
	// sourceIDDisabled          = "disabled-source"
	// testRemoteAddressWithPort = "test.com:80"
	// testRemoteAddress         = "test.com"
	gaDestinationDefinitionID = "gaid1"
	gaDestinationID           = "did1"
	nonexistentDestinationID  = "non-existent-destination-id"
)

var (
	testTimeout             = 10 * time.Second
	customVal               = map[string]string{"GA": "GA"}
	workspaceID             = uuid.Must(uuid.NewV4()).String()
	gaDestinationDefinition = backendconfig.DestinationDefinitionT{
		ID:          gaDestinationDefinitionID,
		Name:        "GA",
		DisplayName: "Google Analytics",
	}
	gaDestinationConfig = destinationConfig{
		name:          "GA",
		destinationID: "GA",
	}
	collectMetricsErrorMap = map[string]int{
		"Error Response 1":  1,
		"Error Response 2":  2,
		"Error Response 3":  3,
		"Error Response 4":  4,
		"Error Response 5":  1,
		"Error Response 6":  2,
		"Error Response 7":  3,
		"Error Response 8":  4,
		"Error Response 9":  1,
		"Error Response 10": 2,
		"Error Response 11": 3,
		"Error Response 12": 4,
		"Error Response 13": 1,
		"Error Response 14": 2,
		"Error Response 15": 3,
		"Error Response 16": 4,
	}
	// This configuration is assumed by all router tests and, is returned on Subscribe of mocked backend config
	sampleBackendConfig = backendconfig.ConfigT{
		WorkspaceID: workspaceID,
		Sources: []backendconfig.SourceT{
			{
				WorkspaceID: workspaceID,
				ID:          sourceIDEnabled,
				WriteKey:    writeKeyEnabled,
				Enabled:     true,
				Destinations: []backendconfig.DestinationT{
					{
						ID:                    gaDestinationID,
						Name:                  "ga dest",
						DestinationDefinition: gaDestinationDefinition,
						Enabled:               true,
						IsProcessorEnabled:    true,
					},
				},
			},
		},
	}
)

type testContext struct {
	asyncHelper     testutils.AsyncTestHelper
	dbReadBatchSize int

	mockCtrl          *gomock.Controller
	mockRouterJobsDB  *mocksJobsDB.MockMultiTenantJobsDB
	mockProcErrorsDB  *mocksJobsDB.MockJobsDB
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
}

// Initialize mocks and common expectations
func (c *testContext) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockRouterJobsDB = mocksJobsDB.NewMockMultiTenantJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)

	tFunc := c.asyncHelper.ExpectAndNotifyCallbackWithName("backend_config")

	// During Setup, router subscribes to backend config
	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			tFunc()
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{workspaceID: sampleBackendConfig}, Topic: string(topic)}
			// on Subscribe, emulate a backend configuration event
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch
		})
	c.dbReadBatchSize = 10000
}

func (c *testContext) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

func initRouter() {
	config.Reset()
	admin.Init()
	logger.Reset()
	Init()
	InitRouterAdmin()
}

var _ = Describe("Router", func() {
	initRouter()

	var c *testContext

	BeforeEach(func() {
		routerUtils.JobRetention = time.Duration(175200) * time.Hour // 20 Years(20*365*24)
		c = &testContext{}
		c.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {
		It("should initialize and recover after crash", func() {
			mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)
			router := &HandleT{
				Reporting:    &reporting.NOOP{},
				MultitenantI: mockMultitenantHandle,
			}
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationConfig, transientsource.NewEmptyService(), rsources.NewNoOpService())
		})
	})

	Context("normal operation - ga", func() {
		BeforeEach(func() {
			maxStatusUpdateWait = 2 * time.Second
		})

		It("should send failed, unprocessed jobs to ga destination", func() {
			mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			router := &HandleT{
				Reporting:    &reporting.NOOP{},
				MultitenantI: mockMultitenantHandle,
				netHandle:    mockNetHandle,
			}
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationConfig, transientsource.NewEmptyService(), rsources.NewNoOpService())

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID)
			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters:  []byte(parameters),
					WorkspaceId: workspaceID,
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters:  []byte(parameters),
					WorkspaceId: workspaceID,
				},
			}

			allJobs := append(toRetryJobsList, unprocessedJobsList...)
			workspaceCount := map[string]int{}
			workspaceCount[workspaceID] = len(unprocessedJobsList) + len(toRetryJobsList)
			workspaceCountOut := workspaceCount

			callGetRouterPickupJobs := mockMultitenantHandle.EXPECT().GetRouterPickupJobs(customVal["GA"], gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(workspaceCountOut, map[string]float64{}).Times(1)

			payloadLimit := router.payloadLimit
			callGetAllJobs := c.mockRouterJobsDB.EXPECT().GetAllJobs(gomock.Any(), workspaceCount,
				jobsdb.GetQueryParamsT{CustomValFilters: []string{customVal["GA"]}, PayloadSizeLimit: payloadLimit, JobsLimit: workspaceCount[workspaceID]}, 10).Times(1).Return(allJobs, nil).After(callGetRouterPickupJobs)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callGetAllJobs)

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(2).Return(
				&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

			mockMultitenantHandle.EXPECT().CalculateSuccessFailureCounts(gomock.Any(), gomock.Any(), true, false).AnyTimes()
			done := make(chan struct{})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", `{"content-type":"","response": "","firstAttemptedAt":"2021-06-28T15:57:30.742+05:30"}`, 2)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, "200", `{"content-type":"","response": "","firstAttemptedAt":"2021-06-28T15:57:30.742+05:30"}`, 1)
				})

			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(2))
			<-done
		})

		It("should abort unprocessed jobs to ga destination because of bad payload", func() {
			mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)

			router := &HandleT{
				Reporting:    &reporting.NOOP{},
				MultitenantI: mockMultitenantHandle,
			}
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationConfig, transientsource.NewEmptyService(), rsources.NewNoOpService())

			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			router.netHandle = mockNetHandle

			gaPayload := `{}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID)

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters:  []byte(parameters),
					WorkspaceId: workspaceID,
				},
			}

			workspaceCount := map[string]int{}
			workspaceCount[workspaceID] = len(unprocessedJobsList)
			workspaceCountOut := workspaceCount

			callGetRouterPickupJobs := mockMultitenantHandle.EXPECT().GetRouterPickupJobs(customVal["GA"], gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(workspaceCountOut, map[string]float64{}).Times(1)

			payloadLimit := router.payloadLimit
			callGetAllJobs := c.mockRouterJobsDB.EXPECT().GetAllJobs(gomock.Any(), workspaceCount, jobsdb.GetQueryParamsT{
				CustomValFilters: []string{customVal["GA"]}, PayloadSizeLimit: payloadLimit, JobsLimit: workspaceCount[workspaceID],
			}, 10).Times(1).Return(unprocessedJobsList, nil).After(callGetRouterPickupJobs)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 0)
				}).After(callGetAllJobs)

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(1).Return(&routerUtils.SendPostResponse{StatusCode: 400, ResponseBody: []byte("")})
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobList []*jobsdb.JobT) {
					job := jobList[0]
					var parameters map[string]interface{}
					err := json.Unmarshal(job.Parameters, &parameters)
					if err != nil {
						panic(err)
					}

					Expect(parameters["stage"]).To(Equal("router"))
					Expect(job.JobID).To(Equal(unprocessedJobsList[0].JobID))
					Expect(job.CustomVal).To(Equal(unprocessedJobsList[0].CustomVal))
					Expect(job.UserID).To(Equal(unprocessedJobsList[0].UserID))
				})
			mockMultitenantHandle.EXPECT().CalculateSuccessFailureCounts(gomock.Any(), gomock.Any(), false, true).AnyTimes()
			done := make(chan struct{})
			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Aborted.State, "400", `{"content-type":"","response":"","firstAttemptedAt":"2021-06-28T15:57:30.742+05:30"}`, 1)
				})

			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(1))
			<-done
		})

		It("aborts events that are older than a configurable duration", func() {
			routerUtils.JobRetention = time.Duration(24) * time.Hour
			mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)
			router := &HandleT{
				Reporting:    &reporting.NOOP{},
				MultitenantI: mockMultitenantHandle,
			}
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationConfig, transientsource.NewEmptyService(), rsources.NewNoOpService())
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			router.netHandle = mockNetHandle
			router.MultitenantI = mockMultitenantHandle

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID)

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters:  []byte(parameters),
					WorkspaceId: workspaceID,
				},
			}

			workspaceCount := map[string]int{}
			workspaceCount[workspaceID] = len(unprocessedJobsList)
			workspaceCountOut := workspaceCount

			callGetRouterPickupJobs := mockMultitenantHandle.EXPECT().GetRouterPickupJobs(customVal["GA"], gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(workspaceCountOut, map[string]float64{}).Times(1)

			payloadLimit := router.payloadLimit
			c.mockRouterJobsDB.EXPECT().GetAllJobs(gomock.Any(), workspaceCount, jobsdb.GetQueryParamsT{
				CustomValFilters: []string{customVal["GA"]}, PayloadSizeLimit: payloadLimit, JobsLimit: workspaceCount[workspaceID],
			}, 10).Times(1).Return(unprocessedJobsList, nil).After(callGetRouterPickupJobs)

			var routerAborted bool
			var procErrorStored bool
			mockMultitenantHandle.EXPECT().CalculateSuccessFailureCounts(gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any()).Times(1)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobList []*jobsdb.JobT) {
					job := jobList[0]
					var parameters map[string]interface{}
					err := json.Unmarshal(job.Parameters, &parameters)
					if err != nil {
						panic(err)
					}

					Expect(job.JobID).To(Equal(unprocessedJobsList[0].JobID))
					Expect(job.CustomVal).To(Equal(unprocessedJobsList[0].CustomVal))
					Expect(job.UserID).To(Equal(unprocessedJobsList[0].UserID))
					procErrorStored = true
				})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, tx jobsdb.UpdateSafeTx, drainList []*jobsdb.JobStatusT, _, _ interface{}) {
					Expect(drainList).To(HaveLen(1))
					assertJobStatus(unprocessedJobsList[0], drainList[0], jobsdb.Aborted.State, "410", `{"reason": "job expired"}`, 0)
					routerAborted = true
				})

			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(len(unprocessedJobsList)))
			Eventually(func() bool { return routerAborted && procErrorStored }, 5*time.Second, 100*time.Millisecond).Should(Equal(true))
		})

		It("can fail jobs if time is more than router timeout", func() {
			mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &HandleT{
				Reporting:    &reporting.NOOP{},
				MultitenantI: mockMultitenantHandle,
				netHandle:    mockNetHandle,
			}
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationConfig, transientsource.NewEmptyService(), rsources.NewNoOpService())
			router.transformer = mockTransformer
			router.noOfWorkers = 1
			router.noOfJobsToBatchInAWorker = 5
			router.routerTimeout = time.Duration(0)

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID)

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u2",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u2",
					JobID:        2012,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u3",
					JobID:        2013,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			allJobs := append(toRetryJobsList, unprocessedJobsList...)
			workspaceCount := map[string]int{}
			workspaceCount[workspaceID] = len(unprocessedJobsList) + len(toRetryJobsList)
			workspaceCountOut := workspaceCount
			callGetRouterPickupJobs := mockMultitenantHandle.EXPECT().GetRouterPickupJobs(customVal["GA"], gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(workspaceCountOut, map[string]float64{}).Times(1)

			payloadLimit := router.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetAllJobs(gomock.Any(), workspaceCount,
				jobsdb.GetQueryParamsT{CustomValFilters: []string{customVal["GA"]}, PayloadSizeLimit: payloadLimit, JobsLimit: len(allJobs)}, 10).Times(1).Return(allJobs, nil).After(
				callGetRouterPickupJobs)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[2], statuses[3], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[3], statuses[4], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callAllJobs)
			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(0)
			done := make(chan struct{})
			mockMultitenantHandle.EXPECT().CalculateSuccessFailureCounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(5))
			<-done
		})

		It("fails jobs if destination is not found in config", func() {
			mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &HandleT{
				Reporting:    &reporting.NOOP{},
				MultitenantI: mockMultitenantHandle,
				netHandle:    mockNetHandle,
			}
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationConfig, transientsource.NewEmptyService(), rsources.NewNoOpService())
			router.transformer = mockTransformer
			router.noOfWorkers = 1
			router.noOfJobsToBatchInAWorker = 5
			router.routerTimeout = time.Duration(60) * time.Second

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, nonexistentDestinationID)

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 3, // done only to check the error response assertion(assertJobStatus) as well
					},
					Parameters: []byte(parameters),
				},
			}
			workspaceCount := map[string]int{}
			workspaceCount[workspaceID] = len(unprocessedJobsList)
			workspaceCountOut := workspaceCount
			callGetRouterPickupJobs := mockMultitenantHandle.EXPECT().
				GetRouterPickupJobs(customVal["GA"], gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(workspaceCountOut, map[string]float64{}).Times(1)

			payloadLimit := router.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetAllJobs(
				gomock.Any(),
				workspaceCount,
				jobsdb.GetQueryParamsT{
					CustomValFilters: []string{customVal["GA"]},
					PayloadSizeLimit: payloadLimit,
					JobsLimit:        len(unprocessedJobsList),
				}, 10).
				Times(1).
				Return(unprocessedJobsList, nil).
				After(callGetRouterPickupJobs)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 3)
				}).Return(nil).After(callAllJobs)

			done := make(chan struct{})
			mockMultitenantHandle.EXPECT().
				CalculateSuccessFailureCounts(
					gomock.Any(),
					gomock.Any(),
					gomock.Any(),
					gomock.Any(),
				).
				AnyTimes()
			c.mockRouterJobsDB.EXPECT().
				WithUpdateSafeTx(
					gomock.Any(),
					gomock.Any()).
				Times(1).
				Do(
					func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
						_ = f(jobsdb.EmptyUpdateSafeTx())
						close(done)
					}).Return(nil)

			c.mockRouterJobsDB.EXPECT().
				UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).
				Times(1).
				Do(func(ctx context.Context, _ jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(
						unprocessedJobsList[0],
						statuses[0],
						jobsdb.Failed.State,
						"",
						`{"reason": "failed because destination is not available in the config" }`,
						3,
					)
				}).Return(nil)
			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(1))
			<-done
		})
	})

	Context("Router Batching", func() {
		BeforeEach(func() {
			maxStatusUpdateWait = 2 * time.Second
		})

		It("can batch jobs together", func() {
			mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &HandleT{
				Reporting:    &reporting.NOOP{},
				MultitenantI: mockMultitenantHandle,
				netHandle:    mockNetHandle,
			}
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationConfig, transientsource.NewEmptyService(), rsources.NewNoOpService())

			router.transformer = mockTransformer

			router.enableBatching = true
			router.noOfJobsToBatchInAWorker = 3
			router.noOfWorkers = 1
			router.routerTimeout = time.Duration(math.MaxInt64)

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID)

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u2",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u3",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			workspaceCount := map[string]int{}
			workspaceCount[workspaceID] = len(unprocessedJobsList) + len(toRetryJobsList)
			workspaceCountOut := workspaceCount
			jobsList := append(toRetryJobsList, unprocessedJobsList...)
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			callGetRouterPickupJobs := mockMultitenantHandle.EXPECT().GetRouterPickupJobs(customVal["GA"], gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(workspaceCountOut, map[string]float64{}).Times(1)

			payloadLimit := router.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetAllJobs(gomock.Any(), workspaceCount, jobsdb.GetQueryParamsT{
				CustomValFilters: []string{customVal["GA"]}, PayloadSizeLimit: payloadLimit, JobsLimit: len(jobsList),
			}, 10).Times(1).Return(jobsList, nil).After(callGetRouterPickupJobs)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil)

			mockTransformer.EXPECT().Transform("BATCH", gomock.Any()).After(callAllJobs).Times(1).
				DoAndReturn(
					func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
						assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
						assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])
						assertRouterJobs(&transformMessage.Data[2], unprocessedJobsList[1])
						return []types.DestinationJobT{
							{
								Message: []byte(`{"message": "some transformed message"}`),
								JobMetadataArray: []types.JobMetadataT{
									{
										UserID: "u1",
										JobID:  2009,
										JobT:   toRetryJobsList[0],
									},
									{
										UserID: "u2",
										JobID:  2010,
										JobT:   unprocessedJobsList[0],
									},
									{
										UserID: "u3",
										JobID:  2011,
										JobT:   unprocessedJobsList[1],
									},
								},
								Batched:    true,
								Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
								StatusCode: 200,
							},
						}
					})

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(1).Return(&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			done := make(chan struct{})
			mockMultitenantHandle.EXPECT().CalculateSuccessFailureCounts(gomock.Any(), gomock.Any(), true, false).AnyTimes()
			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertTransformJobStatuses(toRetryJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", 1)
					assertTransformJobStatuses(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, "200", 1)
					assertTransformJobStatuses(unprocessedJobsList[1], statuses[2], jobsdb.Succeeded.State, "200", 1)
				})

			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(3))
			<-done
		})

		It("aborts jobs if batching fails for few of the jobs", func() {
			mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &HandleT{
				Reporting:    &reporting.NOOP{},
				MultitenantI: mockMultitenantHandle,
				netHandle:    mockNetHandle,
			}
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationConfig, transientsource.NewEmptyService(), rsources.NewNoOpService())

			// we have a job that has failed once(toRetryJobsList), it should abort when picked up next
			// Because we only allow one failure per job with this
			router.transformer = mockTransformer
			router.noOfJobsToBatchInAWorker = 3
			router.maxFailedCountForJob = 5
			router.enableBatching = true

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID)

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}
			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}
			allJobs := append(toRetryJobsList, unprocessedJobsList...)
			workspaceCount := map[string]int{}
			workspaceCount[workspaceID] = len(unprocessedJobsList) + len(toRetryJobsList)
			workspaceCountOut := workspaceCount
			callGetRouterPickupJobs := mockMultitenantHandle.EXPECT().GetRouterPickupJobs(customVal["GA"], gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(workspaceCountOut, map[string]float64{})

			payloadLimit := router.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetAllJobs(gomock.Any(), workspaceCount,
				jobsdb.GetQueryParamsT{CustomValFilters: []string{customVal["GA"]}, PayloadSizeLimit: payloadLimit, JobsLimit: len(allJobs)}, 10).Return(toRetryJobsList, nil).Times(
				1).Return(allJobs, nil).After(callGetRouterPickupJobs)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callAllJobs)

			mockTransformer.EXPECT().Transform("BATCH", gomock.Any()).After(callAllJobs).Times(1).DoAndReturn(
				func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
					assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
					assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])
					assertRouterJobs(&transformMessage.Data[2], unprocessedJobsList[1])

					return []types.DestinationJobT{
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID:           "u1",
									JobID:            2009,
									JobT:             toRetryJobsList[0],
									FirstAttemptedAt: "2021-06-28T15:57:30.742+05:30",
									AttemptNum:       1,
								},
								{
									UserID:           "u1",
									JobID:            2010,
									JobT:             unprocessedJobsList[0],
									FirstAttemptedAt: "2021-06-28T15:57:30.742+05:30",
								},
							},
							Batched:    true,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 500,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2011,
									JobT:   unprocessedJobsList[1],
								},
							},
							Batched:    true,
							Error:      ``,
							StatusCode: 200,
						},
					}
				})

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(0).Return(&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			done := make(chan struct{})
			mockMultitenantHandle.EXPECT().CalculateSuccessFailureCounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertTransformJobStatuses(toRetryJobsList[0], statuses[0], jobsdb.Failed.State, "500", 1)
					assertTransformJobStatuses(unprocessedJobsList[0], statuses[1], jobsdb.Waiting.State, "", 0)
					assertTransformJobStatuses(unprocessedJobsList[1], statuses[2], jobsdb.Waiting.State, "", 0)
				})

			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(3))
			<-done
		})
	})

	Context("Router Transform", func() {
		BeforeEach(func() {
			maxStatusUpdateWait = 2 * time.Second
			jobsBatchTimeout = 10 * time.Second
		})
		/*
			Router transform
				Job1 u1
				Job2 u1
				Job3 u2
				Job4 u2
				Job5 u3
			{[1]: T200 [2]: T500, [3]: T500, [4]: T200, [5]: T200}

			[1] should be sent
			[2] shouldn't be sent
			[3] shouldn't be sent
			[4] should be dropped
			[5] should be sent
		*/
		It("can transform jobs at router", func() {
			mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &HandleT{
				Reporting:    &reporting.NOOP{},
				MultitenantI: mockMultitenantHandle,
				netHandle:    mockNetHandle,
			}
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationConfig, transientsource.NewEmptyService(), rsources.NewNoOpService())
			router.transformer = mockTransformer
			router.noOfWorkers = 1
			router.noOfJobsToBatchInAWorker = 5
			router.routerTimeout = time.Duration(math.MaxInt64)

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "router"}`, gaDestinationID)

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u2",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u2",
					JobID:        2012,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u3",
					JobID:        2013,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			allJobs := append(toRetryJobsList, unprocessedJobsList...)
			workspaceCount := map[string]int{}
			workspaceCount[workspaceID] = len(unprocessedJobsList) + len(toRetryJobsList)
			workspaceCountOut := workspaceCount
			callGetRouterPickupJobs := mockMultitenantHandle.EXPECT().GetRouterPickupJobs(customVal["GA"], gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(workspaceCountOut, map[string]float64{}).Times(1)

			payloadLimit := router.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetAllJobs(gomock.Any(), workspaceCount,
				jobsdb.GetQueryParamsT{CustomValFilters: []string{customVal["GA"]}, PayloadSizeLimit: payloadLimit, JobsLimit: len(allJobs)}, 10).Times(1).Return(allJobs, nil).After(
				callGetRouterPickupJobs)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[2], statuses[3], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[3], statuses[4], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callAllJobs)

			mockTransformer.EXPECT().Transform("ROUTER_TRANSFORM", gomock.Any()).After(callAllJobs).Times(1).DoAndReturn(
				func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
					assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
					assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])
					assertRouterJobs(&transformMessage.Data[2], unprocessedJobsList[1])
					assertRouterJobs(&transformMessage.Data[3], unprocessedJobsList[2])
					assertRouterJobs(&transformMessage.Data[4], unprocessedJobsList[3])

					return []types.DestinationJobT{
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID:     "u1",
									JobID:      2009,
									AttemptNum: 1,
									JobT:       toRetryJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2010,
									JobT:   unprocessedJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 500,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u2",
									JobID:  2011,
									JobT:   unprocessedJobsList[1],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 500,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u2",
									JobID:  2012,
									JobT:   unprocessedJobsList[2],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u3",
									JobID:  2013,
									JobT:   unprocessedJobsList[3],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
					}
				})

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(2).Return(&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			done := make(chan struct{})
			mockMultitenantHandle.EXPECT().CalculateSuccessFailureCounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(5))
			<-done
		})

		/*
				Job1 u1
				Job2 u1
				Job3 u1
			{[1]: T500, [T2]: T200, [3]: T200}

				[1] shouldn't be sent
				[2] should be dropped
				[3] should be dropped
		*/
		It("marks all jobs of a user failed if a preceding job fails due to transformation failure", func() {
			mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			router := &HandleT{
				Reporting:    &reporting.NOOP{},
				MultitenantI: mockMultitenantHandle,
				netHandle:    mockNetHandle,
			}
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationConfig, transientsource.NewEmptyService(), rsources.NewNoOpService())
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router.transformer = mockTransformer

			router.noOfJobsToBatchInAWorker = 3
			router.noOfWorkers = 1

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "router"}`, gaDestinationID)

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u2",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			allJobs := append(toRetryJobsList, unprocessedJobsList...)
			workspaceCount := map[string]int{}
			workspaceCount[workspaceID] = len(unprocessedJobsList) + len(toRetryJobsList)
			workspaceCountOut := workspaceCount
			callGetRouterPickupJobs := mockMultitenantHandle.EXPECT().GetRouterPickupJobs(customVal["GA"], gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(workspaceCountOut, map[string]float64{}).Times(1)

			payloadLimit := router.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetAllJobs(gomock.Any(), workspaceCount,
				jobsdb.GetQueryParamsT{CustomValFilters: []string{customVal["GA"]}, PayloadSizeLimit: payloadLimit, JobsLimit: len(allJobs)}, 10).Times(1).Return(allJobs, nil).After(callGetRouterPickupJobs)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callAllJobs)

			mockTransformer.EXPECT().Transform("ROUTER_TRANSFORM", gomock.Any()).After(callAllJobs).Times(1).DoAndReturn(
				func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
					assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
					assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])
					assertRouterJobs(&transformMessage.Data[2], unprocessedJobsList[1])

					return []types.DestinationJobT{
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID:     "u1",
									JobID:      2009,
									AttemptNum: 1,
									JobT:       toRetryJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 500,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2010,
									JobT:   unprocessedJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2010,
									JobT:   unprocessedJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
					}
				})
			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(0).Return(&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			done := make(chan struct{})
			mockMultitenantHandle.EXPECT().CalculateSuccessFailureCounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(3))
			<-done
		})
	})
})

func assertRouterJobs(routerJob *types.RouterJobT, job *jobsdb.JobT) {
	Expect(routerJob.JobMetadata.JobID).To(Equal(job.JobID))
	Expect(routerJob.JobMetadata.UserID).To(Equal(job.UserID))
}

func assertJobStatus(job *jobsdb.JobT, status *jobsdb.JobStatusT, expectedState, errorCode, errorResponse string, attemptNum int) {
	Expect(status.JobID).To(Equal(job.JobID))
	Expect(status.JobState).To(Equal(expectedState))
	Expect(status.ErrorCode).To(Equal(errorCode))
	if attemptNum > 1 {
		Expect(status.ErrorResponse).To(MatchJSON(errorResponse))
	}
	Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 10*time.Second))
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 10*time.Second))
	Expect(status.AttemptNum).To(Equal(attemptNum))
}

func assertTransformJobStatuses(job *jobsdb.JobT, status *jobsdb.JobStatusT, expectedState, errorCode string, attemptNum int) {
	Expect(status.JobID).To(Equal(job.JobID))
	Expect(status.JobState).To(Equal(expectedState))
	Expect(status.ErrorCode).To(Equal(errorCode))
	Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 10*time.Second))
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 10*time.Second))
	Expect(status.AttemptNum).To(Equal(attemptNum))
}

type webhookCount struct {
	count   *uint64
	webhook *httptest.Server
}

func generatePayloads(t *testing.T, count int) [][]byte {
	payloads := make([][]byte, count)
	for i := 0; i < count; i++ {
		testBody, err := os.ReadFile("../scripts/batch.json")
		require.NoError(t, err)
		payloads[i] = testBody
	}
	return payloads
}

func createNewWebhook(t *testing.T, statusCode int) webhookCount {
	var count uint64 = 0
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		_, err := w.Write([]byte(`{"message": "some transformed message"}`))

		atomic.AddUint64(&count, 1)
		require.NoError(t, err)
	}))
	return webhookCount{
		&count,
		webhook,
	}
}

func Benchmark_SJSON_SET(b *testing.B) {
	var stringValue string
	var err error
	for i := 0; i < b.N; i++ {
		for k, v := range collectMetricsErrorMap {
			stringValue, err = sjson.Set(stringValue, k, v)
			if err != nil {
				stringValue = ""
			}
		}
	}
}

func Benchmark_JSON_MARSHAL(b *testing.B) {
	for i := 0; i < b.N; i++ {
		val, _ := json.Marshal(collectMetricsErrorMap)
		_ = string(val)
	}
}

func Benchmark_FASTJSON_MARSHAL(b *testing.B) {
	jsonfast := jsoniter.ConfigCompatibleWithStandardLibrary

	for i := 0; i < b.N; i++ {
		val, _ := jsonfast.Marshal(collectMetricsErrorMap)
		_ = string(val)
	}
}

func Test_RouterDestIsolation(t *testing.T) {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	var (
		group                errgroup.Group
		postgresContainer    *destination.PostgresResource
		transformerContainer *destination.TransformerResource
	)

	group.Go(func() (err error) {
		postgresContainer, err = destination.SetupPostgres(pool, t)
		return err
	})

	group.Go(func() (err error) {
		transformerContainer, err = destination.SetupTransformer(pool, t)
		return err
	})
	require.NoError(t, group.Wait())

	backendConfRouter := mux.NewRouter()
	if testing.Verbose() {
		backendConfRouter.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("BackendConfig server call: %+v", r)
				next.ServeHTTP(w, r)
			})
		})
	}

	writeKey := trand.String(27)
	workspaceID := trand.String(27)
	webhook1 := createNewWebhook(t, 500)
	defer webhook1.webhook.Close()
	webhook2 := createNewWebhook(t, 200)
	defer webhook2.webhook.Close()

	templateCtx := map[string]string{
		"webhookUrl1": webhook1.webhook.URL,
		"webhookUrl2": webhook2.webhook.URL,
		"writeKey":    writeKey,
		"workspaceId": workspaceID,
	}
	configJsonPath := workspaceConfig.CreateTempFile(t, "../testdata/destIdIsolationTestTemplate.json", templateCtx)

	httpPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	debugPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	rudderTmpDir, err := os.MkdirTemp("", "rudder_server_*_test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(rudderTmpDir) })

	t.Setenv("JOBS_DB_PORT", postgresContainer.Port)
	t.Setenv("JOBS_DB_USER", postgresContainer.User)
	t.Setenv("JOBS_DB_DB_NAME", postgresContainer.Database)
	t.Setenv("JOBS_DB_PASSWORD", postgresContainer.Password)
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_PROFILER_PORT", strconv.Itoa(debugPort))
	t.Setenv("RSERVER_WAREHOUSE_MODE", "off")
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RSERVER_JOBS_DB_BACKUP_ENABLED", "false")
	t.Setenv("RUDDER_TMPDIR", rudderTmpDir)
	t.Setenv("DEST_TRANSFORM_URL", transformerContainer.TransformURL)
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", configJsonPath)
	t.Setenv("RSERVER_ROUTER_WEBHOOK_ISOLATE_DEST_ID", "true")
	t.Setenv("RSERVER_ROUTER_JOB_QUERY_BATCH_SIZE", "10")

	if testing.Verbose() {
		require.NoError(t, os.Setenv("LOG_LEVEL", "DEBUG"))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", "run", "../main.go")
		cmd.Env = os.Environ()

		stdout, err := cmd.StdoutPipe()
		require.NoError(t, err)
		stderr, err := cmd.StderrPipe()
		require.NoError(t, err)

		defer func() {
			_ = stdout.Close()
			_ = stderr.Close()
		}()
		require.NoError(t, cmd.Start())
		if testing.Verbose() {
			go func() { _, _ = io.Copy(os.Stdout, stdout) }()
			go func() { _, _ = io.Copy(os.Stderr, stderr) }()
		}

		if err = cmd.Wait(); err != nil {
			if err.Error() != "signal: killed" {
				t.Errorf("Error running main.go: %v", err)
				return
			}
		}
		t.Log("main.go exited")
	}()
	t.Cleanup(func() { cancel(); <-done })

	healthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t,
		healthEndpoint,
		200*time.Second,
		100*time.Millisecond,
		t.Name(),
	)
	batches := generatePayloads(t, 100)
	client := &http.Client{}
	for _, payload := range batches {
		url := fmt.Sprintf("http://localhost:%d/v1/batch", httpPort)
		req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
		require.NoError(t, err, "should be able to create a new request")
		req.SetBasicAuth(writeKey, "password")
		resp, err := client.Do(req)
		require.NoError(t, err, "should be able to send the request to gateway")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(webhook2.count) == 100 && atomic.LoadUint64(webhook1.count) < 100
	}, 30*time.Second, 1*time.Second, "should have received all the events")
}
