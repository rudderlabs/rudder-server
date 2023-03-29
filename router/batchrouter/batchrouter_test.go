package batchrouter

import (
	"context"
	jsonb "encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksFileManager "github.com/rudderlabs/rudder-server/mocks/services/filemanager"
	mocksMultitenant "github.com/rudderlabs/rudder-server/mocks/services/multitenant"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
	"github.com/rudderlabs/rudder-server/warehouse/client"
)

const (
	WriteKeyEnabled           = "enabled-write-key"
	SourceIDEnabled           = "enabled-source"
	S3DestinationDefinitionID = "s3id1"
	S3DestinationID           = "did1"
)

var testTimeout = 10 * time.Second

var s3DestinationDefinition = backendconfig.DestinationDefinitionT{ID: S3DestinationDefinitionID, Name: "S3", DisplayName: "S3", Config: nil, ResponseRules: nil}

var workspaceID = `workspaceID`

// This configuration is assumed by all router tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = backendconfig.ConfigT{
	WorkspaceID: workspaceID,
	Sources: []backendconfig.SourceT{
		{
			ID:           SourceIDEnabled,
			WriteKey:     WriteKeyEnabled,
			Enabled:      true,
			Destinations: []backendconfig.DestinationT{{ID: S3DestinationID, Name: "s3 dest", DestinationDefinition: s3DestinationDefinition, Enabled: true, IsProcessorEnabled: true}},
		},
	},
}

var (
	sampleConfigPrefix = "config_prefix"
	sampleFileObjects  = []*filemanager.FileObject{
		{
			Key:          fmt.Sprintf("%s/%s/%s/%s/%s", sampleConfigPrefix, SourceIDEnabled, WriteKeyEnabled, "01-02-2006", "tmp1.log"),
			LastModified: time.Now(),
		},
		{
			Key:          fmt.Sprintf("%s/%s/%s/%s/%s", sampleConfigPrefix, SourceIDEnabled, WriteKeyEnabled, "2006-01-02", "tmp2.log"),
			LastModified: time.Now(),
		},
	}
)

type testContext struct {
	asyncHelper       testutils.AsyncTestHelper
	jobQueryBatchSize int

	mockCtrl               *gomock.Controller
	mockBatchRouterJobsDB  *mocksJobsDB.MockJobsDB
	mockProcErrorsDB       *mocksJobsDB.MockJobsDB
	mockBackendConfig      *mocksBackendConfig.MockBackendConfig
	mockFileManagerFactory *mocksFileManager.MockFileManagerFactory
	mockFileManager        *mocksFileManager.MockFileManager
	mockConfigPrefix       string
	mockFileObjects        []*filemanager.FileObject
	mockMultitenantI       *mocksMultitenant.MockMultiTenantI
}

// Initiaze mocks and common expectations
func (c *testContext) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockFileManagerFactory = mocksFileManager.NewMockFileManagerFactory(c.mockCtrl)
	c.mockFileManager = mocksFileManager.NewMockFileManager(c.mockCtrl)
	c.mockMultitenantI = mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)

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
	c.jobQueryBatchSize = 100000
	c.mockConfigPrefix = sampleConfigPrefix
	c.mockFileObjects = sampleFileObjects
}

func (c *testContext) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

var (
	CustomVal           = map[string]string{"S3": "S3"}
	emptyJournalEntries []jobsdb.JournalEntryT
)

func initBatchRouter() {
	config.Reset()
	admin.Init()
	logger.Reset()
	misc.Init()
	Init()
	Init2()
}

var _ = Describe("BatchRouter", func() {
	initBatchRouter()

	var c *testContext

	BeforeEach(func() {
		router_utils.JobRetention = time.Duration(175200) * time.Hour // 20 Years(20*365*24)
		c = &testContext{}
		c.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {
		It("should initialize and recover after crash", func() {
			batchrouter := &HandleT{}

			c.mockBatchRouterJobsDB.EXPECT().GetJournalEntries(gomock.Any()).Times(1).Return(emptyJournalEntries)

			batchrouter.Setup(c.mockBackendConfig, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, s3DestinationDefinition.Name, nil, c.mockMultitenantI, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
		})
	})

	Context("normal operation - s3 - do not readPerDestination", func() {
		BeforeEach(func() {
			// crash recovery check
			c.mockBatchRouterJobsDB.EXPECT().GetJournalEntries(gomock.Any()).Times(1).Return(emptyJournalEntries)
		})

		It("should send failed, unprocessed jobs to s3 destination", func() {
			batchrouter := &HandleT{}

			batchrouter.Setup(c.mockBackendConfig, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, s3DestinationDefinition.Name, nil, c.mockMultitenantI, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
			readPerDestination = false
			batchrouter.fileManagerFactory = c.mockFileManagerFactory

			c.mockFileManagerFactory.EXPECT().New(gomock.Any()).Times(1).Return(c.mockFileManager, nil)
			c.mockFileManager.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return(filemanager.UploadOutput{Location: "local", ObjectName: "file"}, nil)
			c.mockFileManager.EXPECT().GetConfiguredPrefix().Return(c.mockConfigPrefix)
			c.mockFileManager.EXPECT().ListFilesWithPrefix(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(c.mockFileObjects, nil)

			s3Payload := `{
				"userId": "identified user id",
				"anonymousId":"anon-id-new",
				"context": {
				  "traits": {
					 "trait1": "new-val"
				  },
				  "ip": "14.5.67.21",
				  "library": {
					  "name": "http"
				  }
				},
				"timestamp": "2020-02-02T00:23:09.544Z"
			  }`
			parameters := fmt.Sprintf(`{"source_id": %q, "destination_id": %q, "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "none"}`, SourceIDEnabled, S3DestinationID)

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    CustomVal["S3"],
					EventPayload: []byte(s3Payload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    CustomVal["S3"],
					EventPayload: []byte(s3Payload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			payloadLimit := batchrouter.payloadLimit
			callRetry := c.mockBatchRouterJobsDB.EXPECT().GetToRetry(gomock.Any(), jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["S3"]}, JobsLimit: c.jobQueryBatchSize, PayloadSizeLimit: payloadLimit}).Return(jobsdb.JobsResult{Jobs: toRetryJobsList}, nil).Times(1)
			c.mockBatchRouterJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["S3"]}, JobsLimit: c.jobQueryBatchSize - len(toRetryJobsList), PayloadSizeLimit: payloadLimit}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1).After(callRetry)

			c.mockBatchRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{CustomVal["S3"]}, gomock.Any()).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, `{}`, 2)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, `{}`, 1)
				}).Return(nil)

			c.mockBatchRouterJobsDB.EXPECT().JournalMarkStart(gomock.Any(), gomock.Any()).Times(1).Return(int64(1))

			c.mockBatchRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil)
			c.mockBatchRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{CustomVal["S3"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Succeeded.State, `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30", "success": "OK"}`, 2)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30, "success": "OK""}`, 1)
				}).Return(nil)

			c.mockBatchRouterJobsDB.EXPECT().JournalDeleteEntry(gomock.Any()).Times(1)

			<-batchrouter.backendConfigInitialized
			batchrouter.readAndProcess()
		})
	})
})

func assertJobStatus(job *jobsdb.JobT, status *jobsdb.JobStatusT, expectedState, errorResponse string, attemptNum int) {
	Expect(status.JobID).To(Equal(job.JobID))
	Expect(status.JobState).To(Equal(expectedState))
	if attemptNum > 1 {
		Expect(status.ErrorResponse).To(MatchJSON(errorResponse))
	}
	Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 10*time.Second))
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 10*time.Second))
	Expect(status.AttemptNum).To(Equal(attemptNum))
}

func TestPostToWarehouse(t *testing.T) {
	// TOT: Decouple this test from the actual warehouse
	inputs := []struct {
		name string

		responseCode int
		responseBody string

		expectedPayload string
		expectedError   error
	}{
		{
			name: "should successfully post to warehouse",

			responseBody: "OK",
			responseCode: http.StatusOK,

			expectedPayload: `{"WorkspaceID":"test-workspace","Schema":{"tracks":{"id":"string"}},"BatchDestination":{"Source":{"ID":""},"Destination":{"ID":""}},"Location":"","FirstEventAt":"","LastEventAt":"","TotalEvents":1,"TotalBytes":200,"UseRudderStorage":false,"DestinationRevisionID":"","SourceTaskRunID":"","SourceJobID":"","SourceJobRunID":"","TimeWindow":"0001-01-01T00:00:00Z"}`,
		},
		{
			name: "should fail to post to warehouse",

			responseCode: http.StatusNotFound,
			responseBody: "Not Found",

			expectedError: errors.New("unexpected status code \"404 Not Found\" on %s: Not Found"),
		},
	}
	for _, input := range inputs {
		t.Run(input.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				if input.expectedPayload != "" {
					require.JSONEq(t, input.expectedPayload, string(b))
				}

				w.WriteHeader(input.responseCode)
				_, _ = w.Write([]byte(input.responseBody))
			}))
			t.Cleanup(ts.Close)

			job := HandleT{
				netHandle:       ts.Client(),
				logger:          logger.NOP,
				warehouseClient: client.NewWarehouse(ts.URL),
			}
			batchJobs := BatchJobsT{
				Jobs: []*jobsdb.JobT{
					{
						EventPayload: jsonb.RawMessage(`
					{
					  "receivedAt": "2019-10-12T07:20:50.52Z",
					  "metadata": {
						"columns": {
						  "id": "string"
						},
						"table": "tracks"
					  }
					}
				`),
						WorkspaceId: "test-workspace",
						Parameters:  jsonb.RawMessage(`{}`),
					},
				},
				BatchDestination: &DestinationT{},
			}
			err := job.postToWarehouse(&batchJobs, StorageUploadOutput{
				TotalEvents: 1,
				TotalBytes:  200,
			})
			if input.expectedError != nil {
				require.Equal(t, fmt.Sprintf(input.expectedError.Error(), ts.URL), err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
