package batchrouter

import (
	"context"
	jsonb "encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/filemanager/mock_filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
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
	sampleFileObjects  = []*filemanager.FileInfo{
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
	mockFileManagerFactory filemanager.Factory
	mockFileManager        *mock_filemanager.MockFileManager
	mockConfigPrefix       string
	mockFileObjects        []*filemanager.FileInfo
}

// Initiaze mocks and common expectations
func (c *testContext) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockFileManager = mock_filemanager.NewMockFileManager(c.mockCtrl)
	c.mockFileManagerFactory = func(settings *filemanager.Settings) (filemanager.FileManager, error) { return c.mockFileManager, nil }

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
}

var _ = Describe("BatchRouter", func() {
	initBatchRouter()

	var c *testContext

	BeforeEach(func() {
		config.Reset()
		config.Set("Router.jobRetention", "175200h") // 20 Years(20*365*24)
		c = &testContext{}
		c.Setup()
	})

	AfterEach(func() {
		config.Reset()
		c.Finish()
	})

	Context("Initialization", func() {
		It("should initialize and recover after crash", func() {
			batchrouter := &Handle{}

			c.mockBatchRouterJobsDB.EXPECT().GetJournalEntries(gomock.Any()).Times(1).Return(emptyJournalEntries)

			batchrouter.Setup(s3DestinationDefinition.Name, c.mockBackendConfig, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, nil, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
		})
	})

	Context("normal operation - s3 - isolation mode none", func() {
		BeforeEach(func() {
			config.Set("BatchRouter.isolationMode", "none")
			// crash recovery check
			c.mockBatchRouterJobsDB.EXPECT().GetJournalEntries(gomock.Any()).Times(1).Return(emptyJournalEntries)
		})

		It("should send failed, unprocessed jobs to s3 destination", func() {
			batchrouter := &Handle{}
			batchrouter.Setup(s3DestinationDefinition.Name, c.mockBackendConfig, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, nil, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())

			batchrouter.fileManagerFactory = c.mockFileManagerFactory

			c.mockFileManager.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return(filemanager.UploadedFile{Location: "local", ObjectName: "file"}, nil)
			c.mockFileManager.EXPECT().Prefix().Return(c.mockConfigPrefix)
			c.mockFileManager.EXPECT().ListFilesWithPrefix(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(filemanager.MockListSession(c.mockFileObjects, nil))

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
			var getJobsListCalled bool
			c.mockBatchRouterJobsDB.EXPECT().GetJobs(gomock.Any(), []string{jobsdb.Failed.State, jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{CustomValFilters: []string{CustomVal["S3"]}, JobsLimit: c.jobQueryBatchSize, PayloadSizeLimit: payloadLimit.Load()}).DoAndReturn(func(ctx context.Context, states []string, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
				var res jobsdb.JobsResult
				if !getJobsListCalled {
					getJobsListCalled = true
					jobs := append([]*jobsdb.JobT{}, toRetryJobsList...)
					jobs = append(jobs, unprocessedJobsList...)
					res.Jobs = jobs
				}
				return res, nil
			}).AnyTimes()

			c.mockBatchRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{CustomVal["S3"]}, gomock.Any()).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, `{}`, 2)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, `{}`, 1)
				}).Return(nil)

			c.mockBatchRouterJobsDB.EXPECT().JournalMarkStart(gomock.Any(), gomock.Any()).Times(1).Return(int64(1), nil)

			c.mockBatchRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil)
			c.mockBatchRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{CustomVal["S3"]}, gomock.Any()).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Succeeded.State, `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30", "success": "OK"}`, 2)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30, "success": "OK""}`, 1)
				}).Return(nil)

			c.mockBatchRouterJobsDB.EXPECT().JournalDeleteEntry(gomock.Any()).Times(1)

			<-batchrouter.backendConfigInitialized
			batchrouter.minIdleSleep = misc.SingleValueLoader(time.Microsecond)
			batchrouter.uploadFreq = misc.SingleValueLoader(time.Microsecond)
			batchrouter.mainLoopFreq = misc.SingleValueLoader(time.Microsecond)
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				batchrouter.mainLoop(ctx)
				wg.Done()
			}()
			time.Sleep(1 * time.Second)
			cancel()
			wg.Wait()
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

			job := Handle{
				netHandle:       ts.Client(),
				logger:          logger.NOP,
				warehouseClient: client.NewWarehouse(ts.URL),
			}
			batchJobs := BatchedJobs{
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
				Connection: &Connection{},
			}
			err := job.pingWarehouse(&batchJobs, UploadResult{
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

func TestGetDrainedAndOtherStatuses(t *testing.T) {
	t.Run("empty jobs and statuses", func(t *testing.T) {
		ds, os := getDrainedAndOtherStatuses(nil)
		require.Empty(t, ds)
		require.Empty(t, os)
	})
	t.Run("one aborted but non-drained job status", func(t *testing.T) {
		ds, os := getDrainedAndOtherStatuses([]*jobsdb.JobStatusT{
			{
				JobState: jobsdb.Aborted.State,
			},
		})
		require.Empty(t, ds)
		require.Len(t, os, 1)
	})
	t.Run("one aborted status that was drained", func(t *testing.T) {
		ds, os := getDrainedAndOtherStatuses([]*jobsdb.JobStatusT{
			{
				JobState:  jobsdb.Aborted.State,
				ErrorCode: routerutils.DRAIN_ERROR_CODE,
			},
		})
		require.Len(t, ds, 1)
		require.Empty(t, os)
	})
	t.Run("one aborted status that was drained and one aborted status that was not drained", func(t *testing.T) {
		ds, os := getDrainedAndOtherStatuses([]*jobsdb.JobStatusT{
			{
				JobState:  jobsdb.Aborted.State,
				ErrorCode: routerutils.DRAIN_ERROR_CODE,
			},
			{
				JobState: jobsdb.Aborted.State,
			},
		})
		require.Len(t, ds, 1)
		require.Len(t, os, 1)
	})
	t.Run("one success, one non-drain abort, drained abort", func(t *testing.T) {
		ds, os := getDrainedAndOtherStatuses([]*jobsdb.JobStatusT{
			{
				JobState:  jobsdb.Succeeded.State,
				ErrorCode: "200",
			},
			{
				JobState:  jobsdb.Aborted.State,
				ErrorCode: routerutils.DRAIN_ERROR_CODE,
			},
			{
				JobState:  jobsdb.Aborted.State,
				ErrorCode: "400",
			},
		})
		require.Len(t, ds, 1)
		require.Len(t, os, 2)
	})
}
