package batchrouter

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const asyncWorkerTestDestType = "SNOWPIPE_STREAMING"

// fakeAsyncManager records the batches it receives. Its behaviour is set per destination with the hooks below.
type fakeAsyncManager struct {
	destinationID string
	env           *asyncWorkerTestEnv
}

func (m *fakeAsyncManager) Transform(job *jobsdb.JobT) (string, error) {
	return asynccommon.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (m *fakeAsyncManager) Upload(ctx context.Context, s *asynccommon.AsyncDestinationStruct) asynccommon.AsyncUploadOutput {
	jobIDs := slices.Clone(s.ImportingJobIDs)
	m.env.mu.Lock()
	m.env.uploads[m.destinationID] = append(m.env.uploads[m.destinationID], jobIDs)
	upload := m.env.upload
	m.env.mu.Unlock()
	if upload != nil {
		return upload(ctx, m.destinationID, jobIDs)
	}
	return asynccommon.AsyncUploadOutput{DestinationID: m.destinationID, SucceededJobIDs: jobIDs}
}

func (m *fakeAsyncManager) Poll(ctx context.Context, input asynccommon.AsyncPoll) asynccommon.PollStatusResponse {
	m.env.mu.Lock()
	m.env.polls[m.destinationID]++
	poll := m.env.poll
	m.env.mu.Unlock()
	if poll != nil {
		return poll(ctx, m.destinationID, input)
	}
	return asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true}
}

func (m *fakeAsyncManager) GetUploadStats(input asynccommon.GetUploadStatsInput) asynccommon.GetUploadStatsResponse {
	m.env.mu.Lock()
	stats := m.env.uploadStats
	m.env.mu.Unlock()
	if stats != nil {
		return stats(m.destinationID, input)
	}
	return asynccommon.GetUploadStatsResponse{StatusCode: http.StatusOK}
}

type asyncWorkerTestEnv struct {
	t       *testing.T
	pg      *postgres.Resource
	prefix  string
	db      jobsdb.JobsDB
	configs map[string]backendconfig.ConfigT
	dests   []string

	mu          sync.Mutex
	uploads     map[string][][]int64 // destinationID -> job IDs of each upload
	polls       map[string]int
	upload      func(ctx context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput
	poll        func(ctx context.Context, destinationID string, input asynccommon.AsyncPoll) asynccommon.PollStatusResponse
	uploadStats func(destinationID string, input asynccommon.GetUploadStatsInput) asynccommon.GetUploadStatsResponse
}

func newAsyncWorkerTestEnv(t *testing.T, pg *postgres.Resource, destinations int) *asyncWorkerTestEnv {
	misc.Init()
	config.Reset()
	t.Cleanup(config.Reset)
	config.Set("BatchRouter."+asyncWorkerTestDestType+".asyncWorker.enabled", true)
	config.Set("BatchRouter."+asyncWorkerTestDestType+".asyncWorker.pollInterval", "10ms")
	config.Set("BatchRouter.asyncUploadTimeout", "200ms")
	config.Set("BatchRouter.maxEventsInABatch", 10)
	config.Set("BatchRouter.maxPayloadSizeInBytes", 1_000_000)
	config.Set("BatchRouter.minIdleSleep", "1ms")
	config.Set("BatchRouter.uploadFreq", "10ms")
	config.Set("BatchRouter.pingFrequency", "10ms")
	config.Set("Router.jobRetention", "175200h")

	tablePrefix := fmt.Sprintf("brt_%d", time.Now().UnixNano())
	db := jobsdb.NewForReadWrite(tablePrefix, jobsdb.WithDBHandle(pg.DB), jobsdb.WithStats(stats.NOP))
	require.NoError(t, db.Start())
	t.Cleanup(db.TearDown)

	env := &asyncWorkerTestEnv{
		t:       t,
		pg:      pg,
		prefix:  tablePrefix,
		db:      db,
		uploads: map[string][][]int64{},
		polls:   map[string]int{},
	}
	source := backendconfigtest.NewSourceBuilder().WithID("source-1")
	for i := range destinations {
		dest := backendconfigtest.NewDestinationBuilder(asyncWorkerTestDestType).WithID(fmt.Sprintf("dest-%d", i)).Build()
		source.WithConnection(dest)
		env.dests = append(env.dests, dest.ID)
	}
	env.configs = map[string]backendconfig.ConfigT{
		"workspace-1": backendconfigtest.NewConfigBuilder().WithWorkspaceID("workspace-1").WithSource(source.Build()).Build(),
	}
	return env
}

// start starts a batch router with async workers. Stopping it simulates a restart.
func (env *asyncWorkerTestEnv) start() (stop func()) {
	brt := &Handle{
		asyncManagerFactory: func(_ *config.Config, _ logger.Logger, _ stats.Stats, destination *backendconfig.DestinationT, _ backendconfig.BackendConfig, _ oauthv2.SecretVersionProvider) (asynccommon.AsyncDestinationManager, error) {
			return &fakeAsyncManager{destinationID: destination.ID, env: env}, nil
		},
	}
	brt.Setup(asyncWorkerTestDestType, backendconfigtest.NewStaticLibrary(env.configs), env.db, nil,
		transientsource.NewEmptyService(), rsources.NewNoOpService(), rsources.NewStaticSyncSettingDelegate("", nil),
		destinationdebugger.NewNoOpService(), config.Default)
	require.True(env.t, brt.asyncWorkerEnabled)
	brt.Start()
	var once sync.Once
	stop = func() { once.Do(brt.Shutdown) }
	env.t.Cleanup(stop)
	return stop
}

func (env *asyncWorkerTestEnv) storeJobs(destinationID string, n int) {
	jobs := make([]*jobsdb.JobT, n)
	for i := range jobs {
		jobs[i] = &jobsdb.JobT{
			UUID:         uuid.New(),
			UserID:       "user",
			WorkspaceId:  "workspace-1",
			EventPayload: []byte(`{"type":"track","event":"e"}`),
			Parameters:   fmt.Appendf(nil, `{"source_id":"source-1","destination_id":%q,"receivedAt":%q}`, destinationID, time.Now().Format(misc.RFC3339Milli)),
			CustomVal:    asyncWorkerTestDestType,
			CreatedAt:    time.Now(),
			ExpireAt:     time.Now(),
		}
	}
	require.NoError(env.t, env.db.Store(context.Background(), jobs))
}

// jobsInState returns the IDs of the destination's jobs whose last status is the given state
func (env *asyncWorkerTestEnv) jobsInState(destinationID, state string) []int64 {
	rows, err := env.pg.DB.Query(fmt.Sprintf(`SELECT j.job_id FROM %[1]q j
		JOIN (SELECT DISTINCT ON (job_id) job_id, job_state FROM %[2]q ORDER BY job_id, id DESC) s ON s.job_id = j.job_id
		WHERE j.parameters->>'destination_id' = $1 AND s.job_state = $2 ORDER BY j.job_id`, env.prefix+"_jobs_1", env.prefix+"_job_status_1"), destinationID, state)
	require.NoError(env.t, err)
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		require.NoError(env.t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(env.t, rows.Err())
	return ids
}

// everFailed returns true if any job of the destination ever had a failed status
func (env *asyncWorkerTestEnv) everFailed(destinationID string) bool {
	var n int
	require.NoError(env.t, env.pg.DB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %[1]q s JOIN %[2]q j ON s.job_id = j.job_id
		WHERE j.parameters->>'destination_id' = $1 AND s.job_state = 'failed'`, env.prefix+"_job_status_1", env.prefix+"_jobs_1"), destinationID).Scan(&n))
	return n > 0
}

func (env *asyncWorkerTestEnv) uploadsOf(destinationID string) [][]int64 {
	env.mu.Lock()
	defer env.mu.Unlock()
	return slices.Clone(env.uploads[destinationID])
}

func (env *asyncWorkerTestEnv) set(f func()) {
	env.mu.Lock()
	defer env.mu.Unlock()
	f()
}

func TestAsyncWorker(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pg, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	t.Run("batches fill up to the limit, with no overflow jobs", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		env.storeJobs(dest, 35)
		env.start()

		require.Eventually(t, func() bool { return len(env.jobsInState(dest, jobsdb.Succeeded.State)) == 35 }, 30*time.Second, 50*time.Millisecond)
		uploads := env.uploadsOf(dest)
		require.Equal(t, []int{10, 10, 10, 5}, lo.Map(uploads, func(u []int64, _ int) int { return len(u) }))
		require.True(t, slices.IsSorted(slices.Concat(uploads...)), "batches go out in job ID order")
		require.False(t, env.everFailed(dest), "no job is marked failed because its batch was full")
	})

	t.Run("a slow destination doesn't hold up the others", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 2)
		slow, fast := env.dests[0], env.dests[1]
		release := make(chan struct{})
		env.set(func() {
			env.upload = func(ctx context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
				if destinationID == slow {
					<-release
				}
				return asynccommon.AsyncUploadOutput{DestinationID: destinationID, SucceededJobIDs: jobIDs}
			}
		})
		env.storeJobs(slow, 10)
		env.storeJobs(fast, 25)
		stop := env.start()

		require.Eventually(t, func() bool { return len(env.jobsInState(fast, jobsdb.Succeeded.State)) == 25 }, 30*time.Second, 50*time.Millisecond)
		require.Empty(t, env.jobsInState(slow, jobsdb.Succeeded.State))
		close(release)
		require.Eventually(t, func() bool { return len(env.jobsInState(slow, jobsdb.Succeeded.State)) == 10 }, 30*time.Second, 50*time.Millisecond)
		stop()
	})

	t.Run("an import finishes with a partial outcome, and failed jobs are retried before newer jobs", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		var failOnce sync.Once
		var failedJobID, abortedJobID int64
		env.set(func() {
			env.upload = func(_ context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
				return asynccommon.AsyncUploadOutput{
					DestinationID:       destinationID,
					ImportingJobIDs:     jobIDs,
					ImportingParameters: fmt.Appendf(nil, `{"importId":"import-%d","importCount":%d}`, jobIDs[0], len(jobIDs)),
				}
			}
			polls := 0
			env.poll = func(_ context.Context, _ string, _ asynccommon.AsyncPoll) asynccommon.PollStatusResponse {
				polls++
				if polls%2 == 1 {
					return asynccommon.PollStatusResponse{StatusCode: http.StatusOK, InProgress: true}
				}
				return asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true, HasFailed: true}
			}
			env.uploadStats = func(_ string, input asynccommon.GetUploadStatsInput) asynccommon.GetUploadStatsResponse {
				ids := lo.Map(input.ImportingList, func(j *jobsdb.JobT, _ int) int64 { return j.JobID })
				meta := asynccommon.EventStatMeta{FailedReasons: map[int64]string{}}
				failOnce.Do(func() {
					failedJobID, abortedJobID = ids[2], ids[4]
					meta.FailedKeys = []int64{failedJobID}
					meta.FailedReasons[failedJobID] = "rejected"
					meta.AbortedKeys = []int64{abortedJobID}
					meta.AbortedReasons = map[int64]string{abortedJobID: "invalid"}
				})
				meta.SucceededKeys = lo.Without(ids, append(meta.FailedKeys, meta.AbortedKeys...)...)
				return asynccommon.GetUploadStatsResponse{StatusCode: http.StatusOK, Metadata: meta}
			}
		})
		env.storeJobs(dest, 25)
		env.start()

		require.Eventually(t, func() bool { return len(env.jobsInState(dest, jobsdb.Succeeded.State)) == 24 }, 30*time.Second, 50*time.Millisecond)
		require.Equal(t, []int64{abortedJobID}, env.jobsInState(dest, jobsdb.Aborted.State))
		uploads := env.uploadsOf(dest)
		require.Len(t, uploads, 3)
		require.Equal(t, failedJobID, uploads[1][0], "the failed job is retried first, in the next batch")
		for _, u := range uploads {
			require.True(t, slices.IsSorted(u))
		}
	})

	t.Run("an import in progress is finished after a restart, without uploading its jobs again", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		complete := make(chan struct{})
		env.set(func() {
			env.upload = func(_ context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
				return asynccommon.AsyncUploadOutput{
					DestinationID:       destinationID,
					ImportingJobIDs:     jobIDs,
					ImportingParameters: []byte(`{"importId":"import-1","importCount":10}`),
				}
			}
			env.poll = func(_ context.Context, _ string, input asynccommon.AsyncPoll) asynccommon.PollStatusResponse {
				require.Equal(t, "import-1", input.ImportId)
				select {
				case <-complete:
					return asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true}
				default:
					return asynccommon.PollStatusResponse{StatusCode: http.StatusOK, InProgress: true}
				}
			}
		})
		env.storeJobs(dest, 10)
		stop := env.start()
		require.Eventually(t, func() bool { return len(env.jobsInState(dest, jobsdb.Importing.State)) == 10 }, 30*time.Second, 10*time.Millisecond)
		stop()

		close(complete)
		env.start()
		require.Eventually(t, func() bool { return len(env.jobsInState(dest, jobsdb.Succeeded.State)) == 10 }, 30*time.Second, 50*time.Millisecond)
		require.Len(t, env.uploadsOf(dest), 1)
	})

	t.Run("jobs that the manager leaves out of an outcome are retried", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		var first sync.Once
		env.set(func() {
			env.upload = func(_ context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
				out := asynccommon.AsyncUploadOutput{DestinationID: destinationID, SucceededJobIDs: jobIDs}
				first.Do(func() { out.SucceededJobIDs = jobIDs[:5] })
				return out
			}
		})
		env.storeJobs(dest, 10)
		env.start()
		require.Eventually(t, func() bool { return len(env.jobsInState(dest, jobsdb.Succeeded.State)) == 10 }, 30*time.Second, 50*time.Millisecond)
		uploads := env.uploadsOf(dest)
		require.Len(t, uploads, 2)
		require.Equal(t, uploads[0][5:], uploads[1])
	})
}
