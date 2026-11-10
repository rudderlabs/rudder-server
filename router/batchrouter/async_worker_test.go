package batchrouter

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
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
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

const asyncWorkerTestDestType = "SNOWPIPE_STREAMING"

// fakeAsyncManager records the batches it receives. Its behaviour is set per destination with the hooks below.
type fakeAsyncManager struct {
	destinationID string
	env           *asyncWorkerTestEnv
}

func (m *fakeAsyncManager) Transform(job *jobsdb.JobT) (string, error) {
	m.env.mu.Lock()
	transform := m.env.transform
	m.env.mu.Unlock()
	if transform != nil {
		if err := transform(job); err != nil {
			return "", err
		}
	}
	return asynccommon.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (m *fakeAsyncManager) Upload(ctx context.Context, s *asynccommon.AsyncDestinationStruct) asynccommon.AsyncUploadOutput {
	jobIDs := slices.Clone(s.ImportingJobIDs)
	m.env.mu.Lock()
	m.env.uploads[m.destinationID] = append(m.env.uploads[m.destinationID], jobIDs)
	m.env.partNumbers = append(m.env.partNumbers, s.PartFileNumber)
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
	partNumbers []int // part number of each upload
	managerErr  error // why managers can't be created
	transform   func(job *jobsdb.JobT) error
	upload      func(ctx context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput
	poll        func(ctx context.Context, destinationID string, input asynccommon.AsyncPoll) asynccommon.PollStatusResponse
	uploadStats func(destinationID string, input asynccommon.GetUploadStatsInput) asynccommon.GetUploadStatsResponse
}

func newAsyncWorkerTestEnv(t *testing.T, pg *postgres.Resource, destinations int) *asyncWorkerTestEnv {
	misc.Init()
	config.Reset()
	t.Cleanup(config.Reset)
	config.Set("BatchRouter."+asyncWorkerTestDestType+".asyncWorker.enabled", true)
	config.Set("BatchRouter."+asyncWorkerTestDestType+".pollStatusLoopSleep", "10ms")
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

// setup sets up a batch router without starting its main loop
func (env *asyncWorkerTestEnv) setup() (brt *Handle, stop func()) {
	brt = &Handle{
		asyncManagerFactory: func(_ *config.Config, _ logger.Logger, _ stats.Stats, destination *backendconfig.DestinationT, _ backendconfig.BackendConfig, _ oauthv2.SecretVersionProvider) (asynccommon.AsyncDestinationManager, error) {
			env.mu.Lock()
			defer env.mu.Unlock()
			return &fakeAsyncManager{destinationID: destination.ID, env: env}, env.managerErr
		},
	}
	brt.Setup(asyncWorkerTestDestType, backendconfigtest.NewStaticLibrary(env.configs), env.db, nil,
		transientsource.NewEmptyService(), rsources.NewNoOpService(), rsources.NewStaticSyncSettingDelegate("", nil),
		destinationdebugger.NewNoOpService(), config.Default)
	<-brt.backendConfigInitialized
	var once sync.Once
	stop = func() { once.Do(brt.Shutdown) }
	env.t.Cleanup(stop)
	return brt, stop
}

// newWorker returns the worker of the first destination, which works only when the test calls it.
func (env *asyncWorkerTestEnv) newWorker() *asyncWorker {
	brt, _ := env.setup()
	return brt.asyncWorkerSupplier(env.t.Context())(env.dests[0]).(*asyncWorker)
}

// start starts a batch router. Stopping it simulates a restart.
func (env *asyncWorkerTestEnv) start() (stop func()) {
	brt, stop := env.setup()
	brt.Start()
	return stop
}

// replaceManager replaces the manager of a destination, as a config update does. The new manager records its calls
// under its own ID.
func (env *asyncWorkerTestEnv) replaceManager(brt *Handle, destinationID, managerID string) {
	brt.configSubscriberMu.Lock()
	defer brt.configSubscriberMu.Unlock()
	destinations := maps.Clone(brt.asyncDestinations)
	updated := *destinations[destinationID]
	updated.manager = &fakeAsyncManager{destinationID: managerID, env: env}
	destinations[destinationID] = &updated
	brt.asyncDestinations = destinations
}

// releaser returns a function that closes the channel. The channel is also closed at cleanup, before batch routers that
// are already started stop, so that a failed test doesn't hang.
func (env *asyncWorkerTestEnv) releaser(ch chan struct{}) (release func()) {
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	env.t.Cleanup(release)
	return release
}

func (env *asyncWorkerTestEnv) storeJobs(destinationID string, n int) {
	env.storeSourceJobs("source-1", destinationID, n)
}

func (env *asyncWorkerTestEnv) storeSourceJobs(sourceID, destinationID string, n int) {
	jobs := make([]*jobsdb.JobT, n)
	for i := range jobs {
		jobs[i] = &jobsdb.JobT{
			UUID:         uuid.New(),
			UserID:       "user",
			WorkspaceId:  "workspace-1",
			EventPayload: []byte(`{"type":"track","event":"e"}`),
			Parameters:   fmt.Appendf(nil, `{"source_id":%q,"destination_id":%q,"receivedAt":%q}`, sourceID, destinationID, time.Now().Format(misc.RFC3339Milli)),
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

// importAll makes uploads leave all their jobs importing, as import-1
func (env *asyncWorkerTestEnv) importAll() {
	env.set(func() {
		env.upload = func(_ context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
			return asynccommon.AsyncUploadOutput{
				DestinationID:       destinationID,
				ImportingJobIDs:     jobIDs,
				ImportingParameters: fmt.Appendf(nil, `{"importId":"import-1","importCount":%d}`, len(jobIDs)),
			}
		}
	})
}

// pollWith makes polls return the given response
func (env *asyncWorkerTestEnv) pollWith(resp asynccommon.PollStatusResponse) {
	env.set(func() {
		env.poll = func(context.Context, string, asynccommon.AsyncPoll) asynccommon.PollStatusResponse { return resp }
	})
}

func (env *asyncWorkerTestEnv) pollsOf(destinationID string) int {
	env.mu.Lock()
	defer env.mu.Unlock()
	return env.polls[destinationID]
}

func (env *asyncWorkerTestEnv) requireJobsInState(destinationID, state string, n int) {
	env.t.Helper()
	require.Eventually(env.t, func() bool { return len(env.jobsInState(destinationID, state)) == n }, 30*time.Second, 10*time.Millisecond)
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
	require.NoError(t, (&migrator.Migrator{Handle: pg.DB, MigrationsTable: "node_migrations"}).Migrate("node"))

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

	t.Run("with a backlog, a worker continues without waiting for a ping", func(t *testing.T) {
		newEnv := func(t *testing.T) *asyncWorkerTestEnv {
			env := newAsyncWorkerTestEnv(t, pg, 1)
			config.Set("BatchRouter.asyncUploadTimeout", "1h")
			config.Set("BatchRouter.jobQueryBatchSize", 4) // a batch takes 3 fetches
			return env
		}
		batchSizes := func(env *asyncWorkerTestEnv) []int {
			return lo.Map(env.uploadsOf(env.dests[0]), func(u []int64, _ int) int { return len(u) })
		}

		t.Run("until the batch that fills isn't full", func(t *testing.T) {
			env := newEnv(t)
			env.storeJobs(env.dests[0], 23)
			require.True(t, env.newWorker().Work())
			require.Equal(t, []int{10, 10}, batchSizes(env))
			require.Len(t, env.jobsInState(env.dests[0], jobsdb.Executing.State), 3)
		})

		t.Run("until an import starts, and again when it finishes", func(t *testing.T) {
			env := newEnv(t)
			config.Set("BatchRouter."+asyncWorkerTestDestType+".pollStatusLoopSleep", "1s")
			env.importAll()
			env.storeJobs(env.dests[0], 23)
			w := env.newWorker()
			require.True(t, w.Work())
			require.Equal(t, []int{10}, batchSizes(env))
			require.False(t, w.Work(), "the poll isn't due")
			require.Eventually(t, w.Work, 30*time.Second, 10*time.Millisecond)
			require.Equal(t, []int{10, 10}, batchSizes(env))
			require.Equal(t, 1, env.pollsOf(env.dests[0]))
		})

		t.Run("until an upload fails", func(t *testing.T) {
			env := newEnv(t)
			env.set(func() {
				env.upload = func(_ context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
					return asynccommon.AsyncUploadOutput{DestinationID: destinationID, FailedJobIDs: jobIDs, FailedReason: "unavailable"}
				}
			})
			env.storeJobs(env.dests[0], 23)
			require.True(t, env.newWorker().Work())
			require.Equal(t, []int{10}, batchSizes(env))
		})

		t.Run("when a batch is full, even without a backlog", func(t *testing.T) {
			env := newEnv(t)
			config.Set("BatchRouter.maxPayloadSizeInBytes", 50) // more than the job's event, but less than its line
			env.storeJobs(env.dests[0], 1)
			w := env.newWorker()
			require.True(t, w.Work())
			require.False(t, w.backlog)
			require.Equal(t, []int{1}, batchSizes(env))
		})

		t.Run("after a batch that left a job behind because of its bytes", func(t *testing.T) {
			env := newEnv(t)
			// the events of both jobs fit in a batch, but their lines, which are larger, don't
			config.Set("BatchRouter.maxPayloadSizeInBytes", 100)
			env.storeJobs(env.dests[0], 2)
			require.True(t, env.newWorker().Work())
			require.Equal(t, []int{1}, batchSizes(env))
			require.Len(t, env.jobsInState(env.dests[0], jobsdb.Executing.State), 1)
		})
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
		unblock := env.releaser(release)

		require.Eventually(t, func() bool { return len(env.uploadsOf(slow)) == 1 }, 30*time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool { return len(env.jobsInState(fast, jobsdb.Succeeded.State)) == 25 }, 30*time.Second, 50*time.Millisecond)
		require.Empty(t, env.jobsInState(slow, jobsdb.Succeeded.State))
		unblock()
		require.Eventually(t, func() bool { return len(env.jobsInState(slow, jobsdb.Succeeded.State)) == 10 }, 30*time.Second, 50*time.Millisecond)
		stop()
	})

	t.Run("a limited number of destinations upload at the same time", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 4)
		config.Set("BatchRouter."+asyncWorkerTestDestType+".asyncWorker.uploadLimit", 2)
		release := make(chan struct{})
		var uploading int
		env.set(func() {
			env.upload = func(_ context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
				env.set(func() { uploading++ })
				<-release
				return asynccommon.AsyncUploadOutput{DestinationID: destinationID, SucceededJobIDs: jobIDs}
			}
		})
		uploads := func() (n int) {
			env.set(func() { n = uploading })
			return n
		}
		for _, dest := range env.dests {
			env.storeJobs(dest, 10)
		}
		env.start()
		unblock := env.releaser(release)
		require.Eventually(t, func() bool { return uploads() == 2 }, 30*time.Second, 10*time.Millisecond)
		require.Never(t, func() bool { return uploads() > 2 }, 500*time.Millisecond, 50*time.Millisecond)
		unblock()
		for _, dest := range env.dests {
			env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
		}
	})

	t.Run("each destination has its own worker, whatever the isolation mode", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 2)
		config.Set("BatchRouter.isolationMode", "none")
		for _, dest := range env.dests {
			env.storeJobs(dest, 1)
		}
		brt, _ := env.setup()
		require.ElementsMatch(t, env.dests, brt.activePartitions(t.Context()))
	})

	t.Run("a worker fetches no more than its batch can take", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		env.storeJobs(env.dests[0], 20)
		w := env.newWorker()
		require.Len(t, w.getJobs(), 10)
		w.batch.jobs = make([]*jobsdb.JobT, 3)
		require.Len(t, w.getJobs(), 7)
		w.batch.size = w.batch.limits.bytes - 1
		require.Len(t, w.getJobs(), 1)
	})

	t.Run("a batch that is old enough takes the jobs that arrived while it waited", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		w := env.newWorker()
		env.storeJobs(dest, 3)
		require.True(t, w.Work())
		require.Empty(t, env.uploadsOf(dest))

		env.storeJobs(dest, 2)
		w.batch.createdAt = time.Now().Add(-time.Hour)
		require.True(t, w.Work())
		require.Equal(t, [][]int64{{1, 2, 3, 4, 5}}, env.uploadsOf(dest))
	})

	t.Run("a worker doesn't sleep past its next poll or upload", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		config.Set("BatchRouter.minIdleSleep", "10m")
		config.Set("BatchRouter.uploadFreq", "1h")
		config.Set("BatchRouter.asyncUploadTimeout", "2m")
		config.Set("BatchRouter.maxConsecutiveFailures", 1)
		w := env.newWorker()
		requireSleeps := func(minSleep, maxSleep time.Duration) {
			t.Helper()
			actualMin, actualMax := w.SleepDurations()
			require.InDelta(t, minSleep, actualMin, float64(10*time.Second))
			require.InDelta(t, maxSleep, actualMax, float64(10*time.Second))
		}
		requireSleeps(10*time.Minute, 30*time.Minute)

		w.batch.jobs = []*jobsdb.JobT{{JobID: 1}}
		w.batch.createdAt = time.Now()
		requireSleeps(2*time.Minute, 2*time.Minute)
		w.batch.createdAt = time.Now().Add(-time.Hour)
		requireSleeps(0, 0)

		w.imports = []*asyncImport{{nextPoll: time.Now().Add(time.Minute)}}
		requireSleeps(time.Minute, time.Minute)

		w.imports = nil
		w.cb.Failure()
		require.True(t, w.cb.IsOpen())
		requireSleeps(10*time.Minute, 30*time.Minute) // the batch waits for the breaker
	})

	t.Run("part numbers restart with each source job run", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		w := env.newWorker()
		dest, ok := w.brt.asyncDestination(env.dests[0])
		require.True(t, ok)
		for _, run := range []string{"run-1", "run-1", "run-2"} {
			w.upload(dest, &asyncBatch{jobs: []*jobsdb.JobT{{JobID: 1, Parameters: fmt.Appendf(nil, `{"source_job_run_id":%q}`, run)}}})
		}
		env.set(func() { require.Equal(t, []int{1, 2, 1}, env.partNumbers) })
	})

	t.Run("the jobs of a destination whose manager can't be created are aborted, and its worker is paused", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		config.Set("BatchRouter.maxConsecutiveFailures", 1)
		dest := env.dests[0]
		env.set(func() { env.managerErr = errors.New("invalid credentials") })
		env.storeJobs(dest, 25)
		w := env.newWorker()
		require.True(t, w.Work())
		require.Len(t, env.jobsInState(dest, jobsdb.Aborted.State), 10)
		require.True(t, w.cb.IsOpen())
		require.False(t, w.Work())
		require.Len(t, env.jobsInState(dest, jobsdb.Aborted.State), 10)
	})

	for _, refs := range []bool{false, true} {
		for _, enable := range []bool{false, true} {
			t.Run(fmt.Sprintf("switching async workers during an import (on: %t, import ID references: %t) doesn't upload its jobs again", enable, refs), func(t *testing.T) {
				env := newAsyncWorkerTestEnv(t, pg, 1)
				config.Set("BatchRouter.writeImportIdRefs", refs)
				config.Set("BatchRouter.asyncUploadWorkerTimeout", "1h")
				setEnabled := func(enabled bool) {
					config.Set("BatchRouter."+asyncWorkerTestDestType+".asyncWorker.enabled", enabled)
				}
				dest := env.dests[0]
				env.importAll()
				env.pollWith(asynccommon.PollStatusResponse{StatusCode: http.StatusOK, InProgress: true})
				env.storeJobs(dest, 10)
				setEnabled(!enable)
				brt, stop := env.setup()
				if enable { // what the upload loop writes: running it would be a data race with the goroutines that fill its batch
					res, err := env.db.GetUnprocessed(t.Context(), jobsdb.GetQueryParams{CustomValFilters: []string{asyncWorkerTestDestType}, JobsLimit: 10})
					require.NoError(t, err)
					brt.setMultipleJobStatus(setMultipleJobStatusParams{
						asyncJobMetadata: newAsyncJobMetadata(res.Jobs),
						AsyncOutput:      env.upload(t.Context(), dest, jobIDs(res.Jobs)),
						Attempted:        true,
					})
				} else {
					brt.Start()
				}
				env.requireJobsInState(dest, jobsdb.Importing.State, 10)
				require.Eventually(t, func() bool { return env.pollsOf(dest) > 0 }, 30*time.Second, 10*time.Millisecond)
				stop()
				uploads := len(env.uploadsOf(dest))

				var importIDs []string
				env.set(func() {
					env.poll = func(_ context.Context, _ string, input asynccommon.AsyncPoll) asynccommon.PollStatusResponse {
						env.set(func() { importIDs = append(importIDs, input.ImportId) })
						return asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true}
					}
				})
				setEnabled(enable)
				env.start()
				env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
				require.Len(t, env.uploadsOf(dest), uploads)
				env.set(func() { require.Equal(t, []string{"import-1"}, importIDs) })
			})
		}
	}

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

	for _, refs := range []bool{false, true} {
		t.Run(fmt.Sprintf("an import in progress is finished after a restart, without uploading its jobs again (import ID references: %t)", refs), func(t *testing.T) {
			env := newAsyncWorkerTestEnv(t, pg, 1)
			config.Set("BatchRouter.writeImportIdRefs", refs)
			dest := env.dests[0]
			env.importAll()
			var importIDs []string
			complete := false
			env.set(func() {
				env.poll = func(_ context.Context, _ string, input asynccommon.AsyncPoll) asynccommon.PollStatusResponse {
					env.mu.Lock()
					defer env.mu.Unlock()
					importIDs = append(importIDs, input.ImportId)
					return asynccommon.PollStatusResponse{StatusCode: http.StatusOK, InProgress: !complete, Complete: complete}
				}
			})
			env.storeJobs(dest, 10)
			stop := env.start()
			require.Eventually(t, func() bool { return env.pollsOf(dest) > 0 }, 30*time.Second, 10*time.Millisecond)
			stop()
			require.Len(t, env.jobsInState(dest, jobsdb.Importing.State), 10)
			var withRef int
			require.NoError(t, pg.DB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %q WHERE parameters ? 'importIdRef'`, env.prefix+"_job_status_1")).Scan(&withRef))
			require.Equal(t, lo.Ternary(refs, 10, 0), withRef)

			env.set(func() { complete = true })
			env.start()
			env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
			require.Len(t, env.uploadsOf(dest), 1)
			env.set(func() { require.Equal(t, []string{"import-1"}, lo.Uniq(importIDs)) })
		})
	}

	t.Run("an import whose ID is lost is uploaded again", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		config.Set("BatchRouter.writeImportIdRefs", true)
		dest := env.dests[0]
		env.importAll()
		env.pollWith(asynccommon.PollStatusResponse{InProgress: true})
		env.storeJobs(dest, 10)
		stop := env.start()
		env.requireJobsInState(dest, jobsdb.Importing.State, 10)
		stop()

		_, err := pg.DB.Exec(`DELETE FROM async_import_ids`)
		require.NoError(t, err)
		env.pollWith(asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true})
		env.start()
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
		require.Len(t, env.uploadsOf(dest), 2)
	})

	t.Run("an import stays in progress while its ID cannot be looked up", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		config.Set("BatchRouter.writeImportIdRefs", true)
		dest := env.dests[0]
		env.importAll()
		env.pollWith(asynccommon.PollStatusResponse{InProgress: true})
		env.storeJobs(dest, 10)
		stop := env.start()
		env.requireJobsInState(dest, jobsdb.Importing.State, 10)
		stop()

		_, err := pg.DB.Exec(`ALTER TABLE async_import_ids RENAME TO async_import_ids_hidden`)
		require.NoError(t, err)
		polls := env.pollsOf(dest)
		env.pollWith(asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true})
		env.start()
		require.Never(t, func() bool {
			return env.pollsOf(dest) > polls || len(env.jobsInState(dest, jobsdb.Importing.State)) != 10
		}, 500*time.Millisecond, 50*time.Millisecond)
		_, err = pg.DB.Exec(`ALTER TABLE async_import_ids_hidden RENAME TO async_import_ids`)
		require.NoError(t, err)
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
		require.Len(t, env.uploadsOf(dest), 1)
	})

	t.Run("all jobs of an import are found at start, even if it is larger than a batch", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		env.importAll()
		env.pollWith(asynccommon.PollStatusResponse{InProgress: true})
		env.storeJobs(dest, 10)
		stop := env.start()
		env.requireJobsInState(dest, jobsdb.Importing.State, 10)
		stop()

		config.Set("BatchRouter.maxEventsInABatch", 2)
		var found []int
		env.pollWith(asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true, HasFailed: true})
		env.set(func() {
			env.uploadStats = func(_ string, input asynccommon.GetUploadStatsInput) asynccommon.GetUploadStatsResponse {
				env.set(func() { found = append(found, len(input.ImportingList)) })
				return asynccommon.GetUploadStatsResponse{StatusCode: http.StatusOK, Metadata: asynccommon.EventStatMeta{
					SucceededKeys: lo.Map(input.ImportingList, func(j *jobsdb.JobT, _ int) int64 { return j.JobID }),
				}}
			}
		})
		env.start()
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
		env.set(func() { require.Equal(t, []int{10}, found) })
	})

	t.Run("an import without importing parameters is polled", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		env.set(func() {
			env.upload = func(_ context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
				return asynccommon.AsyncUploadOutput{DestinationID: destinationID, ImportingJobIDs: jobIDs}
			}
		})
		env.storeJobs(dest, 25)
		env.start()
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 25)
		require.Len(t, env.uploadsOf(dest), 3)
		require.Equal(t, 3, env.pollsOf(dest))
	})

	t.Run("imports found at start are polled oldest first, before anything else is uploaded", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		env.storeJobs(dest, 15)
		res, err := env.db.GetUnprocessed(context.Background(), jobsdb.GetQueryParams{CustomValFilters: []string{asyncWorkerTestDestType}, JobsLimit: 15})
		require.NoError(t, err)
		require.NoError(t, env.db.UpdateJobStatus(context.Background(), lo.Map(res.Jobs[:10], func(job *jobsdb.JobT, i int) *jobsdb.JobStatusT {
			return &jobsdb.JobStatusT{
				JobID: job.JobID, JobState: jobsdb.Importing.State, AttemptNum: 1, ExecTime: time.Now(), RetryTime: time.Now(), ErrorCode: "200",
				ErrorResponse: []byte(`{}`), Parameters: fmt.Appendf(nil, `{"importId":"import-%d"}`, i/5), JobParameters: job.Parameters, WorkspaceId: job.WorkspaceId, PartitionID: job.PartitionID, CustomVal: job.CustomVal,
			}
		})))
		var importIDs []string
		env.set(func() {
			env.poll = func(_ context.Context, _ string, input asynccommon.AsyncPoll) asynccommon.PollStatusResponse {
				env.set(func() {
					assert.Empty(t, env.uploads[dest])
					importIDs = append(importIDs, input.ImportId)
				})
				return asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true}
			}
		})
		env.start()
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 15)
		env.set(func() { require.Equal(t, []string{"import-0", "import-1"}, importIDs) })
		require.Len(t, env.uploadsOf(dest), 1)
	})

	t.Run("a destination's state survives the worker pool stopping its idle worker", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		config.Set("BatchRouter.asyncUploadTimeout", "1h")
		dest := env.dests[0]
		brt, _ := env.setup()
		ctx, cancel := context.WithCancel(context.Background())
		pool := workerpool.New(ctx, brt.asyncWorkerSupplier(ctx), logger.NOP, workerpool.WithCleanupPeriod(time.Millisecond), workerpool.WithIdleTimeout(time.Millisecond))
		defer pool.Shutdown()
		defer cancel()
		pingUntil := func(condition func() bool) {
			require.Eventually(t, func() bool {
				pool.PingWorker(dest)
				return condition()
			}, 30*time.Second, 10*time.Millisecond)
		}
		waitUntilStopped := func() {
			pool.PingWorker(dest) // a worker becomes idle once a ping finds nothing to do
			require.Eventually(t, func() bool { return pool.Size() == 0 }, 30*time.Second, time.Millisecond)
		}

		for n := range 2 {
			env.storeJobs(dest, 10)
			pingUntil(func() bool { return len(env.jobsInState(dest, jobsdb.Succeeded.State)) == 10*(n+1) })
			waitUntilStopped()
		}
		env.set(func() { require.Equal(t, []int{1, 2}, env.partNumbers) })

		env.storeJobs(dest, 5)
		pingUntil(func() bool { return len(env.jobsInState(dest, jobsdb.Executing.State)) == 5 })
		waitUntilStopped()
		env.storeJobs(dest, 5)
		pingUntil(func() bool { return len(env.jobsInState(dest, jobsdb.Succeeded.State)) == 30 })
		require.Len(t, env.uploadsOf(dest), 3, "the batch that was filling kept its jobs")
	})

	t.Run("a poll uses the manager that is current when it gets its slot", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 2)
		config.Set("BatchRouter."+asyncWorkerTestDestType+".asyncWorker.pollLimit", 1)
		holder, waiter := env.dests[0], env.dests[1]
		env.importAll()
		polling, release := make(chan struct{}), make(chan struct{})
		var first sync.Once
		env.set(func() {
			env.poll = func(_ context.Context, destinationID string, _ asynccommon.AsyncPoll) asynccommon.PollStatusResponse {
				if destinationID == holder {
					first.Do(func() { close(polling) })
					<-release
				}
				return asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true}
			}
		})
		env.storeJobs(holder, 10)
		brt, _ := env.setup()
		brt.Start()
		unblock := env.releaser(release)
		<-polling
		env.storeJobs(waiter, 10)
		env.requireJobsInState(waiter, jobsdb.Importing.State, 10)
		time.Sleep(200 * time.Millisecond) // let the waiter's poll come due and wait for the slot

		env.replaceManager(brt, waiter, "updated")
		unblock()
		env.requireJobsInState(waiter, jobsdb.Succeeded.State, 10)
		require.Zero(t, env.pollsOf(waiter))
		require.Equal(t, 1, env.pollsOf("updated"))
	})

	t.Run("an upload uses the manager that is current when it gets its slot", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 2)
		config.Set("BatchRouter."+asyncWorkerTestDestType+".asyncWorker.uploadLimit", 1)
		holder, waiter := env.dests[0], env.dests[1]
		uploading, release := make(chan struct{}), make(chan struct{})
		env.set(func() {
			env.upload = func(_ context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
				if destinationID == holder {
					close(uploading)
					<-release
				}
				return asynccommon.AsyncUploadOutput{DestinationID: destinationID, SucceededJobIDs: jobIDs}
			}
		})
		env.storeJobs(holder, 10)
		brt, _ := env.setup()
		brt.Start()
		unblock := env.releaser(release)
		<-uploading
		env.storeJobs(waiter, 10)
		env.requireJobsInState(waiter, jobsdb.Executing.State, 10)
		time.Sleep(200 * time.Millisecond) // let the waiter's upload wait for the slot

		env.replaceManager(brt, waiter, "updated")
		unblock()
		env.requireJobsInState(waiter, jobsdb.Succeeded.State, 10)
		require.Empty(t, env.uploadsOf(waiter))
		require.Len(t, env.uploadsOf("updated"), 1)
	})

	t.Run("a job that cannot be transformed is aborted", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		var first sync.Once
		env.set(func() {
			env.transform = func(*jobsdb.JobT) (err error) {
				first.Do(func() { err = errors.New("invalid job") })
				return err
			}
		})
		env.storeJobs(dest, 10)
		env.start()
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 9)
		aborted := env.jobsInState(dest, jobsdb.Aborted.State)
		require.Len(t, aborted, 1)
		var attempt int
		var response string
		require.NoError(t, pg.DB.QueryRow(fmt.Sprintf(`SELECT attempt, error_response::text FROM %q WHERE job_id = $1 AND job_state = 'aborted'`, env.prefix+"_job_status_1"), aborted[0]).Scan(&attempt, &response))
		require.Equal(t, 1, attempt)
		require.Contains(t, response, "invalid job")
	})

	t.Run("with egress disabled, jobs succeed without being uploaded, unless they must be drained", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		config.Set("disableEgress", true)
		dest := env.dests[0]
		env.storeJobs(dest, 5)
		env.storeSourceJobs("disconnected", dest, 3)
		env.start()
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 5)
		env.requireJobsInState(dest, jobsdb.Aborted.State, 3)
		require.Empty(t, env.uploadsOf(dest))
	})

	t.Run("a destination whose uploads keep failing is paused", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		config.Set("BatchRouter.maxConsecutiveFailures", 2)
		config.Set("BatchRouter.timeout", "1h")
		dest := env.dests[0]
		env.set(func() {
			env.upload = func(_ context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
				return asynccommon.AsyncUploadOutput{DestinationID: destinationID, FailedJobIDs: jobIDs, FailedReason: "unavailable"}
			}
		})
		env.storeJobs(dest, 10)
		env.start()
		require.Eventually(t, func() bool { return len(env.uploadsOf(dest)) >= 2 }, 30*time.Second, 10*time.Millisecond)
		time.Sleep(200 * time.Millisecond)
		uploads := len(env.uploadsOf(dest))
		require.Never(t, func() bool { return len(env.uploadsOf(dest)) > uploads }, 500*time.Millisecond, 50*time.Millisecond)
	})

	t.Run("a paused worker continues to poll its import", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		config.Set("BatchRouter.maxConsecutiveFailures", 1)
		config.Set("BatchRouter.timeout", "1h")
		dest := env.dests[0]
		env.importAll()
		env.storeJobs(dest, 25)
		w := env.newWorker()
		require.True(t, w.Work())
		require.Len(t, env.jobsInState(dest, jobsdb.Importing.State), 10)
		w.cb.Failure()
		require.True(t, w.cb.IsOpen())
		require.Eventually(t, w.Work, 30*time.Second, 10*time.Millisecond)
		require.Len(t, env.jobsInState(dest, jobsdb.Succeeded.State), 10)
		require.Len(t, env.uploadsOf(dest), 1)
	})

	for _, tc := range []struct {
		name       string
		statusCode int
		pauses     bool
	}{
		{name: "an import whose jobs all failed counts as a failure", statusCode: http.StatusServiceUnavailable, pauses: true},
		{name: "an import that the destination rejected doesn't count as a failure", statusCode: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newAsyncWorkerTestEnv(t, pg, 1)
			config.Set("BatchRouter.maxConsecutiveFailures", 1)
			config.Set("BatchRouter.timeout", "1h")
			env.importAll()
			env.pollWith(asynccommon.PollStatusResponse{StatusCode: tc.statusCode, Error: "error"})
			env.storeJobs(env.dests[0], 10)
			w := env.newWorker()
			require.True(t, w.Work())
			require.Len(t, w.imports, 1)
			require.Eventually(t, w.Work, 30*time.Second, 10*time.Millisecond)
			require.Empty(t, w.imports)
			require.Equal(t, tc.pauses, w.cb.IsOpen())
		})
	}

	t.Run("a finished import whose outcome can't be read is polled again", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		env.importAll()
		env.pollWith(asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true, HasFailed: true})
		var first sync.Once
		env.set(func() {
			env.uploadStats = func(_ string, input asynccommon.GetUploadStatsInput) asynccommon.GetUploadStatsResponse {
				resp := asynccommon.GetUploadStatsResponse{StatusCode: http.StatusOK, Metadata: asynccommon.EventStatMeta{
					SucceededKeys: lo.Map(input.ImportingList, func(j *jobsdb.JobT, _ int) int64 { return j.JobID }),
				}}
				first.Do(func() {
					resp = asynccommon.GetUploadStatsResponse{StatusCode: http.StatusInternalServerError, Error: "error"}
				})
				return resp
			}
		})
		env.storeJobs(dest, 10)
		env.start()
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
		require.Equal(t, 2, env.pollsOf(dest))
		require.Len(t, env.uploadsOf(dest), 1)
	})

	t.Run("a poll that returns a client error aborts the import", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		env.importAll()
		env.pollWith(asynccommon.PollStatusResponse{StatusCode: http.StatusBadRequest, Error: "bad request"})
		env.storeJobs(dest, 10)
		env.start()
		env.requireJobsInState(dest, jobsdb.Aborted.State, 10)
		require.Len(t, env.uploadsOf(dest), 1)
	})

	t.Run("a poll that returns a server error retries the import", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		env.importAll()
		var first sync.Once
		env.set(func() {
			env.poll = func(context.Context, string, asynccommon.AsyncPoll) asynccommon.PollStatusResponse {
				resp := asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true}
				first.Do(func() {
					resp = asynccommon.PollStatusResponse{StatusCode: http.StatusServiceUnavailable, Error: "unavailable"}
				})
				return resp
			}
		})
		env.storeJobs(dest, 10)
		env.start()
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
		require.Len(t, env.uploadsOf(dest), 2)
	})

	t.Run("shutdown waits for a running upload and writes its statuses", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		uploading, release := make(chan struct{}), make(chan struct{})
		env.set(func() {
			env.upload = func(ctx context.Context, destinationID string, jobIDs []int64) asynccommon.AsyncUploadOutput {
				close(uploading)
				<-release
				assert.NoError(t, ctx.Err(), "shutdown doesn't cancel an upload")
				return asynccommon.AsyncUploadOutput{DestinationID: destinationID, ImportingJobIDs: jobIDs, ImportingParameters: []byte(`{"importId":"import-1"}`)}
			}
		})
		env.storeJobs(dest, 10)
		stop := env.start()
		unblock := env.releaser(release)
		<-uploading
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			stop()
		}()
		require.Never(t, func() bool {
			select {
			case <-stopped:
				return true
			default:
				return false
			}
		}, 200*time.Millisecond, 20*time.Millisecond)
		unblock()
		<-stopped
		require.Len(t, env.jobsInState(dest, jobsdb.Importing.State), 10)

		env.start()
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
		require.Len(t, env.uploadsOf(dest), 1)
	})

	t.Run("a poll that shutdown interrupts writes no statuses", func(t *testing.T) {
		env := newAsyncWorkerTestEnv(t, pg, 1)
		dest := env.dests[0]
		env.importAll()
		polling := make(chan struct{})
		env.set(func() {
			env.poll = func(ctx context.Context, _ string, _ asynccommon.AsyncPoll) asynccommon.PollStatusResponse {
				close(polling)
				<-ctx.Done()
				return asynccommon.PollStatusResponse{StatusCode: http.StatusInternalServerError, Error: ctx.Err().Error()}
			}
		})
		env.storeJobs(dest, 10)
		stop := env.start()
		<-polling
		stop()
		require.Len(t, env.jobsInState(dest, jobsdb.Importing.State), 10)

		env.pollWith(asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true})
		env.start()
		env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
		require.Len(t, env.uploadsOf(dest), 1)
	})

	t.Run("jobs that the manager leaves out of the outcome of an upload are retried", func(t *testing.T) {
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

	for _, limitReached := range []bool{false, true} {
		t.Run(fmt.Sprintf("jobs that the manager leaves out of the outcome of an import are retried until the retry limit (reached: %t)", limitReached), func(t *testing.T) {
			env := newAsyncWorkerTestEnv(t, pg, 1)
			if limitReached {
				config.Set("BatchRouter.maxFailedCountForJob", 1)
				config.Set("BatchRouter.retryTimeWindow", "0s")
			}
			dest := env.dests[0]
			env.importAll()
			env.pollWith(asynccommon.PollStatusResponse{StatusCode: http.StatusOK, Complete: true, HasFailed: true})
			var first sync.Once
			env.set(func() {
				env.uploadStats = func(_ string, input asynccommon.GetUploadStatsInput) asynccommon.GetUploadStatsResponse {
					ids := lo.Map(input.ImportingList, func(j *jobsdb.JobT, _ int) int64 { return j.JobID })
					first.Do(func() { ids = ids[:5] })
					return asynccommon.GetUploadStatsResponse{StatusCode: http.StatusOK, Metadata: asynccommon.EventStatMeta{SucceededKeys: ids}}
				}
			})
			env.storeJobs(dest, 10)
			env.start()
			if limitReached {
				env.requireJobsInState(dest, jobsdb.Aborted.State, 5)
				require.Len(t, env.uploadsOf(dest), 1)
				return
			}
			env.requireJobsInState(dest, jobsdb.Succeeded.State, 10)
			uploads := env.uploadsOf(dest)
			require.Len(t, uploads, 2)
			require.Equal(t, uploads[0][5:], uploads[1])
		})
	}
}

func TestAsyncWorkerBatchLimits(t *testing.T) {
	for destType, limits := range map[string]asyncBatchLimits{
		"ELOQUA": {jobs: 10, bytes: 100},
		"SFTP":   {bytes: 100},
	} {
		w := &asyncWorker{brt: &Handle{destType: destType, maxEventsInABatch: 10, maxPayloadSizeInBytes: 100}}
		require.Equal(t, limits, w.newBatch().limits, destType)
	}
}
