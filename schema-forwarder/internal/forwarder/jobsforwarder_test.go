package forwarder

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pulsarType "github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	pgdocker "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	pulsardocker "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/pulsar"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/testdata"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func Test_JobsForwarder(t *testing.T) {
	conf := config.New()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pulsarContainer := PulsarResource(t)

	conf.Set("Pulsar.Client.url", pulsarContainer.URL)
	conf.Set("SchemaForwarder.loopSleepTime", time.Millisecond)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(gomock.NewController(t))

	postgres, err := pgdocker.Setup(pool, t)
	require.NoError(t, err)
	t.Setenv("JOBS_DB_HOST", postgres.Host)
	t.Setenv("JOBS_DB_PORT", postgres.Port)
	t.Setenv("JOBS_DB_USER", postgres.User)
	t.Setenv("JOBS_DB_DB_NAME", postgres.Database)
	t.Setenv("JOBS_DB_PASSWORD", postgres.Password)
	schemasDB := jobsdb.NewForReadWrite(
		"test_event_schema",
		jobsdb.WithStats(stats.NOP),
	)
	err = schemasDB.Start()
	require.NoError(t, err)
	defer schemasDB.TearDown()

	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{testdata.SampleWorkspaceID: testdata.SampleBackendConfig}, Topic: string(topic)}
			close(ch)
			return ch
		})

	client, err := pulsar.NewClient(conf)
	require.NoError(t, err)
	jf := NewJobsForwarder(func(error) {}, schemasDB, &client, conf, mockBackendConfig, logger.NOP, stats.Default)
	require.NotNil(t, jf)
	require.NoError(t, jf.Start())
	defer jf.Stop()

	t.Run("jobs for a valid source should succeed", func(t *testing.T) {
		generateJobs := func(numOfJob int) []*jobsdb.JobT {
			customVal := "MOCKDS"
			js := make([]*jobsdb.JobT, numOfJob)
			for i := range numOfJob {
				js[i] = &jobsdb.JobT{
					Parameters:   []byte(`{"batch_id":1,"source_id":"enabled-source","source_job_run_id":""}`),
					EventPayload: []byte(`{"testKey":"testValue"}`),
					UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
					UUID:         uuid.New(),
					CustomVal:    customVal,
					EventCount:   1,
					WorkspaceId:  "test_workspace",
				}
			}
			return js
		}
		jobs := generateJobs(10)
		err = schemasDB.Store(context.Background(), jobs)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			jobs, err := schemasDB.GetSucceeded(context.Background(), jobsdb.GetQueryParams{
				JobsLimit: 10,
			})
			require.NoError(t, err)
			return len(jobs.Jobs) == 10
		}, 20*time.Second, 100*time.Millisecond)
	})

	t.Run("jobs for an invalid source should be aborted", func(t *testing.T) {
		generateJobs := func(numOfJob int) []*jobsdb.JobT {
			customVal := "MOCKDS"
			js := make([]*jobsdb.JobT, numOfJob)
			for i := range numOfJob {
				js[i] = &jobsdb.JobT{
					Parameters:   []byte(`{"batch_id":1,"source_id":"invalid","source_job_run_id":""}`),
					EventPayload: []byte(`{"testKey":"testValue"}`),
					UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
					UUID:         uuid.New(),
					CustomVal:    customVal,
					EventCount:   1,
					WorkspaceId:  "test_workspace",
				}
			}
			return js
		}
		jobs := generateJobs(10)
		err = schemasDB.Store(context.Background(), jobs)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			jobs, err := schemasDB.GetAborted(context.Background(), jobsdb.GetQueryParams{
				JobsLimit: 10,
			})
			require.NoError(t, err)
			return len(jobs.Jobs) == 10
		}, 20*time.Second, 100*time.Millisecond)
	})

	t.Run("late producer callbacks after a failed flush are collected", func(t *testing.T) {
		jf, statusesCh := newMockedJobsForwarder(t)
		collecting := &collectingSignal{started: make(chan struct{}), timeout: 10 * time.Second}
		jf.callbackTimeout = collecting
		producer := &lateAckProducer{release: make(chan struct{})}
		jf.pulsarProducer = producer
		jf.start()
		t.Cleanup(jf.Stop)
		t.Cleanup(producer.releaseCallbacks)

		select {
		case <-collecting.started:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for the forwarder to collect producer callbacks")
		}
		producer.releaseCallbacks()

		statuses := receiveStatuses(t, statusesCh)
		require.Len(t, statuses, mockedNumOfJobs)
		seen := make(map[int64]struct{}, mockedNumOfJobs)
		for _, status := range statuses {
			require.Equal(t, jobsdb.Succeeded.State, status.JobState, "job %d", status.JobID)
			seen[status.JobID] = struct{}{}
		}
		require.Len(t, seen, mockedNumOfJobs, "every job should have a status")
	})

	t.Run("stopping while waiting for producer callbacks neither waits for them, blocks them nor aborts jobs", func(t *testing.T) {
		jf, _ := newMockedJobsForwarder(t)
		statsStore, err := memstats.New()
		require.NoError(t, err)
		jf.stat = statsStore
		collecting := &collectingSignal{started: make(chan struct{}), timeout: time.Minute}
		jf.callbackTimeout = collecting
		producer := &lateAckProducer{release: make(chan struct{})}
		jf.pulsarProducer = producer
		jf.start()
		stop := sync.OnceFunc(jf.Stop)
		t.Cleanup(stop)
		t.Cleanup(producer.releaseCallbacks)

		select {
		case <-collecting.started:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for the forwarder to collect producer callbacks")
		}

		stopped := make(chan struct{})
		go func() {
			stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Fatal("stopping the forwarder waited for the producer callbacks")
		}
		require.Nil(t, statsStore.Get("schema_forwarder_jobs", stats.Tags{"state": "abort"}), "no job should be counted as aborted on shutdown")

		producer.releaseCallbacks()
		callbacksReturned := make(chan struct{})
		go func() {
			producer.running.Wait()
			close(callbacksReturned)
		}()
		select {
		case <-callbacksReturned:
		case <-time.After(5 * time.Second):
			t.Fatal("producer callbacks blocked after the forwarder stopped")
		}
	})

	t.Run("stopping before the first forwarding attempt neither forwards nor aborts jobs", func(t *testing.T) {
		jf, statusesCh := newMockedJobsForwarder(t)
		statsStore, err := memstats.New()
		require.NoError(t, err)
		jf.stat = statsStore
		producer := &lateAckProducer{release: make(chan struct{})}
		jf.pulsarProducer = producer
		cancelled := make(chan struct{})
		jf.initialRetryInterval = durationLoaderFunc(func() time.Duration { // loaded right before the first attempt
			jf.cancel()
			close(cancelled)
			return time.Second
		})
		jf.start()
		t.Cleanup(jf.Stop)

		select {
		case <-cancelled:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for the forwarder to start forwarding")
		}
		require.NoError(t, jf.g.Wait())

		producer.mu.Lock()
		require.Zero(t, producer.sent, "no message should be sent after stopping")
		producer.mu.Unlock()
		require.Nil(t, statsStore.Get("schema_forwarder_jobs", stats.Tags{"state": "abort"}), "no job should be counted as aborted on shutdown")
		require.Empty(t, statusesCh, "no job status should be marked on shutdown")
	})

	t.Run("jobs whose producer callbacks never arrive are aborted", func(t *testing.T) {
		jf, statusesCh := newMockedJobsForwarder(t)
		jf.callbackTimeout = config.SingleValueLoader(50 * time.Millisecond)
		jf.pulsarProducer = &silentProducer{}
		jf.start()
		stopped := make(chan struct{})
		var stopOnce sync.Once
		stop := func() bool {
			stopOnce.Do(func() {
				go func() {
					jf.Stop()
					close(stopped)
				}()
			})
			select {
			case <-stopped:
				return true
			case <-time.After(10 * time.Second):
				return false
			}
		}
		t.Cleanup(func() {
			if !stop() {
				t.Error("timed out waiting for the forwarder to stop")
			}
		})

		statuses := receiveStatuses(t, statusesCh)
		require.Len(t, statuses, mockedNumOfJobs)
		for _, status := range statuses {
			require.Equal(t, jobsdb.Aborted.State, status.JobState, "job %d", status.JobID)
		}
		require.True(t, stop(), "timed out waiting for the forwarder to stop")
	})

	t.Run("jobs whose producer callbacks fail are aborted", func(t *testing.T) {
		jf, statusesCh := newMockedJobsForwarder(t)
		jf.pulsarProducer = &failingProducer{}
		jf.start()
		t.Cleanup(jf.Stop)

		statuses := receiveStatuses(t, statusesCh)
		require.Len(t, statuses, mockedNumOfJobs)
		for _, status := range statuses {
			require.Equal(t, jobsdb.Aborted.State, status.JobState, "job %d", status.JobID)
		}
	})
}

// mockedNumOfJobs is the number of jobs that the jobsdb of [newMockedJobsForwarder] returns
const mockedNumOfJobs = 20

// newMockedJobsForwarder returns a forwarder that is not started, without a producer, whose jobsdb returns
// mockedNumOfJobs jobs once and sends every non-empty batch of job statuses it marks to the returned channel
func newMockedJobsForwarder(t *testing.T) (*JobsForwarder, <-chan []*jobsdb.JobStatusT) {
	t.Helper()
	jobs := make([]*jobsdb.JobT, mockedNumOfJobs)
	for i := range jobs {
		jobs[i] = &jobsdb.JobT{
			JobID:        int64(i + 1),
			Parameters:   []byte(`{"source_id":"enabled-source"}`),
			EventPayload: []byte(fmt.Sprintf(`{"type":"track","event":"test","properties":{"key%d":"value"}}`, i)), // distinct schemas produce distinct batches
			WorkspaceId:  testdata.SampleWorkspaceID,
		}
	}

	ctrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{testdata.SampleWorkspaceID: testdata.SampleBackendConfig}, Topic: string(topic)}
			close(ch)
			return ch
		})

	statusesCh := make(chan []*jobsdb.JobStatusT, 1)
	mockJobsDB := mocksJobsDB.NewMockJobsDB(ctrl)
	mockJobsDB.EXPECT().GetUnprocessed(gomock.Any(), gomock.Any()).Return(jobsdb.JobsResult{Jobs: jobs}, nil).Times(1)
	mockJobsDB.EXPECT().GetUnprocessed(gomock.Any(), gomock.Any()).Return(jobsdb.JobsResult{}, nil).AnyTimes()
	mockJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, f func(jobsdb.UpdateSafeTx) error) error {
			return f(nil)
		}).AnyTimes()
	mockJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT) error {
			if len(statuses) > 0 {
				statusesCh <- statuses
			}
			return nil
		}).AnyTimes()

	conf := config.New()
	conf.Set("SchemaForwarder.loopSleepTime", time.Millisecond)
	jf := NewJobsForwarder(func(error) {}, mockJobsDB, nil, conf, mockBackendConfig, logger.NOP, stats.NOP)
	jf.maxRetryElapsedTime = config.SingleValueLoader(time.Millisecond)
	return jf, statusesCh
}

func receiveStatuses(t *testing.T, statusesCh <-chan []*jobsdb.JobStatusT) []*jobsdb.JobStatusT {
	t.Helper()
	select {
	case statuses := <-statusesCh:
		return statuses
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for job statuses")
		return nil
	}
}

// collectingSignal is a callback timeout loader that closes started on its first Load, i.e. once the forwarder has
// flushed the producer and starts collecting producer callbacks
type collectingSignal struct {
	started chan struct{}
	once    sync.Once
	timeout time.Duration
}

func (s *collectingSignal) Load() time.Duration {
	s.once.Do(func() { close(s.started) })
	return s.timeout
}

// durationLoaderFunc is a [config.ValueLoader] that calls itself on every Load
type durationLoaderFunc func() time.Duration

func (f durationLoaderFunc) Load() time.Duration { return f() }

// lateAckProducer is a [pulsar.ProducerAdapter] whose Flush fails without acknowledging any message.
// Pending messages succeed from other goroutines once releaseCallbacks is called.
type lateAckProducer struct {
	release     chan struct{} // closed by releaseCallbacks
	releaseOnce sync.Once
	running     sync.WaitGroup // goroutines that run the callbacks

	mu        sync.Mutex
	sent      int                                                              // messages sent so far
	callbacks []func(pulsarType.MessageID, *pulsarType.ProducerMessage, error) // callbacks of the messages sent since the last Flush
}

func (p *lateAckProducer) releaseCallbacks() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func (*lateAckProducer) SendMessage(context.Context, string, string, []byte) error {
	return errors.New("not implemented")
}

func (p *lateAckProducer) SendMessageAsync(_ context.Context, _, _ string, _ []byte, statusFunc func(pulsarType.MessageID, *pulsarType.ProducerMessage, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent++
	p.callbacks = append(p.callbacks, statusFunc)
}

func (p *lateAckProducer) Flush() error {
	p.mu.Lock()
	callbacks := p.callbacks
	p.callbacks = nil
	p.mu.Unlock()
	if len(callbacks) == 0 {
		return nil
	}
	for _, cb := range callbacks {
		p.running.Go(func() {
			<-p.release
			cb(nil, nil, nil)
		})
	}
	return errors.New("flush timed out")
}

func (*lateAckProducer) Close() {}

// silentProducer is a [pulsar.ProducerAdapter] that never calls back for the messages it is sent
type silentProducer struct{}

func (*silentProducer) SendMessage(context.Context, string, string, []byte) error {
	return errors.New("not implemented")
}

func (*silentProducer) SendMessageAsync(context.Context, string, string, []byte, func(pulsarType.MessageID, *pulsarType.ProducerMessage, error)) {
}

func (*silentProducer) Flush() error { return nil }

func (*silentProducer) Close() {}

// failingProducer is a [pulsar.ProducerAdapter] that fails every message from within SendMessageAsync,
// as pulsar does e.g. when the producer is closed
type failingProducer struct{}

func (*failingProducer) SendMessage(context.Context, string, string, []byte) error {
	return errors.New("not implemented")
}

func (*failingProducer) SendMessageAsync(_ context.Context, _, _ string, _ []byte, statusFunc func(pulsarType.MessageID, *pulsarType.ProducerMessage, error)) {
	statusFunc(nil, nil, pulsarType.ErrProducerClosed)
}

func (*failingProducer) Flush() error { return pulsarType.ErrProducerClosed }

func (*failingProducer) Close() {}

// PulsarResource returns a pulsar container resource
func PulsarResource(t *testing.T) *pulsardocker.Resource {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pulsarContainer, err := pulsardocker.Setup(pool, t)
	require.NoError(t, err)
	return pulsarContainer
}
