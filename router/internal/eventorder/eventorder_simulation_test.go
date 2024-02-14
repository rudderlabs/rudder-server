package eventorder_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
)

const (
	bufferSize = 20
)

func TestSimulateBarrier(t *testing.T) {
	const (
		channelSize = 200
		batchSize   = 50
	)
	workerQueue := make(chan *job, channelSize)
	statusQueue := make(chan *job, channelSize)

	jobs := newRandomJobs(200)
	for _, job := range jobs {
		t.Logf("%+v", *job)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var logger log = t
	barrier := eventorder.NewBarrier()
	generator := &generatorLoop{ctx: ctx, barrier: barrier, batchSize: batchSize, pending: jobs, out: workerQueue, logger: logger}
	worker := &workerProcess{ctx: ctx, barrier: barrier, in: workerQueue, out: statusQueue, logger: logger}
	commit := &commitStatusLoop{ctx: ctx, barrier: barrier, in: statusQueue, putBack: generator.putBack, logger: logger}

	go generator.run()
	go worker.run()
	go commit.run()

	require.Eventually(t, func() bool {
		committed := commit.getCommitted()
		return len(committed)+len(generator.drained) == len(jobs)
	}, 60*time.Second, 1*time.Second, "all jobs should be committed")

	committed := commit.getCommitted()
	var previous int64
	for _, id := range committed {
		require.True(t, id >= previous, "job ids should be committed in order", committed)
		previous = id
	}
}

type job struct {
	id      int64
	user    string
	states  []string
	retries int
	loop    int
}

type generatorLoop struct {
	ctx       context.Context
	barrier   *eventorder.Barrier
	logger    log
	batchSize int
	pendingMu sync.Mutex
	pending   []*job
	out       chan *job
	drained   []*job
	runtime   struct {
		batchSize int
		minJobID  int64
	}
}

func (g *generatorLoop) run() {
	var loop int
	g.runtime.batchSize = g.batchSize
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
			batchSize := g.runtime.batchSize
			g.pendingMu.Lock()
			var toProcess []*job
			if len(g.pending) < batchSize {
				toProcess = make([]*job, len(g.pending))
				copy(toProcess, g.pending)
				g.pending = nil
			} else {
				toProcess = g.pending[0:batchSize]
				g.pending = g.pending[batchSize:]
			}

			var acceptedJobs []*job
			g.barrier.Sync()

			var lastBlockJobID int64
			for i, job := range toProcess {
				if i == 0 {
					g.runtime.minJobID = job.id
				}
				// randomly drain 0.1% of non-previously failed jobs (previously failed jobs cannot be drained at this stage)
				if previousFailedJobID := g.barrier.Peek(eventorder.BarrierKey{UserID: job.user}); previousFailedJobID != nil && *previousFailedJobID != job.id && rand.Intn(1000) < 1 { // skipcq: GSC-G404
					if err := g.barrier.StateChanged(eventorder.BarrierKey{UserID: job.user}, job.id, jobsdb.Aborted.State); err != nil {
						panic(fmt.Errorf("could not drain job:%d: %w", job.id, err))
					}
					g.drained = append(g.drained, job)
					g.logger.Logf("drained job:%d", job.id)
					continue
				}

				if accept, blockJobID := g.barrier.Enter(eventorder.BarrierKey{UserID: job.user}, job.id); accept {
					if blockJobID != nil && *blockJobID > job.id {
						panic(fmt.Errorf("job.JobID:%d < blockJobID:%d", job.id, *blockJobID))
					}
					job.loop = loop
					acceptedJobs = append(acceptedJobs, job)
				} else {
					if blockJobID != nil && job.id == *blockJobID {
						panic(fmt.Errorf("job.JobID:%d == blockJobID:%d", job.id, *blockJobID))
					}
					if blockJobID != nil {
						lastBlockJobID = *blockJobID
					}
					g.pending = append(g.pending, job)
				}
			}
			sort.Slice(g.pending, func(i, j int) bool {
				return g.pending[i].id < g.pending[j].id
			})
			g.pendingMu.Unlock()

			if len(acceptedJobs) == 0 && lastBlockJobID > g.runtime.minJobID {
				newBatchSize := int(lastBlockJobID-g.runtime.minJobID) + 1
				if newBatchSize > g.batchSize {
					g.runtime.batchSize = newBatchSize
					// need to make batch size at least as large as the difference of the last block job id and the min job id
					g.logger.Logf("adapted runtime batch size to %d (lastBlockJobID: %d, minJobID: %d)", g.runtime.batchSize, lastBlockJobID, g.runtime.minJobID)
				}
			} else if g.runtime.batchSize != g.batchSize {
				g.runtime.batchSize = g.batchSize
				g.logger.Logf("reverted runtime batch size back to %d", g.runtime.batchSize)
			}
			for _, job := range acceptedJobs {
				g.out <- job
			}
		}
		loop++
	}
}

func (g *generatorLoop) putBack(jobs []*job) {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	g.pending = append(g.pending, jobs...)
	sort.Slice(g.pending, func(i, j int) bool {
		return g.pending[i].id < g.pending[j].id
	})
}

type workerProcess struct {
	ctx     context.Context
	barrier *eventorder.Barrier
	logger  log
	in      chan *job
	out     chan *job
	jobs    []*job
}

func (wp *workerProcess) run() {
	for {
		select {
		case <-wp.ctx.Done():
			return
		case job := <-wp.in:
			wp.jobs = append(wp.jobs, job)
			if len(wp.jobs) >= bufferSize {
				wp.processJobs()
			}
		case <-time.After(10 * time.Millisecond):
			if len(wp.jobs) > 0 {
				wp.processJobs()
			}
		}
	}
}

func (wp *workerProcess) processJobs() {
	for _, job := range wp.jobs {
		// introduce some random delay during processing so that buffers don't empty at a steady pace
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond) // skipcq: GSC-G404
		wait, previousFailedJobID := wp.barrier.Wait(eventorder.BarrierKey{UserID: job.user}, job.id)

		if wait {
			job.states = append([]string{jobsdb.Waiting.State}, job.states...)
			// wp.logger.Logf("job: %d is waiting", job.id)
			wp.out <- job
			continue
		}
		// randomly drain 10% of previously failed jobs in worker process
		if previousFailedJobID != nil && *previousFailedJobID == job.id && rand.Intn(100) < 10 { // skipcq: GSC-G404
			wp.logger.Logf("drained failed job:%d", job.id)
			job.states = []string{jobsdb.Aborted.State}
			wp.out <- job
			continue
		}
		if job.states[0] == jobsdb.Failed.State {
			_ = wp.barrier.StateChanged(eventorder.BarrierKey{UserID: job.user}, job.id, jobsdb.Failed.State)
		}
		wp.out <- job
	}
	wp.jobs = nil
}

type commitStatusLoop struct {
	ctx     context.Context
	barrier *eventorder.Barrier
	logger  log
	in      chan *job
	jobs    []*job
	putBack func(jobs []*job)
	doneMu  sync.Mutex
	done    []int64
	runtime struct {
		lastCommitID int64
	}
}

func (cl *commitStatusLoop) getCommitted() []int64 {
	cl.doneMu.Lock()
	defer cl.doneMu.Unlock()
	dst := make([]int64, len(cl.done))
	copy(dst, cl.done)
	return dst
}

func (cl *commitStatusLoop) run() {
	for {
		select {
		case <-cl.ctx.Done():
			return
		case job := <-cl.in:
			cl.jobs = append(cl.jobs, job)
			if len(cl.jobs) >= bufferSize {
				cl.commit()
			}
		case <-time.After(10 * time.Millisecond):
			if len(cl.jobs) > 0 {
				cl.commit()
			}
		}
	}
}

func (cl *commitStatusLoop) commit() {
	var putBack []*job
	for _, job := range cl.jobs {
		time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond) // skipcq: GSC-G404
		switch job.states[0] {
		case "aborted", "succeeded", "waiting":
			_ = cl.barrier.StateChanged(eventorder.BarrierKey{UserID: job.user}, job.id, job.states[0])
		}
		if len(job.states) == 1 {
			if job.states[0] != "succeeded" && job.states[0] != "aborted" {
				panic(fmt.Errorf("invalid job %d terminal state: %q", job.id, job.states[0]))
			}
			if cl.runtime.lastCommitID > job.id {
				panic(fmt.Errorf("trying to commit job %d after %d", job.id, cl.runtime.lastCommitID))
			}
			cl.runtime.lastCommitID = job.id
			cl.logger.Logf("job: %d state: %q retries: %d", job.id, job.states[0], job.retries)
			cl.doneMu.Lock()
			cl.done = append(cl.done, job.id)
			cl.doneMu.Unlock()
		} else {
			job.states = job.states[1:]
			job.retries++
			putBack = append(putBack, job)
		}
	}

	if len(putBack) > 0 {
		cl.putBack(putBack)
	}

	cl.jobs = nil
}

func newRandomJobs(num int) []*job {
	jobs := make([]*job, num)
	for i := 0; i < num; i++ {

		var states []string
		var terminal bool
		for !terminal {
			var state string
			state, terminal = randomState()
			states = append(states, state)
		}
		jobs[i] = &job{
			id:     int64(i),
			user:   "user1",
			states: states,
		}
	}
	return jobs
}

func randomState() (state string, terminal bool) {
	newRand := rand.New(rand.NewSource(time.Now().UnixNano())) // skipcq: GSC-G404
	states := []string{
		jobsdb.Failed.State, jobsdb.Failed.State, jobsdb.Failed.State,
		jobsdb.Aborted.State, jobsdb.Aborted.State, jobsdb.Aborted.State,
		jobsdb.Succeeded.State, jobsdb.Succeeded.State, jobsdb.Succeeded.State,
	}
	state = states[newRand.Intn(len(states))] // skipcq: GSC-G404
	terminal = state == jobsdb.Succeeded.State || state == jobsdb.Aborted.State
	return state, terminal
}

type log interface {
	Logf(format string, args ...interface{})
}
