package workerpool

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func newInternalWorker(partition string, log logger.Logger, delegate Worker) *internalWorker {
	w := &internalWorker{
		partition: partition,
		delegate:  delegate,
		logger:    log.Withn(logger.NewStringField("partition", partition)),
	}
	w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(context.Background())
	w.ping = make(chan struct{}, 1)
	w.start()
	return w
}

type internalWorker struct {
	partition string
	delegate  Worker
	logger    logger.Logger

	ping      chan struct{} // ping channel triggers the worker to start working
	lifecycle struct {      // worker lifecycle related fields
		stoppedMu sync.Mutex
		stopped   bool
		ctx       context.Context    // worker context
		cancel    context.CancelFunc // worker context cancel function
		wg        sync.WaitGroup     // worker wait group

		idleMu    sync.RWMutex // idle mutex
		idleSince time.Time    // idle since
	}
}

// start starts the various worker goroutines
func (w *internalWorker) start() {
	// ping loop
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		var exponentialSleep misc.ExponentialNumber[time.Duration]
		defer w.lifecycle.wg.Done()
		defer func() {
			close(w.ping)
		}()
		defer w.logger.Debugn("ping loop stopped for worker")
		for {
			w.logger.Debugn("worker listening for ping")
			select {
			case <-w.lifecycle.ctx.Done():
				return
			case <-w.ping:
				w.logger.Debugn("worker received ping")
			}

			w.setIdleSince(time.Time{})
			if w.delegate.Work() {
				w.logger.Debugn("worker produced work")
				exponentialSleep.Reset()
			} else {
				w.logger.Debugn("worker didn't produce any work")
				if err := misc.SleepCtx(w.lifecycle.ctx, exponentialSleep.Next(w.delegate.SleepDurations())); err != nil {
					w.logger.Debugn("worker sleep interrupted", obskit.Error(err))
					return
				}
				w.setIdleSince(time.Now())
			}
		}
	})
}

func (w *internalWorker) setIdleSince(t time.Time) {
	w.lifecycle.idleMu.Lock()
	defer w.lifecycle.idleMu.Unlock()
	if t.IsZero() || w.lifecycle.idleSince.IsZero() {
		w.lifecycle.idleSince = t
	}
}

// Ping triggers the worker to pick more jobs
func (w *internalWorker) Ping() {
	w.logger.Debugn("worker pinged")
	w.lifecycle.stoppedMu.Lock()
	defer w.lifecycle.stoppedMu.Unlock()
	if w.lifecycle.stopped {
		return
	}
	select {
	case w.ping <- struct{}{}:
	default:
	}
}

// IdleSince returns the time when the worker was last idle. If the worker is not idle, it returns a zero time.
func (w *internalWorker) IdleSince() time.Time {
	w.lifecycle.idleMu.RLock()
	defer w.lifecycle.idleMu.RUnlock()
	return w.lifecycle.idleSince
}

// Stop stops the worker and waits until all its goroutines have stopped
func (w *internalWorker) Stop() {
	w.lifecycle.stoppedMu.Lock()
	if w.lifecycle.stopped {
		w.lifecycle.stoppedMu.Unlock()
		return
	}
	w.lifecycle.stopped = true
	w.lifecycle.stoppedMu.Unlock()

	start := time.Now()
	w.lifecycle.cancel()
	w.lifecycle.wg.Wait()
	w.logger.Debugn("worker ping loop stopped in",
		logger.NewDurationField("duration", time.Since(start)),
	)

	start = time.Now()
	w.delegate.Stop()
	w.logger.Debugn("worker delegate stopped",
		logger.NewDurationField("duration", time.Since(start)),
	)
}
