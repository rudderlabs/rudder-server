package fetcher

import (
	"context"
	"github.com/databricks/databricks-sql-go/internal/config"
	"sync"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
)

type FetchableItems[OutputType any] interface {
	Fetch(ctx context.Context, cfg *config.Config) ([]OutputType, error)
}

type Fetcher[OutputType any] interface {
	Err() error
	Start() (<-chan OutputType, context.CancelFunc, error)
}

type concurrentFetcher[I FetchableItems[O], O any] struct {
	cancelChan chan bool
	inputChan  <-chan FetchableItems[O]
	outChan    chan O
	err        error
	nWorkers   int
	cfg        *config.Config
	mu         sync.Mutex
	start      sync.Once
	ctx        context.Context
	cancelFunc context.CancelFunc
	*dbsqllog.DBSQLLogger
}

func (rf *concurrentFetcher[I, O]) Err() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.err
}

func (f *concurrentFetcher[I, O]) Start() (<-chan O, context.CancelFunc, error) {
	f.start.Do(func() {
		// wait group for the worker routines
		var wg sync.WaitGroup

		for i := 0; i < f.nWorkers; i++ {

			// increment wait group
			wg.Add(1)

			f.logger().Debug().Msgf("concurrent fetcher starting worker %d", i)
			go func(x int) {
				// when work function remove one from the wait group
				defer wg.Done()
				// do the actual work
				work(f, x)
				f.logger().Debug().Msgf("concurrent fetcher worker %d done", x)
			}(i)

		}

		// We want to close the output channel when all
		// the workers are finished. This way the client won't
		// be stuck waiting on the output channel.
		go func() {
			wg.Wait()
			f.logger().Debug().Msg("concurrent fetcher closing output channel")
			close(f.outChan)
		}()

		// We return a cancel function so that the client can
		// cancel fetching.
		var cancelOnce sync.Once = sync.Once{}
		f.cancelFunc = func() {
			f.logger().Debug().Msg("concurrent fetcher cancel func")
			cancelOnce.Do(func() {
				f.logger().Debug().Msg("concurrent fetcher closing cancel channel")
				close(f.cancelChan)
			})
		}
	})

	return f.outChan, f.cancelFunc, nil
}

func (f *concurrentFetcher[I, O]) setErr(err error) {
	f.mu.Lock()
	if f.err == nil {
		f.err = err
	}
	f.mu.Unlock()
}

func (f *concurrentFetcher[I, O]) logger() *dbsqllog.DBSQLLogger {
	if f.DBSQLLogger == nil {

		f.DBSQLLogger = dbsqllog.WithContext(driverctx.ConnIdFromContext(f.ctx), driverctx.CorrelationIdFromContext(f.ctx), "")

	}
	return f.DBSQLLogger
}

func NewConcurrentFetcher[I FetchableItems[O], O any](ctx context.Context, nWorkers int, cfg *config.Config, inputChan <-chan FetchableItems[O]) (Fetcher[O], error) {
	// channel for loaded items
	// TODO: pass buffer size
	outputChannel := make(chan O, 100)

	// channel to signal a cancel
	stopChannel := make(chan bool)

	if ctx == nil {
		ctx = context.Background()
	}

	fetcher := &concurrentFetcher[I, O]{
		inputChan:  inputChan,
		outChan:    outputChannel,
		cancelChan: stopChannel,
		ctx:        ctx,
		nWorkers:   nWorkers,
		cfg:        cfg,
	}

	return fetcher, nil
}

func work[I FetchableItems[O], O any](f *concurrentFetcher[I, O], workerIndex int) {

	for {
		select {
		case <-f.cancelChan:
			f.logger().Debug().Msgf("concurrent fetcher worker %d received cancel signal", workerIndex)
			return

		case <-f.ctx.Done():
			f.logger().Debug().Msgf("concurrent fetcher worker %d context done", workerIndex)
			return

		case input, ok := <-f.inputChan:
			if ok {
				f.logger().Debug().Msgf("concurrent fetcher worker %d loading item", workerIndex)
				result, err := input.Fetch(f.ctx, f.cfg)
				if err != nil {
					f.logger().Debug().Msgf("concurrent fetcher worker %d received error", workerIndex)
					f.setErr(err)
					f.cancelFunc()
					return
				} else {
					f.logger().Debug().Msgf("concurrent fetcher worker %d item loaded", workerIndex)
					for i := range result {
						r := result[i]
						f.outChan <- r
					}
				}
			} else {
				f.logger().Debug().Msgf("concurrent fetcher ending %d", workerIndex)
				return
			}

		}
	}

}
