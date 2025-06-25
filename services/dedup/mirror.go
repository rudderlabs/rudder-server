package dedup

import (
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

type mirror struct {
	Dedup
	mirror Dedup

	waitGroup          *sync.WaitGroup
	waitGroupSemaphore chan struct{}

	errs   chan error
	logger logger.Logger

	metrics struct {
		allowedErrorsCount stats.Counter
		commitErrorsCount  stats.Counter
		maxRoutinesCount   stats.Counter
	}
}

func newMirror(d, m Dedup, conf *config.Config, s stats.Stats, log logger.Logger) *mirror {
	waitGroup := &sync.WaitGroup{}
	waitGroupSemaphore := make(chan struct{}, conf.GetInt("KeyDB.Dedup.Mirror.MaxRoutines", 1000))

	dedupMirror := &mirror{
		Dedup:              d,
		mirror:             m,
		waitGroup:          waitGroup,
		waitGroupSemaphore: waitGroupSemaphore,
		errs:               make(chan error, 1),
		logger:             log,
	}
	dedupMirror.metrics.allowedErrorsCount = s.NewTaggedStat("dedup_err_count", stats.CountType, stats.Tags{
		"mode": "keydb",
		"type": "allowed",
	})
	dedupMirror.metrics.commitErrorsCount = s.NewTaggedStat("dedup_err_count", stats.CountType, stats.Tags{
		"mode": "keydb",
		"type": "commit",
	})
	dedupMirror.metrics.maxRoutinesCount = s.NewTaggedStat("dedup_max_routines_count", stats.CountType, stats.Tags{
		"mode": "keydb",
	})

	waitGroup.Add(1) // no need to use the semaphore here
	go func() {
		defer waitGroup.Done()
		dedupMirror.printErrs(conf.GetDuration("KeyDB.Dedup.Mirror.PrintErrorsInterval", 10, time.Second))
	}()

	return dedupMirror
}

func (m *mirror) Allowed(keys ...BatchKey) (map[BatchKey]bool, error) {
	select {
	case m.waitGroupSemaphore <- struct{}{}:
		m.waitGroup.Add(1)
		go func() { // fire & forget
			defer m.waitGroup.Done()

			_, err := m.mirror.Allowed(keys...)
			if err == nil {
				return
			}

			m.metrics.allowedErrorsCount.Increment()
			m.logLeakyErr(fmt.Errorf("call to Allowed failed: %w", err))
		}()
	default:
		m.metrics.maxRoutinesCount.Increment()
		m.logLeakyErr(fmt.Errorf("max routines limit reached"))
	}

	return m.Dedup.Allowed(keys...)
}

func (m *mirror) Commit(keys []string) error {
	select {
	case m.waitGroupSemaphore <- struct{}{}:
		m.waitGroup.Add(1)
		go func() { // fire & forget
			defer m.waitGroup.Done()

			err := m.mirror.Commit(keys)
			if err == nil {
				return
			}

			m.metrics.commitErrorsCount.Increment()
			m.logLeakyErr(fmt.Errorf("call to Commit failed: %w", err))
		}()
	default:
		m.metrics.maxRoutinesCount.Increment()
		m.logLeakyErr(fmt.Errorf("max routines limit reached"))
	}

	return m.Dedup.Commit(keys)
}

func (m *mirror) logLeakyErr(err error) {
	select {
	case m.errs <- err:
	default: // leaky bucket to avoid filling the logs if the system fails badly
	}
}

func (m *mirror) printErrs(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		select {
		case err, ok := <-m.errs:
			if !ok {
				return
			}
			m.logger.Errorn("Dedup mirroring error", obskit.Error(err))
		default:
		}
	}
}

func (m *mirror) Close() {
	m.mirror.Close()
	close(m.errs)
	m.Dedup.Close()
	m.waitGroup.Wait()
}
