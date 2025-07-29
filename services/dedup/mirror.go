package dedup

import (
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

const (
	defaultMaxRoutines = 3000
)

type mirror struct {
	Dedup
	mirror Dedup

	group         *errgroup.Group
	groupLimit    int
	errs          chan error
	stopPrintErrs chan struct{}

	logger logger.Logger

	metrics struct {
		allowedErrorsCount stats.Counter
		commitErrorsCount  stats.Counter
		maxRoutinesCount   stats.Counter
	}
}

func newMirror(d, m Dedup, conf *config.Config, s stats.Stats, log logger.Logger) *mirror {
	groupLimit := conf.GetInt("KeyDB.Dedup.Mirror.MaxRoutines", defaultMaxRoutines)
	if groupLimit < 1 {
		groupLimit = defaultMaxRoutines
	}

	group := &errgroup.Group{}
	group.SetLimit(groupLimit)

	dedupMirror := &mirror{
		Dedup:         d,
		mirror:        m,
		group:         group,
		groupLimit:    groupLimit, // storing limit for enriching error
		errs:          make(chan error, 1),
		stopPrintErrs: make(chan struct{}),
		logger:        log,
	}
	dedupMirror.metrics.allowedErrorsCount = s.NewTaggedStat("dedup_mirroring_err_count", stats.CountType, stats.Tags{
		"mode": "keydb",
		"type": "allowed",
	})
	dedupMirror.metrics.commitErrorsCount = s.NewTaggedStat("dedup_mirroring_err_count", stats.CountType, stats.Tags{
		"mode": "keydb",
		"type": "commit",
	})
	dedupMirror.metrics.maxRoutinesCount = s.NewTaggedStat("dedup_mirroring_max_routines_count", stats.CountType, stats.Tags{
		"mode": "keydb",
	})

	group.Go(func() error {
		dedupMirror.printErrs(conf.GetDuration("KeyDB.Dedup.Mirror.PrintErrorsInterval", 10, time.Second))
		return nil
	})

	return dedupMirror
}

func (m *mirror) Allowed(keys ...BatchKey) (map[BatchKey]bool, error) {
	fired := m.group.TryGo(func() error {
		_, err := m.mirror.Allowed(keys...)
		if err == nil {
			return nil
		}

		m.metrics.allowedErrorsCount.Increment()
		m.logLeakyErr(fmt.Errorf("call to Allowed failed: %w", err))

		return nil
	})

	if !fired {
		m.metrics.maxRoutinesCount.Increment()
		m.logLeakyErr(fmt.Errorf("max routines limit reached: current limit %d", m.groupLimit))
	}

	return m.Dedup.Allowed(keys...)
}

func (m *mirror) Commit(keys []string) error {
	fired := m.group.TryGo(func() error {
		err := m.mirror.Commit(keys)
		if err == nil {
			return nil
		}

		m.metrics.commitErrorsCount.Increment()
		m.logLeakyErr(fmt.Errorf("call to Commit failed: %w", err))

		return nil
	})

	if !fired {
		m.metrics.maxRoutinesCount.Increment()
		m.logLeakyErr(fmt.Errorf("max routines limit reached: current limit %d", m.groupLimit))
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
	for {
		select {
		case <-m.stopPrintErrs:
			return
		case <-ticker.C:
			select {
			case <-m.stopPrintErrs:
				return
			case err := <-m.errs:
				m.logger.Errorn("Dedup mirroring error", obskit.Error(err))
			}
		}
	}
}

func (m *mirror) Close() {
	// first we need to stop all mirroring goroutines
	close(m.stopPrintErrs)
	_ = m.group.Wait()
	close(m.errs)
	// then can close both mirror and primary dedup services
	m.mirror.Close()
	m.Dedup.Close()
}
