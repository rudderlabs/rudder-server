package dedup

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/services/dedup/types"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

const defaultMaxRoutines = 3000

type mirrorDB struct {
	primary       types.DB
	mirror        types.DB
	mode          mode
	group         *errgroup.Group
	groupLimit    int
	errs          chan error
	stopPrintErrs chan struct{}

	logger logger.Logger

	metrics struct {
		getErrorsCount   stats.Counter
		setErrorsCount   stats.Counter
		maxRoutinesCount stats.Counter
	}
}

func NewMirrorDB(primary, mirror types.DB, mode mode, conf *config.Config, s stats.Stats, log logger.Logger) *mirrorDB {
	groupLimit := conf.GetInt("KeyDB.Dedup.Mirror.MaxRoutines", defaultMaxRoutines)
	if groupLimit < 1 {
		groupLimit = defaultMaxRoutines
	}

	group := &errgroup.Group{}
	group.SetLimit(groupLimit)

	mirrorDB := &mirrorDB{
		primary:       primary,
		mirror:        mirror,
		mode:          mode,
		group:         group,
		groupLimit:    groupLimit,
		errs:          make(chan error, 1),
		stopPrintErrs: make(chan struct{}),
		logger:        log,
	}

	mirrorDB.metrics.getErrorsCount = s.NewTaggedStat("dedup_mirroring_err_count", stats.CountType, stats.Tags{
		"mode": mode.MirrorLabel(),
		"type": "get",
	})
	mirrorDB.metrics.setErrorsCount = s.NewTaggedStat("dedup_mirroring_err_count", stats.CountType, stats.Tags{
		"mode": mode.MirrorLabel(),
		"type": "set",
	})
	mirrorDB.metrics.maxRoutinesCount = s.NewTaggedStat("dedup_mirroring_max_routines_count", stats.CountType, stats.Tags{
		"mode": mode.MirrorLabel(),
	})

	group.Go(func() error {
		mirrorDB.printErrs(conf.GetDuration("KeyDB.Dedup.Mirror.PrintErrorsInterval", 10, time.Second))
		return nil
	})

	return mirrorDB
}

func (m *mirrorDB) Get(keys []string) (map[string]bool, error) {
	// Execute on primary and return results
	result, err := m.primary.Get(keys)
	if err != nil {
		return nil, err
	}

	// Asynchronously execute on mirror
	fired := m.group.TryGo(func() error {
		_, err := m.mirror.Get(keys)
		if err != nil {
			m.metrics.getErrorsCount.Increment()
			m.logLeakyErr(fmt.Errorf("mirror Get failed: %w", err))
		}
		return nil
	})

	if !fired {
		m.metrics.maxRoutinesCount.Increment()
		m.logLeakyErr(fmt.Errorf("max routines limit reached during Get: current limit %d", m.groupLimit))
	}

	return result, nil
}

func (m *mirrorDB) Set(keys []string) error {
	// Execute on primary and return results
	err := m.primary.Set(keys)
	if err != nil {
		return err
	}

	// Asynchronously execute on mirror
	fired := m.group.TryGo(func() error {
		err := m.mirror.Set(keys)
		if err != nil {
			m.metrics.setErrorsCount.Increment()
			m.logLeakyErr(fmt.Errorf("mirror Set failed: %w", err))
		}
		return nil
	})

	if !fired {
		m.metrics.maxRoutinesCount.Increment()
		m.logLeakyErr(fmt.Errorf("max routines limit reached during Set: current limit %d", m.groupLimit))
	}

	return nil
}

func (m *mirrorDB) Close() {
	// First we need to stop all mirroring goroutines
	close(m.stopPrintErrs)
	_ = m.group.Wait()
	close(m.errs)

	// Then close both primary and mirror DBs
	m.primary.Close()
	m.mirror.Close()
}

func (m *mirrorDB) logLeakyErr(err error) {
	select {
	case m.errs <- err:
	default: // leaky bucket to avoid filling the logs if the system fails badly
	}
}

func (m *mirrorDB) printErrs(interval time.Duration) {
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
