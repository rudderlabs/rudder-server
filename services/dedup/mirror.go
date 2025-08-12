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

type MirrorDB struct {
	primary       types.DB
	mirror        types.DB
	mode          Mode
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

func NewMirrorDB(primary, mirror types.DB, mode Mode, conf *config.Config, s stats.Stats, log logger.Logger) *MirrorDB {
	groupLimit := conf.GetInt("KeyDB.Dedup.Mirror.MaxRoutines", defaultMaxRoutines)
	if groupLimit < 1 {
		groupLimit = defaultMaxRoutines
	}

	group := &errgroup.Group{}
	group.SetLimit(groupLimit)

	mirrorDB := &MirrorDB{
		primary:       primary,
		mirror:        mirror,
		mode:          mode,
		group:         group,
		groupLimit:    groupLimit,
		errs:          make(chan error, 1),
		stopPrintErrs: make(chan struct{}),
		logger:        log,
	}

	// Set up metrics
	mirrorModeStr := "keydb"
	if mode == mirrorBadgerMode {
		mirrorModeStr = "badgerdb"
	}

	mirrorDB.metrics.getErrorsCount = s.NewTaggedStat("dedup_mirroring_err_count", stats.CountType, stats.Tags{
		"mode": mirrorModeStr,
		"type": "get",
	})
	mirrorDB.metrics.setErrorsCount = s.NewTaggedStat("dedup_mirroring_err_count", stats.CountType, stats.Tags{
		"mode": mirrorModeStr,
		"type": "set",
	})
	mirrorDB.metrics.maxRoutinesCount = s.NewTaggedStat("dedup_mirroring_max_routines_count", stats.CountType, stats.Tags{
		"mode": mirrorModeStr,
	})

	group.Go(func() error {
		mirrorDB.printErrs(conf.GetDuration("KeyDB.Dedup.Mirror.PrintErrorsInterval", 10, time.Second))
		return nil
	})

	return mirrorDB
}

func (m *MirrorDB) Get(keys []string) (map[string]bool, error) {
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

func (m *MirrorDB) Set(keys []string) error {
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

func (m *MirrorDB) Close() {
	// First we need to stop all mirroring goroutines
	close(m.stopPrintErrs)
	_ = m.group.Wait()
	close(m.errs)

	// Then close both primary and mirror DBs
	m.primary.Close()
	m.mirror.Close()
}

func (m *MirrorDB) logLeakyErr(err error) {
	select {
	case m.errs <- err:
	default: // leaky bucket to avoid filling the logs if the system fails badly
	}
}

func (m *MirrorDB) printErrs(interval time.Duration) {
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

// Implement the DedupInterface methods for MirrorDB

func (m *MirrorDB) Allowed(batchKeys ...types.BatchKey) (map[types.BatchKey]bool, error) {
	// Convert BatchKey to string keys for the Get operation
	stringKeys := make([]string, len(batchKeys))
	for i, key := range batchKeys {
		stringKeys[i] = key.Key
	}

	// Use Get method to check which keys exist
	existingKeys, err := m.Get(stringKeys)
	if err != nil {
		return nil, err
	}

	// Build result map with keys that don't exist
	result := make(map[types.BatchKey]bool)
	for _, key := range batchKeys {
		if !existingKeys[key.Key] {
			result[key] = true
		}
	}

	return result, nil
}

func (m *MirrorDB) Commit(keys []string) error {
	return m.Set(keys)
}
