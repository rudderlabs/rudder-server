//go:generate mockgen -destination=../../mocks/services/dedup/mock_dedup.go -package mock_dedup github.com/rudderlabs/rudder-server/services/dedup Dedup

package dedup

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/services/dedup/badger"
	"github.com/rudderlabs/rudder-server/services/dedup/keydb"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type BatchKey = types.BatchKey

// SingleKey creates a BatchKey with index 0
func SingleKey(key string) BatchKey {
	return types.BatchKey{Key: key}
}

// New creates a new deduplication service. The service needs to be closed after use.
func New(conf *config.Config, stats stats.Stats, log logger.Logger) (Dedup, error) {
	if conf.GetBool("KeyDB.Dedup.Enabled", false) {
		return keydb.NewKeyDB(conf, stats, log)
	}

	badgerDedup := badger.NewBadgerDB(conf, stats, badger.DefaultPath())

	if conf.GetBool("KeyDB.Dedup.Mirror", false) {
		keydbDedup, err := keydb.NewKeyDB(conf, stats, log)
		if err == nil {
			m := &mirror{
				Dedup:  badgerDedup,
				mirror: keydbDedup,
				errs:   make(chan error, 1),
				logger: log,
			}
			go m.printErrs(conf)
			return m, nil
		}
		log.Errorn("Failed to create keydb dedup", obskit.Error(err))
	}

	return badgerDedup, nil
}

// Dedup is the interface for deduplication service
type Dedup interface {
	// Allowed returns a map containing all keys which are being encountered for the first time, i.e. not present in the deduplication database.
	// Keys that are not allowed are not present in the map.
	Allowed(keys ...BatchKey) (map[BatchKey]bool, error)

	// Commit commits a list of allowed keys to the deduplication service
	Commit(keys []string) error

	// Close closes the deduplication service
	Close()
}

type mirror struct {
	Dedup
	mirror Dedup
	errs   chan error
	logger logger.Logger
}

func (m *mirror) Allowed(keys ...BatchKey) (map[BatchKey]bool, error) {
	go func() { // fire & forget
		if _, err := m.mirror.Allowed(keys...); err != nil {
			select {
			case m.errs <- fmt.Errorf("call to Allowed failed: %w", err):
			default: // leaky bucket to avoid filling the logs if the system fails badly
			}
		}
	}()
	return m.Dedup.Allowed(keys...)
}

func (m *mirror) Commit(keys []string) error {
	go func() { // fire & forget
		if err := m.mirror.Commit(keys); err != nil {
			select {
			case m.errs <- fmt.Errorf("call to Commit failed: %w", err):
			default: // leaky bucket to avoid filling the logs if the system fails badly
			}
		}
	}()
	return m.Dedup.Commit(keys)
}

func (m *mirror) Close() {
	m.mirror.Close()
	close(m.errs)
	m.Dedup.Close()
}

func (m *mirror) printErrs(conf *config.Config) {
	ticker := time.NewTicker(conf.GetDuration("KeyDB.Dedup.PrintErrorsInterval", 10, time.Second))
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
