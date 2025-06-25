//go:generate mockgen -destination=../../mocks/services/dedup/mock_dedup.go -package mock_dedup github.com/rudderlabs/rudder-server/services/dedup Dedup

package dedup

import (
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
	badgerDedup := badger.NewBadgerDB(conf, stats, badger.DefaultPath())

	if conf.GetBool("KeyDB.Dedup.Mirror.Enabled", false) {
		keydbDedup, err := keydb.NewKeyDB(conf, stats, log)
		if err == nil {
			return newMirror(badgerDedup, keydbDedup, conf, stats, log), nil
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
