//go:generate mockgen -destination=../../mocks/services/dedup/mock_dedup.go -package mock_dedup github.com/rudderlabs/rudder-server/services/dedup Dedup

package dedup

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/services/dedup/badger"
	"github.com/rudderlabs/rudder-server/services/dedup/keydb"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type BatchKey = types.BatchKey

type mirrorMode string

const (
	mirrorBadger mirrorMode = "mirrorBadger"
	mirrorKeyDB  mirrorMode = "mirrorKeyDB"
)

// SingleKey creates a BatchKey with index 0
func SingleKey(key string) BatchKey {
	return types.BatchKey{Key: key}
}

// New creates a new deduplication service. The service needs to be closed after use.
func New(conf *config.Config, stats stats.Stats, log logger.Logger) (Dedup, error) {
	badgerDedup := badger.NewBadgerDB(conf, stats, badger.DefaultPath())

	mode := conf.GetString("Dedup.Mirror.Mode", string(mirrorBadger))

	if mode == string(mirrorBadger) {
		keyDBDedup, err := keydb.NewKeyDB(conf, stats, log)
		if err == nil {
			return newMirror(badgerDedup, keyDBDedup, conf, stats, log), nil
		}
		log.Errorn("Failed to create keydb dedup", obskit.Error(err))
	}

	if mode == string(mirrorKeyDB) {
		keyDBDedup, err := keydb.NewKeyDB(conf, stats, log)
		if err != nil {
			return nil, fmt.Errorf("intializing keydb dedup: %w", err)
		}
		return newMirror(keyDBDedup, badgerDedup, conf, stats, log), nil
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
