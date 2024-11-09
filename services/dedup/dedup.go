//go:generate mockgen -destination=../../mocks/services/dedup/mock_dedup.go -package mock_dedup github.com/rudderlabs/rudder-server/services/dedup Dedup

package dedup

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/dedup/badger"
	"github.com/rudderlabs/rudder-server/services/dedup/mirrorBadger"
	"github.com/rudderlabs/rudder-server/services/dedup/mirrorScylla"
	"github.com/rudderlabs/rudder-server/services/dedup/scylla"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type Mode string

const (
	Badger       Mode = "Badger"
	Scylla       Mode = "Scylla"
	MirrorScylla Mode = "MirrorScylla"
	MirrorBadger Mode = "MirrorBadger"
)

// New creates a new deduplication service. The service needs to be closed after use.
func New(conf *config.Config, stats stats.Stats) (Dedup, error) {
	mode := Mode(conf.GetString("Dedup.Mode", string(Badger)))
	switch mode {
	case Badger:
		return badger.NewBadgerDB(conf, stats, badger.DefaultPath()), nil
	case Scylla:
		scylla, err := scylla.New(conf, stats)
		if err != nil {
			return nil, err
		}
		return scylla, nil
	case MirrorScylla:
		// Writes happen to both
		// Read only from Scylla
		return mirrorScylla.NewMirrorScylla(conf, stats)
	case MirrorBadger:
		// Writes happen to both
		// Read only from Badger
		return mirrorBadger.NewMirrorBadger(conf, stats)
	default:
		return badger.NewBadgerDB(conf, stats, badger.DefaultPath()), nil
	}
}

// Dedup is the interface for deduplication service
type Dedup interface {
	// Get returns [true] if it was the first time the key was encountered, otherwise it returns [false] along with the previous value
	Get(kv types.KeyValue) (bool, error)

	// GetBatch
	GetBatch(kvs []types.KeyValue) (map[types.KeyValue]bool, error)

	// Commit commits a list of previously set keys to the DB
	Commit(keys []string) error

	// Close closes the deduplication service
	Close()
}
