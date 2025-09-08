package dedup

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/services/dedup/badger"
	kdb "github.com/rudderlabs/rudder-server/services/dedup/keydb"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type mode string

const (
	badgerOnlyMode mode = "badger"
	keyDBOnlyMode  mode = "keydb"
	mirrorToKeyDB  mode = "mirrorToKeyDB"
	mirrorToBadger mode = "mirrorToBadger"
)

// getMode determines which mode to use based on configuration
func getMode(conf *config.Config) mode {
	mirrorMode := conf.GetString("Dedup.Mirror.Mode", string(badgerOnlyMode))
	return mode(mirrorMode)
}

// NewDB creates a new DB implementation based on configuration
func NewDB(conf *config.Config, stats stats.Stats, log logger.Logger) (types.DB, error) {
	mirrorMode := getMode(conf)
	log.Infon("Starting deduplication db", logger.NewStringField("mode", string(mirrorMode)))
	switch mirrorMode {
	case badgerOnlyMode:
		return badger.NewBadgerDB(conf, stats, badger.DefaultPath())
	case keyDBOnlyMode:
		keydb, err := kdb.NewKeyDB(conf, stats, log)
		if err != nil {
			return nil, fmt.Errorf("create keydb: %w", err)
		}
		return keydb, nil
	case mirrorToKeyDB:
		// primary is badger, then we mirror to keydb
		primary, err := badger.NewBadgerDB(conf, stats, badger.DefaultPath())
		if err != nil {
			return nil, fmt.Errorf("create badger primary: %w", err)
		}
		mirror, err := kdb.NewKeyDB(conf, stats, log)
		if err != nil {
			log.Errorn("Failed to create keydb mirror, falling back to badger only", obskit.Error(err))
			return primary, nil
		}
		return NewMirrorDB(primary, mirror, mirrorToKeyDB, conf, stats, log), nil
	case mirrorToBadger:
		// primary is keydb, then we mirror to badger
		primary, err := kdb.NewKeyDB(conf, stats, log)
		if err != nil {
			return nil, fmt.Errorf("create keydb primary: %w", err)
		}
		mirror, err := badger.NewBadgerDB(conf, stats, badger.DefaultPath())
		if err != nil {
			primary.Close()
			log.Errorn("Failed to create badger mirror, falling back to keydb only", obskit.Error(err))
			return nil, fmt.Errorf("create badger mirror: %w", err)
		}
		return NewMirrorDB(primary, mirror, mirrorToBadger, conf, stats, log), nil
	default:
		log.Warnn("Invalid mirror mode, falling back to badger only", logger.NewStringField("mode", string(mirrorMode)))
		// Default to badger only
		return badger.NewBadgerDB(conf, stats, badger.DefaultPath())
	}
}
