package dedup

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/services/dedup/badger"
	kdb "github.com/rudderlabs/rudder-server/services/dedup/keydb"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type Mode string

const (
	badgerOnlyMode   Mode = "badger"
	keyDBOnlyMode    Mode = "keydb"
	mirrorBadgerMode Mode = "mirrorBadger"
	mirrorKeyDBMode  Mode = "mirrorKeyDB"
)

// getMode determines which mode to use based on configuration
func getMode(conf *config.Config) Mode {
	mode := conf.GetString("Dedup.Mirror.Mode", string(badgerOnlyMode))
	switch Mode(mode) {
	case mirrorBadgerMode, mirrorKeyDBMode, keyDBOnlyMode:
		return Mode(mode)
	default:
		// Default to mirrorBadgerMode if invalid mode specified
		return badgerOnlyMode
	}
}

// NewDB creates a new DB implementation based on configuration
func NewDB(conf *config.Config, stats stats.Stats, log logger.Logger) (types.DB, error) {
	mode := getMode(conf)

	switch mode {
	case badgerOnlyMode:
		return badger.NewBadgerDB(conf, stats, badger.DefaultPath())
	case keyDBOnlyMode:
		keydb, err := kdb.NewKeyDB(conf, stats, log)
		if err != nil {
			return nil, err
		}
		return keydb, nil
	case mirrorBadgerMode:
		// Primary is badger, mirror is keydb
		primary, err := badger.NewBadgerDB(conf, stats, badger.DefaultPath())
		if err != nil {
			return nil, err
		}
		// Try to create keydb mirror
		mirror, err := kdb.NewKeyDB(conf, stats, log)
		if err != nil {
			// If keydb fails, log and fallback to badger only
			log.Errorn("failed to create keydb mirror, falling back to badger only", obskit.Error(err))
			return primary, nil
		}

		return NewMirrorDB(primary, mirror, mirrorBadgerMode, conf, stats, log), nil
	case mirrorKeyDBMode:
		// Primary is keydb, mirror is badger
		primary, err := kdb.NewKeyDB(conf, stats, log)
		if err != nil {
			return nil, err
		}

		mirror, err := badger.NewBadgerDB(conf, stats, badger.DefaultPath())
		if err != nil {
			// If badger fails, log and fallback to keydb only
			log.Errorn("failed to create badger mirror, falling back to keydb only", obskit.Error(err))
			return primary, nil
		}
		return NewMirrorDB(primary, mirror, mirrorKeyDBMode, conf, stats, log), nil
	default:
		// Default to badger only
		return badger.NewBadgerDB(conf, stats, badger.DefaultPath())
	}
}
