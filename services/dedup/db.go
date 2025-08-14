package dedup

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/dedup/badger"
	kdb "github.com/rudderlabs/rudder-server/services/dedup/keydb"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type mode string

const (
	badgerOnlyMode   mode = "badger"
	keyDBOnlyMode    mode = "keydb"
	mirrorBadgerMode mode = "mirrorBadger"
	mirrorKeyDBMode  mode = "mirrorKeyDB"
)

func (m mode) MirrorLabel() string {
	mirrorModeStr := "keydb"
	if m == mirrorBadgerMode {
		mirrorModeStr = "badgerdb"
	}
	return mirrorModeStr
}

// getMode determines which mode to use based on configuration
func getMode(conf *config.Config) mode {
	mirrorMode := conf.GetString("Dedup.Mirror.Mode", string(badgerOnlyMode))
	return mode(mirrorMode)
}

// NewDB creates a new DB implementation based on configuration
func NewDB(conf *config.Config, stats stats.Stats, log logger.Logger) (types.DB, error) {
	mirrorMode := getMode(conf)
	log.Infon("starting deduplication db", logger.NewStringField("mode", string(mirrorMode)))
	switch mirrorMode {
	case badgerOnlyMode:
		return badger.NewBadgerDB(conf, stats, badger.DefaultPath())
	case keyDBOnlyMode:
		keydb, err := kdb.NewKeyDB(conf, stats, log)
		if err != nil {
			return nil, fmt.Errorf("create keydb: %w", err)
		}
		return keydb, nil
	case mirrorBadgerMode:
		// primary is badger, mirror is keydb
		primary, err := badger.NewBadgerDB(conf, stats, badger.DefaultPath())
		if err != nil {
			return nil, fmt.Errorf("create badger primary: %w", err)
		}
		// try to create keydb mirror
		mirror, err := kdb.NewKeyDB(conf, stats, log)
		if err != nil {
			primary.Close()
			return nil, fmt.Errorf("create keydb mirror: %w", err)
		}

		return NewMirrorDB(primary, mirror, mirrorBadgerMode, conf, stats, log), nil
	case mirrorKeyDBMode:
		// primary is keydb, mirror is badger
		primary, err := kdb.NewKeyDB(conf, stats, log)
		if err != nil {
			return nil, fmt.Errorf("create keydb primary: %w", err)
		}

		mirror, err := badger.NewBadgerDB(conf, stats, badger.DefaultPath())
		if err != nil {
			primary.Close()
			return nil, fmt.Errorf("create badger mirror: %w", err)
		}
		return NewMirrorDB(primary, mirror, mirrorKeyDBMode, conf, stats, log), nil
	default:
		log.Warnn("invalid mirror mode, falling back to badger only", logger.NewStringField("mode", string(mirrorMode)))
		// Default to badger only
		return badger.NewBadgerDB(conf, stats, badger.DefaultPath())
	}
}
