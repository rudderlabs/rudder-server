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
func NewDB(conf *config.Config, s stats.Stats, log logger.Logger) (types.DB, error) {
	mirrorMode := getMode(conf)
	log = log.Withn(logger.NewStringField("mode", string(mirrorMode)))
	log.Infon("Starting deduplication db")

	modeGauge := func(primary, mirror string) {
		s.NewTaggedStat("processor_dedup_mode", stats.GaugeType, stats.Tags{
			"primary": primary,
			"mirror":  mirror,
		}).Gauge(1)
	}

	switch mirrorMode {
	case badgerOnlyMode:
		modeGauge("badger", "none")
		return badger.NewBadgerDB(conf, s, badger.DefaultPath())
	case keyDBOnlyMode:
		keydb, err := kdb.NewKeyDB(conf, s, log)
		if err != nil {
			return nil, fmt.Errorf("create keydb: %w", err)
		}
		modeGauge("keydb", "none")
		return keydb, nil
	case mirrorToKeyDB:
		// primary is badger, then we mirror to keydb
		primary, err := badger.NewBadgerDB(conf, s, badger.DefaultPath())
		if err != nil {
			return nil, fmt.Errorf("create badger primary: %w", err)
		}
		mirror, err := kdb.NewKeyDB(conf, s, log)
		if err != nil {
			modeGauge("badger", "none")
			log.Errorn("Failed to create keydb mirror, falling back to badger only", obskit.Error(err))
			return primary, nil
		}
		modeGauge("badger", "keydb")
		return NewMirrorDB(primary, mirror, mirrorToKeyDB, conf, s, log), nil
	case mirrorToBadger:
		// primary is keydb, then we mirror to badger
		primary, err := kdb.NewKeyDB(conf, s, log)
		if err != nil {
			return nil, fmt.Errorf("create keydb primary: %w", err)
		}
		mirror, err := badger.NewBadgerDB(conf, s, badger.DefaultPath())
		if err != nil {
			primary.Close()
			return nil, fmt.Errorf("create badger mirror: %w", err)
		}
		modeGauge("keydb", "badger")
		return NewMirrorDB(primary, mirror, mirrorToBadger, conf, s, log), nil
	default:
		modeGauge("badger", "none")
		log.Warnn("Invalid mirror mode, falling back to badger only")
		// Default to badger only
		return badger.NewBadgerDB(conf, s, badger.DefaultPath())
	}
}
