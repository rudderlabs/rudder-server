package keydb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type keyDB struct {
	client *client.Client
	window config.ValueLoader[time.Duration]
	logger logger.Logger

	stats struct {
		getTimer stats.Timer
		setTimer stats.Timer
	}
}

func NewKeyDB(conf *config.Config, stat stats.Stats, log logger.Logger) (types.DB, error) {
	dedupWindow := conf.GetReloadableDurationVar(3600, time.Second, "KeyDB.Dedup.dedupWindow", "Dedup.dedupWindow", "Dedup.dedupWindowInS")
	nodeAddresses := conf.GetString("KeyDB.Dedup.Addresses", "")
	if len(nodeAddresses) == 0 {
		return nil, fmt.Errorf("keydb dedup: no node addresses provided")
	}
	c, err := client.NewClient(client.Config{
		Addresses:       strings.Split(nodeAddresses, ","),
		TotalHashRanges: uint32(conf.GetInt("KeyDB.Dedup.TotalHashRanges", 128)),
		RetryPolicy: client.RetryPolicy{
			Disabled:        conf.GetBool("KeyDB.Dedup.RetryPolicy.Disabled", false),
			InitialInterval: conf.GetDuration("KeyDB.Dedup.RetryPolicy.InitialInterval", 100, time.Millisecond),
			Multiplier:      conf.GetFloat64("KeyDB.Dedup.RetryPolicy.Multiplier", 1.5),
			MaxInterval:     conf.GetDuration("KeyDB.Dedup.RetryPolicy.MaxInterval", 30, time.Second),
			// No MaxElapsedTime, the client will retry forever.
			// To detect issues monitor the client metrics:
			// https://github.com/rudderlabs/keydb/blob/v0.4.2-alpha/client/client.go#L160
		},
	}, log.Child("keydb"))
	if err != nil {
		return nil, err
	}

	db := &keyDB{
		client: c,
		window: dedupWindow,
		logger: log,
	}
	db.stats.getTimer = stat.NewTaggedStat("dedup_get_duration_seconds", stats.TimerType, stats.Tags{"mode": "keydb"})
	db.stats.setTimer = stat.NewTaggedStat("dedup_set_duration_seconds", stats.TimerType, stats.Tags{"mode": "keydb"})

	return db, nil
}

func (d *keyDB) Get(keys []string) (map[string]bool, error) {
	defer d.stats.getTimer.RecordDuration()()
	results := make(map[string]bool, len(keys))
	exist, err := d.client.Get(context.TODO(), keys)
	if err != nil {
		return nil, err
	}
	for i, key := range keys {
		if exist[i] {
			results[key] = true
		}
	}
	return results, err
}

func (d *keyDB) Set(keys []string) error {
	defer d.stats.setTimer.RecordDuration()()
	return d.client.Put(context.TODO(), keys, d.window.Load())
}

func (d *keyDB) Close() {
	if d.client != nil {
		_ = d.client.Close()
	}
}
