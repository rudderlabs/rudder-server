package mirrorBadger

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/dedup/badger"
	"github.com/rudderlabs/rudder-server/services/dedup/scylla"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type MirrorBadger struct {
	badger *badger.Dedup
	scylla *scylla.ScyllaDB
	stat   stats.Stats
}

func NewMirrorBadger(conf *config.Config, stats stats.Stats) (*MirrorBadger, error) {
	badger := badger.NewBadgerDB(conf, stats, badger.DefaultPath())
	scylla, err := scylla.New(conf, stats)
	if err != nil {
		return nil, err
	}
	return &MirrorBadger{
		badger: badger,
		scylla: scylla,
		stat:   stats,
	}, nil
}

func (mb *MirrorBadger) Close() {
	mb.scylla.Close()
	mb.badger.Close()
}

func (mb *MirrorBadger) Get(kv types.KeyValue) (bool, int64, error) {
	_, _, err := mb.scylla.Get(kv)
	if err != nil {
		mb.stat.NewTaggedStat("dedup_mirror_badger_get_error", stats.CountType, stats.Tags{}).Increment()
	}
	return mb.badger.Get(kv)
}

func (mb *MirrorBadger) Commit(keys []string) error {
	_ = mb.scylla.Commit(keys)
	return mb.badger.Commit(keys)
}
