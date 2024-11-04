package mirrorScylla

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/dedup/badger"
	"github.com/rudderlabs/rudder-server/services/dedup/scylla"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type MirrorScylla struct {
	badger *badger.Dedup
	scylla *scylla.ScyllaDB
	stat   stats.Stats
}

func NewMirrorScylla(conf *config.Config, stats stats.Stats) (*MirrorScylla, error) {
	badger := badger.NewBadgerDB(conf, stats, badger.DefaultPath())
	scylla, err := scylla.New(conf, stats)
	if err != nil {
		return nil, err
	}
	return &MirrorScylla{
		stat:   stats,
		badger: badger,
		scylla: scylla,
	}, nil
}

func (ms *MirrorScylla) Close() {
	ms.scylla.Close()
	ms.badger.Close()
}

func (ms *MirrorScylla) GetBatch(kvs []types.KeyValue) (map[types.KeyValue]bool, error) {
	defer ms.stat.NewTaggedStat("dedup_get_batch_duration_seconds", stats.TimerType, stats.Tags{"mode": "mirror_scylla"}).RecordDuration()()
	_, err := ms.badger.GetBatch(kvs)
	if err != nil {
		ms.stat.NewTaggedStat("dedup_mirror_scylla_get_batch_error", stats.CountType, stats.Tags{}).Increment()
	}
	return ms.scylla.GetBatch(kvs)
}

func (ms *MirrorScylla) Get(kv types.KeyValue) (bool, error) {
	defer ms.stat.NewTaggedStat("dedup_get_duration_seconds", stats.TimerType, stats.Tags{"mode": "mirror_scylla"}).RecordDuration()()

	_, err := ms.badger.Get(kv)
	if err != nil {
		ms.stat.NewTaggedStat("dedup_mirror_scylla_get_error", stats.CountType, stats.Tags{}).Increment()
	}
	return ms.scylla.Get(kv)
}

func (ms *MirrorScylla) Commit(keys []string) error {
	defer ms.stat.NewTaggedStat("dedup_commit_duration_seconds", stats.TimerType, stats.Tags{"mode": "mirror_scylla"}).RecordDuration()()

	_ = ms.badger.Commit(keys)
	return ms.scylla.Commit(keys)
}
