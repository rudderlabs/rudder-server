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
}

func NewMirrorScylla(conf *config.Config, stats stats.Stats) (*MirrorScylla, error) {
	badger := badger.NewBadgerDB(conf, stats, badger.DefaultPath())
	scylla, err := scylla.New(conf, stats)
	if err != nil {
		return nil, err
	}
	return &MirrorScylla{
		badger: badger,
		scylla: scylla,
	}, nil
}

func (ms *MirrorScylla) Close() {
	ms.scylla.Close()
	ms.badger.Close()
}

func (ms *MirrorScylla) Get(kv types.KeyValue) (bool, int64, error) {
	_, _, _ = ms.badger.Get(kv)
	return ms.scylla.Get(kv)
}

func (ms *MirrorScylla) Commit(keys []string) error {
	_ = ms.badger.Commit(keys)
	return ms.scylla.Commit(keys)
}
