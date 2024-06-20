package trackedusers

import (
	"context"

	txn "github.com/rudderlabs/rudder-server/utils/tx"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

type NoopDataCollector struct {
}

func NewNoopDataCollector() *NoopDataCollector {
	return &NoopDataCollector{}
}

func (n *NoopDataCollector) CollectData(context.Context, []*jobsdb.JobT, *txn.Tx) error {
	return nil
}
