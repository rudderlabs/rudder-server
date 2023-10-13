package reporting

import (
	"database/sql"

	"github.com/rudderlabs/rudder-server/utils/types"
)

// NOOP reporting implementation that does nothing
type NOOP struct{}

func (*NOOP) Report(_ []*types.PUReportedMetric, _ *sql.Tx) error {
	return nil
}

func (*NOOP) DatabaseSyncer(c types.SyncerConfig) types.ReportingSyncer {
	return func() {}
}
