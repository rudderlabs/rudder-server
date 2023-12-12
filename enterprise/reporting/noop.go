package reporting

import (
	"context"

	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

// NOOP reporting implementation that does nothing
type NOOP struct{}

func (*NOOP) Report(_ context.Context, _ []*types.PUReportedMetric, _ *Tx) error {
	return nil
}

func (*NOOP) DatabaseSyncer(c types.SyncerConfig) types.ReportingSyncer {
	return func() {}
}

func (*NOOP) Stop() {}
