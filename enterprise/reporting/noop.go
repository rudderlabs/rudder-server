package reporting

import (
	"context"
	"database/sql"

	"github.com/rudderlabs/rudder-server/utils/types"
)

// NOOP reporting implementation that does nothing
type NOOP struct{}

func (*NOOP) Report(_ []*types.PUReportedMetric, _ *sql.Tx) {
}

func (*NOOP) WaitForSetup(_ context.Context, _ string) error {
	return nil
}

func (*NOOP) AddClient(_ context.Context, _ types.Config) {
}

func (*NOOP) GetClient(_ string) *types.Client {
	return nil
}

func (*NOOP) IsPIIReportingDisabled(_ string) bool {
	return false
}
