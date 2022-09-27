package reporting

import (
	"context"
	"database/sql"

	"github.com/rudderlabs/rudder-server/utils/types"
)

// NOOP reporting implementation that does nothing
type NOOP struct{}

func (n *NOOP) Report(_ []*types.PUReportedMetric, _ *sql.Tx) {
}

func (n *NOOP) WaitForSetup(_ context.Context, _ string) error {
	return nil
}

func (n *NOOP) AddClient(_ context.Context, _ types.Config) {
}

func (n *NOOP) GetClient(_ string) *types.Client {
	return nil
}
