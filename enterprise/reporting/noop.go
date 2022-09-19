package reporting

import (
	"context"
	"database/sql"

	"github.com/rudderlabs/rudder-server/utils/types"
)

// NOOP reporting implementation that does nothing
type NOOP struct{}

func (*NOOP) Report(_ context.Context, _ []*types.PUReportedMetric, _ *sql.Tx) {
}

func (n *NOOP) WaitForSetup(ctx context.Context, clientName string) {
}

func (n *NOOP) AddClient(ctx context.Context, c types.Config) {
}

func (n *NOOP) GetClient(clientName string) *types.Client {
	return nil
}
