package maxcompute

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/clickhouse"
)

// New creates a batch-only MaxCompute integration manager.
// It currently reuses the clickhouse-style warehouse batch loading implementation.
func New(conf *config.Config, log logger.Logger, stat stats.Stats) *clickhouse.Clickhouse {
	return clickhouse.New(conf, log, stat)
}
