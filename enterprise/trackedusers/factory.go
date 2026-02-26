package trackedusers

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type Factory struct {
	Log logger.Logger
}

func (f *Factory) Setup(conf *config.Config) (UsersReporter, error) {
	if !conf.GetBoolVar(false, "TrackedUsers.enabled") {
		return NewNoopDataCollector(), nil
	}
	return NewUniqueUsersReporter(f.Log, conf, stats.Default)
}
