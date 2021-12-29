package initialize

import (
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var once sync.Once

func Init() {
	once.Do(func() {
		config.Load()
		logger.Init()
		stats.Init()
		stats.Setup()
	})
}
