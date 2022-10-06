package initialize

import (
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var once sync.Once

func Init() {
	once.Do(func() {
		config.Reset()
		logger.Reset()
	})
}
