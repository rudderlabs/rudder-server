package configenv

import (
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type configEnvFeatureImpl struct{}

// Setup initializes Suppress User feature
func (m *configEnvFeatureImpl) Setup() types.ConfigEnvI {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("enterprise").Child("config-env")

	pkgLogger.Info("[[ ConfigEnv ]] Setting up config env handler")
	handle := &HandleT{}

	return handle
}
