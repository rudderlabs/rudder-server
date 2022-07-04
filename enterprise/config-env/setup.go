package configenv

import (
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken string
}

// Setup initializes Suppress User feature
func (m *Factory) Setup() types.ConfigEnvI {
	if m.EnterpriseToken == "" {
		return &NOOP{}
	}

	loadConfig()
	pkgLogger = logger.NewLogger().Child("enterprise").Child("config-env")

	pkgLogger.Info("[[ ConfigEnv ]] Setting up config env handler")
	handle := &HandleT{}

	return handle
}
