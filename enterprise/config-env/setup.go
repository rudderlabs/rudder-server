package configenv

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken string
	Log             logger.Logger
}

// Setup initializes Suppress User feature
func (m *Factory) Setup() types.ConfigEnvI {
	if m.Log == nil {
		m.Log = logger.NewLogger().Child("enterprise").Child("config-env")
	}
	if m.EnterpriseToken == "" {
		return &NOOP{}
	}

	loadConfig()
	m.Log = logger.NewLogger().Child("enterprise").Child("config-env")

	m.Log.Info("[[ ConfigEnv ]] Setting up config env handler")
	handle := &HandleT{Log: m.Log}

	return handle
}
