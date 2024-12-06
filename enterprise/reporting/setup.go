package reporting

import (
	"context"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken string
	Log             logger.Logger

	oneInstance sync.Once
	instance    types.Reporting
}

// Setup initializes Suppress User feature
func (m *Factory) Setup(ctx context.Context, conf *config.Config, backendConfig backendconfig.BackendConfig) types.Reporting {
	m.oneInstance.Do(func() {
		if m.Log == nil {
			m.Log = logger.NewLogger().Child("enterprise").Child("reporting")
		}
		m.instance = NewReportingMediator(ctx, conf, m.Log, m.EnterpriseToken, backendConfig)
	})
	return m.instance
}
