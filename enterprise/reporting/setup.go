package reporting

import (
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken   string
	Log               logger.Logger
	once              sync.Once
	reportingInstance types.ReportingI
}

// Setup initializes Suppress User feature
func (m *Factory) Setup(backendConfig backendconfig.BackendConfig) types.ReportingI {
	if m.Log == nil {
		m.Log = logger.NewLogger().Child("enterprise").Child("reporting")
	}
	m.once.Do(func() {
		reportingEnabled := config.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
		if !reportingEnabled {
			m.reportingInstance = &NOOP{}
			return
		}

		if m.EnterpriseToken == "" {
			m.reportingInstance = &NOOP{}
			return
		}

		h := NewFromEnvConfig(m.Log)
		rruntime.Go(func() {
			h.setup(backendConfig)
		})
		m.reportingInstance = h
	})

	return m.reportingInstance
}

func (m *Factory) GetReportingInstance() types.ReportingI {
	if m.reportingInstance == nil {
		panic(fmt.Errorf("reporting instance not initialised. You should call Setup before GetReportingInstance"))
	}

	return m.reportingInstance
}
