package reporting

import (
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken string

	once              sync.Once
	reportingInstance types.ReportingI

	// for debug purposes, to be removed
	init uint32
}

// Setup initializes Suppress User feature
func (m *Factory) Setup(backendConfig backendconfig.BackendConfig) types.ReportingI {
	m.once.Do(func() {
		reportingEnabled := config.GetBool("Reporting.enabled", types.DEFAULT_REPORTING_ENABLED)
		if !reportingEnabled {
			m.reportingInstance = &NOOP{}
			return
		}

		if m.EnterpriseToken == "" {
			m.reportingInstance = &NOOP{}
			return
		}

		h := NewFromEnvConfig()
		rruntime.Go(func() {
			h.setup(backendConfig)
		})
		m.reportingInstance = h
	})

	return m.reportingInstance
}

func (m *Factory) GetReportingInstance() types.ReportingI {
	if m.reportingInstance == nil {
		panic(fmt.Errorf("Reporting instance not initialised. You should call Setup before GetReportingInstance"))
	}

	return m.reportingInstance
}
