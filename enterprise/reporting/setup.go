package reporting

import (
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken string
	Log             logger.Logger
	once            sync.Once
	// reportingInstance      types.ReportingI
	// errorReportingInstance types.ReportingI
	reportingInstance types.MasterReportingI
}

// Setup initializes Suppress User feature
func (m *Factory) Setup(backendConfig backendconfig.BackendConfig) types.MasterReportingI {
	if m.Log == nil {
		m.Log = logger.NewLogger().Child("enterprise").Child("reporting")
	}
	mediator := NewReportingMediator(m.Log, m.EnterpriseToken)
	m.once.Do(func() {
		mediator.Setup(backendConfig)
		m.reportingInstance = mediator
	})
	return m.reportingInstance
}

func (m *Factory) GetReportingInstance() types.MasterReportingI {
	if m.reportingInstance == nil {
		panic(fmt.Errorf("reporting instance not initialised. You should call Setup before GetReportingInstance"))
	}

	return m.reportingInstance
}
