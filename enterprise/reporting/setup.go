package reporting

import (
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken     string
	Log                 logger.Logger
	once                sync.Once
	reportingInstance   types.ReportingI
	edReportingInstance types.ReportingI
}

// Setup initializes Suppress User feature
func (m *Factory) Setup(backendConfig backendconfig.BackendConfig) types.ReportingInstances {
	if m.Log == nil {
		m.Log = logger.NewLogger().Child("enterprise").Child("reporting")
	}
	m.once.Do(func() {
		reportingEnabled := config.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
		if !reportingEnabled {
			m.reportingInstance = &NOOP{}
			m.edReportingInstance = &NOOP{}
			return
		}

		if m.EnterpriseToken == "" {
			m.reportingInstance = &NOOP{}
			m.edReportingInstance = &NOOP{}
			return
		}

		h := NewFromEnvConfig(m.Log)
		ed := NewEdReporterFromEnvConfig()
		rruntime.Go(func() {
			h.setup(backendConfig)
		})
		rruntime.Go(func() {
			ed.setup(backendConfig)
		})
		m.reportingInstance = h
		m.edReportingInstance = ed
	})
	return types.ReportingInstances{
		ReportingInstance:   m.reportingInstance,
		EdReportingInstance: m.edReportingInstance,
	}
}

func (m *Factory) GetReportingInstance(names ...string) types.ReportingI {
	if len(names) > 1 {
		panic(fmt.Errorf("only 1 or no arguments is expected but provided %v", len(names)))
	}
	// Inner fn
	returnReportingI := func(repI types.ReportingI) types.ReportingI {
		if repI == nil {
			panic(fmt.Errorf("reporting instance not initialised. You should call Setup before GetReportingInstance"))
		}
		return repI
	}
	// error detail reporting instance
	if len(names) > 0 && (names[0] == "error_detail_report") {
		return returnReportingI(m.edReportingInstance)
	}
	return returnReportingI(m.reportingInstance)
}
