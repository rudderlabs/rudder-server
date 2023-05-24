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
	EnterpriseToken        string
	Log                    logger.Logger
	once                   sync.Once
	reportingInstance      types.ReportingI
	errorReportingInstance types.ReportingI
}

func (m *Factory) formReportInstance(backendConfig backendconfig.BackendConfig) types.ReportingI {
	m.Log.Debug("Forming reporting instance")
	reportingEnabled := config.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
	if !reportingEnabled || m.EnterpriseToken == "" {
		return &NOOP{}
	}
	reportingHandle := NewFromEnvConfig(m.Log)
	rruntime.Go(func() {
		reportingHandle.setup(backendConfig)
	})
	return reportingHandle
}

func (m *Factory) formErrorReportInstance(backendConfig backendconfig.BackendConfig) types.ReportingI {
	m.Log.Debug("Forming error reporting instance")
	reportingEnabled := config.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
	errorReportingEnabled := config.GetBool("Reporting.errorReporting.enabled", false)

	if !(reportingEnabled && errorReportingEnabled) || m.EnterpriseToken == "" {
		return &NOOP{}
	}
	errorReporter := NewEdReporterFromEnvConfig()
	rruntime.Go(func() {
		errorReporter.setup(backendConfig)
	})
	return errorReporter
}

// Setup initializes Suppress User feature
func (m *Factory) Setup(backendConfig backendconfig.BackendConfig) types.ReportingInstances {
	if m.Log == nil {
		m.Log = logger.NewLogger().Child("enterprise").Child("reporting")
	}
	m.once.Do(func() {
		m.reportingInstance = m.formReportInstance(backendConfig)
		m.errorReportingInstance = m.formErrorReportInstance(backendConfig)
	})
	return types.ReportingInstances{
		ReportingInstance:      m.reportingInstance,
		ErrorReportingInstance: m.errorReportingInstance,
	}
}

func (m *Factory) GetReportingInstance(reporterType types.ReporterType) types.ReportingI {
	// Inner fn
	returnReportingI := func(repI types.ReportingI) types.ReportingI {
		if repI == nil {
			panic(fmt.Errorf("reporting instance not initialised. You should call Setup before GetReportingInstance"))
		}
		return repI
	}
	switch reporterType {
	case types.ErrorDetailReport:
		return returnReportingI(m.errorReportingInstance)
	case types.Report:
		return returnReportingI(m.reportingInstance)
	}
	panic(fmt.Errorf("valid reporter type is not provided"))
}
