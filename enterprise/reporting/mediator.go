package reporting

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type ReportingMediator struct {
	log             logger.Logger
	once            sync.Once
	enterpriseToken string
	reporting       types.Reporter
	errorReporting  types.Reporter
}

func (med *ReportingMediator) createReportInstance(backendConfig backendconfig.BackendConfig) types.Reporter {
	med.log.Debug("Forming reporting instance")
	reportingEnabled := config.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
	if !reportingEnabled || med.enterpriseToken == "" {
		return &NOOP{}
	}
	reportingHandle := NewFromEnvConfig(med.log)
	rruntime.Go(func() {
		reportingHandle.setup(backendConfig)
	})
	return reportingHandle
}

func (med *ReportingMediator) createErrorReportInstance(backendConfig backendconfig.BackendConfig) types.Reporter {
	med.log.Debug("Forming error reporting instance")
	reportingEnabled := config.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
	errorReportingEnabled := config.GetBool("Reporting.errorReporting.enabled", false)

	if !(reportingEnabled && errorReportingEnabled) || med.enterpriseToken == "" {
		return &NOOP{}
	}
	errorReporter := NewEdReporterFromEnvConfig()
	rruntime.Go(func() {
		errorReporter.setup(backendConfig)
	})
	return errorReporter
}

func NewReportingMediator(log logger.Logger, enterpriseToken string) *ReportingMediator {
	return &ReportingMediator{
		log:             log,
		enterpriseToken: enterpriseToken,
	}
}

func (med *ReportingMediator) Setup(backendConfig backendconfig.BackendConfig) *ReportingMediator {
	med.once.Do(func() {
		med.reporting = med.createReportInstance(backendConfig)
		med.errorReporting = med.createErrorReportInstance(backendConfig)
	})
	return med
}

func (med *ReportingMediator) WaitForSetup(ctx context.Context, clientName string) error {
	var err error
	err = med.reporting.WaitForSetup(ctx, clientName)
	if err != nil {
		// TODO: Should we panic here ?
		med.log.Errorf("Error while waiting to setup reporting instance: %w", err)
	}
	err = med.errorReporting.WaitForSetup(ctx, clientName)
	return err
}

func (med *ReportingMediator) Report(metrics []*types.PUReportedMetric, txn *sql.Tx) {
	med.reporting.Report(metrics, txn)
	med.errorReporting.Report(metrics, txn)
}

func (med *ReportingMediator) AddClient(ctx context.Context, c types.Config) {
	rruntime.Go(func() {
		med.reporting.AddClient(ctx, c)
	})
	rruntime.Go(func() {
		med.errorReporting.AddClient(ctx, c)
	})
}

func (med *ReportingMediator) GetReportingInstance(reporterType types.ReporterType) types.Reporter {
	// Inner fn
	returnReportingI := func(repI types.Reporter) types.Reporter {
		if repI == nil {
			panic(fmt.Errorf("reporting instance not initialised. You should call Setup before GetReportingInstance"))
		}
		return repI
	}
	switch reporterType {
	case types.ErrorDetailReport:
		return returnReportingI(med.errorReporting)
	case types.Report:
		return returnReportingI(med.reporting)
	}
	panic(fmt.Errorf("valid reporter type is not provided"))
}
