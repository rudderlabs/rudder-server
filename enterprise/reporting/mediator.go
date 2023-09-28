package reporting

import (
	"context"
	"database/sql"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type ReportingMediator struct {
	log logger.Logger

	g         *errgroup.Group
	ctx       context.Context
	reporters []types.Reporting
}

func NewReportingMediator(ctx context.Context, log logger.Logger, enterpriseToken string, backendConfig backendconfig.BackendConfig) *ReportingMediator {
	rm := &ReportingMediator{
		log: log,
	}
	rm.g, rm.ctx = errgroup.WithContext(ctx)

	reportingEnabled := config.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
	if enterpriseToken == "" || !reportingEnabled {
		return rm
	}

	// default reporting implementation
	defaultReporter := NewDefaultReporter(rm.ctx, rm.log)
	rm.g.Go(func() error {
		defaultReporter.backendConfigSubscriber(backendConfig)
		return nil
	})
	rm.reporters = append(rm.reporters, defaultReporter)

	// error reporting implementation
	if config.GetBool("Reporting.errorReporting.enabled", false) {
		errorReporter := NewErrorDetailReporter(rm.ctx)
		rm.g.Go(func() error {
			errorReporter.backendConfigSubscriber(backendConfig)
			return nil
		})
		rm.reporters = append(rm.reporters, errorReporter)
	}

	return rm
}

func (rm *ReportingMediator) Report(metrics []*types.PUReportedMetric, txn *sql.Tx) {
	for _, reporter := range rm.reporters {
		reporter.Report(metrics, txn)
	}
}

func (rm *ReportingMediator) DatabaseSyncer(c types.SyncerConfig) types.ReportingSyncer {
	var syncers []types.ReportingSyncer

	for i := range rm.reporters {
		reporter := rm.reporters[i]
		syncers = append(syncers, reporter.DatabaseSyncer(c))
	}

	return func() {
		for i := range syncers {
			syncer := syncers[i]
			rm.g.Go(func() error {
				syncer()
				return nil
			})
		}
		rm.g.Wait()
	}
}
