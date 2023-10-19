package reporting

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Mediator struct {
	log logger.Logger

	g         *errgroup.Group
	ctx       context.Context
	cancel    context.CancelFunc
	reporters []types.Reporting
}

func NewReportingMediator(ctx context.Context, log logger.Logger, enterpriseToken string, backendConfig backendconfig.BackendConfig) *Mediator {
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	rm := &Mediator{
		log:    log,
		g:      g,
		ctx:    ctx,
		cancel: cancel,
	}
	rm.ctx, rm.cancel = context.WithCancel(ctx)
	rm.g, rm.ctx = errgroup.WithContext(rm.ctx)

	reportingEnabled := config.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
	if enterpriseToken == "" || !reportingEnabled {
		return rm
	}

	configSubscriber := newConfigSubscriber(rm.log)
	rm.g.Go(func() error {
		configSubscriber.Subscribe(rm.ctx, backendConfig)
		return nil
	})

	// default reporting implementation
	defaultReporter := NewDefaultReporter(rm.ctx, rm.log, configSubscriber)
	rm.reporters = append(rm.reporters, defaultReporter)

	// error reporting implementation
	if config.GetBool("Reporting.errorReporting.enabled", false) {
		errorReporter := NewErrorDetailReporter(rm.ctx, configSubscriber)
		rm.reporters = append(rm.reporters, errorReporter)
	}

	// error index reporting implementation
	if config.GetBool("Reporting.errorIndexReporting.enabled", false) {
		conf := config.Default
		errIndexDB := jobsdb.NewForReadWrite(
			"err_idx",
			jobsdb.WithDSLimit(conf.GetReloadableIntVar(0, 1, "Reporting.errorIndexReporting.dsLimit")),
			jobsdb.WithConfig(conf),
			jobsdb.WithSkipMaintenanceErr(conf.GetBool("Reporting.errorIndexReporting.skipMaintenanceError", false)),
			jobsdb.WithJobMaxAge(
				func() time.Duration {
					return conf.GetDurationVar(24, time.Hour, "Reporting.errorIndexReporting.jobRetention")
				},
			),
		)
		if err := errIndexDB.Start(); err != nil {
			panic(fmt.Sprintf("failed to start error index db: %v", err))
		}
		errorIndexReporter := NewErrorIndexReporter(rm.ctx, rm.log, configSubscriber, errIndexDB)
		rm.reporters = append(rm.reporters, errorIndexReporter)
		rm.g.Go(func() error {
			// Once the context is done, it stops the errorIndex jobsDB
			<-rm.ctx.Done()
			errIndexDB.Stop()
			return nil
		})
	}

	return rm
}

func (rm *Mediator) Report(metrics []*types.PUReportedMetric, txn *Tx) error {
	for _, reporter := range rm.reporters {
		if err := reporter.Report(metrics, txn); err != nil {
			return err
		}
	}
	return nil
}

func (rm *Mediator) DatabaseSyncer(c types.SyncerConfig) types.ReportingSyncer {
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
		_ = rm.g.Wait()
	}
}

func (rm *Mediator) Stop() {
	rm.cancel()
	_ = rm.g.Wait()
}
