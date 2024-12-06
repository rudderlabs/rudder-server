package reporting

import (
	"context"

	erridx "github.com/rudderlabs/rudder-server/enterprise/reporting/error_index"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	TrackedUsersReportsTable = "tracked_users_reports"
)

type Mediator struct {
	log logger.Logger

	g         *errgroup.Group
	ctx       context.Context
	cancel    context.CancelFunc
	reporters []types.Reporting
	stats     stats.Stats

	cronRunners []flusher.Runner
}

func NewReportingMediator(ctx context.Context, conf *config.Config, log logger.Logger, enterpriseToken string, backendConfig backendconfig.BackendConfig) *Mediator {
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	rm := &Mediator{
		log:    log,
		stats:  stats.Default,
		g:      g,
		ctx:    ctx,
		cancel: cancel,
	}

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
	defaultReporter := NewDefaultReporter(rm.ctx, conf, rm.log, configSubscriber, rm.stats)
	rm.reporters = append(rm.reporters, defaultReporter)

	// error reporting implementation
	if config.GetBool("Reporting.errorReporting.enabled", false) {
		errorReporter := NewErrorDetailReporter(rm.ctx, configSubscriber, rm.stats, config.Default)
		rm.reporters = append(rm.reporters, errorReporter)
	}

	// error index reporting implementation
	if config.GetBool("Reporting.errorIndexReporting.enabled", false) {
		errorIndexReporter := erridx.NewErrorIndexReporter(rm.ctx, rm.log, configSubscriber, config.Default, stats.Default)
		rm.reporters = append(rm.reporters, errorIndexReporter)
	}
	eventStatsReporter := NewEventStatsReporter(configSubscriber, rm.stats)
	rm.reporters = append(rm.reporters, eventStatsReporter)

	return rm
}

func (rm *Mediator) Report(ctx context.Context, metrics []*types.PUReportedMetric, txn *Tx) error {
	for _, reporter := range rm.reporters {
		if err := reporter.Report(ctx, metrics, txn); err != nil {
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

	if c.Label == types.CoreReportingLabel || c.Label == "" {
		trackedUsersFlusher, err := flusher.CreateRunner(rm.ctx, TrackedUsersReportsTable, rm.log, rm.stats, config.Default, c.Label)
		if err != nil {
			rm.log.Errorn("error creating tracked users flusher", obskit.Error(err))
			panic(err) //  TODO: Should we panic here?
		}
		rm.cronRunners = append(rm.cronRunners, trackedUsersFlusher)
	}

	return func() {
		for i := range syncers {
			syncer := syncers[i]
			rm.g.Go(func() error {
				syncer()
				return nil
			})
		}

		for _, f := range rm.cronRunners {
			rm.g.Go(func() error {
				f.Run()
				return nil
			})
		}

		_ = rm.g.Wait()
	}
}

func (rm *Mediator) Stop() {
	rm.cancel()
	_ = rm.g.Wait()
	for _, reporter := range rm.reporters {
		reporter.Stop()
	}

	for _, f := range rm.cronRunners {
		f.Stop()
	}
}
