package jobforwarder

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/baseforwarder"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type JobsForwarder struct {
	baseforwarder.BaseForwarder
	pulsarProducer   pulsar.ProducerAdapter
	transientSources transientsource.Service
}

func New(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, transientSources transientsource.Service, log logger.Logger) (*JobsForwarder, error) {
	baseForwarder := baseforwarder.BaseForwarder{}
	baseForwarder.LoadMetaData(ctx, g, schemaDB, log)
	forwarder := JobsForwarder{
		transientSources: transientSources,
		BaseForwarder:    baseForwarder,
	}
	client, err := pulsar.New()
	if err != nil {
		return nil, err
	}
	forwarder.pulsarProducer = client

	return &forwarder, nil
}

func (jf *JobsForwarder) Start(ctx context.Context) {
	jf.ErrGroup.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				unprocessedList, err := misc.QueryWithRetriesAndNotify(ctx, jf.BaseConfig.JobsDBQueryRequestTimeout, jf.BaseConfig.JobsDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
					return jf.JobsDB.GetUnprocessed(ctx, jf.generateQueryParams())
				}, jf.sendQueryRetryStats)
				if err != nil {
					jf.Log.Errorf("Error while querying jobsDB: %v", err)
					continue // Should we do a panic here like elsewhere
				}
				time.Sleep(jf.GetSleepTime(unprocessedList))
			}
		}
	}))
}

func (jf *JobsForwarder) Stop() {
	jf.pulsarProducer.Close()
}

func (jf *JobsForwarder) generateQueryParams() jobsdb.GetQueryParamsT {
	return jobsdb.GetQueryParamsT{
		EventsLimit: jf.BaseConfig.PickupSize,
	}
}

func (jf *JobsForwarder) sendQueryRetryStats(attempt int) {
	jf.Log.Warnf("Timeout during query jobs in processor module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "jobs_forwarder"}).Count(1)
}
