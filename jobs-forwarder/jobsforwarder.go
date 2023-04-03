package jobs_forwarder

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type jobsForwarder struct {
	BaseForwarder
	pulsarProducer   pulsar.ProducerAdapter
	transientSources transientsource.Service
}

type noopForwarder struct {
	BaseForwarder
}

type Forwarder interface {
	Start(ctx context.Context)
	Stop()
}

func New(ctx context.Context, schemaDB jobsdb.JobsDB, transientSources transientsource.Service, log logger.Logger) (Forwarder, error) {
	forwarderMetaData := BaseForwarder{}
	g, ctx := errgroup.WithContext(ctx)
	forwarderMetaData.ctx = ctx
	forwarderMetaData.g = g
	forwarderMetaData.loadMetaData(schemaDB, log)
	if !config.GetBool("JobsForwarder.enabled", false) {
		return &noopForwarder{
			BaseForwarder: forwarderMetaData,
		}, nil
	}

	forwarder := jobsForwarder{
		transientSources: transientSources,
		BaseForwarder:    forwarderMetaData,
	}
	client, err := pulsar.New()
	if err != nil {
		return nil, err
	}
	forwarder.pulsarProducer = client

	return &forwarder, nil
}

func (jf *jobsForwarder) Start(ctx context.Context) {
	jf.g.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				unprocessedList, err := misc.QueryWithRetriesAndNotify(ctx, jf.baseConfig.jobsDBQueryRequestTimeout, jf.baseConfig.jobsDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
					return jf.jobsDB.GetUnprocessed(ctx, jf.generateQueryParams())
				}, jf.sendQueryRetryStats)
				if err != nil {
					jf.log.Errorf("Error while querying jobsDB: %v", err)
					continue // Should we do a panic here like elsewhere
				}
				time.Sleep(jf.getSleepTime(unprocessedList))
			}
		}
	}))
}

func (jf *jobsForwarder) Stop() {
	_ = jf.g.Wait()
	jf.pulsarProducer.Close()
}

func (jf *jobsForwarder) generateQueryParams() jobsdb.GetQueryParamsT {
	return jobsdb.GetQueryParamsT{
		EventsLimit: jf.baseConfig.pickupSize,
	}
}

func (jf *jobsForwarder) sendQueryRetryStats(attempt int) {
	jf.log.Warnf("Timeout during query jobs in processor module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "jobs_forwarder"}).Count(1)
}

func (nf *noopForwarder) Start(ctx context.Context) {
	nf.g.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				unprocessedList, err := nf.GetJobs(ctx)
				if err != nil {
					panic(err)
				}
				var statusList []*jobsdb.JobStatusT
				for _, job := range unprocessedList.Jobs {
					statusList = append(statusList, &jobsdb.JobStatusT{
						JobID:         job.JobID,
						JobState:      jobsdb.Aborted.State,
						AttemptNum:    0,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "400",
						ErrorResponse: []byte(`{"success":false,"message":"JobsForwarder is disabled"}`),
						Parameters:    []byte(`{}`),
						WorkspaceId:   job.WorkspaceId,
					})
				}
				err = nf.MarkJobStatuses(ctx, statusList)
				if err != nil {
					nf.log.Errorf("Error while updating job status: %v", err)
					panic(err)
				}
				time.Sleep(nf.getSleepTime(unprocessedList))
			}
		}
	}))
}

func (nf *noopForwarder) Stop() {
	_ = nf.g.Wait()
	nf.jobsDB.Close()
}
