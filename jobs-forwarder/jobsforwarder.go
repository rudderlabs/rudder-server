package jobs_forwarder

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type JobsForwarder struct {
	ForwarderMetaData
	pulsarProducer   pulsar.ProducerAdapter
	transientSources transientsource.Service
}

type NOOPForwarder struct {
	ForwarderMetaData
}

type Forwarder interface {
	Start(ctx context.Context)
	Stop()
}

func New(schemaDB *jobsdb.HandleT, transientSources transientsource.Service, log logger.Logger) (Forwarder, error) {
	forwarderMetaData := ForwarderMetaData{}
	forwarderMetaData.loadMetaData(schemaDB, log)
	if !config.GetBool("JobsForwarder.enabled", false) {
		return &NOOPForwarder{
			ForwarderMetaData: forwarderMetaData,
		}, nil
	}

	jobsForwarder := JobsForwarder{
		transientSources:  transientSources,
		ForwarderMetaData: forwarderMetaData,
	}
	client, err := pulsar.New()
	if err != nil {
		return &JobsForwarder{}, err
	}
	jobsForwarder.pulsarProducer = client

	return &jobsForwarder, nil
}

func (jf *JobsForwarder) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			unprocessedList, err := misc.QueryWithRetriesAndNotify(ctx, jf.jobsDBQueryRequestTimeout, jf.jobsDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
				return jf.jobsDB.GetUnprocessed(ctx, jf.generateQueryParams())
			}, jf.sendQueryRetryStats)
			if err != nil {
				jf.log.Errorf("Error while querying jobsDB: %v", err)
				continue // Should we do a panic here like elsewhere
			}
			time.Sleep(jf.GetSleepTime(unprocessedList))
		}
	}

}

func (jf *JobsForwarder) Stop() {
	jf.pulsarProducer.Close()
	jf.jobsDB.Close()
}

func (jf *JobsForwarder) generateQueryParams() jobsdb.GetQueryParamsT {
	return jobsdb.GetQueryParamsT{
		EventsLimit: jf.eventCount,
	}
}

func (jf *JobsForwarder) sendQueryRetryStats(attempt int) {
	jf.log.Warnf("Timeout during query jobs in processor module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "jobs_forwarder"}).Count(1)
}

func (nf *NOOPForwarder) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
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
			time.Sleep(nf.GetSleepTime(unprocessedList))
		}
	}
}

func (nf *NOOPForwarder) Stop() {
	nf.jobsDB.Close()
}
