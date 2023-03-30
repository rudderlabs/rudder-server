package jobs_forwarder

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type ForwarderMetaData struct {
	jobsDB                    *jobsdb.HandleT
	eventCount                int
	log                       logger.Logger
	loopSleepTime             time.Duration
	jobsDBQueryRequestTimeout time.Duration
	jobsDBMaxRetries          int
}

func (fm *ForwarderMetaData) loadMetaData(schemaDB *jobsdb.HandleT, log logger.Logger) {
	fm.eventCount = config.GetInt("JobsForwarder.eventCount", 10000)
	fm.loopSleepTime = config.GetDuration("JobsForwarder.loopSleepTime", 10, time.Second)
	fm.jobsDBQueryRequestTimeout = config.GetDuration("JobsForwarder.queryTimeout", 10, time.Second)
	fm.jobsDBMaxRetries = config.GetInt("JobsForwarder.maxRetries", 3)
	fm.log = log
	fm.jobsDB = schemaDB
}

func (fm *ForwarderMetaData) sendQueryRetryStats(attempt int) {
	fm.log.Warnf("Timeout during query jobs in processor module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "jobs_forwarder"}).Count(1)
}

func (fm *ForwarderMetaData) generateQueryParams() jobsdb.GetQueryParamsT {
	return jobsdb.GetQueryParamsT{
		EventsLimit: fm.eventCount,
	}
}

func (fm *ForwarderMetaData) GetJobs(ctx context.Context) (jobsdb.JobsResult, error) {
	unprocessedList, err := misc.QueryWithRetriesAndNotify(ctx, fm.jobsDBQueryRequestTimeout, fm.jobsDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
		return fm.jobsDB.GetUnprocessed(ctx, fm.generateQueryParams())
	}, fm.sendQueryRetryStats)
	if err != nil {
		fm.log.Errorf("Error while querying jobsDB: %v", err)
		return jobsdb.JobsResult{}, err
	}
	return unprocessedList, nil
}

func (fm *ForwarderMetaData) MarkJobStatuses(ctx context.Context, statusList []*jobsdb.JobStatusT) error {
	err := misc.RetryWithNotify(ctx, fm.jobsDBQueryRequestTimeout, fm.jobsDBMaxRetries, func(ctx context.Context) error {
		return fm.jobsDB.WithUpdateSafeTx(ctx, func(txn jobsdb.UpdateSafeTx) error {
			return fm.jobsDB.UpdateJobStatusInTx(ctx, txn, statusList, nil, nil)
		})
	}, fm.sendQueryRetryStats)
	return err
}

func (fm *ForwarderMetaData) GetSleepTime(unprocessedList jobsdb.JobsResult) time.Duration {
	return time.Duration(float64(fm.loopSleepTime) * (1 - math.Min(1, float64(unprocessedList.EventsCount)/float64(fm.eventCount))))
}
