package jobs_forwarder

import (
	"context"
	"fmt"
	"math"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type BaseForwarder struct {
	ctx        context.Context
	g          *errgroup.Group
	jobsDB     jobsdb.JobsDB
	log        logger.Logger
	config     *config.Config
	baseConfig struct {
		pickupSize                int
		loopSleepTime             time.Duration
		jobsDBQueryRequestTimeout time.Duration
		jobsDBMaxRetries          int
	}
}

func (bf *BaseForwarder) loadMetaData(schemaDB jobsdb.JobsDB, log logger.Logger) {
	bf.config = config.New()
	bf.baseConfig.pickupSize = bf.config.GetInt("JobsForwarder.eventCount", 10000)
	bf.baseConfig.loopSleepTime = config.GetDuration("JobsForwarder.loopSleepTime", 10, time.Second)
	bf.baseConfig.jobsDBQueryRequestTimeout = config.GetDuration("JobsForwarder.queryTimeout", 10, time.Second)
	bf.baseConfig.jobsDBMaxRetries = config.GetInt("JobsForwarder.maxRetries", 3)
	bf.log = log
	bf.jobsDB = schemaDB
}

func (bf *BaseForwarder) GetJobs(ctx context.Context) (jobsdb.JobsResult, error) {
	unprocessedList, err := misc.QueryWithRetriesAndNotify(ctx, bf.baseConfig.jobsDBQueryRequestTimeout, bf.baseConfig.jobsDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
		return bf.jobsDB.GetUnprocessed(ctx, bf.generateQueryParams())
	}, bf.sendQueryRetryStats)
	if err != nil {
		bf.log.Errorf("Error while querying jobsDB: %v", err)
		return jobsdb.JobsResult{}, err
	}
	return unprocessedList, nil
}

func (bf *BaseForwarder) MarkJobStatuses(ctx context.Context, statusList []*jobsdb.JobStatusT) error {
	err := misc.RetryWithNotify(ctx, bf.baseConfig.jobsDBQueryRequestTimeout, bf.baseConfig.jobsDBMaxRetries, func(ctx context.Context) error {
		return bf.jobsDB.WithUpdateSafeTx(ctx, func(txn jobsdb.UpdateSafeTx) error {
			return bf.jobsDB.UpdateJobStatusInTx(ctx, txn, statusList, nil, nil)
		})
	}, bf.sendQueryRetryStats)
	return err
}

func (bf *BaseForwarder) getSleepTime(unprocessedList jobsdb.JobsResult) time.Duration {
	return time.Duration(float64(bf.baseConfig.loopSleepTime) * (1 - math.Min(1, float64(unprocessedList.EventsCount)/float64(bf.baseConfig.pickupSize))))
}

func (bf *BaseForwarder) sendQueryRetryStats(attempt int) {
	bf.log.Warnf("Timeout during query jobs in processor module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "jobs_forwarder"}).Count(1)
}

func (bf *BaseForwarder) generateQueryParams() jobsdb.GetQueryParamsT {
	return jobsdb.GetQueryParamsT{
		EventsLimit: bf.baseConfig.pickupSize,
	}
}
