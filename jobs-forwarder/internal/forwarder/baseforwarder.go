package forwarder

import (
	"context"
	"fmt"
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

func (bf *BaseForwarder) LoadMetaData(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, log logger.Logger, config *config.Config) {
	bf.config = config
	bf.baseConfig.pickupSize = bf.config.GetInt("JobsForwarder.eventCount", 10000)
	bf.baseConfig.loopSleepTime = config.GetDuration("JobsForwarder.loopSleepTime", 10, time.Second)
	bf.baseConfig.jobsDBQueryRequestTimeout = config.GetDuration("JobsForwarder.queryTimeout", 10, time.Second)
	bf.baseConfig.jobsDBMaxRetries = config.GetInt("JobsForwarder.maxRetries", 3)
	bf.log = log
	bf.jobsDB = schemaDB
	bf.ctx = ctx
	bf.g = g
}

func (bf *BaseForwarder) GetJobs(ctx context.Context) ([]*jobsdb.JobT, bool, error) {
	var combinedList []*jobsdb.JobT
	queryParams := bf.generateQueryParams()
	toRetry, err := misc.QueryWithRetriesAndNotify(ctx, bf.baseConfig.jobsDBQueryRequestTimeout, bf.baseConfig.jobsDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
		return bf.jobsDB.GetToRetry(ctx, queryParams)
	}, bf.sendQueryRetryStats)
	if err != nil {
		bf.log.Errorf("base forwarder: Error while reading failed from DB: %v", err)
		return nil, false, err
	}
	combinedList = toRetry.Jobs
	if toRetry.LimitsReached {
		bf.log.Infof("base forwarder: Reached limit while reading failed from DB")
		return combinedList, true, nil
	}
	queryParams.JobsLimit -= len(toRetry.Jobs)
	if queryParams.PayloadSizeLimit > 0 {
		queryParams.PayloadSizeLimit -= toRetry.PayloadSize
	}
	unprocessed, err := misc.QueryWithRetriesAndNotify(ctx, bf.baseConfig.jobsDBQueryRequestTimeout, bf.baseConfig.jobsDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
		return bf.jobsDB.GetUnprocessed(ctx, queryParams)
	}, bf.sendQueryRetryStats)
	if err != nil {
		bf.log.Errorf("base forwarder: Error while reading unprocessed from DB: %v", err)
		return nil, false, err
	}
	combinedList = append(combinedList, unprocessed.Jobs...)
	return combinedList, unprocessed.LimitsReached, nil
}

func (bf *BaseForwarder) MarkJobStatuses(ctx context.Context, statusList []*jobsdb.JobStatusT) error {
	err := misc.RetryWithNotify(ctx, bf.baseConfig.jobsDBQueryRequestTimeout, bf.baseConfig.jobsDBMaxRetries, func(ctx context.Context) error {
		return bf.jobsDB.WithUpdateSafeTx(ctx, func(txn jobsdb.UpdateSafeTx) error {
			return bf.jobsDB.UpdateJobStatusInTx(ctx, txn, statusList, nil, nil)
		})
	}, bf.sendQueryRetryStats)
	return err
}

func (bf *BaseForwarder) GetSleepTime(limitReached bool) time.Duration {
	if limitReached {
		return time.Duration(0)
	}
	return bf.baseConfig.loopSleepTime
}

func (bf *BaseForwarder) sendQueryRetryStats(attempt int) {
	bf.log.Warnf("Timeout during query jobs in processor module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "jobs_forwarder"}).Count(1)
}

func (bf *BaseForwarder) generateQueryParams() jobsdb.GetQueryParamsT {
	return jobsdb.GetQueryParamsT{
		EventsLimit: bf.baseConfig.pickupSize,
		JobsLimit:   bf.baseConfig.pickupSize,
	}
}
