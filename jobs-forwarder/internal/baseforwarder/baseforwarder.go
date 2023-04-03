package baseforwarder

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
	ErrGroup   *errgroup.Group
	JobsDB     jobsdb.JobsDB
	Log        logger.Logger
	config     *config.Config
	BaseConfig struct {
		PickupSize                int
		loopSleepTime             time.Duration
		JobsDBQueryRequestTimeout time.Duration
		JobsDBMaxRetries          int
	}
}

func (bf *BaseForwarder) LoadMetaData(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, log logger.Logger) {
	bf.config = config.New()
	bf.BaseConfig.PickupSize = bf.config.GetInt("JobsForwarder.eventCount", 10000)
	bf.BaseConfig.loopSleepTime = config.GetDuration("JobsForwarder.loopSleepTime", 10, time.Second)
	bf.BaseConfig.JobsDBQueryRequestTimeout = config.GetDuration("JobsForwarder.queryTimeout", 10, time.Second)
	bf.BaseConfig.JobsDBMaxRetries = config.GetInt("JobsForwarder.maxRetries", 3)
	bf.Log = log
	bf.JobsDB = schemaDB
	bf.ctx = ctx
	bf.ErrGroup = g
}

func (bf *BaseForwarder) GetJobs(ctx context.Context) (jobsdb.JobsResult, error) {
	unprocessedList, err := misc.QueryWithRetriesAndNotify(ctx, bf.BaseConfig.JobsDBQueryRequestTimeout, bf.BaseConfig.JobsDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
		return bf.JobsDB.GetUnprocessed(ctx, bf.generateQueryParams())
	}, bf.sendQueryRetryStats)
	if err != nil {
		bf.Log.Errorf("Error while querying jobsDB: %v", err)
		return jobsdb.JobsResult{}, err
	}
	return unprocessedList, nil
}

func (bf *BaseForwarder) MarkJobStatuses(ctx context.Context, statusList []*jobsdb.JobStatusT) error {
	err := misc.RetryWithNotify(ctx, bf.BaseConfig.JobsDBQueryRequestTimeout, bf.BaseConfig.JobsDBMaxRetries, func(ctx context.Context) error {
		return bf.JobsDB.WithUpdateSafeTx(ctx, func(txn jobsdb.UpdateSafeTx) error {
			return bf.JobsDB.UpdateJobStatusInTx(ctx, txn, statusList, nil, nil)
		})
	}, bf.sendQueryRetryStats)
	return err
}

func (bf *BaseForwarder) GetSleepTime(unprocessedList jobsdb.JobsResult) time.Duration {
	return time.Duration(float64(bf.BaseConfig.loopSleepTime) * (1 - math.Min(1, float64(unprocessedList.EventsCount)/float64(bf.BaseConfig.PickupSize))))
}

func (bf *BaseForwarder) sendQueryRetryStats(attempt int) {
	bf.Log.Warnf("Timeout during query jobs in processor module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "jobs_forwarder"}).Count(1)
}

func (bf *BaseForwarder) generateQueryParams() jobsdb.GetQueryParamsT {
	return jobsdb.GetQueryParamsT{
		EventsLimit: bf.BaseConfig.PickupSize,
	}
}
