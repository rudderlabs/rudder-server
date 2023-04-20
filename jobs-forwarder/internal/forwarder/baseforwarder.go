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
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type BaseForwarder struct {
	parentCancel context.CancelFunc
	cancel       context.CancelFunc
	g            *errgroup.Group
	jobsDB       jobsdb.JobsDB
	log          logger.Logger
	baseConfig   struct {
		pickupSize                int
		loopSleepTime             time.Duration
		jobsDBQueryRequestTimeout time.Duration
		jobsDBMaxRetries          int
		jobsDBPayloadSize         int64
	}
}

func (bf *BaseForwarder) LoadMetaData(parentCancel context.CancelFunc, schemaDB jobsdb.JobsDB, log logger.Logger, config *config.Config) {
	bf.parentCancel = parentCancel
	bf.baseConfig.pickupSize = config.GetInt("JobsForwarder.eventCount", 10000)
	bf.baseConfig.loopSleepTime = config.GetDuration("JobsForwarder.loopSleepTime", 10, time.Second)
	bf.baseConfig.jobsDBQueryRequestTimeout = config.GetDuration("JobsForwarder.queryTimeout", 10, time.Second)
	bf.baseConfig.jobsDBMaxRetries = config.GetInt("JobsForwarder.maxRetries", 3)
	bf.baseConfig.jobsDBPayloadSize = config.GetInt64("JobsForwarder.payloadSize", 20*bytesize.MB)
	bf.log = log
	bf.jobsDB = schemaDB
}

func (bf *BaseForwarder) GetJobs(ctx context.Context) ([]*jobsdb.JobT, bool, error) {
	queryParams := bf.generateQueryParams()
	unprocessed, err := misc.QueryWithRetriesAndNotify(ctx, bf.baseConfig.jobsDBQueryRequestTimeout, bf.baseConfig.jobsDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
		return bf.jobsDB.GetUnprocessed(ctx, queryParams)
	}, bf.sendQueryRetryStats)
	if err != nil {
		bf.log.Errorf("forwarder error while reading unprocessed from DB: %v", err)
		return nil, false, err
	}
	return unprocessed.Jobs, unprocessed.LimitsReached, nil
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
	bf.log.Warnf("Timeout during query jobs in jobs forwarder, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "jobs_forwarder"}).Count(1)
}

func (bf *BaseForwarder) generateQueryParams() jobsdb.GetQueryParamsT {
	return jobsdb.GetQueryParamsT{
		EventsLimit:      bf.baseConfig.pickupSize,
		JobsLimit:        bf.baseConfig.pickupSize,
		PayloadSizeLimit: bf.baseConfig.jobsDBPayloadSize,
	}
}
