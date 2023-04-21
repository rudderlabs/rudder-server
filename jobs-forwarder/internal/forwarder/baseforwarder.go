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
	terminalErrFn func(error)
	cancel        context.CancelFunc
	g             *errgroup.Group
	jobsDB        jobsdb.JobsDB
	log           logger.Logger
	baseConfig    struct {
		pickupSize                int
		loopSleepTime             time.Duration
		jobsDBQueryRequestTimeout time.Duration
		jobsDBMaxRetries          int
		jobsDBPayloadSize         int64
	}
}

// LoadMetaData loads the metadata required by the forwarders
func (bf *BaseForwarder) LoadMetaData(terminalErrFn func(error), schemaDB jobsdb.JobsDB, log logger.Logger, config *config.Config) {
	bf.terminalErrFn = terminalErrFn
	bf.baseConfig.pickupSize = config.GetInt("JobsForwarder.eventCount", 10000)
	bf.baseConfig.loopSleepTime = config.GetDuration("JobsForwarder.loopSleepTime", 10, time.Second)
	bf.baseConfig.jobsDBQueryRequestTimeout = config.GetDuration("JobsForwarder.queryTimeout", 10, time.Second)
	bf.baseConfig.jobsDBMaxRetries = config.GetInt("JobsForwarder.maxRetries", 3)
	bf.baseConfig.jobsDBPayloadSize = config.GetInt64("JobsForwarder.payloadSize", 20*bytesize.MB)
	bf.log = log
	bf.jobsDB = schemaDB
}

// GetJobs is an abstraction over the GetUnprocessed method of the jobsdb which includes retries
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

// MarkJobStatuses is an abstraction over the UpdateJobStatusInTx method of the jobsdb which includes retries
func (bf *BaseForwarder) MarkJobStatuses(ctx context.Context, statusList []*jobsdb.JobStatusT) error {
	err := misc.RetryWithNotify(ctx, bf.baseConfig.jobsDBQueryRequestTimeout, bf.baseConfig.jobsDBMaxRetries, func(ctx context.Context) error {
		return bf.jobsDB.WithUpdateSafeTx(ctx, func(txn jobsdb.UpdateSafeTx) error {
			return bf.jobsDB.UpdateJobStatusInTx(ctx, txn, statusList, nil, nil)
		})
	}, bf.sendQueryRetryStats)
	return err
}

// GetSleepTime returns the sleep time based on the limitReached flag
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
