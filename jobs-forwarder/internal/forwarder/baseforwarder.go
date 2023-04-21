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
	terminalErrFn func(error)        // function to call when a terminal error occurs
	cancel        context.CancelFunc // cancel function for the Start context (used to stop all goroutines during Stop)
	g             *errgroup.Group    // errgroup for the Start context (used to wait for all goroutines to exit)

	jobsDB jobsdb.JobsDB
	log    logger.Logger
	conf   struct {
		pickupSize                int           // number of jobs to pickup in a single query
		loopSleepTime             time.Duration // time to sleep between each loop
		jobsDBQueryRequestTimeout time.Duration // timeout for jobsdb query
		jobsDBMaxRetries          int           // max retries for jobsdb query
		jobsDBPayloadSize         int64         // max payload size for jobsdb query
	}
}

// LoadMetaData loads the metadata required by the forwarders
func (bf *BaseForwarder) LoadMetaData(terminalErrFn func(error), schemaDB jobsdb.JobsDB, log logger.Logger, config *config.Config) {
	bf.terminalErrFn = terminalErrFn
	bf.conf.pickupSize = config.GetInt("JobsForwarder.eventCount", 10000)
	bf.conf.loopSleepTime = config.GetDuration("JobsForwarder.loopSleepTime", 10, time.Second)
	bf.conf.jobsDBQueryRequestTimeout = config.GetDuration("JobsForwarder.queryTimeout", 10, time.Second)
	bf.conf.jobsDBMaxRetries = config.GetInt("JobsForwarder.maxRetries", 3)
	bf.conf.jobsDBPayloadSize = config.GetInt64("JobsForwarder.payloadSize", 20*bytesize.MB)
	bf.log = log
	bf.jobsDB = schemaDB
}

// GetJobs is an abstraction over the GetUnprocessed method of the jobsdb which includes retries
func (bf *BaseForwarder) GetJobs(ctx context.Context) ([]*jobsdb.JobT, bool, error) {
	queryParams := bf.generateQueryParams()
	unprocessed, err := misc.QueryWithRetriesAndNotify(ctx, bf.conf.jobsDBQueryRequestTimeout, bf.conf.jobsDBMaxRetries, func(ctx context.Context) (jobsdb.JobsResult, error) {
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
	err := misc.RetryWithNotify(ctx, bf.conf.jobsDBQueryRequestTimeout, bf.conf.jobsDBMaxRetries, func(ctx context.Context) error {
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
	return bf.conf.loopSleepTime
}

func (bf *BaseForwarder) sendQueryRetryStats(attempt int) {
	bf.log.Warnf("Timeout during query jobs in jobs forwarder, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "jobs_forwarder"}).Count(1)
}

func (bf *BaseForwarder) generateQueryParams() jobsdb.GetQueryParamsT {
	return jobsdb.GetQueryParamsT{
		EventsLimit:      bf.conf.pickupSize,
		JobsLimit:        bf.conf.pickupSize,
		PayloadSizeLimit: bf.conf.jobsDBPayloadSize,
	}
}
