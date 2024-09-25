package forwarder

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// AbortingForwarder is a forwarder which aborts all jobs instead of forwarding them
type AbortingForwarder struct {
	BaseForwarder
}

// NewAbortingForwarder returns a new, properly initialized, AbortingForwarder
func NewAbortingForwarder(terminalErrFn func(error), schemaDB jobsdb.JobsDB, config *config.Config, log logger.Logger, stat stats.Stats) *AbortingForwarder {
	var forwarder AbortingForwarder
	forwarder.LoadMetaData(terminalErrFn, schemaDB, log, config, stat)
	return &forwarder
}

// Start starts the forwarder which reads jobs from the database and aborts them
func (nf *AbortingForwarder) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	nf.g, ctx = errgroup.WithContext(ctx)
	nf.cancel = cancel

	nf.g.Go(crash.Wrapper(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				jobList, limitReached, err := nf.GetJobs(ctx)
				if err != nil {
					if ctx.Err() != nil { // we are shutting down
						return nil //nolint:nilerr
					}
					nf.terminalErrFn(err) // we are signaling to shutdown the app
					return err
				}
				nf.log.Debugf("NoopForwarder: Got %d jobs", len(jobList))
				var statusList []*jobsdb.JobStatusT
				for _, job := range jobList {
					statusList = append(statusList, &jobsdb.JobStatusT{
						JobID:         job.JobID,
						JobState:      jobsdb.Aborted.State,
						AttemptNum:    0,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "400",
						ErrorResponse: []byte(`{"success":false,"message":"JobsForwarder is disabled"}`),
						Parameters:    []byte(`{}`),
						JobParameters: job.Parameters,
						WorkspaceId:   job.WorkspaceId,
					})
				}
				err = nf.MarkJobStatuses(ctx, statusList)
				if err != nil {
					if ctx.Err() != nil { // we are shutting down
						return nil //nolint:nilerr
					}
					nf.log.Errorf("Error while updating job status: %v", err)
					nf.terminalErrFn(err) // we are signaling to shutdown the app
					return err
				}
				_ = misc.SleepCtx(ctx, nf.GetSleepTime(limitReached))
			}
		}
	}))
	return nil
}

// Stop stops the forwarder
func (nf *AbortingForwarder) Stop() {
	nf.cancel()
	_ = nf.g.Wait()
}
