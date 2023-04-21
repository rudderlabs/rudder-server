package forwarder

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type AbortingForwarder struct {
	BaseForwarder
}

// NewAbortingForwarder returns a new instance of AbortingForwarder
func NewAbortingForwarder(terminalErrFn func(error), schemaDB jobsdb.JobsDB, config *config.Config, log logger.Logger) (*AbortingForwarder, error) {
	baseForwarder := BaseForwarder{}
	baseForwarder.LoadMetaData(terminalErrFn, schemaDB, log, config)
	return &AbortingForwarder{baseForwarder}, nil
}

// Start starts the forwarder which reads jobs from the database and aborts them
func (nf *AbortingForwarder) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	nf.g, ctx = errgroup.WithContext(ctx)
	nf.cancel = cancel

	nf.g.Go(misc.WithBugsnag(func() error {
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
					nf.terminalErrFn(err)
					return err
				}
				nf.log.Infof("NoopForwarder: Got %d jobs", len(jobList))
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
						WorkspaceId:   job.WorkspaceId,
					})
				}
				err = nf.MarkJobStatuses(ctx, statusList)
				if err != nil {
					if ctx.Err() != nil { // we are shutting down
						return nil //nolint:nilerr
					}
					nf.log.Errorf("Error while updating job status: %v", err)
					nf.terminalErrFn(err)
					return err
				}
				time.Sleep(nf.GetSleepTime(limitReached))
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
