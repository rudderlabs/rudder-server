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

type NoopForwarder struct {
	BaseForwarder
}

func NewNOOPForwarder(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, config *config.Config, log logger.Logger) (*NoopForwarder, error) {
	baseForwarder := BaseForwarder{}
	baseForwarder.LoadMetaData(ctx, g, schemaDB, log, config)
	return &NoopForwarder{baseForwarder}, nil
}

func (nf *NoopForwarder) Start() error {
	nf.g.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-nf.ctx.Done():
				return nil
			default:
				jobList, limitReached, err := nf.GetJobs(nf.ctx)
				if err != nil {
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
				err = nf.MarkJobStatuses(nf.ctx, statusList)
				if err != nil {
					nf.log.Errorf("Error while updating job status: %v", err)
					return err
				}
				time.Sleep(nf.GetSleepTime(limitReached))
			}
		}
	}))
	return nil
}

func (nf *NoopForwarder) Stop() {
}
