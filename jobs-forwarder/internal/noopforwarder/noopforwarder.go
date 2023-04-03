package noopforwarder

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/baseforwarder"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type NoopForwarder struct {
	baseforwarder.BaseForwarder
}

func New(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, log logger.Logger) (*NoopForwarder, error) {
	baseForwarder := baseforwarder.BaseForwarder{}
	baseForwarder.LoadMetaData(ctx, g, schemaDB, log)
	return &NoopForwarder{baseForwarder}, nil
}

func (nf *NoopForwarder) Start(ctx context.Context) {
	nf.ErrGroup.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				jobList, limitReached, err := nf.GetJobs(ctx)
				if err != nil {
					panic(err)
				}
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
					nf.Log.Errorf("Error while updating job status: %v", err)
					panic(err)
				}
				time.Sleep(nf.GetSleepTime(limitReached))
			}
		}
	}))
}

func (nf *NoopForwarder) Stop() {
	nf.JobsDB.Close()
}
