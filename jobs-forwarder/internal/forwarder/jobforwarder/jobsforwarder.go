package jobforwarder

import (
	"context"
	"encoding/json"
	"time"

	"golang.org/x/sync/errgroup"

	pulsarType "github.com/apache/pulsar-client-go/pulsar"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/forwarder/baseforwarder"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/schematransformer"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type JobsForwarder struct {
	baseforwarder.BaseForwarder
	pulsarProducer   pulsar.ProducerAdapter
	transientSources transientsource.Service
	backendConfig    backendconfig.BackendConfig
	transformer      schematransformer.Transformer
	retryAttempts    int
	key              string
}

func New(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, config *config.Config, transientSources transientsource.Service, backendConfig backendconfig.BackendConfig, log logger.Logger) (*JobsForwarder, error) {
	baseForwarder := baseforwarder.BaseForwarder{}
	baseForwarder.LoadMetaData(ctx, g, schemaDB, log, config)
	forwarder := JobsForwarder{
		transientSources: transientSources,
		BaseForwarder:    baseForwarder,
		retryAttempts:    config.GetInt("JobsForwarder.retryAttempts", 3),
		key:              config.GetString("JobsForwarder.key", "event-schema"),
	}
	client, err := pulsar.New(config)
	if err != nil {
		return nil, err
	}
	forwarder.pulsarProducer = client
	forwarder.backendConfig = backendConfig
	forwarder.transformer = schematransformer.New(ctx, g, backendConfig, transientSources, config)
	return &forwarder, nil
}

func (jf *JobsForwarder) Start(ctx context.Context) {
	jf.ErrGroup.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				jobs, limitReached, err := jf.GetJobs(ctx)
				if err != nil {
					return err
				}
				var statusList []*jobsdb.JobStatusT
				filteredJobs := jf.filterJobs(jobs, statusList)
				for _, job := range filteredJobs {
					transformedBytes, err := jf.transformer.Transform(job)
					if err != nil {
						statusList = append(statusList, &jobsdb.JobStatusT{
							JobID:         job.JobID,
							AttemptNum:    job.LastJobStatus.AttemptNum + 1,
							JobState:      jobsdb.Failed.State,
							ExecTime:      time.Now(),
							RetryTime:     time.Now(),
							ErrorCode:     "500",
							Parameters:    []byte{},
							ErrorResponse: json.RawMessage(err.Error()),
						})
					}
					statusFunc := func(_ pulsarType.MessageID, _ *pulsarType.ProducerMessage, err error) {
						if err != nil {
							statusList = append(statusList, &jobsdb.JobStatusT{
								JobID:         job.JobID,
								AttemptNum:    job.LastJobStatus.AttemptNum + 1,
								JobState:      jobsdb.Failed.State,
								ExecTime:      time.Now(),
								RetryTime:     time.Now(),
								ErrorCode:     "500",
								Parameters:    []byte{},
								ErrorResponse: json.RawMessage(err.Error()),
							})
						} else {
							statusList = append(statusList, &jobsdb.JobStatusT{
								JobID:      job.JobID,
								AttemptNum: job.LastJobStatus.AttemptNum + 1,
								JobState:   jobsdb.Succeeded.State,
								ExecTime:   time.Now(),
							})
						}
					}
					jf.pulsarProducer.SendMessageAsync(ctx, jf.key, "", transformedBytes, statusFunc)
					err = jf.pulsarProducer.Flush()
					if err != nil {
						return err
					}
					err = jf.MarkJobStatuses(ctx, statusList)
					if err != nil {
						return err
					}
				}
				time.Sleep(jf.GetSleepTime(limitReached))
			}
		}
	}))
}

func (jf *JobsForwarder) Stop() {
	jf.pulsarProducer.Close()
}

func (jf *JobsForwarder) filterJobs(jobs []*jobsdb.JobT, list []*jobsdb.JobStatusT) []*jobsdb.JobT {
	var filteredJobs []*jobsdb.JobT
	for _, job := range jobs {
		if job.LastJobStatus.JobState == "failed" && job.LastJobStatus.AttemptNum >= jf.retryAttempts {
			list = append(list, &jobsdb.JobStatusT{
				JobID:      job.JobID,
				AttemptNum: job.LastJobStatus.AttemptNum + 1,
				JobState:   jobsdb.Aborted.State,
				ExecTime:   time.Now(),
				RetryTime:  time.Now(),
				ErrorCode:  "500",
				Parameters: []byte{},
			})
		} else {
			filteredJobs = append(filteredJobs, job)
		}
	}
	return filteredJobs
}
