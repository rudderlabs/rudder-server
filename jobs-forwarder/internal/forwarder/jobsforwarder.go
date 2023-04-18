package forwarder

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
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/schematransformer"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type JobsForwarder struct {
	BaseForwarder
	pulsarProducer pulsar.ProducerAdapter
	backendConfig  backendconfig.BackendConfig
	transformer    schematransformer.Transformer
	retryAttempts  int
	key            string
}

func NewJobsForwarder(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, client *pulsar.Client, config *config.Config, backendConfig backendconfig.BackendConfig, log logger.Logger) (*JobsForwarder, error) {
	forwarder := JobsForwarder{
		retryAttempts: config.GetInt("JobsForwarder.retryAttempts", 3),
		key:           config.GetString("JobsForwarder.key", "event-schema"),
	}
	forwarder.LoadMetaData(ctx, g, schemaDB, log, config)
	producer, err := client.NewProducer(config)
	if err != nil {
		return nil, err
	}
	forwarder.pulsarProducer = producer
	forwarder.backendConfig = backendConfig
	forwarder.transformer = schematransformer.New(ctx, g, backendConfig, config)
	return &forwarder, nil
}

func (jf *JobsForwarder) Start() {
	var sleepTime time.Duration
	jf.transformer.Start()
	jf.g.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-jf.ctx.Done():
				return nil
			case <-time.After(sleepTime):
				jobs, limitReached, err := jf.GetJobs(jf.ctx)
				if err != nil {
					return err
				}
				filteredJobs, statusList := jf.filterJobs(jobs)
				for _, job := range filteredJobs {
					transformedBytes, orderingKey, err := jf.transformer.Transform(job)
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
						continue
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
					// Temporary to make it work
					eventPayload, err := transformedBytes.MustMarshal()
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
						continue
					}
					jf.pulsarProducer.SendMessageAsync(jf.ctx, orderingKey, orderingKey, eventPayload, statusFunc)
				}
				err = jf.pulsarProducer.Flush()
				if err != nil {
					return err
				}
				err = jf.MarkJobStatuses(jf.ctx, statusList)
				if err != nil {
					return err
				}
				sleepTime = jf.GetSleepTime(limitReached)
			}
		}
	}))
}

func (jf *JobsForwarder) Stop() {
	jf.transformer.Stop()
	jf.pulsarProducer.Close()
}

func (jf *JobsForwarder) filterJobs(jobs []*jobsdb.JobT) ([]*jobsdb.JobT, []*jobsdb.JobStatusT) {
	var filteredJobs []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	for _, job := range jobs {
		if job.LastJobStatus.JobState == "failed" && job.LastJobStatus.AttemptNum >= jf.retryAttempts {
			statusList = append(statusList, &jobsdb.JobStatusT{
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
	return filteredJobs, statusList
}
