package forwarder

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	pulsarType "github.com/apache/pulsar-client-go/pulsar"
	"github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/schematransformer"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
)

type JobsForwarder struct {
	BaseForwarder
	transformer          schematransformer.Transformer
	sampler              *sampler[string]
	pulsarProducer       pulsar.ProducerAdapter
	initialRetryInterval time.Duration // 10 * time.Second
	maxRetryInterval     time.Duration // 1 * time.Minute
	maxRetryElapsedTime  time.Duration // 1 * time.Hour
}

func NewJobsForwarder(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, client *pulsar.Client, config *config.Config, backendConfig backendconfig.BackendConfig, log logger.Logger) (*JobsForwarder, error) {
	var forwarder JobsForwarder
	config.RegisterDurationConfigVariable(10, &forwarder.initialRetryInterval, true, time.Second, "JobsForwarder.initialRetryInterval")
	config.RegisterDurationConfigVariable(60, &forwarder.maxRetryInterval, true, time.Second, "JobsForwarder.maxRetryInterval")
	config.RegisterDurationConfigVariable(60, &forwarder.maxRetryElapsedTime, true, time.Minute, "JobsForwarder.maxRetryElapsedTime")

	forwarder.LoadMetaData(ctx, g, schemaDB, log, config)
	producer, err := client.NewProducer(pulsarType.ProducerOptions{
		Topic:              config.GetString("Pulsar.Producer.topic", ""),
		BatcherBuilderType: pulsarType.KeyBasedBatchBuilder,
	})
	if err != nil {
		return nil, err
	}
	forwarder.pulsarProducer = producer
	forwarder.transformer = schematransformer.New(ctx, g, backendConfig, config)
	forwarder.sampler = newSampler[string](config.GetDuration("JobsForwarder.samplingPeriod", 10, time.Minute), config.GetInt("JobsForwarder.sampleCacheSize", 10000))
	return &forwarder, nil
}

func (jf *JobsForwarder) Start() error {
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
				var mu sync.Mutex // protects statuses and retryJobs
				var statuses []*jobsdb.JobStatusT
				retryJobs := append([]*jobsdb.JobT{}, jobs...)

				removeJob := func(job *jobsdb.JobT) {
					_, idx, _ := lo.FindIndexOf(retryJobs, func(j *jobsdb.JobT) bool {
						return j.JobID == job.JobID
					})
					retryJobs = slices.Delete(retryJobs, idx, idx+1)
				}
				tryForwardJobs := func() error {
					toForward := append([]*jobsdb.JobT{}, retryJobs...)
					for _, job := range toForward {
						job := job // job can be used in a goroutine
						msg, orderKey, err := jf.transformer.Transform(job)
						if err != nil { // mark job as aborted and remove from retryJobs
							mu.Lock()
							removeJob(job)
							statuses = append(statuses, &jobsdb.JobStatusT{
								JobID:         job.JobID,
								AttemptNum:    job.LastJobStatus.AttemptNum + 1,
								JobState:      jobsdb.Aborted.State,
								ExecTime:      time.Now(),
								RetryTime:     time.Now(),
								ErrorCode:     "400",
								Parameters:    []byte{},
								ErrorResponse: json.RawMessage(fmt.Sprintf(`{"transform_error": %q}`, err.Error())),
							})
							mu.Unlock()
							continue
						}
						if !jf.sampler.Sample(msg.Key.String()) {
							msg.Sample = nil
						}
						jf.pulsarProducer.SendMessageAsync(jf.ctx, orderKey, orderKey, msg.MustMarshal(),
							func(_ pulsarType.MessageID, _ *pulsarType.ProducerMessage, err error) {
								if err == nil { // mark job as succeeded and remove from retryJobs
									mu.Lock()
									defer mu.Unlock()
									removeJob(job)
									statuses = append(statuses, &jobsdb.JobStatusT{
										JobID:      job.JobID,
										AttemptNum: job.LastJobStatus.AttemptNum + 1,
										JobState:   jobsdb.Succeeded.State,
										ExecTime:   time.Now(),
									})
								} else {
									jf.log.Errorf("failed to forward job %s: %v", job.JobID, err)
								}
							})
					}
					err = jf.pulsarProducer.Flush()
					if err != nil {
						return err
					}
					if len(retryJobs) > 0 {
						return fmt.Errorf("failed to forward %d jobs", len(retryJobs))
					}
					return nil
				}

				expB := backoff.NewExponentialBackOff()
				expB.InitialInterval = jf.initialRetryInterval
				expB.MaxInterval = jf.maxRetryInterval
				expB.MaxElapsedTime = jf.maxRetryElapsedTime
				if err = backoff.Retry(tryForwardJobs, backoff.WithContext(expB, jf.ctx)); err != nil {
					// mark all to retry jobs as aborted
					for _, job := range retryJobs {
						statuses = append(statuses, &jobsdb.JobStatusT{
							JobID:         job.JobID,
							AttemptNum:    job.LastJobStatus.AttemptNum + 1,
							JobState:      jobsdb.Aborted.State,
							ExecTime:      time.Now(),
							RetryTime:     time.Now(),
							ErrorCode:     "400",
							Parameters:    []byte{},
							ErrorResponse: json.RawMessage(fmt.Sprintf(`{"error": %q}`, err.Error())),
						})
					}
				}
				err = jf.MarkJobStatuses(jf.ctx, statuses)
				if err != nil {
					return err
				}
				sleepTime = jf.GetSleepTime(limitReached)
			}
		}
	}))
	return nil
}

func (jf *JobsForwarder) Stop() {
	jf.transformer.Stop()
	jf.pulsarProducer.Close()
}
