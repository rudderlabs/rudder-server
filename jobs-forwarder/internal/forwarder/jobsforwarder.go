package forwarder

import (
	"bytes"
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
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
)

type JobsForwarder struct {
	BaseForwarder
	transformer    schematransformer.Transformer // transformer to transform jobs to destination schema
	sampler        *sampler[string]              // sampler to decide when payloads should be sampled
	pulsarProducer pulsar.ProducerAdapter

	maxSampleSize        int64         // max sample size for the payload
	initialRetryInterval time.Duration // 10 * time.Second
	maxRetryInterval     time.Duration // 1 * time.Minute
	maxRetryElapsedTime  time.Duration // 1 * time.Hour
}

func NewJobsForwarder(terminalErrFn func(error), schemaDB jobsdb.JobsDB, client *pulsar.Client, config *config.Config, backendConfig backendconfig.BackendConfig, log logger.Logger) (*JobsForwarder, error) {
	producer, err := client.NewProducer(pulsarType.ProducerOptions{
		Topic:              config.GetString("JobsForwarder.pulsarTopic", "event-schema"),
		BatcherBuilderType: pulsarType.KeyBasedBatchBuilder,
	})
	if err != nil {
		return nil, err
	}
	var forwarder JobsForwarder
	forwarder.LoadMetaData(terminalErrFn, schemaDB, log, config)
	config.RegisterDurationConfigVariable(10, &forwarder.initialRetryInterval, true, time.Second, "JobsForwarder.initialRetryInterval")
	config.RegisterDurationConfigVariable(60, &forwarder.maxRetryInterval, true, time.Second, "JobsForwarder.maxRetryInterval")
	config.RegisterDurationConfigVariable(60, &forwarder.maxRetryElapsedTime, true, time.Minute, "JobsForwarder.maxRetryElapsedTime")
	config.RegisterInt64ConfigVariable(10*bytesize.KB, &forwarder.maxSampleSize, true, 1, "JobsForwarder.maxSampleSize")

	forwarder.pulsarProducer = producer
	forwarder.transformer = schematransformer.New(backendConfig, config)
	forwarder.sampler = newSampler[string](config.GetDuration("JobsForwarder.samplingPeriod", 10, time.Minute), config.GetInt("JobsForwarder.sampleCacheSize", 10000))
	return &forwarder, nil
}

func (jf *JobsForwarder) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	jf.cancel = cancel
	jf.g, ctx = errgroup.WithContext(ctx)

	var sleepTime time.Duration
	jf.transformer.Start()
	jf.g.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(sleepTime):
				jobs, limitReached, err := jf.GetJobs(ctx)
				if err != nil {
					if ctx.Err() != nil { // we are shutting down
						return nil //nolint:nilerr
					}
					jf.terminalErrFn(err)
					return err
				}
				var mu sync.Mutex // protects statuses and retryJobs
				var statuses []*jobsdb.JobStatusT
				toRetry := append([]*jobsdb.JobT{}, jobs...)

				jobDone := func(job *jobsdb.JobT) {
					_, idx, _ := lo.FindIndexOf(toRetry, func(j *jobsdb.JobT) bool {
						return j.JobID == job.JobID
					})
					toRetry = slices.Delete(toRetry, idx, idx+1)
				}

				tryForwardJobs := func() error {
					if ctx.Err() != nil { // we are shutting down
						return nil //nolint:nilerr
					}
					toForward := append([]*jobsdb.JobT{}, toRetry...)
					for _, job := range toForward {
						job := job // job can be used in a goroutine
						msg, orderKey, err := jf.transformer.Transform(job)
						if err != nil { // mark job as aborted and remove from toRetry
							mu.Lock()
							jobDone(job)
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
						if !bytes.Equal(msg.Sample, []byte("{}")) && // if the sample is not an empty json object (redacted) and
							(len(msg.Sample) > int(jf.maxSampleSize) || // sample is too big or
								!jf.sampler.Sample(msg.Key.String())) { // sample should be skipped
							msg.Sample = nil // by setting to sample to nil we are instructing the schema worker to keep the previous sample
						}
						jf.pulsarProducer.SendMessageAsync(ctx, orderKey, orderKey, msg.MustMarshal(),
							func(_ pulsarType.MessageID, _ *pulsarType.ProducerMessage, err error) {
								if err == nil { // mark job as succeeded and remove from toRetry
									mu.Lock()
									defer mu.Unlock()
									jobDone(job)
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
					if err := jf.pulsarProducer.Flush(); err != nil {
						return fmt.Errorf("failed to flush pulsar producer: %w", err)
					}
					if len(toRetry) > 0 {
						return fmt.Errorf("failed to forward %d jobs", len(toRetry))
					}
					return nil
				}

				expB := backoff.NewExponentialBackOff()
				expB.InitialInterval = jf.initialRetryInterval
				expB.MaxInterval = jf.maxRetryInterval
				expB.MaxElapsedTime = jf.maxRetryElapsedTime
				if err = backoff.Retry(tryForwardJobs, backoff.WithContext(expB, ctx)); err != nil {
					// mark all to retry jobs as aborted
					for _, job := range toRetry {
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
				if err := jf.MarkJobStatuses(ctx, statuses); err != nil {
					if ctx.Err() != nil { // we are shutting down
						return nil //nolint:nilerr
					}
					jf.terminalErrFn(err)
					return fmt.Errorf("failed to mark schema job statuses in forwarder: %w", err)
				}
				sleepTime = jf.GetSleepTime(limitReached)
			}
		}
	}))
	return nil
}

func (jf *JobsForwarder) Stop() {
	jf.cancel()
	_ = jf.g.Wait()
	jf.transformer.Stop()
	jf.pulsarProducer.Close()
}
