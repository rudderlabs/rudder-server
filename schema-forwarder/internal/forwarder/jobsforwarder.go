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
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/transformer"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
)

// JobsForwarder is a forwarder that transforms and forwards jobs to a pulsar topic
type JobsForwarder struct {
	BaseForwarder

	transformer    transformer.Transformer // transformer used to transform jobs to destination schema
	sampler        *sampler[string]        // sampler used to decide how often payloads should be sampled
	pulsarClient   *pulsar.Client          // pulsar client used to create producers
	pulsarProducer pulsar.ProducerAdapter  // pulsar producer used to forward jobs to pulsar

	topic                string        // topic to which jobs are forwarded
	maxSampleSize        int64         // max payload size for the samples to include in the schema messages
	initialRetryInterval time.Duration // initial retry interval for the backoff mechanism
	maxRetryInterval     time.Duration // max retry interval for the backoff mechanism
	maxRetryElapsedTime  time.Duration // max retry elapsed time for the backoff mechanism
}

// NewJobsForwarder returns a new, properly initialized, JobsForwarder
func NewJobsForwarder(terminalErrFn func(error), schemaDB jobsdb.JobsDB, client *pulsar.Client, config *config.Config, backendConfig backendconfig.BackendConfig, log logger.Logger, stat stats.Stats) *JobsForwarder {
	var forwarder JobsForwarder
	forwarder.LoadMetaData(terminalErrFn, schemaDB, log, config, stat)

	forwarder.transformer = transformer.New(backendConfig, config)
	forwarder.sampler = newSampler[string](config.GetDuration("SchemaForwarder.samplingPeriod", 10, time.Minute), config.GetInt("SchemaForwarder.sampleCacheSize", 10000))
	forwarder.pulsarClient = client

	forwarder.topic = config.GetString("SchemaForwarder.pulsarTopic", "event-schema")
	config.RegisterDurationConfigVariable(10, &forwarder.initialRetryInterval, true, time.Second, "SchemaForwarder.initialRetryInterval")
	config.RegisterDurationConfigVariable(60, &forwarder.maxRetryInterval, true, time.Second, "SchemaForwarder.maxRetryInterval")
	config.RegisterDurationConfigVariable(60, &forwarder.maxRetryElapsedTime, true, time.Minute, "SchemaForwarder.maxRetryElapsedTime")
	config.RegisterInt64ConfigVariable(10*bytesize.KB, &forwarder.maxSampleSize, true, 1, "SchemaForwarder.maxSampleSize")

	return &forwarder
}

// Start starts the forwarder which will start forwarding jobs from database to the appropriate pulsar topics
func (jf *JobsForwarder) Start() error {
	producer, err := jf.pulsarClient.NewProducer(pulsarType.ProducerOptions{
		Topic:              jf.topic,
		BatcherBuilderType: pulsarType.KeyBasedBatchBuilder,
	})
	if err != nil {
		return err
	}
	jf.pulsarProducer = producer

	jf.transformer.Start()

	ctx, cancel := context.WithCancel(context.Background())
	jf.cancel = cancel
	jf.g, ctx = errgroup.WithContext(ctx)

	jf.g.Go(misc.WithBugsnag(func() error {
		var sleepTime time.Duration
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(sleepTime):
				start := time.Now()
				jobs, limitReached, err := jf.GetJobs(ctx)
				if err != nil {
					if ctx.Err() != nil { // we are shutting down
						return nil //nolint:nilerr
					}
					jf.terminalErrFn(err) // we are signaling to shutdown the app
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

				// try to forward toRetry jobs to pulsar. Succeeded or invalid jobs are removed from toRetry
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
							errorResponse, _ := json.Marshal(map[string]string{"transform_error": err.Error()})
							statuses = append(statuses, &jobsdb.JobStatusT{
								JobID:         job.JobID,
								AttemptNum:    job.LastJobStatus.AttemptNum + 1,
								JobState:      jobsdb.Aborted.State,
								ExecTime:      time.Now(),
								RetryTime:     time.Now(),
								ErrorCode:     "400",
								JobParameters: job.Parameters,
								ErrorResponse: errorResponse,
							})
							jf.stat.NewTaggedStat("schema_forwarder_jobs", stats.CountType, stats.Tags{"state": "invalid"}).Increment()
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
										JobID:         job.JobID,
										AttemptNum:    job.LastJobStatus.AttemptNum + 1,
										JobState:      jobsdb.Succeeded.State,
										ExecTime:      time.Now(),
										JobParameters: job.Parameters,
									})
									jf.stat.NewTaggedStat("schema_forwarder_processed_jobs", stats.CountType, stats.Tags{"state": "succeeded"}).Increment()
								} else {
									jf.stat.NewTaggedStat("schema_forwarder_processed_jobs", stats.CountType, stats.Tags{"state": "failed"}).Increment()
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

				// Retry to forward the jobs batch to pulsar until there are no more jobs to retry or until maxRetryElapsedTime is reached
				expB := backoff.NewExponentialBackOff()
				expB.InitialInterval = jf.initialRetryInterval
				expB.MaxInterval = jf.maxRetryInterval
				expB.MaxElapsedTime = jf.maxRetryElapsedTime
				if err = backoff.Retry(tryForwardJobs, backoff.WithContext(expB, ctx)); err != nil {
					for _, job := range toRetry { // mark all to retry jobs as aborted
						errorResponse, _ := json.Marshal(map[string]string{"error": err.Error()})
						statuses = append(statuses, &jobsdb.JobStatusT{
							JobID:         job.JobID,
							AttemptNum:    job.LastJobStatus.AttemptNum + 1,
							JobState:      jobsdb.Aborted.State,
							ExecTime:      time.Now(),
							RetryTime:     time.Now(),
							ErrorCode:     "400",
							JobParameters: job.Parameters,
							ErrorResponse: errorResponse,
						})
					}
					jf.stat.NewTaggedStat("schema_forwarder_processed_jobs", stats.CountType, stats.Tags{"state": "abort"}).Count(len(toRetry))
				}
				if err := jf.MarkJobStatuses(ctx, statuses); err != nil {
					if ctx.Err() != nil { // we are shutting down
						return nil //nolint:nilerr
					}
					jf.terminalErrFn(err) // we are signaling to shutdown the app
					return fmt.Errorf("failed to mark schema job statuses in forwarder: %w", err)
				}
				jf.stat.NewStat("schema_forwarder_loop", stats.TimerType).Since(start)
				sleepTime = jf.GetSleepTime(limitReached)
			}
		}
	}))
	return nil
}

// Stop stops the forwarder
func (jf *JobsForwarder) Stop() {
	jf.cancel()
	_ = jf.g.Wait()
	jf.transformer.Stop()
	jf.pulsarProducer.Close()
}
