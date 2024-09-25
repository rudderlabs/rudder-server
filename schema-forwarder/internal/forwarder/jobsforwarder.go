package forwarder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	pulsarType "github.com/apache/pulsar-client-go/pulsar"
	"github.com/cenkalti/backoff/v4"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/batcher"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/transformer"
	"github.com/rudderlabs/rudder-server/utils/crash"
)

// JobsForwarder is a forwarder that transforms and forwards jobs to a pulsar topic
type JobsForwarder struct {
	BaseForwarder

	transformer    transformer.Transformer // transformer used to transform jobs to destination schema
	sampler        *sampler[string]        // sampler used to decide how often payloads should be sampled
	pulsarClient   *pulsar.Client          // pulsar client used to create producers
	pulsarProducer pulsar.ProducerAdapter  // pulsar producer used to forward jobs to pulsar

	topic                string                            // topic to which jobs are forwarded
	maxSampleSize        config.ValueLoader[int64]         // max payload size for the samples to include in the schema messages
	initialRetryInterval config.ValueLoader[time.Duration] // initial retry interval for the backoff mechanism
	maxRetryInterval     config.ValueLoader[time.Duration] // max retry interval for the backoff mechanism
	maxRetryElapsedTime  config.ValueLoader[time.Duration] // max retry elapsed time for the backoff mechanism
}

// NewJobsForwarder returns a new, properly initialized, JobsForwarder
func NewJobsForwarder(terminalErrFn func(error), schemaDB jobsdb.JobsDB, client *pulsar.Client, config *config.Config, backendConfig backendconfig.BackendConfig, log logger.Logger, stat stats.Stats) *JobsForwarder {
	var forwarder JobsForwarder
	forwarder.LoadMetaData(terminalErrFn, schemaDB, log, config, stat)

	forwarder.transformer = transformer.New(backendConfig, config)
	forwarder.sampler = newSampler[string](config.GetDuration("SchemaForwarder.samplingPeriod", 10, time.Minute), config.GetInt("SchemaForwarder.sampleCacheSize", 10000))
	forwarder.pulsarClient = client

	forwarder.topic = config.GetString("SchemaForwarder.pulsarTopic", "event-schema")
	forwarder.setupReloadableVars()

	return &forwarder
}

func (jf *JobsForwarder) setupReloadableVars() {
	jf.initialRetryInterval = config.GetReloadableDurationVar(10, time.Second, "SchemaForwarder.initialRetryInterval")
	jf.maxRetryInterval = config.GetReloadableDurationVar(60, time.Second, "SchemaForwarder.maxRetryInterval")
	jf.maxRetryElapsedTime = config.GetReloadableDurationVar(60, time.Minute, "SchemaForwarder.maxRetryElapsedTime")
	jf.maxSampleSize = config.GetReloadableInt64Var(10*bytesize.KB, 1, "SchemaForwarder.maxSampleSize")
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

	jf.g.Go(crash.Wrapper(func() error {
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
					jf.terminalErrFn(err) // we are signaling to shut down the app
					return err
				}
				var mu sync.Mutex // protects statuses and toRetry
				var statuses []*jobsdb.JobStatusT
				schemaBatcher := batcher.NewEventSchemaMessageBatcher(jf.transformer)
				for _, job := range jobs {
					if err := schemaBatcher.Add(job); err != nil { // mark job as aborted
						errorResponse, _ := json.Marshal(map[string]string{"transform_error": err.Error()})
						statuses = append(statuses, &jobsdb.JobStatusT{
							JobID:         job.JobID,
							AttemptNum:    job.LastJobStatus.AttemptNum + 1,
							JobState:      jobsdb.Aborted.State,
							ExecTime:      time.Now(),
							RetryTime:     time.Now(),
							ErrorCode:     "400",
							Parameters:    []byte(`{}`),
							JobParameters: job.Parameters,
							ErrorResponse: errorResponse,
						})
						jf.stat.NewTaggedStat("schema_forwarder_jobs", stats.CountType, stats.Tags{"state": "invalid"}).Increment()
						continue
					}
				}
				messageBatches := schemaBatcher.GetMessageBatches()
				for _, messageBatch := range messageBatches {
					if !bytes.Equal(messageBatch.Message.Sample, []byte("{}")) && // if the sample is not an empty json object (redacted) and
						(len(messageBatch.Message.Sample) > int(jf.maxSampleSize.Load()) || // sample is too big or
							!jf.sampler.Sample(messageBatch.Message.Key.String()+messageBatch.Message.Hash)) { // sample should be skipped
						messageBatch.Message.Sample = nil // by setting to sample to nil we are instructing the schema worker to keep the previous sample
					}
				}
				jf.stat.NewStat("schema_forwarder_batch_size", stats.CountType).Count(len(messageBatches))

				// try to forward messageBatches to pulsar. Succeeded jobs are removed from messageBatches
				tryForward := func() error {
					if ctx.Err() != nil { // we are shutting down
						return nil //nolint:nilerr
					}
					toForward := append([]*batcher.EventSchemaMessageBatch{}, messageBatches...)
					for _, batch := range toForward {
						batch := batch // can be used in a goroutine
						msg := batch.Message
						orderKey := msg.Key.WriteKey
						jf.pulsarProducer.SendMessageAsync(ctx, orderKey, orderKey, msg.MustMarshal(),
							func(_ pulsarType.MessageID, _ *pulsarType.ProducerMessage, err error) {
								if err == nil { // mark job as succeeded and remove from toRetry
									mu.Lock()
									defer mu.Unlock()

									_, idx, _ := lo.FindIndexOf(messageBatches, func(item *batcher.EventSchemaMessageBatch) bool {
										return item.Index == batch.Index
									})
									messageBatches = slices.Delete(messageBatches, idx, idx+1)
									for _, job := range batch.Jobs {
										statuses = append(statuses, &jobsdb.JobStatusT{
											JobID:         job.JobID,
											AttemptNum:    job.LastJobStatus.AttemptNum + 1,
											JobState:      jobsdb.Succeeded.State,
											ExecTime:      time.Now(),
											Parameters:    []byte(`{}`),
											ErrorResponse: []byte(`{}`),
											JobParameters: job.Parameters,
										})
									}

									jf.stat.NewTaggedStat("schema_forwarder_processed_jobs", stats.CountType, stats.Tags{"state": "succeeded"}).Count(len(batch.Jobs))
								} else {
									jf.stat.NewTaggedStat("schema_forwarder_processed_jobs", stats.CountType, stats.Tags{"state": "failed"}).Count(len(batch.Jobs))
									jf.log.Errorf("failed to forward %d jobs : %v", len(batch.Jobs), err)
								}
							})
					}
					if err := jf.pulsarProducer.Flush(); err != nil {
						return fmt.Errorf("failed to flush pulsar producer: %w", err)
					}
					if len(messageBatches) > 0 {
						return fmt.Errorf("failed to forward %d jobs", len(messageBatches))
					}
					return nil
				}

				// Retry to forward the batches to pulsar until there are no more messageBatches to retry or until maxRetryElapsedTime is reached
				expB := backoff.NewExponentialBackOff()
				expB.InitialInterval = jf.initialRetryInterval.Load()
				expB.MaxInterval = jf.maxRetryInterval.Load()
				expB.MaxElapsedTime = jf.maxRetryElapsedTime.Load()
				if err = backoff.Retry(tryForward, backoff.WithContext(expB, ctx)); err != nil {
					errorResponse, _ := json.Marshal(map[string]string{"error": err.Error()})
					var abortedCount int
					for _, schemaBatch := range messageBatches { // mark all messageBatches left over as aborted
						for _, job := range schemaBatch.Jobs {
							statuses = append(statuses, &jobsdb.JobStatusT{
								JobID:         job.JobID,
								AttemptNum:    job.LastJobStatus.AttemptNum + 1,
								JobState:      jobsdb.Aborted.State,
								ExecTime:      time.Now(),
								RetryTime:     time.Now(),
								ErrorCode:     "400",
								Parameters:    []byte(`{}`),
								JobParameters: job.Parameters,
								ErrorResponse: errorResponse,
							})
							abortedCount++
						}
					}
					jf.stat.NewTaggedStat("schema_forwarder_jobs", stats.CountType, stats.Tags{"state": "abort"}).Count(abortedCount)
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
