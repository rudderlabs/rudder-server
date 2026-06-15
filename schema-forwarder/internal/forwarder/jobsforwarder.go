package forwarder

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	pulsarType "github.com/apache/pulsar-client-go/pulsar"
	"github.com/cenkalti/backoff/v5"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/batcher"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/transformer"
	"github.com/rudderlabs/rudder-server/utils/backoffvoid"
	"github.com/rudderlabs/rudder-server/utils/crash"
)

// JobsForwarder is a forwarder that transforms and forwards jobs to a pulsar topic
type JobsForwarder struct {
	BaseForwarder

	transformer    transformer.Transformer // transformer used to transform jobs to destination schema
	sampler        *sampler[string]        // sampler used to decide how often payloads should be sampled
	pulsarClient   *pulsar.Client          // pulsar client used to create producers
	pulsarProducer pulsar.ProducerAdapter  // pulsar producer used to forward jobs to pulsar

	topic                string                              // topic to which jobs are forwarded
	maxSampleSize        config.ValueLoader[int64]           // max payload size for the samples to include in the schema messages
	initialRetryInterval config.ValueLoader[time.Duration]   // initial retry interval for the backoff mechanism
	maxRetryInterval     config.ValueLoader[time.Duration]   // max retry interval for the backoff mechanism
	maxRetryElapsedTime  config.ValueLoader[time.Duration]   // max retry elapsed time for the backoff mechanism
	conf                 *config.Config                      // config used to resolve hierarchical settings at runtime
	fullOrderKeyCache    map[string]*config.Reloadable[bool] // sourceID -> reloadable "full order key" setting, resolved hierarchically; only accessed by the single forwarder goroutine
}

// NewJobsForwarder returns a new, properly initialized, JobsForwarder
func NewJobsForwarder(terminalErrFn func(error), schemaDB jobsdb.JobsDB, client *pulsar.Client, config *config.Config, backendConfig backendconfig.BackendConfig, log logger.Logger, stat stats.Stats) *JobsForwarder {
	var forwarder JobsForwarder
	forwarder.LoadMetaData(terminalErrFn, schemaDB, log, config, stat)

	forwarder.conf = config
	forwarder.transformer = transformer.New(backendConfig, config)
	forwarder.sampler = newSampler[string](config.GetDurationVar(10, time.Minute, "SchemaForwarder.samplingPeriod"), config.GetIntVar(10000, 1, "SchemaForwarder.sampleCacheSize"))
	forwarder.pulsarClient = client

	forwarder.topic = config.GetStringVar("event-schema", "SchemaForwarder.pulsarTopic")
	forwarder.setupReloadableVars()

	return &forwarder
}

func (jf *JobsForwarder) setupReloadableVars() {
	jf.initialRetryInterval = config.GetReloadableDurationVar(10, time.Second, "SchemaForwarder.initialRetryInterval")
	jf.maxRetryInterval = config.GetReloadableDurationVar(60, time.Second, "SchemaForwarder.maxRetryInterval")
	jf.maxRetryElapsedTime = config.GetReloadableDurationVar(60, time.Minute, "SchemaForwarder.maxRetryElapsedTime")
	jf.maxSampleSize = config.GetReloadableInt64Var(10*bytesize.KB, 1, "SchemaForwarder.maxSampleSize")
	jf.fullOrderKeyCache = make(map[string]*config.Reloadable[bool])
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
						errorResponse, _ := jsonrs.Marshal(map[string]string{"transform_error": err.Error()})
						statuses = append(statuses, &jobsdb.JobStatusT{
							JobID:         job.JobID,
							JobState:      jobsdb.Aborted.State,
							AttemptNum:    job.LastJobStatus.AttemptNum + 1,
							ExecTime:      time.Now(),
							RetryTime:     time.Now(),
							ErrorCode:     "400",
							ErrorResponse: errorResponse,
							Parameters:    []byte(`{}`),
							JobParameters: job.Parameters,
							WorkspaceId:   job.WorkspaceId,
							PartitionID:   job.PartitionID,
							CustomVal:     job.CustomVal,
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
						msg := batch.Message
						orderKey := jf.orderKey(batch)
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
											JobState:      jobsdb.Succeeded.State,
											AttemptNum:    job.LastJobStatus.AttemptNum + 1,
											ExecTime:      time.Now(),
											RetryTime:     time.Now(),
											ErrorCode:     "200",
											ErrorResponse: []byte(`{}`),
											Parameters:    []byte(`{}`),
											JobParameters: job.Parameters,
											WorkspaceId:   job.WorkspaceId,
											PartitionID:   job.PartitionID,
											CustomVal:     job.CustomVal,
										})
									}

									jf.stat.NewTaggedStat("schema_forwarder_processed_jobs", stats.CountType, stats.Tags{"state": "succeeded"}).Count(len(batch.Jobs))
								} else {
									jf.stat.NewTaggedStat("schema_forwarder_processed_jobs", stats.CountType, stats.Tags{"state": "failed"}).Count(len(batch.Jobs))
									jf.log.Errorn("failed to forward jobs",
										logger.NewIntField("noOfJobs", int64(len(batch.Jobs))),
										obskit.Error(err))
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
				bo := backoff.NewExponentialBackOff()
				bo.InitialInterval = jf.initialRetryInterval.Load()
				bo.MaxInterval = jf.maxRetryInterval.Load()
				if err = backoffvoid.Retry(ctx,
					tryForward,
					backoff.WithBackOff(bo),
					backoff.WithMaxElapsedTime(jf.maxRetryElapsedTime.Load()),
				); err != nil {
					errorResponse, _ := jsonrs.Marshal(map[string]string{"error": err.Error()})
					var abortedCount int
					for _, schemaBatch := range messageBatches { // mark all messageBatches left over as aborted
						for _, job := range schemaBatch.Jobs {
							statuses = append(statuses, &jobsdb.JobStatusT{
								JobID:         job.JobID,
								JobState:      jobsdb.Aborted.State,
								AttemptNum:    job.LastJobStatus.AttemptNum + 1,
								ExecTime:      time.Now(),
								RetryTime:     time.Now(),
								ErrorCode:     "400",
								ErrorResponse: errorResponse,
								Parameters:    []byte(`{}`),
								JobParameters: job.Parameters,
								WorkspaceId:   job.WorkspaceId,
								PartitionID:   job.PartitionID,
								CustomVal:     job.CustomVal,
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

// orderKey builds the pulsar ordering/partition key for a batch.
//
// By default the key is the source's write key, which guarantees ordering and
// co-location of all events belonging to the same source. When the full order
// key is enabled (hierarchically, per source or globally) the event type and
// event identifier are appended to the key, trading a coarser ordering
// guarantee for finer-grained partitioning and higher downstream parallelism.
func (jf *JobsForwarder) orderKey(batch *batcher.EventSchemaMessageBatch) string {
	msg := batch.Message
	if !jf.fullOrderKey(batch.SourceID).Load() {
		return msg.Key.WriteKey
	}
	return strings.Join([]string{msg.Key.WriteKey, msg.Key.EventType, msg.Key.EventIdentifier}, ":")
}

// fullOrderKey returns the reloadable setting controlling whether the event type
// and identifier should be included in the ordering key for the given source.
// The setting is resolved hierarchically, the first key that is set wins, and
// the resulting reloadable is cached per source id:
//
//	SchemaForwarder.<source-id>.fullOrderKey
//	SchemaForwarder.fullOrderKey
func (jf *JobsForwarder) fullOrderKey(sourceID string) *config.Reloadable[bool] {
	if setting, ok := jf.fullOrderKeyCache[sourceID]; ok {
		return setting
	}
	setting := jf.conf.GetReloadableBoolVar(false,
		"SchemaForwarder."+sourceID+".fullOrderKey",
		"SchemaForwarder.fullOrderKey",
	)
	jf.fullOrderKeyCache[sourceID] = setting
	return setting
}
