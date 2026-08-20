package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

const rsourcesPublishTime = "rsources_publish_time_second"

// StatsPublisher publishes stats
type StatsPublisher interface {
	// Publish publishes statistics
	Publish(ctx context.Context, tx *sql.Tx) error
}

// StatsCollector collects and publishes stats as jobs are
// being created, processed and their statuses are being updated.
type StatsCollector interface {
	StatsPublisher
	// JobsStored captures incoming job statistics
	JobsStored(jobs []*jobsdb.JobT)

	// JobsStoredWithErrors captures incoming job statistics
	JobsStoredWithErrors(jobs []*jobsdb.JobT, failedJobs map[uuid.UUID]string)

	// JobsForked captures the fan-out of an already-tracked job into an intermediate
	// stage that will later be drained once per consumer (e.g. proc pool fan-out): In
	// is incremented by len(job.Consumers) per job (or 1 if Consumers is empty), so a
	// source is not reported complete while forked work is still pending downstream.
	// Unlike JobsStored, it does not index jobs for a later CollectStats call — the
	// forked job's own JobID isn't known yet at fork time, and its terminal status is
	// tracked by a separate collector when it's eventually drained.
	JobsForked(jobs []*jobsdb.JobT)

	// BeginProcessing prepares the necessary indices in order to
	// be ready for capturing JobStatus statistics
	BeginProcessing(jobs []*jobsdb.JobT)

	// CollectStats captures outgoing job statistics.
	// A call to BeginProcessing must precede a call to this method,
	// so that all necessary indices can be created, since a JobStatus
	// doesn't carry all necessary job metadata such as jobRunId, taskRunId, etc.
	CollectStats(jobStatuses []*jobsdb.JobStatusT)

	// CollectFailedRecords captured `recordId`s for the jobs that were aborted.
	// A call to BeginProcessing must precede a call to this method,
	// so that all necessary indices can be created, since a JobStatus
	// doesn't carry all necessary job metadata such as jobRunId, taskRunId, etc.
	CollectFailedRecords(jobStatuses []*jobsdb.JobStatusT)
}

// FailedJobsStatsCollector collects stats for failed jobs
type FailedJobsStatsCollector interface {
	StatsPublisher
	JobsDropped(jobs []*jobsdb.JobT)
}

// NewStatsCollector creates a new stats collector
func NewStatsCollector(jobservice JobService, component string, statFactory stats.Stats, opts ...OptFunc) StatsCollector {
	return newStatsCollector(jobservice, component, statFactory, opts...)
}

// NewDroppedJobsCollector creates a new stats collector for publishing failed job stats and records
func NewDroppedJobsCollector(jobservice JobService, component string, statFactory stats.Stats, opts ...OptFunc) FailedJobsStatsCollector {
	return newStatsCollector(jobservice, component, statFactory, opts...)
}

func newStatsCollector(jobservice JobService, component string, statFactory stats.Stats, opts ...OptFunc) *statsCollector {
	sc := &statsCollector{
		jobService:            jobservice,
		jobIdsToStatKeyIndex:  map[int64]statKey{},
		jobIdsToRecordIdIndex: map[int64]json.RawMessage{},
		statsIndex:            map[statKey]*Stats{},
		failedRecordsIndex:    map[statKey][]FailedRecord{},
		parametersParser:      defaultParametersParser,
		errorCapture:          sharedErrorCaptureSettings(),
		statFactory:           statFactory,
	}
	for _, opt := range opts {
		opt(sc)
	}
	sc.stats.publishTime = statFactory.NewTaggedStat(rsourcesPublishTime, stats.TimerType, stats.Tags{"module": component})
	return sc
}

type statKey struct {
	jobRunId string
	JobTargetKey
}

func (sk statKey) String() string {
	return strings.Join([]string{sk.jobRunId, sk.TaskRunID, sk.SourceID, sk.DestinationID}, `#`)
}

var _ StatsCollector = (*statsCollector)(nil)

type statsCollector struct {
	processing            bool
	jobService            JobService
	jobIdsToStatKeyIndex  map[int64]statKey
	jobIdsToRecordIdIndex map[int64]json.RawMessage
	statsIndex            map[statKey]*Stats
	failedRecordsIndex    map[statKey][]FailedRecord
	parametersParser      parametersParser
	// captureErrorJobIds holds the job ids whose connection opted in to error
	// capture. Lazily allocated: nil while the feature is unused.
	captureErrorJobIds map[int64]struct{}
	errorCapture       errorCaptureSettings
	// errorCaptureIndex accumulates the per-connection capture outcome, reported
	// once per publish. Lazily allocated.
	errorCaptureIndex map[statKey]*errorCaptureCounters
	statFactory       stats.Stats
	stats             struct {
		publishTime stats.Timer
	}
}

func (r *statsCollector) orderedStatMapKeys() []statKey {
	keys := make([]statKey, 0, len(r.statsIndex))
	for k := range r.statsIndex {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})
	return keys
}

func (r *statsCollector) orderedFailedRecordsKeys() []statKey {
	keys := make([]statKey, 0, len(r.failedRecordsIndex))
	for k := range r.failedRecordsIndex {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})
	return keys
}

func (r *statsCollector) JobsStored(jobs []*jobsdb.JobT) {
	r.buildStats(jobs, nil, true)
}

func (r *statsCollector) JobsDropped(jobs []*jobsdb.JobT) {
	r.processing = true
	r.buildStats(jobs, nil, true)
	jobStatuses := make([]*jobsdb.JobStatusT, 0, len(jobs))
	for i := range jobs {
		jobStatuses = append(jobStatuses, &jobsdb.JobStatusT{
			JobID:    jobs[i].JobID,
			JobState: jobsdb.Aborted.State,
		})
	}
	r.CollectStats(jobStatuses)
}

func (r *statsCollector) JobsStoredWithErrors(jobs []*jobsdb.JobT, failedJobs map[uuid.UUID]string) {
	r.buildStats(jobs, failedJobs, true)
}

func (r *statsCollector) JobsForked(jobs []*jobsdb.JobT) {
	for i := range jobs {
		job := jobs[i]
		p := r.parametersParser(job.Parameters)
		if p.jobRunID == "" {
			continue
		}
		sk := statKey{
			jobRunId:     p.jobRunID,
			JobTargetKey: p.target,
		}
		stats, ok := r.statsIndex[sk]
		if !ok {
			stats = &Stats{}
			r.statsIndex[sk] = stats
		}
		consumers := uint(len(job.Consumers))
		if consumers == 0 {
			consumers = 1
		}
		stats.In += consumers
	}
}

func (r *statsCollector) BeginProcessing(jobs []*jobsdb.JobT) {
	r.buildStats(jobs, nil, false)
	r.processing = true
}

func (r *statsCollector) CollectStats(jobStatuses []*jobsdb.JobStatusT) {
	if !r.processing {
		panic(fmt.Errorf("cannot update job statuses without having previously called BeginProcessing"))
	}
	if len(r.jobIdsToStatKeyIndex) == 0 {
		return
	}
	for i := range jobStatuses {
		jobStatus := jobStatuses[i]
		if statKey, statKeyOk := r.jobIdsToStatKeyIndex[jobStatus.JobID]; statKeyOk {
			stats, ok := r.statsIndex[statKey]
			if ok {
				switch jobStatus.JobState {
				// Filtered state is being considered as a success. If we want to report them separately, we can add a new field in stats
				case jobsdb.Succeeded.State, jobsdb.Filtered.State:
					stats.Out++
				case jobsdb.Aborted.State:
					stats.Failed++
				}
			}
		}
	}
}

func (r *statsCollector) CollectFailedRecords(jobStatuses []*jobsdb.JobStatusT) {
	if !r.processing {
		panic(fmt.Errorf("cannot update job statuses without having previously called BeginProcessing"))
	}

	if len(r.jobIdsToRecordIdIndex) == 0 || len(r.jobIdsToStatKeyIndex) == 0 {
		return
	}
	// One snapshot per batch: every record is judged by the same settings, and a
	// blacklist edit takes effect on the next batch without a restart.
	gate := r.errorCapture.snapshot()
	for i := range jobStatuses {
		jobStatus := jobStatuses[i]
		statKey, statKeyOk := r.jobIdsToStatKeyIndex[jobStatus.JobID]
		if !statKeyOk {
			continue
		}
		recordId, recordIdOK := r.jobIdsToRecordIdIndex[jobStatus.JobID]
		if !recordIdOK || len(recordId) == 0 {
			continue
		}
		if jobStatus.JobState != jobsdb.Aborted.State {
			continue
		}
		code, _ := strconv.Atoi(jobStatus.ErrorCode)
		r.failedRecordsIndex[statKey] = append(r.failedRecordsIndex[statKey], FailedRecord{
			Record:        recordId,
			Code:          code,
			ErrorResponse: r.captureErrorResponse(gate, statKey, jobStatus),
		})
	}
}

func (r *statsCollector) Publish(ctx context.Context, tx *sql.Tx) error {
	if r.jobService == nil {
		return fmt.Errorf("no JobService provided during initialization")
	}
	startTime := time.Now()

	// sort the maps to avoid deadlocks
	statKeys := r.orderedStatMapKeys()
	for i := range statKeys {
		k := statKeys[i]
		v := r.statsIndex[k]
		if v.Failed+v.In+v.Out == 0 {
			continue
		}
		err := r.jobService.IncrementStats(ctx, tx, k.jobRunId, k.JobTargetKey, *v)
		if err != nil {
			return fmt.Errorf("failed to increment stats for job %s, idx %d,: %w", k.String(), i, err)
		}
	}
	failedRecordsKeys := r.orderedFailedRecordsKeys()
	for i := range failedRecordsKeys {
		k := failedRecordsKeys[i]
		v := r.failedRecordsIndex[k]
		// sort the records as well to avoid deadlocks
		sort.Slice(v, func(i, j int) bool {
			return string(v[i].Record) < string(v[j].Record)
		})
		err := r.jobService.AddFailedRecords(ctx, tx, k.jobRunId, k.JobTargetKey, v)
		if err != nil {
			return fmt.Errorf("failed to add failed records for job %s: %w", k.String(), err)
		}
		r.reportErrorCapture(k)
	}
	r.stats.publishTime.SendTiming(time.Since(startTime))

	return nil
}

func (r *statsCollector) buildStats(jobs []*jobsdb.JobT, failedJobs map[uuid.UUID]string, incrementIn bool) { // skipcq: RVV-A0005
	for i := range jobs {
		job := jobs[i]
		if _, ok := failedJobs[job.UUID]; ok {
			continue
		}
		p := r.parametersParser(job.Parameters)
		if p.jobRunID != "" {
			sk := statKey{
				jobRunId:     p.jobRunID,
				JobTargetKey: p.target,
			}
			var stats *Stats
			stats, ok := r.statsIndex[sk]
			if !ok {
				stats = &Stats{}
				r.statsIndex[sk] = stats
			}
			if incrementIn {
				stats.In++
			}
			r.jobIdsToStatKeyIndex[job.JobID] = sk
			if p.recordID != "" && p.recordID != "null" && p.recordID != `""` {
				recordIdJson := json.RawMessage(p.recordID)
				if json.Valid(recordIdJson) {
					r.jobIdsToRecordIdIndex[job.JobID] = recordIdJson
					if p.captureError {
						// Sparse on purpose: nothing is allocated while the
						// feature is off or the connection has not opted in.
						if r.captureErrorJobIds == nil {
							r.captureErrorJobIds = make(map[int64]struct{})
						}
						r.captureErrorJobIds[job.JobID] = struct{}{}
					}
				}
			}
		}
	}
}

// jobParameters is the subset of a job's parameters the collector needs. It is a
// struct rather than a tuple so that adding a parameter does not ripple through
// every parser implementation and call site.
type jobParameters struct {
	jobRunID string
	recordID string
	// captureError is the per-connection opt-in for capturing the final recorded
	// error text, stamped by rudder-sources on the event context and carried here
	// through the gateway and the processor.
	captureError bool
	target       JobTargetKey
}

type parametersParser func(jp json.RawMessage) jobParameters

type OptFunc func(*statsCollector)

// IgnoreDestinationID ignores the destinationID parameter of the job and while capturing statistics
func IgnoreDestinationID() OptFunc {
	return func(r *statsCollector) {
		r.parametersParser = func(jobParams json.RawMessage) jobParameters {
			p := defaultParametersParser(jobParams)
			p.target.DestinationID = ""
			return p
		}
	}
}

func defaultParametersParser(jobParams json.RawMessage) jobParameters {
	var p jobParameters
	// One decrement per key below; the ForEach stops as soon as all of them have
	// been seen. capture_error is marshalled with omitempty, so it is only present
	// on opted-in rETL jobs - exactly the traffic that benefits from the early exit.
	remaining := 6
	jp := gjson.ParseBytes(jobParams)
	jp.ForEach(func(key, value gjson.Result) bool {
		switch key.Str {
		case "source_job_run_id":
			p.jobRunID = value.Str
			remaining--
		case "source_task_run_id":
			p.target.TaskRunID = value.Str
			remaining--
		case "source_id":
			p.target.SourceID = value.Str
			remaining--
		case "destination_id":
			p.target.DestinationID = value.Str
			remaining--
		case "record_id":
			p.recordID = value.Raw
			remaining--
		case "capture_error":
			// Strictly a JSON bool: the string "true" is a rejected shape.
			p.captureError = value.Type == gjson.True
			remaining--
		}
		return remaining != 0
	})
	return p
}
