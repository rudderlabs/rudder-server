package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

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

	// BeginProcessing prepares the necessary indices in order to
	// be ready for capturing JobStatus statistics
	BeginProcessing(jobs []*jobsdb.JobT)

	// JobStatusesUpdated captures outgoing job statistics.
	// A call to BeginProcessing must precede a call to this method,
	// so that all necessary indices can be created, since a JobStatus
	// doesn't carry all necessary job metadata such as jobRunId, taskRunId, etc.
	JobStatusesUpdated(jobStatuses []*jobsdb.JobStatusT)
}

// FailedJobsStatsCollector collects stats for failed jobs
type FailedJobsStatsCollector interface {
	StatsPublisher
	JobsDropped(jobs []*jobsdb.JobT)
}

// NewStatsCollector creates a new stats collector
func NewStatsCollector(jobservice JobService) StatsCollector {
	return &statsCollector{
		jobService:            jobservice,
		jobIdsToStatKeyIndex:  map[int64]statKey{},
		jobIdsToRecordIdIndex: map[int64]json.RawMessage{},
		statsIndex:            map[statKey]*Stats{},
		failedRecordsIndex:    map[statKey][]json.RawMessage{},
	}
}

// NewDroppedJobsCollector creates a new stats collector for publishing failed job stats and records
func NewDroppedJobsCollector(jobservice JobService) FailedJobsStatsCollector {
	return &statsCollector{
		skipFailedRecords:     true,
		jobService:            jobservice,
		jobIdsToStatKeyIndex:  map[int64]statKey{},
		jobIdsToRecordIdIndex: map[int64]json.RawMessage{},
		statsIndex:            map[statKey]*Stats{},
		failedRecordsIndex:    map[statKey][]json.RawMessage{},
	}
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
	skipFailedRecords     bool
	processing            bool
	jobService            JobService
	jobIdsToStatKeyIndex  map[int64]statKey
	jobIdsToRecordIdIndex map[int64]json.RawMessage
	statsIndex            map[statKey]*Stats
	failedRecordsIndex    map[statKey][]json.RawMessage
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
	r.JobStatusesUpdated(jobStatuses)
}

func (r *statsCollector) JobsStoredWithErrors(jobs []*jobsdb.JobT, failedJobs map[uuid.UUID]string) {
	r.buildStats(jobs, failedJobs, true)
}

func (r *statsCollector) BeginProcessing(jobs []*jobsdb.JobT) {
	r.buildStats(jobs, nil, false)
	r.processing = true
}

func (r *statsCollector) JobStatusesUpdated(jobStatuses []*jobsdb.JobStatusT) {
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
				case jobsdb.Succeeded.State:
					stats.Out++
				case jobsdb.Aborted.State:
					stats.Failed++
					recordId := r.jobIdsToRecordIdIndex[jobStatus.JobID]
					if len(recordId) > 0 {
						r.failedRecordsIndex[statKey] = append(r.failedRecordsIndex[statKey], recordId)
					}
				}
			}
		}
	}
}

func (r *statsCollector) Publish(ctx context.Context, tx *sql.Tx) error {
	if r.jobService == nil {
		return fmt.Errorf("No JobService provided during initialization")
	}
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
			return err
		}
	}
	failedRecordsKeys := r.orderedFailedRecordsKeys()
	for i := range failedRecordsKeys {
		k := failedRecordsKeys[i]
		v := r.failedRecordsIndex[k]
		err := r.jobService.AddFailedRecords(ctx, tx, k.jobRunId, k.JobTargetKey, v)
		if err != nil {
			return err
		}
	}

	// reset state so that the collector can be
	// reused for another stats collecting cycle
	r.processing = false
	r.jobIdsToStatKeyIndex = map[int64]statKey{}
	r.statsIndex = map[statKey]*Stats{}
	r.jobIdsToRecordIdIndex = map[int64]json.RawMessage{}
	r.failedRecordsIndex = map[statKey][]json.RawMessage{}

	return nil
}

func (r *statsCollector) buildStats(jobs []*jobsdb.JobT, failedJobs map[uuid.UUID]string, incrementIn bool) { // skipcq: RVV-A0005
	for i := range jobs {
		job := jobs[i]
		if _, ok := failedJobs[job.UUID]; ok {
			continue
		}
		var jobRunId string
		var jobTargetKey JobTargetKey
		var recordId string
		remaining := 5
		jp := gjson.ParseBytes(job.Parameters)
		jp.ForEach(func(key, value gjson.Result) bool {
			switch key.Str {
			case "source_job_run_id":
				jobRunId = value.Str
				remaining--
			case "source_task_run_id":
				jobTargetKey.TaskRunID = value.Str
				remaining--
			case "source_id":
				jobTargetKey.SourceID = value.Str
				remaining--
			case "destination_id":
				jobTargetKey.DestinationID = value.Str
				remaining--
			case "record_id":
				recordId = value.Raw
				remaining--
			}
			return remaining != 0
		})
		if jobRunId != "" {
			sk := statKey{
				jobRunId:     jobRunId,
				JobTargetKey: jobTargetKey,
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
			if r.skipFailedRecords {
				continue
			}
			if recordId != "" && recordId != "null" && recordId != `""` {
				recordIdJson := json.RawMessage(recordId)
				if json.Valid(recordIdJson) {
					r.jobIdsToRecordIdIndex[job.JobID] = recordIdJson
				}
			}
		}
	}
}
