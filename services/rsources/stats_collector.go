package rsources

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

// StatsCollector collects and publishes stats as jobs are
// being created, processed and their statuses are being updated.
type StatsCollector interface {
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

	// Publish publishes statistics
	Publish(ctx context.Context, tx *sql.Tx) error
}

// NewStatsCollector creates a new stats collector
func NewStatsCollector(incrementer StatsIncrementer) StatsCollector {
	return &statsCollector{
		incrementer:        incrementer,
		jobIdsToStatsIndex: map[int64]*Stats{},
		statsIndex:         map[statKey]*Stats{},
	}
}

type statKey struct {
	jobRunId string
	JobTargetKey
}

var _ StatsCollector = (*statsCollector)(nil)

type statsCollector struct {
	processing         bool
	incrementer        StatsIncrementer
	jobIdsToStatsIndex map[int64]*Stats
	statsIndex         map[statKey]*Stats
}

func (r *statsCollector) JobsStored(jobs []*jobsdb.JobT) {
	r.buildStats(jobs, nil, true)
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
	if len(r.jobIdsToStatsIndex) == 0 {
		return
	}
	for i := range jobStatuses {
		jobStatus := jobStatuses[i]
		stats, ok := r.jobIdsToStatsIndex[jobStatus.JobID]
		if ok {
			switch jobStatus.JobState {
			case jobsdb.Succeeded.State:
				stats.Out++
			case jobsdb.Aborted.State:
				stats.Failed++
			}
		}
	}
}

func (r *statsCollector) Publish(ctx context.Context, tx *sql.Tx) error {
	if r.incrementer == nil {
		return fmt.Errorf("No StatsIncrementer provided during initialization")
	}
	for k, v := range r.statsIndex {
		if v.Failed+v.In+v.Out == 0 {
			continue
		}
		err := r.incrementer.IncrementStats(ctx, tx, k.jobRunId, k.JobTargetKey, *v)
		if err != nil {
			return err
		}
	}

	// reset state so that the collector can be
	// reused for another stats collecting cycle
	r.processing = false
	r.jobIdsToStatsIndex = map[int64]*Stats{}
	r.statsIndex = map[statKey]*Stats{}

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
		remaining := 4
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
			r.jobIdsToStatsIndex[job.JobID] = stats
		}
	}
}
