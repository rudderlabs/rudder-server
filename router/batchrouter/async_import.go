package batchrouter

import (
	stdjson "encoding/json"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

// asyncImport is an import in progress at the destination
type asyncImport struct {
	parameters stdjson.RawMessage // importing parameters of the import, with its import ID or a reference to it
	jobs       []*jobsdb.JobT     // the importing jobs of the import, in job ID order
	startedAt  time.Time
	nextPoll   time.Time
	polls      int
	tracked    func() // stops tracking the import's age
}

// newAsyncImport returns the import of some importing jobs, which must be in job ID order.
func newAsyncImport(parameters stdjson.RawMessage, jobs []*jobsdb.JobT) *asyncImport {
	return &asyncImport{parameters: parameters, jobs: jobs, startedAt: jobs[0].LastJobStatus.ExecTime}
}

// asyncImportsOf groups importing jobs, which must be in job ID order, by import, oldest import first.
func asyncImportsOf(jobs []*jobsdb.JobT) []*asyncImport {
	groups := lo.PartitionBy(jobs, func(job *jobsdb.JobT) string {
		if ref := gjson.GetBytes(job.LastJobStatus.Parameters, importIDRefKey); ref.Exists() {
			return importIDRefKey + ref.Raw
		}
		return importIDKey + gjson.GetBytes(job.LastJobStatus.Parameters, importIDKey).Raw
	})
	return lo.Map(groups, func(group []*jobsdb.JobT, _ int) *asyncImport {
		return newAsyncImport(group[0].LastJobStatus.Parameters, group)
	})
}

// importingJobs returns the jobs that the statuses of an upload leave importing, each with its status, as jobsdb would
// return them.
func importingJobs(jobs []*jobsdb.JobT, statusList []*jobsdb.JobStatusT) []*jobsdb.JobT {
	importing := make(map[int64]*jobsdb.JobStatusT, len(statusList))
	for _, status := range statusList {
		if status.JobState == jobsdb.Importing.State {
			importing[status.JobID] = status
		}
	}
	var result []*jobsdb.JobT
	for _, job := range jobs {
		if status, ok := importing[job.JobID]; ok {
			job.LastJobStatus = *status
			result = append(result, job)
		}
	}
	return result
}
