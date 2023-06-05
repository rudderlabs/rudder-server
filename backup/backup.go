package backup

import (
	"context"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
)

type jobQueue interface {
	GetProcessed(
		ctx context.Context,
		params jobsdb.GetQueryParamsT,
	) (jobsdb.JobsResult, error)

	UpdateJobStatus(
		ctx context.Context,
		statusList []*jobsdb.JobStatusT,
		customValFilters []string,
		parameterFilters []jobsdb.ParameterFilterT,
	) error
}

func Backup(
	ctx context.Context,
	queryParams jobsdb.GetQueryParamsT,
	jobsDB jobQueue,
	fileUploaderProvider fileuploader.Provider,
) {
	// retry and backoff applied to all the steps below

	// 1. Get the jobs from the jobsdb

	// 2. Upload the jobs to the file uploader

	// 3. Update the job status in the jobsdb
}
