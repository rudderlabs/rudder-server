package backup

import (
	"context"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
)

var successfulBackupResponse = []byte(`{"status": "backup successful"}`)

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

	Identifier() string
}

type BackupContext struct {
	QueryParams          jobsdb.GetQueryParamsT
	Queue                jobQueue
	FileUploaderProvider fileuploader.Provider
	Marshaller           func(*jobsdb.JobT) ([]byte, error)
}
