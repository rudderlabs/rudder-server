package source

import (
	"context"
	"errors"

	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

var ErrReceivingChannelClosed = errors.New("receiving channel closed")
var ErrProcessingTimedOut = errors.New("processing timed out")

type insertJobRequest struct {
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
	StartTime     string `json:"start_time"`
	JobRunID      string `json:"job_run_id"`
	TaskRunID     string `json:"task_run_id"`
	JobType       string `json:"async_job_type"`
	WorkspaceID   string `json:"workspace_id"`
}

type insertJobResponse struct {
	JobIds []int64 `json:"jobids"`
	Err    error   `json:"error"`
}

type jobStatusResponse struct {
	Status string
	Err    string
}

type notifierResponse struct {
	Id int64 `json:"id"`
}

type publisher interface {
	Publish(context.Context, *notifier.PublishRequest) (<-chan *notifier.PublishResponse, error)
}

type sourceRepo interface {
	Insert(context.Context, []model.SourceJob) ([]int64, error)
	Reset(context.Context) error
	GetToProcess(context.Context, int64) ([]model.SourceJob, error)
	GetByJobRunTaskRun(context.Context, string, string) (*model.SourceJob, error)
	OnUpdateSuccess(context.Context, int64) error
	OnUpdateFailure(context.Context, int64, error, int) error
	MarkExecuting(context.Context, []int64) error
}

type tableUploadsRepo interface {
	GetByJobRunTaskRun(ctx context.Context, sourceID, destinationID, jobRunID, taskRunID string) ([]model.TableUpload, error)
}
