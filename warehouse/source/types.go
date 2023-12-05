package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

var (
	ErrReceivingChannelClosed = errors.New("receiving channel closed")
	ErrProcessingTimedOut     = errors.New("processing timed out")
)

type insertJobRequest struct {
	SourceID      string     `json:"source_id"`
	DestinationID string     `json:"destination_id"`
	StartTime     CustomTime `json:"start_time"`
	JobRunID      string     `json:"job_run_id"`
	TaskRunID     string     `json:"task_run_id"`
	JobType       string     `json:"async_job_type"`
	WorkspaceID   string     `json:"workspace_id"`
}

type CustomTime struct {
	time.Time
}

const CustomTimeLayout = "01-02-2006 15:04:05"

func (ct *CustomTime) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")
	if s == "null" {
		ct.Time = time.Time{}
		return
	}
	ct.Time, err = time.Parse(CustomTimeLayout, s)
	return
}

func (ct CustomTime) MarshalJSON() ([]byte, error) {
	if ct.IsZero() {
		return []byte("null"), nil
	}
	return []byte(fmt.Sprintf("\"%s\"", ct.Format(CustomTimeLayout))), nil
}

type insertJobResponse struct {
	JobIds []int64 `json:"jobids"`
	Err    error   `json:"error"`
}

type jobStatusResponse struct {
	Status string
	Err    string
}

type NotifierRequest struct {
	ID            int64           `json:"id"`
	SourceID      string          `json:"source_id"`
	DestinationID string          `json:"destination_id"`
	WorkspaceID   string          `json:"workspace_id"`
	TableName     string          `json:"tablename"`
	JobType       string          `json:"async_job_type"`
	MetaData      json.RawMessage `json:"metadata"`
}

type NotifierResponse struct {
	ID int64 `json:"id"`
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
