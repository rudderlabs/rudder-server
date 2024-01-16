//go:generate mockgen -destination=../../mocks/utils/types/mock_types.go -package mock_types github.com/rudderlabs/rudder-server/utils/types UserSuppression,Reporting

package types

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

const (
	FilterEventCode   = 298
	SuppressEventCode = 299
	DrainEventCode    = 410
)

// SingularEventT single event structure
type SingularEventT map[string]interface{}

type SingularEventWithReceivedAt struct {
	SingularEvent SingularEventT
	ReceivedAt    time.Time
}

// GatewayBatchRequest batch request structure
type GatewayBatchRequest struct {
	Batch      []SingularEventT `json:"batch"`
	RequestIP  string           `json:"requestIP"`
	ReceivedAt time.Time        `json:"receivedAt"`
}

type EventParams struct {
	SourceJobRunId  string `json:"source_job_run_id"`
	SourceId        string `json:"source_id"`
	SourceTaskRunId string `json:"source_task_run_id"`
	TraceParent     string `json:"traceparent"`
	DestinationID   string `json:"destination_id"`
}

// UserSuppression is interface to access Suppress user feature
type UserSuppression interface {
	GetSuppressedUser(workspaceID, userID, sourceID string) *model.Metadata
}

// ConfigEnvI is interface to inject env variables into config
type ConfigEnvI interface {
	ReplaceConfigWithEnvVariables(workspaceConfig []byte) (updatedConfig []byte)
}

// Reporting is interface to report metrics
type Reporting interface {
	// Report reports metrics to reporting service
	Report(ctx context.Context, metrics []*PUReportedMetric, tx *Tx) error

	// DatabaseSyncer creates reporting tables in the database and returns a function to periodically sync the data
	DatabaseSyncer(c SyncerConfig) ReportingSyncer

	// Stop the reporting service
	Stop()
}

type ReportingSyncer func()

// ConfigT simple map config structure
type ConfigT map[string]interface{}
