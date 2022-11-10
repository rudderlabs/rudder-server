//go:generate mockgen -destination=../../mocks/utils/types/mock_types.go -package mock_types github.com/rudderlabs/rudder-server/utils/types UserSuppression,ReportingI

package types

import (
	"context"
	"database/sql"
	"net/http"
	"time"
)

// SingularEventT single event structrue
type SingularEventT map[string]interface{}

type SingularEventWithReceivedAt struct {
	SingularEvent SingularEventT
	ReceivedAt    time.Time
}

// GatewayBatchRequestT batch request structure
type GatewayBatchRequestT struct {
	Batch []SingularEventT `json:"batch"`
}

// UserSuppression is interface to access Suppress user feature
type UserSuppression interface {
	IsSuppressedUser(workspaceID, userID, sourceID string) bool
}

// EventSchemasI is interface to access EventSchemas feature
type EventSchemasI interface {
	RecordEventSchema(writeKey, eventBatch string) bool
	GetEventModels(w http.ResponseWriter, r *http.Request)
	GetEventVersions(w http.ResponseWriter, r *http.Request)
	GetSchemaVersionMetadata(w http.ResponseWriter, r *http.Request)
	GetSchemaVersionMissingKeys(w http.ResponseWriter, r *http.Request)
	GetKeyCounts(w http.ResponseWriter, r *http.Request)
	GetEventModelMetadata(w http.ResponseWriter, r *http.Request)
	GetJsonSchemas(w http.ResponseWriter, r *http.Request)
}

// ConfigEnvI is interface to inject env variables into config
type ConfigEnvI interface {
	ReplaceConfigWithEnvVariables(workspaceConfig []byte) (updatedConfig []byte)
}

// ReportingI is interface to report metrics
type ReportingI interface {
	WaitForSetup(ctx context.Context, clientName string) error
	AddClient(ctx context.Context, c Config)
	Report(metrics []*PUReportedMetric, txn *sql.Tx)
	IsPIIReportingDisabled(string) bool
}

// ConfigT simple map config structure
type ConfigT map[string]interface{}
