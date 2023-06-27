//go:generate mockgen -destination=../../mocks/utils/types/mock_types.go -package mock_types github.com/rudderlabs/rudder-server/utils/types UserSuppression,Reporting

package types

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
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
	GetSuppressedUser(workspaceID, userID, sourceID string) *model.Metadata
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

// Reporting is interface to report metrics
type Reporting interface {
	WaitForSetup(ctx context.Context, clientName string) error
	Report(metrics []*PUReportedMetric, txn *sql.Tx)
	AddClient(ctx context.Context, c Config)
}

// ConfigT simple map config structure
type ConfigT map[string]interface{}
