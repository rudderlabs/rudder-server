//go:generate mockgen -destination=../../mocks/utils/types/mock_types.go -package mock_types github.com/rudderlabs/rudder-server/utils/types SuppressUserI

package types

import "net/http"

//SingularEventT single event structrue
type SingularEventT map[string]interface{}

//GatewayBatchRequestT batch request structure
type GatewayBatchRequestT struct {
	Batch []SingularEventT `json:"batch"`
}

// SuppressUserI is interface to access Suppress user feature
type SuppressUserI interface {
	IsSuppressedUser(userID, sourceID, writeKey string) bool
}

// EventSchemasI is interface to access EventSchemas feature
type EventSchemasI interface {
	RecordEventSchema(writeKey string, eventBatch string) bool
	GetEventModels(w http.ResponseWriter, r *http.Request)
	GetEventVersions(w http.ResponseWriter, r *http.Request)
	GetSchemaVersionMetadata(w http.ResponseWriter, r *http.Request)
	GetSchemaVersionMissingKeys(w http.ResponseWriter, r *http.Request)
	GetKeyCounts(w http.ResponseWriter, r *http.Request)
	GetEventModelMetadata(w http.ResponseWriter, r *http.Request)
}

// ConfigEnvI is interface to inject env variables into config
type ConfigEnvI interface {
	ReplaceConfigWithEnvVariables(workspaceConfig []byte) (updatedConfig []byte)
}

// ConfigT simple map config structure
type ConfigT map[string]interface{}
