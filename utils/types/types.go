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

// ProtocolsI is interface to access Protocols user feature
type ProtocolsI interface {
	RecordEventSchema(writeKey string, eventBatch string) bool
	GetEventModels(w http.ResponseWriter, r *http.Request)
	GetEventVersions(w http.ResponseWriter, r *http.Request)
}

// ConfigEnvI is interface to inject env variables into config
type ConfigEnvI interface {
	ReplaceConfigWithEnvVariables(workspaceConfig []byte) (updatedConfig []byte)
}
