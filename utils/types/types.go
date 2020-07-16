//go:generate mockgen -destination=../../mocks/utils/types/mock_types.go -package mock_types github.com/rudderlabs/rudder-server/utils/types GatewayWebhookI,WebHookI,SuppressUserI

package types

import "net/http"

//SingularEventT single event structrue
type SingularEventT map[string]interface{}

//GatewayBatchRequestT batch request structure
type GatewayBatchRequestT struct {
	Batch []SingularEventT `json:"batch"`
}

// GatewayWebhookI is interface to access Webhook feature
type GatewayWebhookI interface {
	IncrementRecvCount(count uint64)
	IncrementAckCount(count uint64)
	UpdateWriteKeyStats(writeKeyStats map[string]int, bucket string)
	TrackRequestMetrics(errorMessage string)
	AddToWebRequestQ(req *http.Request, writer *http.ResponseWriter, done chan string, reqType string)
	GetWebhookSourceDefName(writeKey string) (name string, ok bool)
}

// WebhookI is interface to access Webhook feature
type WebHookI interface {
	RequestHandler(w http.ResponseWriter, r *http.Request)
	Register(name string)
}

// SuppressUserI is interface to access Suppress user feature
type SuppressUserI interface {
	IsSuppressedUser(userID, sourceID, writeKey string) bool
}
