package types

import "net/http"

//SingularEventT single event structrue
type SingularEventT map[string]interface{}

//GatewayBatchRequestT batch request structure
type GatewayBatchRequestT struct {
	Batch []SingularEventT `json:"batch"`
}

type GatewayWebhookI interface {
	IncrementRecvCount(count uint64)
	IncrementAckCount(count uint64)
	UpdateWriteKeyStats(writeKeyStats map[string]int, bucket string)
	TrackRequestMetrics(errorMessage string)
	AddToWebRequestQ(req *http.Request, writer *http.ResponseWriter, done chan string, reqType string)
	GetWebhookSourceDefName(writeKey string) (name string, ok bool)
}

type WebHookI interface {
	RequestHandler(w http.ResponseWriter, r *http.Request)
	Register(name string)
}

type SuppressUserI interface {
	IsSuppressedUser(userID, sourceID, writeKey string) bool
}
