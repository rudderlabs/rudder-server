package types

import "net/http"

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
