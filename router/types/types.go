package types

import (
	"encoding/json"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/samber/lo"
)

const (
	RouterUnMarshalErrorCode = 599
	RouterTimedOutStatusCode = 1113
)

// RouterJobT holds the router job and its related metadata
type RouterJobT struct {
	Message     json.RawMessage            `json:"message"`
	JobMetadata JobMetadataT               `json:"metadata"`
	Destination backendconfig.DestinationT `json:"destination"`
}

// DestinationJobT holds the job to be sent to destination
// and metadata of all the router jobs from which this job is cooked up
type DestinationJobT struct {
	Message          json.RawMessage            `json:"batchedRequest"`
	JobMetadataArray []JobMetadataT             `json:"metadata"` // multiple jobs may be batched in a single message
	Destination      backendconfig.DestinationT `json:"destination"`
	Batched          bool                       `json:"batched"`
	StatusCode       int                        `json:"statusCode"`
	Error            string                     `json:"error"`
}

func (dj *DestinationJobT) MinJobID() int64 {
	return lo.Min(lo.Map(dj.JobMetadataArray, func(item JobMetadataT, _ int) int64 {
		return item.JobID
	}))
}

// JobIDs returns the set of all job ids contained in the message
func (dj *DestinationJobT) JobIDs() map[int64]struct{} {
	jobIDs := make(map[int64]struct{})
	for i := range dj.JobMetadataArray {
		jobIDs[dj.JobMetadataArray[i].JobID] = struct{}{}
	}
	return jobIDs
}

// JobMetadataT holds the job metadata
type JobMetadataT struct {
	UserID             string          `json:"userId"`
	JobID              int64           `json:"jobId"`
	SourceID           string          `json:"sourceId"`
	DestinationID      string          `json:"destinationId"`
	AttemptNum         int             `json:"attemptNum"`
	ReceivedAt         string          `json:"receivedAt"`
	CreatedAt          string          `json:"createdAt"`
	FirstAttemptedAt   string          `json:"firstAttemptedAt"`
	TransformAt        string          `json:"transformAt"`
	WorkspaceID        string          `json:"workspaceId"`
	Secret             json.RawMessage `json:"secret"`
	JobT               *jobsdb.JobT    `json:"jobsT"`
	WorkerAssignedTime time.Time       `json:"workerAssignedTime"`
	DestInfo           json.RawMessage `json:"destInfo,omitempty"`
}

// TransformMessageT is used to pass message to the transformer workers
type TransformMessageT struct {
	Data     []RouterJobT `json:"input"`
	DestType string       `json:"destType"`
}

// JobIDs returns the set of all job ids of the jobs in the message
func (tm *TransformMessageT) JobIDs() map[int64]struct{} {
	jobIDs := make(map[int64]struct{})
	for i := range tm.Data {
		jobIDs[tm.Data[i].JobMetadata.JobID] = struct{}{}
	}
	return jobIDs
}

func NewEventTypeThrottlingCost(m map[string]interface{}) (v EventTypeThrottlingCost) {
	if et, ok := m["eventType"].(map[string]interface{}); ok {
		v = et
	}
	return v
}

type EventTypeThrottlingCost map[string]interface{}

func (e *EventTypeThrottlingCost) Cost(eventType string) (cost int64) {
	if v, ok := (*e)[eventType].(float64); ok && v > 0 {
		return int64(v)
	}
	if defaultCost, ok := (*e)["default"].(float64); ok && defaultCost > 0 {
		return int64(defaultCost)
	}
	return 1
}
