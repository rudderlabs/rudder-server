package types

import (
	"encoding/json"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

const (
	RouterTimedOutStatusCode = 1113
)

//RouterJobT holds the router job and its related metadata
type RouterJobT struct {
	Message     json.RawMessage            `json:"message"`
	JobMetadata JobMetadataT               `json:"metadata"`
	Destination backendconfig.DestinationT `json:"destination"`
}

//DestinationJobT holds the job to be sent to destination
//and metadata of all the router jobs from which this job is cooked up
type DestinationJobT struct {
	Message          json.RawMessage            `json:"batchedRequest"`
	JobMetadataArray []JobMetadataT             `json:"metadata"`
	Destination      backendconfig.DestinationT `json:"destination"`
	Batched          bool                       `json:"batched"`
	StatusCode       int                        `json:"statusCode"`
	Error            string                     `json:"error"`
}

//JobMetadataT holds the job metadata
type JobMetadataT struct {
	UserID           string          `json:"userId"`
	JobID            int64           `json:"jobId"`
	SourceID         string          `json:"sourceId"`
	DestinationID    string          `json:"destinationId"`
	AttemptNum       int             `json:"attemptNum"`
	ReceivedAt       string          `json:"receivedAt"`
	CreatedAt        string          `json:"createdAt"`
	FirstAttemptedAt string          `json:"firstAttemptedAt"`
	TransformAt      string          `json:"transformAt"`
	WorkspaceId      string          `json:"workspaceId"`
	Secret           json.RawMessage `json:"secret"`
	JobT             *jobsdb.JobT    `json:"jobsT"`
	PickedAtTime     time.Time       `json:"pickedAtTime"`
	ResultSetID      int64           `json:"resultSetID"`
}

//TransformMessageT is used to pass message to the transformer workers
type TransformMessageT struct {
	Data     []RouterJobT `json:"input"`
	DestType string       `json:"destType"`
}
