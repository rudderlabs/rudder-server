package utils

import (
	"slices"
	"strings"
	"time"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var EmptyPayload = []byte(`{}`)

const (
	DRAIN_ERROR_CODE = "410"
	// transformation(router or batch)
	ERROR_AT_TF = "transformation"
	// event delivery
	ERROR_AT_DEL = "delivery"
	// custom destination manager
	ERROR_AT_CUST = "custom"
)

type DestinationWithSources struct {
	Destination backendconfig.DestinationT
	Sources     []backendconfig.SourceT
}

type DrainStats struct {
	Count     int
	Reasons   []string
	Workspace string
}

type SendPostResponse struct {
	StatusCode          int
	ResponseContentType string
	ResponseBody        []byte
}

func getRetentionTimeForDestination(destID string) time.Duration {
	return config.GetDurationVar(720, time.Hour, "Router."+destID+".jobRetention", "Router.jobRetention")
}

type JobParameters struct {
	SourceID                string      `json:"source_id"`
	DestinationID           string      `json:"destination_id"`
	ReceivedAt              string      `json:"received_at"`
	TransformAt             string      `json:"transform_at"`
	SourceTaskRunID         string      `json:"source_task_run_id"`
	SourceJobID             string      `json:"source_job_id"`
	SourceJobRunID          string      `json:"source_job_run_id"`
	SourceDefinitionID      string      `json:"source_definition_id"`
	DestinationDefinitionID string      `json:"destination_definition_id"`
	SourceCategory          string      `json:"source_category"`
	RecordID                interface{} `json:"record_id"`
	MessageID               string      `json:"message_id"`
	EventName               string      `json:"event_name"`
	EventType               string      `json:"event_type"`
	WorkspaceID             string      `json:"workspaceId"`
	RudderAccountID         string      `json:"rudderAccountId"`
}

// ParseReceivedAtTime parses the [ReceivedAt] field and returns the parsed time or a zero value time if parsing fails
func (jp *JobParameters) ParseReceivedAtTime() time.Time {
	receivedAt, _ := time.Parse(misc.RFC3339Milli, jp.ReceivedAt)
	return receivedAt
}

// rawMsg passed must be a valid JSON
func EnhanceJSON(rawMsg []byte, key, val string) []byte {
	resp, err := sjson.SetBytes(rawMsg, key, val)
	if err != nil {
		return []byte(`{}`)
	}

	return resp
}

func EnhanceJsonWithTime(t time.Time, key string, resp []byte) []byte {
	firstAttemptedAtString := t.Format(misc.RFC3339Milli)

	errorRespString, err := sjson.Set(string(resp), key, firstAttemptedAtString)
	if err == nil {
		resp = []byte(errorRespString)
	}

	return resp
}

func IsNotEmptyString(s string) bool {
	return len(strings.TrimSpace(s)) > 0
}

type Drainer interface {
	Drain(
		job *jobsdb.JobT,
		jobParams JobParameters,
		destinationsMap map[string]*DestinationWithSources,
	) (bool, string)
}

func NewDrainer(conf *config.Config) Drainer {
	return &drainer{
		destinationIDs: conf.GetReloadableStringSliceVar(
			[]string{},
			"Router.toAbortDestinationIDs",
		),
		jobRunIDs: conf.GetReloadableStringSliceVar(
			[]string{},
			"RSources.toAbortJobRunIDs",
		),
	}
}

type drainer struct {
	destinationIDs misc.ValueLoader[[]string]
	jobRunIDs      misc.ValueLoader[[]string]
}

func (d *drainer) Drain(
	job *jobsdb.JobT,
	jobParams JobParameters,
	destinationsMap map[string]*DestinationWithSources,
) (bool, string) {
	createdAt := job.CreatedAt
	destID := jobParams.DestinationID
	if time.Since(createdAt) > getRetentionTimeForDestination(destID) {
		return true, "job expired"
	}

	if dest, ok := destinationsMap[destID]; ok && !dest.Destination.Enabled {
		return true, "destination is disabled"
	}

	if len(d.destinationIDs.Load()) > 0 {
		if slices.Contains(d.destinationIDs.Load(), destID) {
			return true, "destination configured to abort"
		}
	}

	if len(d.jobRunIDs.Load()) > 0 {
		if slices.Contains(d.jobRunIDs.Load(), jobParams.SourceJobRunID) {
			return true, "cancelled jobRunID"
		}
	}

	return false, ""
}
