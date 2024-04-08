package utils

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
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

	DrainReasonDestNotFound      = "destination is not available in the config"
	DrainReasonDestDisabled      = "destination is disabled"
	DrainReasonDestAbort         = "destination configured to abort"
	DrainReasonJobRunIDCancelled = "cancelled jobRunID"
	DrainReasonJobExpired        = "job expired"
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
	DontBatch               bool        `json:"dontBatch"`
	TraceParent             string      `json:"traceparent"`
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
	) (bool, string)
}

func NewDrainer(
	conf *config.Config,
	destDrainFunc func(string) (*DestinationWithSources, bool),
) Drainer {
	return &drainer{
		destinationIDs: conf.GetReloadableStringSliceVar(
			nil,
			"Router.toAbortDestinationIDs",
		),
		jobRunIDs: conf.GetReloadableStringSliceVar(
			nil,
			"drain.jobRunIDs",
		),
		destinationResolver: destDrainFunc,
	}
}

type drainer struct {
	destinationIDs config.ValueLoader[[]string]
	jobRunIDs      config.ValueLoader[[]string]

	destinationResolver func(string) (*DestinationWithSources, bool)
}

func (d *drainer) Drain(
	job *jobsdb.JobT,
) (bool, string) {
	createdAt := job.CreatedAt
	var jobParams JobParameters
	_ = json.Unmarshal(job.Parameters, &jobParams)
	destID := jobParams.DestinationID
	if time.Since(createdAt) > getRetentionTimeForDestination(destID) {
		return true, DrainReasonJobExpired
	}

	if destination, ok := d.destinationResolver(destID); !ok {
		return true, DrainReasonDestNotFound
	} else if !destination.Destination.Enabled {
		return true, DrainReasonDestDisabled
	}

	if slices.Contains(d.destinationIDs.Load(), destID) {
		return true, DrainReasonDestAbort
	}

	if jobParams.SourceJobRunID != "" &&
		slices.Contains(d.jobRunIDs.Load(), jobParams.SourceJobRunID) {
		return true, DrainReasonJobRunIDCancelled
	}

	return false, ""
}

func UpdateProcessedEventsMetrics(statsHandle stats.Stats, module, destType string, statusList []*jobsdb.JobStatusT, jobIDConnectionDetailsMap map[int64]jobsdb.ConnectionDetails) {
	eventsPerConnectionInfoAndStateAndCode := map[string]map[string]map[string]int{}
	for i := range statusList {
		sourceID := jobIDConnectionDetailsMap[statusList[i].JobID].SourceID
		destinationID := jobIDConnectionDetailsMap[statusList[i].JobID].DestinationID
		connectionKey := strings.Join([]string{sourceID, destinationID}, ",")
		state := statusList[i].JobState
		code := statusList[i].ErrorCode
		if _, ok := eventsPerConnectionInfoAndStateAndCode[connectionKey]; !ok {
			eventsPerConnectionInfoAndStateAndCode[connectionKey] = map[string]map[string]int{}
			eventsPerStateAndCode := eventsPerConnectionInfoAndStateAndCode[connectionKey]
			eventsPerStateAndCode[state] = map[string]int{}
			eventsPerStateAndCode[state][code]++

		} else {
			eventsPerStateAndCode := eventsPerConnectionInfoAndStateAndCode[connectionKey]
			if _, ok := eventsPerStateAndCode[state]; !ok {
				eventsPerStateAndCode[state] = map[string]int{}
			}
			eventsPerStateAndCode[state][code]++
		}

	}
	for connectionKey, eventsPerStateAndCode := range eventsPerConnectionInfoAndStateAndCode {
		sourceID := strings.Split(connectionKey, ",")[0]
		destinationID := strings.Split(connectionKey, ",")[1]
		for state, codes := range eventsPerStateAndCode {
			for code, count := range codes {
				statsHandle.NewTaggedStat(`pipeline_processed_events`, stats.CountType, stats.Tags{
					"module":        module,
					"destType":      destType,
					"state":         state,
					"code":          code,
					"sourceId":      sourceID,
					"destinationId": destinationID,
				}).Count(count)
			}
		}
	}
}
