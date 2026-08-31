package utils

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
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

type processedEventsMetricKey struct {
	sourceID       string
	destinationID  string
	state          string
	code           string
	reasonCategory string
	reason         string
}

type SendPostResponse struct {
	StatusCode          int
	ResponseContentType string
	ResponseBody        []byte
}

type JobParameters struct {
	SourceID                string `json:"source_id"`
	DestinationID           string `json:"destination_id"`
	ReceivedAt              string `json:"received_at"`
	TransformAt             string `json:"transform_at"`
	SourceTaskRunID         string `json:"source_task_run_id"`
	SourceJobID             string `json:"source_job_id"`
	SourceJobRunID          string `json:"source_job_run_id"`
	SourceDefinitionID      string `json:"source_definition_id"`
	DestinationDefinitionID string `json:"destination_definition_id"`
	SourceCategory          string `json:"source_category"`
	RecordID                any    `json:"record_id"`
	MessageID               string `json:"message_id"`
	EventName               string `json:"event_name"`
	EventType               string `json:"event_type"`
	WorkspaceID             string `json:"workspaceId"`
	RudderAccountID         string `json:"rudderAccountId"`
	DontBatch               bool   `json:"dontBatch"`
	TraceParent             string `json:"traceparent"`
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
		createdAt time.Time,
		destID string,
		sourceJobRunID string,
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
		retentionTimes:      make(map[string]config.ValueLoader[time.Duration]),
	}
}

type drainer struct {
	destinationIDs config.ValueLoader[[]string]
	jobRunIDs      config.ValueLoader[[]string]

	destinationResolver func(string) (*DestinationWithSources, bool)
	retentionTimesMu    sync.Mutex
	retentionTimes      map[string]config.ValueLoader[time.Duration]
}

func (d *drainer) Drain(
	createdAt time.Time,
	destID string,
	sourceJobRunID string,
) (bool, string) {
	if time.Since(createdAt) > d.getRetentionTimeForDestination(destID) {
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

	if sourceJobRunID != "" &&
		slices.Contains(d.jobRunIDs.Load(), sourceJobRunID) {
		return true, DrainReasonJobRunIDCancelled
	}

	return false, ""
}

func (d *drainer) getRetentionTimeForDestination(destID string) time.Duration {
	d.retentionTimesMu.Lock()
	defer d.retentionTimesMu.Unlock()
	var (
		c  config.ValueLoader[time.Duration]
		ok bool
	)
	if c, ok = d.retentionTimes[destID]; !ok {
		c = config.GetReloadableDurationVar(720, time.Hour, "Router."+destID+".jobRetention", "Router.jobRetention")
		d.retentionTimes[destID] = c
	}
	return c.Load()
}

func UpdateProcessedEventsMetrics(statsHandle stats.Stats, module, destType string, statusList []*jobsdb.JobStatusT, jobIDConnectionDetailsMap map[int64]jobsdb.ConnectionID) {
	eventsByKey := map[processedEventsMetricKey]int{}
	for i := range statusList {
		connection := jobIDConnectionDetailsMap[statusList[i].JobID]
		key := processedEventsMetricKey{
			sourceID:      connection.SourceID,
			destinationID: connection.DestinationID,
			state:         statusList[i].JobState,
			code:          statusList[i].ErrorCode,
		}
		if isDrainStatus(statusList[i]) {
			key.reasonCategory, key.reason = drainReasonLabels(statusList[i])
		}
		eventsByKey[key]++
	}

	for key, count := range eventsByKey {
		tags := stats.Tags{
			"module":        module,
			"destType":      destType,
			"state":         key.state,
			"code":          key.code,
			"sourceId":      key.sourceID,
			"destinationId": key.destinationID,
		}
		if key.reasonCategory != "" {
			tags["reasonCategory"] = key.reasonCategory
			tags["reason"] = key.reason
		}
		statsHandle.NewTaggedStat(`pipeline_processed_events`, stats.CountType, tags).Count(count)
	}
}

func isDrainStatus(status *jobsdb.JobStatusT) bool {
	return status.JobState == jobsdb.Aborted.State && status.ErrorCode == DRAIN_ERROR_CODE
}

func drainReasonLabels(status *jobsdb.JobStatusT) (reasonCategory, reason string) {
	drainReason := strings.TrimSpace(gjson.GetBytes(status.ErrorResponse, "reason").String())
	if drainReason == "" {
		drainReason = strings.TrimSpace(gjson.GetBytes(status.JobParameters, "reason").String())
	}
	if drainReason == "" && strings.TrimSpace(gjson.GetBytes(status.ErrorResponse, "error").String()) != "" {
		return "retry_limit", "retry limit reached"
	}

	switch drainReason {
	case DrainReasonJobRunIDCancelled:
		return "cancelled_job_run", DrainReasonJobRunIDCancelled
	case DrainReasonJobExpired:
		return "expired", DrainReasonJobExpired
	case DrainReasonDestNotFound:
		return "configuration", DrainReasonDestNotFound
	case DrainReasonDestDisabled:
		return "configuration", DrainReasonDestDisabled
	case DrainReasonDestAbort:
		return "configuration", DrainReasonDestAbort
	case "source_not_found":
		return "configuration", "source_not_found"
	case "retry limit reached":
		return "retry_limit", "retry limit reached"
	case "":
		return "unknown", "unknown"
	default:
		return "retry_limit", "retry limit reached"
	}
}
