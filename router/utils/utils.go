package utils

import (
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var EmptyPayload = []byte(`{}`)

const (
	DRAIN_ERROR_CODE int = 410
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

func ToBeDrained(job *jobsdb.JobT, destID, toAbortDestinationIDs string, destinationsMap map[string]*DestinationWithSources) (bool, string) {
	// drain if job is older than the destination's retention time
	createdAt := job.CreatedAt
	if time.Since(createdAt) > getRetentionTimeForDestination(destID) {
		return true, "job expired"
	}

	if d, ok := destinationsMap[destID]; ok && !d.Destination.Enabled {
		return true, "destination is disabled"
	}

	if toAbortDestinationIDs != "" {
		abortIDs := strings.Split(toAbortDestinationIDs, ",")
		if slices.Contains(abortIDs, destID) {
			return true, "destination configured to abort"
		}
	}

	return false, ""
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
