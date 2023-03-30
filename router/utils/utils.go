package utils

import (
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/sjson"
)

var (
	JobRetention time.Duration
	EmptyPayload = []byte(`{}`)
)

const (
	DRAIN_ERROR_CODE int = 410
	// transformation(router or batch)
	ERROR_AT_TF = "transformation"
	// event delivery
	ERROR_AT_DEL = "delivery"
	// custom destination manager
	ERROR_AT_CUST = "custom"
)

type BatchDestinationT struct {
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

func Init() {
	loadConfig()
}

func loadConfig() {
	config.RegisterDurationConfigVariable(720, &JobRetention, true, time.Hour, "Router.jobRetention")
}

func ToBeDrained(job *jobsdb.JobT, destID, toAbortDestinationIDs string, destinationsMap map[string]*BatchDestinationT) (bool, string) {
	if d, ok := destinationsMap[destID]; ok && !d.Destination.Enabled {
		return true, "destination is disabled"
	}

	if toAbortDestinationIDs != "" {
		abortIDs := strings.Split(toAbortDestinationIDs, ",")
		if misc.Contains(abortIDs, destID) {
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

func IsNotEmptyString(s string) bool {
	return len(strings.TrimSpace(s)) > 0
}
