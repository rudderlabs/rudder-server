package utils

import (
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	JobRetention time.Duration
)

type BatchDestinationT struct {
	Destination backendconfig.DestinationT
	Sources     []backendconfig.SourceT
}

type DrainStats struct {
	Count   int
	Reasons []string
}

func Init() {
	loadConfig()
}

func loadConfig() {
	config.RegisterDurationConfigVariable(time.Duration(720), &JobRetention, true, time.Hour, "Router.jobRetention")
}

func ToBeDrained(job *jobsdb.JobT, destID, toAbortDestinationIDs string, destinationsMap map[string]*BatchDestinationT) (bool, string) {
	//drain if job is older than a day
	jobReceivedAt := gjson.GetBytes(job.Parameters, "received_at")
	if jobReceivedAt.Exists() {
		jobReceivedAtTime, err := time.Parse(misc.RFC3339Milli, jobReceivedAt.String())
		if err == nil {
			if time.Now().UTC().Sub(jobReceivedAtTime.UTC()) > JobRetention {
				return true, "job expired"
			}
		}
	}

	if d, ok := destinationsMap[destID]; ok && !d.Destination.Enabled {
		return true, "destination is disabled"
	}

	if toAbortDestinationIDs != "" {
		abortIDs := strings.Split(toAbortDestinationIDs, ",")
		if misc.ContainsString(abortIDs, destID) {
			return true, "destination configured to abort"
		}
	}

	return false, ""
}

//rawMsg passed must be a valid JSON
func EnhanceJSON(rawMsg []byte, key, val string) []byte {
	resp, err := sjson.SetBytes(rawMsg, key, val)
	if err != nil {
		return []byte(`{}`)
	}

	return resp
}
