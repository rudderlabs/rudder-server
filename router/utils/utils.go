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

type BatchDestinationT struct {
	Destination backendconfig.DestinationT
	Sources     []backendconfig.SourceT
}

func ToBeDrained(job *jobsdb.JobT, destID, toAbortDestinationIDs string, destinationsMap map[string]*BatchDestinationT) (bool, string) {
	//drain if job is older than a day
	jobReceivedAt := gjson.GetBytes(job.Parameters, "received_at")
	if jobReceivedAt.Exists() {
		jobReceivedAtTime, err := time.Parse(misc.RFC3339Milli, jobReceivedAt.String())
		if err == nil {
			if time.Now().UTC().Sub(jobReceivedAtTime.UTC()) > config.GetDuration("Router.jobRetention", time.Duration(24), time.Hour) {
				return true, "job expired"
			}
		}
	}

	if d, ok := destinationsMap[destID]; ok && !d.Destination.Enabled {
		return true, "destination is disabled"
	}

	if toAbortDestinationIDs != "" {
		abortIDs := strings.Split(toAbortDestinationIDs, ",")
		return misc.ContainsString(abortIDs, destID), "destination configured to abort"
	}

	return false, ""
}

//rawMsg passed must be a valid JSON
func EnhanceResponse(rawMsg []byte, key, val string) []byte {
	resp, err := sjson.SetBytes(rawMsg, key, val)
	if err != nil {
		return []byte(`{}`)
	}

	return resp
}
