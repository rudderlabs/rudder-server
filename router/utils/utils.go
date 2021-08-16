package utils

import (
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

type BatchDestinationT struct {
	Destination backendconfig.DestinationT
	Sources     []backendconfig.SourceT
}

func ToBeDrained(job *jobsdb.JobT, destID, toAbortDestinationIDs string, destinationsMap map[string]*BatchDestinationT) bool {
	//drain if job is older than a day
	jobReceivedAt := gjson.GetBytes(job.Parameters, "received_at")
	jobReceivedAtTime, err := time.Parse(misc.RFC3339Milli, jobReceivedAt.String())
	if err == nil {
		if jobReceivedAt.Exists() && time.Now().UTC().Sub(jobReceivedAtTime.UTC()) > config.GetDuration("Router.jobRetention", time.Duration(24), time.Hour) {
			return true
		}
	}
	if d, ok := destinationsMap[destID]; ok && !d.Destination.Enabled {
		return true
	}
	if toAbortDestinationIDs != "" {
		abortIDs := strings.Split(toAbortDestinationIDs, ",")
		return misc.ContainsString(abortIDs, destID)
	}
	return false
}
