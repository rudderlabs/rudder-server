package utils

import (
	"strings"
	"time"

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
	if jobReceivedAt.Exists() && time.Since(jobReceivedAt.Time()).Hours() > 24 {
		return true
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
