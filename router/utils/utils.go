package utils

import (
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	JobRetention time.Duration
	pkgLogger    logger.LoggerI
)

type BatchDestinationT struct {
	Destination backendconfig.DestinationT
	Sources     []backendconfig.SourceT
}

func Init() {
	loadConfig()
}

func loadConfig() {
	pkgLogger = logger.NewLogger().Child("router_utils")
	config.RegisterDurationConfigVariable(time.Duration(24), &JobRetention, true, time.Hour, "Router.jobRetention")
}

func ToBeDrained(job *jobsdb.JobT, destID, toAbortDestinationIDs string, destinationsMap map[string]*BatchDestinationT) (toBeDrained bool, reason string) {
	//drain if job is older than a day
	jobReceivedAt := gjson.GetBytes(job.Parameters, "received_at")
	if jobReceivedAt.Exists() {
		jobReceivedAtTime, err := time.Parse(misc.RFC3339Milli, jobReceivedAt.String())
		if err == nil {
			if time.Now().UTC().Sub(jobReceivedAtTime.UTC()) > JobRetention {
				toBeDrained = true
				reason = "job expired"
			}
		}
	}

	if d, ok := destinationsMap[destID]; ok && !d.Destination.Enabled {
		toBeDrained = true
		reason = "destination is disabled"
	}

	if toAbortDestinationIDs != "" {
		abortIDs := strings.Split(toAbortDestinationIDs, ",")
		isInAbortIds := misc.ContainsString(abortIDs, destID)
		if isInAbortIds {
			toBeDrained = true
			reason = "destination configured to abort"
		}
	}

	if toBeDrained {
		paramsWithDrainReason, err := sjson.SetBytes(job.Parameters, "reason", reason)
		if err != nil {
			pkgLogger.Errorf("couldn't set reason into parameters: %s", err.Error())
		} else {
			job.Parameters = paramsWithDrainReason
		}
	}

	return
}

//rawMsg passed must be a valid JSON
func EnhanceResponse(rawMsg []byte, key, val string) []byte {
	resp, err := sjson.SetBytes(rawMsg, key, val)
	if err != nil {
		return []byte(`{}`)
	}

	return resp
}
