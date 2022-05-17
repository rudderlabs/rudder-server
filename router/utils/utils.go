package utils

import (
	"encoding/base64"
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

func getRetentionTimeForDestination(destID string) time.Duration {
	destJobRetentionFound := config.IsSet("Router." + destID + ".jobRetention")
	if destJobRetentionFound {
		return config.GetDuration("Router."+destID+".jobRetention", 720, time.Hour)
	}

	return JobRetention
}

func ToBeDrained(job *jobsdb.JobT, destID, toAbortDestinationIDs string, destinationsMap map[string]*BatchDestinationT) (bool, string) {
	//drain if job is older than a day
	jobReceivedAt := gjson.GetBytes(job.Parameters, "received_at")
	if jobReceivedAt.Exists() {
		jobReceivedAtTime, err := time.Parse(misc.RFC3339Milli, jobReceivedAt.String())
		if err == nil {
			if time.Since(jobReceivedAtTime) > getRetentionTimeForDestination(destID) {
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

func IsNotEmptyString(s string) bool {
	return len(strings.TrimSpace(s)) > 0
}

func GetAuthType(dest backendconfig.DestinationT) (authType string) {
	destConfig := dest.DestinationDefinition.Config
	var lookupErr error
	var authValue interface{}
	if authValue, lookupErr = misc.NestedMapLookup(destConfig, "auth", "type"); lookupErr != nil {
		// pkgLogger.Infof(`OAuthsupport for %s not supported`, dest.DestinationDefinition.Name)
		return ""
	}
	authType = authValue.(string)
	return authType
}

func BasicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func GetRudderAccountId(destination *backendconfig.DestinationT) string {
	if rudderAccountIdInterface, found := destination.Config["rudderAccountId"]; found {
		if rudderAccountId, ok := rudderAccountIdInterface.(string); ok {
			return rudderAccountId
		}
	}
	return ""
}
