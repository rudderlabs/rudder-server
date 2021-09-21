package utils

import (
	"encoding/base64"
	"fmt"
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

func Init() {
	loadConfig()
}

func loadConfig() {
	config.RegisterDurationConfigVariable(time.Duration(24), &JobRetention, true, time.Hour, "Router.jobRetention")
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

func IsNotEmptyString(s string) bool {
	return len(strings.TrimSpace(s)) > 0
}

// NestedMapLookup
// m:  a map from strings to other maps or values, of arbitrary depth
// ks: successive keys to reach an internal or leaf node (variadic)
// If an internal node is reached, will return the internal map
//
// Returns: (Exactly one of these will be nil)
// rval: the target node (if found)
// err:  an error created by fmt.Errorf
//
func NestedMapLookup(m map[string]interface{}, ks ...string) (rval interface{}, err error) {
	var ok bool

	if len(ks) == 0 { // degenerate input
		return nil, fmt.Errorf("NestedMapLookup needs at least one key")
	}
	if rval, ok = m[ks[0]]; !ok {
		return nil, fmt.Errorf("key not found; remaining keys: %v", ks)
	} else if len(ks) == 1 { // we've reached the final key
		return rval, nil
	} else if m, ok = rval.(map[string]interface{}); !ok {
		return nil, fmt.Errorf("malformed structure at %#v", rval)
	} else { // 1+ more keys
		return NestedMapLookup(m, ks[1:]...)
	}
}

// Gets the workspace token data for a single workspace or multi workspace case
func GetWorkspaceToken() (workspaceToken string) {
	workspaceToken = config.GetWorkspaceToken()
	isMultiWorkspace := config.GetEnvAsBool("HOSTED_SERVICE", false)
	if isMultiWorkspace {
		workspaceToken = config.GetEnv("HOSTED_SERVICE_SECRET", "password")
	}
	return workspaceToken
}

func GetAuthType(dest backendconfig.DestinationT) (authType string) {
	destConfig := dest.DestinationDefinition.Config
	var lookupErr error
	var authValue interface{}
	if authValue, lookupErr = NestedMapLookup(destConfig, "auth", "type"); lookupErr != nil {
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
