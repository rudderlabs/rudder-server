package router

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	kit_sync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/isolation"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func isSuccessStatus(status int) bool {
	return status >= 200 && status < 300
}

func isJobTerminated(status int) bool {
	if status == 429 {
		return false
	}
	return status >= 200 && status < 500
}

func nextAttemptAfter(attempt int, minRetryBackoff, maxRetryBackoff time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return time.Duration(math.Min(float64(maxRetryBackoff), float64(minRetryBackoff)*math.Exp2(float64(attempt-1))))
}

func getIterableStruct(payload []byte, transformAt string) ([]integrations.PostParametersT, error) {
	var err error
	var response integrations.PostParametersT
	responseArray := make([]integrations.PostParametersT, 0)
	if transformAt == "router" {
		err = json.Unmarshal(payload, &response)
		if err != nil {
			err = json.Unmarshal(payload, &responseArray)
		} else {
			responseArray = append(responseArray, response)
		}
	} else {
		err = json.Unmarshal(payload, &response)
		if err == nil {
			responseArray = append(responseArray, response)
		}
	}

	return responseArray, err
}

func getWorkerPartition(key string, noOfWorkers int) int {
	return misc.GetHash(key) % noOfWorkers
}

func jobOrderKey(userID, destinationID string) string {
	return userID + ":" + destinationID
}

func isolationMode(destType string, config *config.Config) isolation.Mode {
	defaultIsolationMode := isolation.ModeDestination
	if config.IsSet("WORKSPACE_NAMESPACE") {
		defaultIsolationMode = isolation.ModeWorkspace
	}
	destTypeKey := fmt.Sprintf("Router.%s.isolationMode", destType)
	if config.IsSet(destTypeKey) {
		return isolation.Mode(config.GetString(destTypeKey, string(defaultIsolationMode)))
	}
	return isolation.Mode(config.GetString("Router.isolationMode", string(defaultIsolationMode)))
}

func LimiterPriorityValueFrom(v, max int) kit_sync.LimiterPriorityValue {
	if v <= 0 {
		return kit_sync.LimiterPriorityValueLow
	}
	if v > max {
		return kit_sync.LimiterPriorityValueHigh
	}
	return kit_sync.LimiterPriorityValue(int(math.Ceil(float64(kit_sync.LimiterPriorityValueHigh) * float64(v) / float64(max))))
}
