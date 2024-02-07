package router

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	"github.com/rudderlabs/rudder-server/router/isolation"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func isSuccessStatus(status int) bool {
	return status >= 200 && status < 300
}

func isJobTerminated(status int) bool {
	if status == http.StatusTooManyRequests || status == http.StatusRequestTimeout {
		return false
	}
	return status >= http.StatusOK && status < http.StatusInternalServerError
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

func getWorkerPartition(key eventorder.BarrierKey, noOfWorkers int) int {
	return misc.GetHash(key.String()) % noOfWorkers
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

func LimiterPriorityValueFrom(v, max int) kitsync.LimiterPriorityValue {
	if v <= 0 {
		return kitsync.LimiterPriorityValueLow
	}
	if v > max {
		return kitsync.LimiterPriorityValueHigh
	}
	return kitsync.LimiterPriorityValue(int(math.Ceil(float64(kitsync.LimiterPriorityValueHigh) * float64(v) / float64(max))))
}
