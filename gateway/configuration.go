package gateway

import (
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

func loadConfig() {
	config.Initialize()

	//Port where GW is running
	webPort = config.GetInt("Gateway.webPort", 8080)
	//Number of incoming requests that are batched before initiating write
	maxBatchSize = config.GetInt("Gateway.maxBatchSize", 32)
	//Timeout after which batch is formed anyway with whatever requests
	//are available
	batchTimeout = (config.GetDuration("Gateway.batchTimeoutInMS", time.Duration(20)) * time.Millisecond)
	//Multiple DB writers are used to write data to DB
	maxDBWriterProcess = config.GetInt("Gateway.maxDBWriterProcess", 4)
	// CustomVal is used as a key in the jobsDB customval column
	CustomVal = config.GetString("Gateway.CustomVal", "GW")
	// Maximum request size to gateway
	maxReqSize = config.GetInt("Gateway.maxReqSizeInKB", 100000) * 1024
	// Enable dedup of incoming events by default
	enableDedup = config.GetBool("Gateway.enableDedup", false)
	// Dedup time window in hours
	dedupWindow = config.GetDuration("Gateway.dedupWindowInS", time.Duration(86400))
	// Enable rate limit on incoming events. false by default
	enableRateLimit = config.GetBool("Gateway.enableRateLimit", false)
}

// MaxReqSize is the maximum request body size, in bytes, accepted by gateway web handlers
func (*HandleT) MaxReqSize() int {
	return maxReqSize
}

//SetEnableRateLimit overrides enableRateLimit configuration and returns previous value
func SetEnableRateLimit(b bool) bool {
	prev := enableRateLimit
	enableRateLimit = b
	return prev
}

//SetEnableDedup overrides enableDedup configuration and returns previous value
func SetEnableDedup(b bool) bool {
	prev := enableDedup
	enableDedup = b
	return prev
}
