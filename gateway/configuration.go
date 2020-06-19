package gateway

import (
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

func loadConfig() {
	//Port where GW is running
	webPort = config.GetInt("Gateway.webPort", 8080)
	//Number of incoming requests that are batched before handing off to write workers
	maxUserWebRequestBatchSize = config.GetInt("Gateway.maxUserRequestBatchSize", 4)
	//Number of userWorkerBatchRequest that are batched before initiating write
	maxDBBatchSize = config.GetInt("Gateway.maxDBBatchSize", 32)
	//Timeout after which batch is formed anyway with whatever requests
	//are available
	userWebRequestBatchTimeout = (config.GetDuration("Gateway.userWebRequestBatchTimeoutInMS", time.Duration(15)) * time.Millisecond)
	dbBatchWriteTimeout = (config.GetDuration("Gateway.dbBatchWriteTimeoutInMS", time.Duration(5)) * time.Millisecond)
	//Multiple workers are used to batch user web requests
	maxUserWebRequestWorkerProcess = config.GetInt("Gateway.maxUserWebRequestWorkerProcess", 64)
	//Multiple DB writers are used to write data to DB
	maxDBWriterProcess = config.GetInt("Gateway.maxDBWriterProcess", 64)
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
	// Time period for diagnosis ticker
	diagnosisTickerTime = config.GetDuration("Diagnostics.gatewayTimePeriodInS", 60) * time.Second
}

// MaxReqSize is the maximum request body size, in bytes, accepted by gateway web handlers
func (*HandleT) MaxReqSize() int {
	return maxReqSize
}

// IsEnableRateLimit is true if rate limiting is enabled on gateway
func IsEnableRateLimit() bool {
	return enableRateLimit
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
