package gateway

import (
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

func loadConfig() {
	//Port where GW is running
	webPort = config.GetInt("Gateway.webPort", 8080)
	//Port where AdminHandler is running
	adminWebPort = config.GetInt("Gateway.adminWebPort", 8089)
	//Number of incoming requests that are batched before handing off to write workers
	maxUserWebRequestBatchSize = config.GetInt("Gateway.maxUserRequestBatchSize", 128)
	//Number of userWorkerBatchRequest that are batched before initiating write
	maxDBBatchSize = config.GetInt("Gateway.maxDBBatchSize", 128)
	//Timeout after which batch is formed anyway with whatever requests
	//are available
	config.RegisterDurationConfigVariable(time.Duration(15), &userWebRequestBatchTimeout, true, time.Millisecond, "Gateway.userWebRequestBatchTimeoutInMS")
	config.RegisterDurationConfigVariable(time.Duration(5), &dbBatchWriteTimeout, true, time.Millisecond, "Gateway.dbBatchWriteTimeoutInMS")
	//Multiple workers are used to batch user web requests
	maxUserWebRequestWorkerProcess = config.GetInt("Gateway.maxUserWebRequestWorkerProcess", 64)
	//Multiple DB writers are used to write data to DB
	maxDBWriterProcess = config.GetInt("Gateway.maxDBWriterProcess", 256)
	// CustomVal is used as a key in the jobsDB customval column
	CustomVal = config.GetString("Gateway.CustomVal", "GW")
	// Maximum request size to gateway
	config.RegisterIntConfigVariable(4000, &maxReqSize, true, 1024, "Gateway.maxReqSizeInKB")
	// Enable rate limit on incoming events. false by default
	config.RegisterBoolConfigVariable(false, &enableRateLimit, true, "Gateway.enableRateLimit")
	// Enable suppress user feature. false by default
	enableSuppressUserFeature = config.GetBool("Gateway.enableSuppressUserFeature", true)
	// EventSchemas feature. false by default
	enableEventSchemasFeature = config.GetBool("EventSchemas.enableEventSchemasFeature", false)
	// Time period for diagnosis ticker
	config.RegisterDurationConfigVariable(time.Duration(60),&diagnosisTickerTime,false,time.Second,"Diagnostics.gatewayTimePeriodInS")
	// Enables accepting requests without user id and anonymous id. This is added to prevent client 4xx retries.
	config.RegisterBoolConfigVariable(false, &allowReqsWithoutUserIDAndAnonymousID, true, "Gateway.allowReqsWithoutUserIDAndAnonymousID")
	config.RegisterBoolConfigVariable(true, &gwAllowPartialWriteWithErrors, true, "Gateway.allowPartialWriteWithErrors")

	config.RegisterDurationConfigVariable(time.Duration(0),&ReadTimeout,false,time.Second,"ReadTimeOutInSec")
	config.RegisterDurationConfigVariable(time.Duration(0),&ReadHeaderTimeout,false,time.Second,"ReadHeaderTimeoutInSec")
	config.RegisterDurationConfigVariable(time.Duration(10),&WriteTimeout,false,time.Second,"WriteTimeoutInSec")
	config.RegisterDurationConfigVariable(time.Duration(720),&IdleTimeout,false,time.Second,"IdleTimeoutInSec")
}

// MaxReqSize is the maximum request body size, in bytes, accepted by gateway web handlers
func (*HandleT) MaxReqSize() int {
	return maxReqSize
}

// IsEnableRateLimit is true if rate limiting is enabled on gateway
func IsEnableRateLimit() bool {
	return enableRateLimit
}

//SetEnableEventSchemasFeature overrides enableEventSchemasFeature configuration and returns previous value
func SetEnableEventSchemasFeature(b bool) bool {
	prev := enableEventSchemasFeature
	enableEventSchemasFeature = b
	return prev
}

//SetEnableRateLimit overrides enableRateLimit configuration and returns previous value
func SetEnableRateLimit(b bool) bool {
	prev := enableRateLimit
	enableRateLimit = b
	return prev
}

//SetEnableSuppressUserFeature overrides enableSuppressUserFeature configuration and returns previous value
func SetEnableSuppressUserFeature(b bool) bool {
	prev := enableSuppressUserFeature
	enableSuppressUserFeature = b
	return prev
}
