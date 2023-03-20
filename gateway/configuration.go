package gateway

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func loadConfig() {
	// Port where GW is running
	config.RegisterIntConfigVariable(8080, &webPort, false, 1, "Gateway.webPort")
	// Port where AdminHandler is running
	config.RegisterIntConfigVariable(8089, &adminWebPort, false, 1, "Gateway.adminWebPort")
	// Number of incoming requests that are batched before handing off to write workers
	config.RegisterIntConfigVariable(128, &maxUserWebRequestBatchSize, false, 1, "Gateway.maxUserRequestBatchSize")
	// Number of userWorkerBatchRequest that are batched before initiating write
	config.RegisterIntConfigVariable(128, &maxDBBatchSize, false, 1, "Gateway.maxDBBatchSize")
	// Timeout after which batch is formed anyway with whatever requests
	// are available
	config.RegisterDurationConfigVariable(15, &userWebRequestBatchTimeout, true, time.Millisecond, []string{"Gateway.userWebRequestBatchTimeout", "Gateway.userWebRequestBatchTimeoutInMS"}...)
	config.RegisterDurationConfigVariable(5, &dbBatchWriteTimeout, true, time.Millisecond, []string{"Gateway.dbBatchWriteTimeout", "Gateway.dbBatchWriteTimeoutInMS"}...)
	// Multiple workers are used to batch user web requests
	config.RegisterIntConfigVariable(64, &maxUserWebRequestWorkerProcess, false, 1, "Gateway.maxUserWebRequestWorkerProcess")
	// Multiple DB writers are used to write data to DB
	config.RegisterIntConfigVariable(256, &maxDBWriterProcess, false, 1, "Gateway.maxDBWriterProcess")
	// CustomVal is used as a key in the jobsDB customval column
	config.RegisterStringConfigVariable("GW", &CustomVal, false, "Gateway.CustomVal")
	// Maximum request size to gateway
	config.RegisterIntConfigVariable(4000, &maxReqSize, true, 1024, "Gateway.maxReqSizeInKB")
	// Enable rate limit on incoming events. false by default
	config.RegisterBoolConfigVariable(false, &enableRateLimit, true, "Gateway.enableRateLimit")
	// Enable suppress user feature. false by default
	config.RegisterBoolConfigVariable(true, &enableSuppressUserFeature, false, "Gateway.enableSuppressUserFeature")
	// EventSchemas feature. false by default
	config.RegisterBoolConfigVariable(false, &enableEventSchemasFeature, false, "EventSchemas.enableEventSchemasFeature")
	// Time period for diagnosis ticker
	config.RegisterDurationConfigVariable(60, &diagnosisTickerTime, false, time.Second, []string{"Diagnostics.gatewayTimePeriod", "Diagnostics.gatewayTimePeriodInS"}...)
	// Enables accepting requests without user id and anonymous id. This is added to prevent client 4xx retries.
	config.RegisterBoolConfigVariable(false, &allowReqsWithoutUserIDAndAnonymousID, true, "Gateway.allowReqsWithoutUserIDAndAnonymousID")
	config.RegisterBoolConfigVariable(true, &gwAllowPartialWriteWithErrors, true, "Gateway.allowPartialWriteWithErrors")
	config.RegisterDurationConfigVariable(0, &ReadTimeout, false, time.Second, []string{"ReadTimeout", "ReadTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(0, &ReadHeaderTimeout, false, time.Second, []string{"ReadHeaderTimeout", "ReadHeaderTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(10, &WriteTimeout, false, time.Second, []string{"WriteTimeout", "WriteTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(720, &IdleTimeout, false, time.Second, []string{"IdleTimeout", "IdleTimeoutInSec"}...)
	config.RegisterIntConfigVariable(524288, &maxHeaderBytes, false, 1, "MaxHeaderBytes")
	// if set to '0', it means disabled.
	config.RegisterIntConfigVariable(50000, &maxConcurrentRequests, false, 1, "Gateway.maxConcurrentRequests")
}

// MaxReqSize is the maximum request body size, in bytes, accepted by gateway web handlers
func (*HandleT) MaxReqSize() int {
	return maxReqSize
}

// IsEnableRateLimit is true if rate limiting is enabled on gateway
func IsEnableRateLimit() bool {
	return enableRateLimit
}

// SetEnableEventSchemasFeature overrides enableEventSchemasFeature configuration and returns previous value
func SetEnableEventSchemasFeature(b bool) bool {
	prev := enableEventSchemasFeature
	enableEventSchemasFeature = b
	return prev
}

// SetEnableRateLimit overrides enableRateLimit configuration and returns previous value
func SetEnableRateLimit(b bool) bool {
	prev := enableRateLimit
	enableRateLimit = b
	return prev
}

// SetEnableSuppressUserFeature overrides enableSuppressUserFeature configuration and returns previous value
func SetEnableSuppressUserFeature(b bool) bool {
	prev := enableSuppressUserFeature
	enableSuppressUserFeature = b
	return prev
}
