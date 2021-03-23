package gateway

import (
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

func loadConfig() {
	//Port where GW is running
	webPort = config.GetInt("Gateway.webPort", 8080)
	//Number of incoming requests that are batched before handing off to write workers
	maxUserWebRequestBatchSize = config.GetInt("Gateway.maxUserRequestBatchSize", 128)
	//Number of userWorkerBatchRequest that are batched before initiating write
	maxDBBatchSize = config.GetInt("Gateway.maxDBBatchSize", 128)
	//Timeout after which batch is formed anyway with whatever requests
	//are available
	userWebRequestBatchTimeout = (config.GetDuration("Gateway.userWebRequestBatchTimeoutInMS", time.Duration(15)) * time.Millisecond)
	dbBatchWriteTimeout = (config.GetDuration("Gateway.dbBatchWriteTimeoutInMS", time.Duration(5)) * time.Millisecond)
	//Multiple workers are used to batch user web requests
	maxUserWebRequestWorkerProcess = config.GetInt("Gateway.maxUserWebRequestWorkerProcess", 64)
	//Multiple DB writers are used to write data to DB
	maxDBWriterProcess = config.GetInt("Gateway.maxDBWriterProcess", 256)
	// CustomVal is used as a key in the jobsDB customval column
	CustomVal = config.GetString("Gateway.CustomVal", "GW")
	// Maximum request size to gateway
	maxReqSize = config.GetInt("Gateway.maxReqSizeInKB", 4000) * 1024
	// Enable rate limit on incoming events. false by default
	enableRateLimit = config.GetBool("Gateway.enableRateLimit", false)
	// Enable suppress user feature. false by default
	enableSuppressUserFeature = config.GetBool("Gateway.enableSuppressUserFeature", false)
	// EventSchemas feature. false by default
	enableEventSchemasFeature = config.GetBool("EventSchemas.enableEventSchemasFeature", false)
	// Time period for diagnosis ticker
	diagnosisTickerTime = config.GetDuration("Diagnostics.gatewayTimePeriodInS", 60) * time.Second
	// Enables accepting requests without user id and anonymous id. This is added to prevent client 4xx retries.
	allowReqsWithoutUserIDAndAnonymousID = config.GetBool("Gateway.allowReqsWithoutUserIDAndAnonymousID", false)

	gwAllowPartialWriteWithErrors = config.GetBool("Gateway.allowPartialWriteWithErrors", true)

}

func gatewayReloadableConfig() {
	_userWebRequestBatchTimeout := (config.GetDuration("Gateway.userWebRequestBatchTimeoutInMS", time.Duration(15)) * time.Millisecond)
	if _userWebRequestBatchTimeout != userWebRequestBatchTimeout {
		userWebRequestBatchTimeout = _userWebRequestBatchTimeout
		pkgLogger.Info("Gateway.userWebRequestBatchTimeoutInMS changes to %s", userWebRequestBatchTimeout)
	}
	_dbBatchWriteTimeout := (config.GetDuration("Gateway.dbBatchWriteTimeoutInMS", time.Duration(5)) * time.Millisecond)
	if _dbBatchWriteTimeout != dbBatchWriteTimeout {
		dbBatchWriteTimeout = _dbBatchWriteTimeout
		pkgLogger.Info("Gateway.dbBatchWriteTimeoutInMS changes to %s", userWebRequestBatchTimeout)
	}
	_maxReqSize := config.GetInt("Gateway.maxReqSizeInKB", 4000) * 1024
	if _maxReqSize != maxReqSize {
		maxReqSize = _maxReqSize
		pkgLogger.Info("Gateway.maxReqSizeInKB changes to %s", maxReqSize)
	}
	_enableRateLimit := config.GetBool("Gateway.enableRateLimit", false)
	if _enableRateLimit != enableRateLimit {
		enableRateLimit = _enableRateLimit
		pkgLogger.Info("Gateway.enableRateLimit changes to %s", enableRateLimit)
	}
	_allowReqsWithoutUserIDAndAnonymousID := config.GetBool("Gateway.allowReqsWithoutUserIDAndAnonymousID", false)
	if _allowReqsWithoutUserIDAndAnonymousID != allowReqsWithoutUserIDAndAnonymousID {
		allowReqsWithoutUserIDAndAnonymousID = _allowReqsWithoutUserIDAndAnonymousID
		pkgLogger.Info("Gateway.allowReqsWithoutUserIDAndAnonymousID changes to %s", allowReqsWithoutUserIDAndAnonymousID)
	}
	_gwAllowPartialWriteWithErrors := config.GetBool("Gateway.allowPartialWriteWithErrors", true)
	if _gwAllowPartialWriteWithErrors != gwAllowPartialWriteWithErrors {
		gwAllowPartialWriteWithErrors = _gwAllowPartialWriteWithErrors
		pkgLogger.Info("Gateway.gwAllowPartialWriteWithErrors changes to %s", gwAllowPartialWriteWithErrors)
	}

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
