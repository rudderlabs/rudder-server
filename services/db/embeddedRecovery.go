package db

import (
	"github.com/rudderlabs/rudder-server/rruntime"

	"github.com/rudderlabs/rudder-server/config"
)

// HandleEmbeddedRecovery decides the recovery Mode in which app should run based on earlier crashes
func HandleEmbeddedRecovery(forceNormal, forceDegraded bool, currTime int64, appType string) {
	enabled := config.GetBool("recovery.enabled", true)
	if !enabled {
		return
	}

	isForced := false

	forceMode := getForceRecoveryMode(forceNormal, forceDegraded)

	recoveryData := getRecoveryData()
	if forceMode != "" {
		isForced = true
		recoveryData.Mode = forceMode
	}
	recoveryHandler := NewRecoveryHandler(&recoveryData)

	if !isForced && recoveryHandler.HasThresholdReached() {
		pkgLogger.Info("DB Recovery: Moving to next State. Threshold reached for " + recoveryData.Mode)
		recoveryData.Mode = degradedMode
		recoveryHandler = NewRecoveryHandler(&recoveryData)
		alertOps(recoveryData.Mode)
	}

	recoveryHandler.RecordAppStart(currTime)
	saveRecoveryData(recoveryData)
	recoveryHandler.Handle()
	pkgLogger.Infof("Starting in %s mode", recoveryData.Mode)
	CurrentMode = recoveryData.Mode
	rruntime.Go(func() {
		sendRecoveryModeStat(appType)
	})
}
