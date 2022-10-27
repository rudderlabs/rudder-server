package db

import (
	"github.com/rudderlabs/rudder-server/rruntime"

	"github.com/rudderlabs/rudder-server/config"
)

// HandleNullRecovery decides the recovery Mode (normal/degraded) in which app should run
func HandleNullRecovery(forceNormal, forceDegraded bool, currTime int64, appType string) {
	enabled := config.GetBool("recovery.enabled", true)
	if !enabled {
		return
	}

	forceMode := getForceRecoveryMode(forceNormal, forceDegraded)
	recoveryData := getRecoveryData()
	if forceMode != "" {
		recoveryData.Mode = forceMode
	} else if recoveryData.Mode != normalMode {
		// If no mode is forced (through env or cli) then setting server mode to normal.
		recoveryData.Mode = normalMode
	}
	recoveryHandler := NewRecoveryHandler(&recoveryData)

	recoveryHandler.RecordAppStart(currTime)
	saveRecoveryData(recoveryData)
	recoveryHandler.Handle()
	pkgLogger.Infof("Starting in %s mode", recoveryData.Mode)
	CurrentMode = recoveryData.Mode
	rruntime.Go(func() {
		sendRecoveryModeStat(appType)
	})
}
