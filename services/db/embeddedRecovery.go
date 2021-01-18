package db

import (
	"github.com/rudderlabs/rudder-server/rruntime"

	"github.com/rudderlabs/rudder-server/config"
)

// HandleEmbeddedRecovery decides the recovery Mode in which app should run based on earlier crashes
func HandleEmbeddedRecovery(forceNormal bool, forceDegraded bool, forceMigrationMode string, currTime int64, appType string) {

	enabled := config.GetBool("recovery.enabled", false)
	if !enabled {
		return
	}

	var forceMode string
	isForced := false

	//If MIGRATION_MODE environment variable is present and is equal to "import", "export", "import-export", then server mode is forced to be Migration.
	if IsValidMigrationMode(forceMigrationMode) {
		pkgLogger.Info("Setting server mode to Migration. If this is not intended remove environment variables related to Migration.")
		forceMode = migrationMode
	} else {
		forceMode = getForceRecoveryMode(forceNormal, forceDegraded)
	}

	recoveryData := getRecoveryData()
	if forceMode != "" {
		isForced = true
		recoveryData.Mode = forceMode
	} else {
		//If no mode is forced (through env or cli) and if previous mode is migration then setting server mode to normal.
		if recoveryData.Mode == migrationMode {
			recoveryData.Mode = normalMode
		}
	}
	recoveryHandler := NewRecoveryHandler(&recoveryData)

	if !isForced && recoveryHandler.HasThresholdReached() {
		pkgLogger.Info("DB Recovery: Moving to next State. Threshold reached for " + recoveryData.Mode)
		nextMode := getNextMode(recoveryData.Mode)
		recoveryData.Mode = nextMode
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
