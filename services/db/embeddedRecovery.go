package db

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/rruntime"
)

// HandleEmbeddedRecovery decides the recovery Mode in which app should run based on earlier crashes
func HandleEmbeddedRecovery(forceNormal, forceDegraded bool, currTime int64, appType string) error {
	isForced := false
	forceMode := getForceRecoveryMode(forceNormal, forceDegraded)

	recoveryData, err := getRecoveryData()
	if err != nil {
		return fmt.Errorf("getting recovery data: %w", err)
	}
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
	if err := saveRecoveryData(recoveryData); err != nil {
		return fmt.Errorf("saving recovery data: %w", err)
	}
	recoveryHandler.Handle()
	pkgLogger.Infof("Starting in %s mode", recoveryData.Mode)
	CurrentMode = recoveryData.Mode
	rruntime.Go(func() {
		sendRecoveryModeStat(appType)
	})
	return nil
}
