package db

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/rruntime"
)

// HandleNullRecovery decides the recovery Mode (normal/degraded) in which app should run
func HandleNullRecovery(forceNormal, forceDegraded bool, currTime int64, appType string) error {
	forceMode := getForceRecoveryMode(forceNormal, forceDegraded)
	recoveryData, err := getRecoveryData()
	if err != nil {
		return fmt.Errorf("getting recovery data: %w", err)
	}
	if forceMode != "" {
		recoveryData.Mode = forceMode
	} else if recoveryData.Mode != normalMode {
		// If no mode is forced (through env or cli) then setting server mode to normal.
		recoveryData.Mode = normalMode
	}
	recoveryHandler := NewRecoveryHandler(&recoveryData)

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
