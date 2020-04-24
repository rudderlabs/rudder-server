package db

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

func setupMigrationMode() {
	config.SetBool("enableProcessor", false)
	config.SetBool("enableRouter", false)
	config.SetBool("enableMigrator", true)
}

func (handler *MigrationModeHandler) RecordAppStart(currTime int64) {
	handler.recoveryData.MigrationModeStartTimes = append(handler.recoveryData.MigrationModeStartTimes, currTime)
	handler.recoveryData.ReadableMigrationModeStartTimes = append(handler.recoveryData.ReadableMigrationModeStartTimes, fmt.Sprint(time.Unix(currTime, 0)))

}

func (handler *MigrationModeHandler) HasThresholdReached() bool {
	return false
}

func (handler *MigrationModeHandler) Handle() {
	setupMigrationMode()
}

type MigrationModeHandler struct {
	recoveryData *RecoveryDataT
}
