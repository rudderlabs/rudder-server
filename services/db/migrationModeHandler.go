package db

import (
	"fmt"
	"time"
)

const (
	EXPORT        = "export"
	IMPORT        = "import"
	IMPORT_EXPORT = "import-export"
)

func (handler *MigrationModeHandler) RecordAppStart(currTime int64) {
	handler.recoveryData.MigrationModeStartTimes = append(handler.recoveryData.MigrationModeStartTimes, currTime)
	handler.recoveryData.ReadableMigrationModeStartTimes = append(handler.recoveryData.ReadableMigrationModeStartTimes, fmt.Sprint(time.Unix(currTime, 0)))

}

func (handler *MigrationModeHandler) HasThresholdReached() bool {
	return false
}

func (handler *MigrationModeHandler) Handle() {
}

type MigrationModeHandler struct {
	recoveryData *RecoveryDataT
}
