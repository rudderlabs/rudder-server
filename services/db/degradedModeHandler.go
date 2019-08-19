package db

import (
	"github.com/rudderlabs/rudder-server/config"
)

func setupDegradedMode() {
	config.SetBool("enableProcessor", false)
	config.SetBool("enableRouter", false)
	config.SetBool("JobsDB.enableBackup", false)
}

func (handler *DegradedModeHandler) RecordAppStart(currTime int64) {
	handler.recoveryData.DegradedModeStartTimes = append(handler.recoveryData.DegradedModeStartTimes, currTime)
}

func (handler *DegradedModeHandler) HasThresholdReached() bool {
	maxCrashes := config.GetInt("recovery.degraded.crashThreshold", 5)
	duration := config.GetInt("recovery.degraded.durationInS", 300)
	return CheckOccurences(handler.recoveryData.DegradedModeStartTimes, maxCrashes, duration)
}

func (handler *DegradedModeHandler) Handle() {
	setupDegradedMode()
}

type DegradedModeHandler struct {
	recoveryData *RecoveryDataT
}
