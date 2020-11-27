package db

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

func setupDegradedMode() {
	config.SetBool("enableProcessor", false)
	config.SetBool("enableRouter", false)
}

func (handler *DegradedModeHandler) RecordAppStart(currTime int64) {
	handler.recoveryData.DegradedModeStartTimes = append(handler.recoveryData.DegradedModeStartTimes, currTime)
	handler.recoveryData.ReadableDegradedModeStartTimes = append(handler.recoveryData.ReadableDegradedModeStartTimes, fmt.Sprint(time.Unix(currTime, 0)))

}

func (handler *DegradedModeHandler) HasThresholdReached() bool {
	return false
}

func (handler *DegradedModeHandler) Handle() {
	setupDegradedMode()
}

type DegradedModeHandler struct {
	recoveryData *RecoveryDataT
}
