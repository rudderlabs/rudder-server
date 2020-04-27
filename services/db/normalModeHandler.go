package db

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

func (handler *NormalModeHandler) RecordAppStart(currTime int64) {
	handler.recoveryData.StartTimes = append(handler.recoveryData.StartTimes, currTime)
	handler.recoveryData.ReadableStartTimes = append(handler.recoveryData.ReadableStartTimes, fmt.Sprint(time.Unix(currTime, 0)))

}

func (handler *NormalModeHandler) HasThresholdReached() bool {
	maxCrashes := config.GetInt("recovery.normal.crashThreshold", 5)
	duration := config.GetInt("recovery.normal.durationInS", 300)
	return CheckOccurences(handler.recoveryData.StartTimes, maxCrashes, duration)
}

func (handler *NormalModeHandler) Handle() {
}

type NormalModeHandler struct {
	recoveryData *RecoveryDataT
}
