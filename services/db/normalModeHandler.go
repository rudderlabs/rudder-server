package db

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

var duration, maxCrashes int

func (handler *NormalModeHandler) RecordAppStart(currTime int64) {
	handler.recoveryData.StartTimes = append(handler.recoveryData.StartTimes, currTime)
	handler.recoveryData.ReadableStartTimes = append(handler.recoveryData.ReadableStartTimes, fmt.Sprint(time.Unix(currTime, 0)))
}

func (handler *NormalModeHandler) HasThresholdReached() bool {
	maxCrashes = config.GetIntVar(5, 1, "recovery.normal.crashThreshold")
	duration = config.GetIntVar(300, 1, "recovery.normal.durationInS")
	return CheckOccurrences(handler.recoveryData.StartTimes, maxCrashes, duration)
}

func (*NormalModeHandler) Handle() {}

type NormalModeHandler struct {
	recoveryData *RecoveryDataT
}
