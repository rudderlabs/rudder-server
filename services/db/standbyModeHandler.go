package db

import (
	"fmt"
	"time"
)

func (handler *StandByModeHandler) RecordAppStart(currTime int64) {
	handler.recoveryData.StandbyModeStartTimes = append(handler.recoveryData.StandbyModeStartTimes, currTime)
	handler.recoveryData.ReadableStandbyModeStartTimes = append(handler.recoveryData.ReadableStandbyModeStartTimes, fmt.Sprint(time.Unix(currTime, 0)))

}

func (handler *StandByModeHandler) HasThresholdReached() bool {
	return false
}

func (handler *StandByModeHandler) Handle() {
}

type StandByModeHandler struct {
	recoveryData *RecoveryDataT
}
