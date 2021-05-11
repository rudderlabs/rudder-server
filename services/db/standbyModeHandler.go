package db

import (
	"fmt"
	"time"
)

func (handler *StandByModeHandler) RecordAppStart(currTime int64) {
	handler.recoveryData.StartTimes = append(handler.recoveryData.StartTimes, currTime)
	handler.recoveryData.ReadableStartTimes = append(handler.recoveryData.ReadableStartTimes, fmt.Sprint(time.Unix(currTime, 0)))

}

func (handler *StandByModeHandler) HasThresholdReached() bool {
	return false
}

func (handler *StandByModeHandler) Handle() {
}

type StandByModeHandler struct {
	recoveryData *RecoveryDataT
}
