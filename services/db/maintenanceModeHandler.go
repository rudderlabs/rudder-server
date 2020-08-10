package db

import (
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func (handler *MaintenanceModeHandler) RecordAppStart(currTime int64) {
	handler.recoveryData.MaintenanceModeStartTimes =
		append(handler.recoveryData.MaintenanceModeStartTimes, currTime)
	handler.recoveryData.ReadableMaintenanceModeStartTimes = append(handler.recoveryData.ReadableMaintenanceModeStartTimes, fmt.Sprint(time.Unix(currTime, 0)))

}

func (handler *MaintenanceModeHandler) HasThresholdReached() bool {
	maxCrashes := config.GetInt("recovery.maintenance.crashThreshold", 5)
	duration := config.GetInt("recovery.maintenance.durationInS", 300)
	return CheckOccurences(handler.recoveryData.MaintenanceModeStartTimes, maxCrashes, duration)
}

func (handler *MaintenanceModeHandler) Handle() {
	logger.Info("Starting Maintenance Mode. Connecting to default DB 'postgres'")
	dbname := config.GetEnv("JOBS_DB_DB_NAME", "ubuntu")
	targetName := "original_" + dbname + "_" + strconv.FormatInt(time.Now().Unix(), 10)
	misc.ReplaceDB(dbname, targetName)
	loadDBConfig(targetName)
	dbHandle := createDBConnection()
	createWorkspaceTable(dbHandle)
	token, created_at, parameters := getWorkspaceData(targetName)
	insertWorkspaceParams := fmt.Sprintf(`INSERT INTO workspace (token, created_at, parameters)
									   VALUES ($1, $2, $3)`)
	stmt, err := dbHandle.Prepare(insertWorkspaceParams)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(token, created_at, parameters)
	if err != nil {
		panic(err)
	}
	degradedModeHandler := &DegradedModeHandler{recoveryData: handler.recoveryData}
	degradedModeHandler.Handle()
}

type MaintenanceModeHandler struct {
	recoveryData *RecoveryDataT
}
