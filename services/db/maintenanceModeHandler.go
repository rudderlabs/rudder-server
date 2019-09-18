package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	host, user, password, dbname string
	port                         int
)

func loadConfig() {
	host = config.GetEnv("JOBS_DB_HOST", "localhost")
	user = config.GetEnv("JOBS_DB_USER", "ubuntu")
	dbname = config.GetEnv("JOBS_DB_DB_NAME", "ubuntu")
	port, _ = strconv.Atoi(config.GetEnv("JOBS_DB_PORT", "5432"))
	password = config.GetEnv("JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
}

/*
ReplaceDB : Rename the OLD DB and create a new one.
Since we are not journaling, this should be idemponent
*/
func replaceDB() {
	loadConfig()
	logger.Info("Starting Maintenance Mode. Connecting to default DB 'postgres'")
	connInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		host, port, user, password)
	db, err := sql.Open("postgres", connInfo)
	misc.AssertError(err)
	defer db.Close()

	renameDBStatement := fmt.Sprintf("ALTER DATABASE %s RENAME TO %s",
		dbname, "original_"+dbname+"_"+strconv.FormatInt(time.Now().Unix(), 10))
	logger.Debug(renameDBStatement)
	_, err = db.Exec(renameDBStatement)

	// If we crashed earlier, after ALTER but before CREATE, we can create again
	// So just logging the error instead of assert
	if err != nil {
		logger.Error(err.Error())
	}

	createDBStatement := fmt.Sprintf("CREATE DATABASE %s", dbname)
	_, err = db.Exec(createDBStatement)
	misc.AssertError(err)
}

func (handler *MaintenanceModeHandler) RecordAppStart(currTime int64) {
	handler.recoveryData.MaintenanceModeStartTimes =
		append(handler.recoveryData.MaintenanceModeStartTimes, currTime)
}

func (handler *MaintenanceModeHandler) HasThresholdReached() bool {
	maxCrashes := config.GetInt("recovery.maintenance.crashThreshold", 5)
	duration := config.GetInt("recovery.maintenance.durationInS", 300)
	return CheckOccurences(handler.recoveryData.MaintenanceModeStartTimes, maxCrashes, duration)
}

func (handler *MaintenanceModeHandler) Handle() {
	replaceDB()
	degradedModeHandler := &DegradedModeHandler{recoveryData: handler.recoveryData}
	degradedModeHandler.Handle()
}

type MaintenanceModeHandler struct {
	recoveryData *RecoveryDataT
}
