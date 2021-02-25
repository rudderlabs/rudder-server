package db

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/rudderlabs/rudder-server/services/alert"
	"github.com/rudderlabs/rudder-server/services/stats"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	normalMode    = "normal"
	degradedMode  = "degraded"
	migrationMode = "migration"
)

type RecoveryHandler interface {
	RecordAppStart(int64)
	HasThresholdReached() bool
	Handle()
}

var CurrentMode string = normalMode // default mode

// RecoveryDataT : DS to store the recovery process data
type RecoveryDataT struct {
	StartTimes                      []int64
	ReadableStartTimes              []string
	DegradedModeStartTimes          []int64
	ReadableDegradedModeStartTimes  []string
	MigrationModeStartTimes         []int64
	ReadableMigrationModeStartTimes []string
	Mode                            string
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("db").Child("recovery")
}

func getRecoveryData() RecoveryDataT {
	storagePath := config.GetString("recovery.storagePath", "/tmp/recovery_data.json")
	data, err := ioutil.ReadFile(storagePath)
	if os.IsNotExist(err) {
		defaultRecoveryJSON := "{\"mode\":\"" + normalMode + "\"}"
		data = []byte(defaultRecoveryJSON)
	} else {
		if err != nil {
			panic(err)
		}
	}
	var recoveryData RecoveryDataT
	err = json.Unmarshal(data, &recoveryData)
	if err != nil {
		pkgLogger.Errorf("Failed to Unmarshall %s. Error:  %v", storagePath, err)
		if renameErr := os.Rename(storagePath, fmt.Sprintf("%s.bkp", storagePath)); renameErr != nil {
			pkgLogger.Errorf("Failed to back up: %s. Error: %v", storagePath, err)
		}
		recoveryData = RecoveryDataT{Mode: normalMode}
	}
	return recoveryData
}

func saveRecoveryData(recoveryData RecoveryDataT) {
	recoveryDataJSON, err := json.MarshalIndent(&recoveryData, "", " ")
	if err != nil {
		panic(err)
	}
	storagePath := config.GetString("recovery.storagePath", "/tmp/recovery_data.json")
	err = ioutil.WriteFile(storagePath, recoveryDataJSON, 0644)
	if err != nil {
		panic(err)
	}
}

// IsNormalMode checks if the current mode is normal
func IsNormalMode() bool {
	return CurrentMode == normalMode
}

/*
CheckOccurences : check if this occurred numTimes times in numSecs seconds
*/
func CheckOccurences(occurences []int64, numTimes int, numSecs int) (occurred bool) {

	sort.Slice(occurences, func(i, j int) bool {
		return occurences[i] < occurences[j]
	})

	recentOccurences := 0
	checkPointTime := time.Now().Unix() - int64(numSecs)

	for i := len(occurences) - 1; i >= 0; i-- {
		if occurences[i] < checkPointTime {
			break
		}
		recentOccurences++
	}
	if recentOccurences >= numTimes {
		occurred = true
	}
	return
}

func getForceRecoveryMode(forceNormal bool, forceDegraded bool) string {
	switch {
	case forceNormal:
		return normalMode
	case forceDegraded:
		return degradedMode
	}
	return ""

}

func getNextMode(currentMode string) string {
	switch currentMode {
	case normalMode:
		return degradedMode
	case degradedMode:
		return degradedMode
	case migrationMode: //Staying in the migrationMode forever on repeated restarts.
		return migrationMode
	}

	return degradedMode
}

func NewRecoveryHandler(recoveryData *RecoveryDataT) RecoveryHandler {
	var recoveryHandler RecoveryHandler
	switch recoveryData.Mode {
	case normalMode:
		recoveryHandler = &NormalModeHandler{recoveryData: recoveryData}
	case degradedMode:
		recoveryHandler = &DegradedModeHandler{recoveryData: recoveryData}
	case migrationMode:
		recoveryHandler = &MigrationModeHandler{recoveryData: recoveryData}
	default:
		//If the recovery mode is not one of the above modes, defaulting to degraded mode.
		pkgLogger.Info("DB Recovery: Invalid recovery mode. Defaulting to degraded mode.")
		recoveryData.Mode = degradedMode
		recoveryHandler = &DegradedModeHandler{recoveryData: recoveryData}
	}
	return recoveryHandler
}

func alertOps(mode string) {
	instanceName := config.GetEnv("INSTANCE_ID", "")

	alertManager, err := alert.New()
	if err != nil {
		pkgLogger.Errorf("Unable to initialize the alertManager: %s", err.Error())
	} else {
		alertManager.Alert(fmt.Sprintf("Dataplane server %s entered %s mode", instanceName, mode))
	}
}

// sendRecoveryModeStat sends the recovery mode metric every 10 seconds
func sendRecoveryModeStat(appType string) {
	recoveryModeStat := stats.NewTaggedStat("recovery.mode_normal", stats.GaugeType, stats.Tags{
		"appType": appType,
	})
	for {
		time.Sleep(10 * time.Second)
		switch CurrentMode {
		case normalMode:
			recoveryModeStat.Gauge(1)
		case degradedMode:
			recoveryModeStat.Gauge(2)
		case migrationMode:
			recoveryModeStat.Gauge(4)
		}
	}
}
