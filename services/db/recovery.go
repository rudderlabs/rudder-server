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
	normalMode      = "normal"
	degradedMode    = "degraded"
	maintenanceMode = "maintenance"
)

type RecoveryHandler interface {
	RecordAppStart(int64)
	HasThresholdReached() bool
	Handle()
}

var CurrentMode string = normalMode // default mode

// RecoveryDataT : DS to store the recovery process data
type RecoveryDataT struct {
	StartTimes                        []int64
	ReadableStartTimes                []string
	DegradedModeStartTimes            []int64
	ReadableDegradedModeStartTimes    []string
	MaintenanceModeStartTimes         []int64
	ReadableMaintenanceModeStartTimes []string
	Mode                              string
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
		panic(err)
	}

	return recoveryData
}

func saveRecoveryData(recoveryData RecoveryDataT) {
	recoveryDataJSON, err := json.MarshalIndent(&recoveryData, "", " ")
	storagePath := config.GetString("recovery.storagePath", "/tmp/recovery_data.json")
	err = ioutil.WriteFile(storagePath, recoveryDataJSON, 0644)
	if err != nil {
		panic(err)
	}
}

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

func getForceRecoveryMode(forceNormal bool, forceDegraded bool, forceMaintenance bool) string {
	switch {
	case forceNormal:
		return normalMode
	case forceDegraded:
		return degradedMode
	case forceMaintenance:
		return maintenanceMode
	}
	return ""

}

func getNextMode(currentMode string) string {
	switch currentMode {
	case normalMode:
		return degradedMode
	case degradedMode:
		return maintenanceMode
	case maintenanceMode:
		return ""
	}
	return ""
}

func NewRecoveryHandler(recoveryData *RecoveryDataT) RecoveryHandler {
	var recoveryHandler RecoveryHandler
	switch recoveryData.Mode {
	case normalMode:
		recoveryHandler = &NormalModeHandler{recoveryData: recoveryData}
	case degradedMode:
		recoveryHandler = &DegradedModeHandler{recoveryData: recoveryData}
	case maintenanceMode:
		recoveryHandler = &MaintenanceModeHandler{recoveryData: recoveryData}
	default:
		panic("Invalid Recovery Mode " + recoveryData.Mode)
	}
	return recoveryHandler
}

func alertOps(mode string) {
	instanceName := config.GetEnv("INSTANCE_ID", "")

	alertManager, err := alert.New()
	if err != nil {
		logger.Errorf("Unable to initialize the alertManager: %s", err.Error())
	} else {
		alertManager.Alert(fmt.Sprintf("Dataplane server %s entered %s mode", instanceName, mode))
	}
}

func HandleRecovery(forceNormal bool, forceDegraded bool, forceMaintenance bool, currTime int64) {

	enabled := config.GetBool("recovery.enabled", false)
	if !enabled {
		return
	}
	forceMode := getForceRecoveryMode(forceNormal, forceDegraded, forceMaintenance)
	isForced := false

	recoveryData := getRecoveryData()
	if forceMode != "" {
		isForced = true
		recoveryData.Mode = forceMode
	}
	recoveryHandler := NewRecoveryHandler(&recoveryData)

	if !isForced && recoveryHandler.HasThresholdReached() {
		logger.Info("DB Recovery: Moving to next State. Threshold reached for " + recoveryData.Mode)
		nextMode := getNextMode(recoveryData.Mode)
		if nextMode == "" {
			logger.Fatal("Threshold reached for maintenance mode")
			panic("Not a valid mode")
		} else {
			recoveryData.Mode = nextMode
			recoveryHandler = NewRecoveryHandler(&recoveryData)
			alertOps(recoveryData.Mode)
		}
	}

	recoveryModeStat := stats.NewStat("recovery.mode_normal", stats.GaugeType)
	if recoveryData.Mode != normalMode {
		if recoveryData.Mode == degradedMode {
			recoveryModeStat.Gauge(2)
		} else if recoveryData.Mode == maintenanceMode {
			recoveryModeStat.Gauge(3)
		}
	} else {
		recoveryModeStat.Gauge(1)
	}
	recoveryHandler.RecordAppStart(currTime)
	saveRecoveryData(recoveryData)
	recoveryHandler.Handle()
	logger.Infof("Starting in %s mode", recoveryData.Mode)
	CurrentMode = recoveryData.Mode
}
