package db

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/alert"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	normalMode   = "normal"
	degradedMode = "degraded"
)

type RecoveryHandler interface {
	RecordAppStart(int64)
	HasThresholdReached() bool
	Handle()
}

var (
	CurrentMode = normalMode // default mode
	storagePath string
)

// RecoveryDataT : DS to store the recovery process data
type RecoveryDataT struct {
	StartTimes                     []int64
	ReadableStartTimes             []string
	DegradedModeStartTimes         []int64
	ReadableDegradedModeStartTimes []string
	Mode                           string
}

var pkgLogger logger.Logger

func Init() {
	storagePath = config.GetStringVar("/tmp/recovery_data.json", "recovery.storagePath")
	pkgLogger = logger.NewLogger().Child("db").Child("recovery")
}

func getRecoveryData() (RecoveryDataT, error) {
	var recoveryData RecoveryDataT

	data, err := os.ReadFile(storagePath)
	if os.IsNotExist(err) {
		defaultRecoveryJSON := "{\"mode\":\"" + normalMode + "\"}"
		data = []byte(defaultRecoveryJSON)
	} else if err != nil {
		return recoveryData, err
	}

	err = json.Unmarshal(data, &recoveryData)
	if err != nil {
		pkgLogger.Errorf("Failed to Unmarshall %s. Error:  %v", storagePath, err)
		if renameErr := os.Rename(storagePath, fmt.Sprintf("%s.bkp", storagePath)); renameErr != nil {
			pkgLogger.Errorf("Failed to back up: %s. Error: %v", storagePath, err)
		}
		recoveryData = RecoveryDataT{Mode: normalMode}
	}
	return recoveryData, nil
}

func saveRecoveryData(recoveryData RecoveryDataT) error {
	recoveryDataJSON, err := json.MarshalIndent(&recoveryData, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(storagePath, recoveryDataJSON, 0o644)
}

// IsNormalMode checks if the current mode is normal
func IsNormalMode() bool {
	return CurrentMode == normalMode
}

// CheckOccurrences : check if this occurred numTimes times in numSecs seconds
func CheckOccurrences(occurrences []int64, numTimes, numSecs int) (occurred bool) {
	sort.Slice(occurrences, func(i, j int) bool {
		return occurrences[i] < occurrences[j]
	})

	recentOccurrences := 0
	checkPointTime := time.Now().Unix() - int64(numSecs)

	for i := len(occurrences) - 1; i >= 0; i-- {
		if occurrences[i] < checkPointTime {
			break
		}
		recentOccurrences++
	}
	if recentOccurrences >= numTimes {
		occurred = true
	}
	return
}

func getForceRecoveryMode(forceNormal, forceDegraded bool) string {
	switch {
	case forceNormal:
		return normalMode
	case forceDegraded:
		return degradedMode
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
	default:
		// If the recovery mode is not one of the above modes, defaulting to degraded mode.
		pkgLogger.Info("DB Recovery: Invalid recovery mode. Defaulting to degraded mode.")
		recoveryData.Mode = degradedMode
		recoveryHandler = &DegradedModeHandler{recoveryData: recoveryData}
	}
	return recoveryHandler
}

func alertOps(mode string) {
	instanceName := config.GetString("INSTANCE_ID", "")

	alertManager, err := alert.New()
	if err != nil {
		pkgLogger.Errorf("Unable to initialize the alertManager: %s", err.Error())
	} else {
		alertManager.Alert(fmt.Sprintf("Dataplane server %s entered %s mode", instanceName, mode))
	}
}

// sendRecoveryModeStat sends the recovery mode metric every 10 seconds
func sendRecoveryModeStat(appType string) {
	recoveryModeStat := stats.Default.NewTaggedStat("recovery.mode_normal", stats.GaugeType, stats.Tags{
		"appType": appType,
	})
	for {
		time.Sleep(10 * time.Second)
		switch CurrentMode {
		case normalMode:
			recoveryModeStat.Gauge(1)
		case degradedMode:
			recoveryModeStat.Gauge(2)
		}
	}
}
