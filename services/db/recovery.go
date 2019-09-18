package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
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

// RecoveryDataT : DS to store the recovery process data
type RecoveryDataT struct {
	StartTimes                []int64
	DegradedModeStartTimes    []int64
	MaintenanceModeStartTimes []int64
	Mode                      string
}

func getRecoveryData() RecoveryDataT {
	storagePath := config.GetString("recovery.storagePath", "/tmp/recovery_data.json")
	data, err := ioutil.ReadFile(storagePath)
	if os.IsNotExist(err) {
		defaultRecoveryJSON := "{\"mode\":\"" + normalMode + "\"}"
		data = []byte(defaultRecoveryJSON)
	} else {
		misc.AssertError(err)
	}

	var recoveryData RecoveryDataT
	err = json.Unmarshal(data, &recoveryData)
	misc.AssertError(err)

	return recoveryData
}

func saveRecoveryData(recoveryData RecoveryDataT) {
	recoveryDataJSON, err := json.MarshalIndent(&recoveryData, "", " ")
	storagePath := config.GetString("recovery.storagePath", "/tmp/recovery_data.json")
	err = ioutil.WriteFile(storagePath, recoveryDataJSON, 0644)
	misc.AssertError(err)
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
	instanceName := config.GetEnv("INSTANCE_NAME", "")
	url := config.GetEnv("OPS_ALERT_URL", "")
	event := map[string]interface{}{
		"message_type":  "CRITICAL",
		"entity_id":     fmt.Sprintf("%s-data-plane-%s-mode", instanceName, mode),
		"state_message": fmt.Sprintf("Dataplane server %s entered %s mode", instanceName, mode),
	}
	eventJSON, _ := json.Marshal(event)
	client := &http.Client{}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(eventJSON))
	// Not handling errors when sending alert to victorops
	if err == nil {
		defer resp.Body.Close()
	}
}

func HandleRecovery(forceNormal bool, forceDegraded bool, forceMaintenance bool) {

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
			// If we can't recover in maintenance mode, just panic
			panic("Not a valid mode")
		}
		recoveryData.Mode = nextMode
		recoveryHandler = NewRecoveryHandler(&recoveryData)
	}

	if recoveryData.Mode != normalMode {
		alertOps(recoveryData.Mode)
	}
	currTime := time.Now().Unix()
	recoveryHandler.RecordAppStart(currTime)
	saveRecoveryData(recoveryData)
	recoveryHandler.Handle()
	logger.Infof("Starting in %s mode\n", recoveryData.Mode)
}
