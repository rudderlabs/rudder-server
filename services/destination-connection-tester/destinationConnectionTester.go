package destinationConnectionTester

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/sjson"
)

var (
	configBackendURL string
	maxRetry         int
	retrySleep       time.Duration
	instanceID       string
)

type destinationConnectionTesterResponse struct {
	DestinationId string      `json:"destinationId"`
	Payload       interface{} `json:"payload"`
}

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	maxRetry = config.GetInt("DestinationConnectionTester.maxRetry", 3)
	retrySleep = config.GetDuration("DestinationConnectionTester.retrySleepInMS", time.Duration(100)) * time.Millisecond
	instanceID = config.GetEnv("INSTANCE_ID", "1")

}

func UploadDestinationConnectionTesterResonse(payload interface{}, destinationId string) {
	rawJSON, err := json.Marshal(payload)
	if err != nil {
		logger.Debugf(string(rawJSON))
		misc.AssertErrorIfDev(err)
		return
	}
	rawJSON, _ = sjson.SetBytes(rawJSON, "instanceId", misc.GetNodeID())
	var body interface{}
	_ = json.Unmarshal(rawJSON, &body)
	testConnectionPayload := destinationConnectionTesterResponse{
		DestinationId: destinationId,
		Payload:       body,
	}
	fmt.Println(testConnectionPayload)
	rawJSON, err = json.Marshal(testConnectionPayload)
	if err != nil {
		logger.Debugf(string(rawJSON))
		misc.AssertErrorIfDev(err)
		return
	}
	client := &http.Client{}
	url := fmt.Sprintf("%s/dataplane/testConnectionResponse", configBackendURL)

	retryCount := 0
	var resp *http.Response
	//Sending destination connection test response to Config Backend
	for {

		req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(rawJSON)))
		if err != nil {
			misc.AssertErrorIfDev(err)
			return
		}
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
		req.SetBasicAuth(config.GetWorkspaceToken(), "")

		resp, err = client.Do(req)
		if err != nil {
			logger.Error("Config Backend connection error", err)
			if retryCount > maxRetry {
				logger.Errorf("Max retries exceeded trying to connect to config backend")
				return
			}
			retryCount++
			time.Sleep(retrySleep)
			//Refresh the connection
			continue
		}
		break
	}

	if !(resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusBadRequest) {
		logger.Errorf("Response Error from Config Backend: Status: %v, Body: %v ", resp.StatusCode, resp.Body)
	}
}
