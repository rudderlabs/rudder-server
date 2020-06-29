package destination_connection_tester

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	configBackendURL string
	maxRetry         int
	retrySleep       time.Duration
	instanceID       string
)

const destinationConnectionTesterEndpoint = "dataplane/testConnectionResponse"

type DestinationConnectionTesterResponse struct {
	DestinationId string    `json:"destinationId"`
	InstanceId    string    `json:instanceId`
	Error         string    `json: error`
	TestedAt      time.Time `json:testedAt`
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

func UploadDestinationConnectionTesterResponse(payload DestinationConnectionTesterResponse) {
	payload.InstanceId = misc.GetNodeID()
	url := fmt.Sprintf("%s/%s", configBackendURL, destinationConnectionTesterEndpoint)
	makePostRequest(url, payload)
}

func makePostRequest(url string, payload interface{}) error {
	rawJSON, err := json.Marshal(payload)
	if err != nil {
		logger.Debugf(string(rawJSON))
		misc.AssertErrorIfDev(err)
		return err
	}
	client := &http.Client{}
	retryCount := 0
	var resp *http.Response
	//Sending destination connection test response to Config Backend
	for {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(rawJSON))
		if err != nil {
			misc.AssertErrorIfDev(err)
			return err
		}
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
		req.SetBasicAuth(config.GetWorkspaceToken(), "")

		resp, err = client.Do(req)
		if err != nil {
			logger.Error("Config Backend connection error", err)
			if retryCount > maxRetry {
				logger.Errorf("Max retries exceeded trying to connect to config backend")
				return err
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
	return nil
}
