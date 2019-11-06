package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

func (ops *VictorOps) Alert(message string) {

	event := map[string]interface{}{
		"message_type":  "CRITICAL",
		"entity_id":     ops.instanceName,
		"state_message": message,
	}
	eventJSON, _ := json.Marshal(event)
	client := &http.Client{}
	victorOpsUrl := fmt.Sprintf("https://alert.victorops.com/integrations/generic/20131114/alert/%s/rudderRecovery", ops.routingKey)
	resp, err := client.Post(victorOpsUrl, "application/json", bytes.NewBuffer(eventJSON))
	// Not handling errors when sending alert to victorops
	if err != nil {
		logger.Errorf("Alert: Failed to alert service: %s", err.Error())
		return
	}

	if resp.StatusCode != 200 || resp.StatusCode != 202 {
		logger.Errorf("Alert: Got error response %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	logger.Info("Alert: Successful %s", string(body))
}

type VictorOps struct {
	routingKey   string
	instanceName string
}
