package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

var victorOpsMessageTypesMap = map[string]string{
	"CRITICAL": "CRITICAL",
	"RECOVERY": "RECOVERY",
}

func (ops *VictorOps) messageType(payload PayloadT) string {
	var messageType string
	var ok bool
	if messageType, ok = victorOpsMessageTypesMap[payload.MessageType]; !ok {
		messageType = "CRITICAL"
	}
	return messageType
}

func (ops *VictorOps) incidentID(payload PayloadT) string {
	incidentID := payload.IncidentID
	if incidentID == "" {
		incidentID = ops.instanceName
	}
	return incidentID
}

func (ops *VictorOps) Alert(payload PayloadT) {
	event := map[string]interface{}{
		"message_type":  ops.messageType(payload),
		"entity_id":     ops.incidentID(payload),
		"state_message": payload.Message,
	}
	eventJSON, _ := json.Marshal(event)
	client := &http.Client{}
	victorOpsURL := fmt.Sprintf("https://alert.victorops.com/integrations/generic/20131114/alert/%s/rudderRecovery", ops.routingKey)
	resp, err := client.Post(victorOpsURL, "application/json", bytes.NewBuffer(eventJSON))
	// Not handling errors when sending alert to victorops
	if err != nil {
		logger.Errorf("Alert: Failed to alert service: %s", err.Error())
		return
	}

	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		logger.Errorf("Alert: Got error response %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	logger.Infof("Alert: Successful %s", string(body))
}

type VictorOps struct {
	routingKey   string
	instanceName string
}
