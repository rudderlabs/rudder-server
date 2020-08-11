package alert

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

var pagerDutyEndPoint = "https://events.pagerduty.com/v2/enqueue"

var pagerdutyMessageTypesMap = map[string]string{
	"CRITICAL": "critical",
	"RECOVERY": "info",
}

func (ops *PagerDuty) messageType(payload PayloadT) string {
	var messageType string
	var ok bool
	if messageType, ok = pagerdutyMessageTypesMap[payload.MessageType]; !ok {
		messageType = "CRITICAL"
	}
	return messageType
}

func (ops *PagerDuty) incidentID(payload PayloadT) string {
	incidentID := payload.IncidentID
	if incidentID == "" {
		incidentID = ops.instanceName
	}
	return incidentID
}

func (ops *PagerDuty) action(payload PayloadT) string {
	eventAction := "trigger"
	if payload.MessageType == "RECOVERY" {
		eventAction = "resolve"
	}
	return eventAction
}

func (ops *PagerDuty) Alert(payload PayloadT) {
	eventDetails := map[string]interface{}{
		"summary":  payload.Message,
		"severity": ops.messageType(payload),
		"source":   ops.instanceName,
	}

	event := map[string]interface{}{
		"payload":      eventDetails,
		"event_action": ops.action(payload),
		"routing_key":  ops.routingKey,
		"dedup_key":    ops.incidentID(payload),
	}

	eventJSON, _ := json.Marshal(event)
	client := &http.Client{}
	resp, err := client.Post(pagerDutyEndPoint, "application/json", bytes.NewBuffer(eventJSON))
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

type PagerDuty struct {
	instanceName string
	routingKey   string
}
