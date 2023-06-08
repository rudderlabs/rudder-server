package alert

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

var (
	pagerDutyEndPoint = "https://events.pagerduty.com/v2/enqueue"
	pkgLogger         logger.Logger
)

func (ops *PagerDuty) Alert(message string) {
	payload := map[string]interface{}{
		"summary":  message,
		"severity": "critical",
		"source":   ops.instanceName,
	}

	event := map[string]interface{}{
		"payload":      payload,
		"event_action": "trigger",
		"routing_key":  ops.routingKey,
	}

	eventJSON, _ := json.Marshal(event)
	client := &http.Client{Timeout: config.GetDuration("HttpClient.pagerduty.timeout", 30, time.Second)}
	resp, err := client.Post(pagerDutyEndPoint, "application/json", bytes.NewBuffer(eventJSON))
	// Not handling errors when sending alert to victorops
	if err != nil {
		pkgLogger.Errorf("Alert: Failed to alert service: %s", err.Error())
		return
	}

	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		pkgLogger.Errorf("Alert: Got error response %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	defer func() { kithttputil.CloseResponse(resp) }()
	if err != nil {
		pkgLogger.Errorf("Alert: Failed to read response body: %s", err.Error())
		return
	}

	pkgLogger.Infof("Alert: Successful %s", string(body))
}

type PagerDuty struct {
	instanceName string
	routingKey   string
}
