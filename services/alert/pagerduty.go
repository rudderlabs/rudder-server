package alert

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
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

	eventJSON, _ := jsonrs.Marshal(event)
	client := &http.Client{Timeout: config.GetDuration("HttpClient.pagerduty.timeout", 30, time.Second)}
	resp, err := client.Post(pagerDutyEndPoint, "application/json", bytes.NewBuffer(eventJSON))
	// Not handling errors when sending alert to victorops
	if err != nil {
		pkgLogger.Errorn("Alert: Failed to alert service", obskit.Error(err))
		return
	}

	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		pkgLogger.Errorn("Alert: Got error response", logger.NewIntField("statusCode", int64(resp.StatusCode)))
	}

	body, err := io.ReadAll(resp.Body)
	defer func() { kithttputil.CloseResponse(resp) }()
	if err != nil {
		pkgLogger.Errorn("Alert: Failed to read response body", obskit.Error(err))
		return
	}

	pkgLogger.Infon("Alert: Successful", logger.NewStringField("body", string(body)))
}

type PagerDuty struct {
	instanceName string
	routingKey   string
}
