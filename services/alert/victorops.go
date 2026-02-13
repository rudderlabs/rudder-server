package alert

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

func (ops *VictorOps) Alert(message string) {
	event := map[string]any{
		"message_type":  "CRITICAL",
		"entity_id":     ops.instanceName,
		"state_message": message,
	}
	eventJSON, _ := jsonrs.Marshal(event)
	client := &http.Client{Timeout: config.GetDuration("HttpClient.victorops.timeout", 30, time.Second)}
	victorOpsUrl := fmt.Sprintf("https://alert.victorops.com/integrations/generic/20131114/alert/%s/rudderRecovery", ops.routingKey)
	resp, err := client.Post(victorOpsUrl, "application/json", bytes.NewBuffer(eventJSON))
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

type VictorOps struct {
	routingKey   string
	instanceName string
}
