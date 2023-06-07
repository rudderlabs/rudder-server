package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
)

func (ops *VictorOps) Alert(message string) {
	event := map[string]interface{}{
		"message_type":  "CRITICAL",
		"entity_id":     ops.instanceName,
		"state_message": message,
	}
	eventJSON, _ := json.Marshal(event)
	client := &http.Client{Timeout: config.GetDuration("HttpClient.victorops.timeout", 30, time.Second)}
	victorOpsUrl := fmt.Sprintf("https://alert.victorops.com/integrations/generic/20131114/alert/%s/rudderRecovery", ops.routingKey)
	resp, err := client.Post(victorOpsUrl, "application/json", bytes.NewBuffer(eventJSON))
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

type VictorOps struct {
	routingKey   string
	instanceName string
}
