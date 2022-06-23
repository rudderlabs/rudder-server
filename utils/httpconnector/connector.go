package httpconnector

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

// MakeHTTPPostRequest makes HTTP Post call to the url with the body as payload and returns the error if any.
// This uses basic authentication
func MakeHTTPPostRequest(url string, payload []byte) error {
	rawJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second)}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(rawJSON))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(config.GetWorkspaceToken(), "")

	resp, err := client.Do(req)
	if resp == nil {
		return err
	}
	if err != nil {
		return err
	}
	return nil
}
