package warehouseChore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGKILL, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	workspaceToken := getEnvString("WORKSPACE_TOKEN", "")
	destinationId := getEnvString("DESTINATION_ID", "")
	resp, err := makeGetHTTPCall(ctx, workspaceToken)
	if err != nil {
		fmt.Println(err)
		return
	}
	var wsConfig ConfigT
	err = json.Unmarshal(resp, &wsConfig)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, _ = getDestinationCreds(wsConfig, destinationId)

}

func getDestinationCreds(wsConfig ConfigT, destinationId string) (map[string]interface{}, string) {
	for _, sources := range wsConfig.Sources {
		for _, destination := range sources.Destinations {
			if destination.ID == destinationId {
				return destination.Config, destination.DestinationDefinition.Name
			}
		}
	}
	return nil, ""
}

func makeGetHTTPCall(ctx context.Context, workspaceToken string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.rudderstack.com/workspaceConfig?fetchAll=true", http.NoBody)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(workspaceToken, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: config.GetDuration("HttpClient.backendConfig.timeout", 30, time.Second)}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	respBody, err := io.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP status code: %d, response: %s", resp.StatusCode, respBody)
	}

	return respBody, nil
}

func getEnvString(envVar, defaultValue string) string {
	v, ok := os.LookupEnv(envVar)
	if !ok {
		return defaultValue
	}
	return v
}

func getEnvInt64(envVar string, defaultValue int64) int64 {
	v, ok := os.LookupEnv(envVar)
	if !ok {
		return defaultValue
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}
