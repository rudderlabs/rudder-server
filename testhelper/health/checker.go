package health

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func WaitUntilReady(
	ctx context.Context, t *testing.T, endpoint string, atMost, interval time.Duration, caller string,
) {
	t.Helper()
	probe := time.NewTicker(interval)
	timeout := time.After(atMost)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeout:
			t.Fatalf(
				"Application was not ready after %s, for the endpoint: %s, caller: %s", atMost, endpoint, caller,
			)
		case <-probe.C:
			resp, err := http.Get(endpoint)
			if err != nil {
				continue
			}
			if resp.StatusCode == http.StatusOK {
				t.Log("Application ready")
				return
			}
		}
	}
}
