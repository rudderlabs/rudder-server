package health

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

func WaitUntilReady(
	ctx context.Context, t testing.TB, endpoint string, atMost, interval time.Duration, caller string,
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
			func() { httputil.CloseResponse(resp) }()
			if resp.StatusCode == http.StatusOK {
				t.Log("Application ready")
				return
			}
		}
	}
}
