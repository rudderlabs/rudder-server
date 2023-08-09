package gateway

import (
	"context"

	"github.com/rudderlabs/rudder-server/services/diagnostics"
)

// TrackRequestMetrics updates the track counters (success and failure counts)
func (gw *Handle) TrackRequestMetrics(errorMessage string) {
	if diagnostics.EnableGatewayMetric {
		gw.trackCounterMu.Lock()
		defer gw.trackCounterMu.Unlock()
		if errorMessage != "" {
			gw.trackFailureCount = gw.trackFailureCount + 1
		} else {
			gw.trackSuccessCount = gw.trackSuccessCount + 1
		}
	}
}

// collectMetrics collects the gateway metrics and sends them using diagnostics
func (gw *Handle) collectMetrics(ctx context.Context) {
	if diagnostics.EnableGatewayMetric {
		for {
			select {
			case <-ctx.Done():
				return
			case <-gw.diagnosisTicker.C:
				gw.trackCounterMu.Lock()
				if gw.trackSuccessCount > 0 || gw.trackFailureCount > 0 {
					diagnostics.Diagnostics.Track(diagnostics.GatewayEvents, map[string]interface{}{
						diagnostics.GatewaySuccess: gw.trackSuccessCount,
						diagnostics.GatewayFailure: gw.trackFailureCount,
					})
					gw.trackSuccessCount = 0
					gw.trackFailureCount = 0
				}
				gw.trackCounterMu.Unlock()
			}
		}
	}
}
