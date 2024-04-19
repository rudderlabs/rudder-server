package throttler

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/router/throttler/adaptivethrottlercounter"
)

type adaptiveAlgorithm interface {
	// ResponseCodeReceived is called when a response is received from the destination
	ResponseCodeReceived(code int)
	// Shutdown is called when the throttler is shutting down
	Shutdown()
	// limitFactor returns a factor that is used to multiply the limit, a number between 0 and 1
	LimitFactor() float64
}

func newAdaptiveAlgorithm(config *config.Config, window config.ValueLoader[time.Duration]) adaptiveAlgorithm {
	name := config.GetString("Router.throttler.adaptive.algorithm", "")
	switch name {
	default:
		return adaptivethrottlercounter.New(config, window)
	}
}
