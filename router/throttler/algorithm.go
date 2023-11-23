package throttler

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	adaptiveAlgorithmCounter "github.com/rudderlabs/rudder-server/router/throttler/adaptiveAlgorithmCounter"
)

type adaptiveAlgorithm interface {
	// ResponseCodeReceived is called when a response is received from the destination
	ResponseCodeReceived(code int)
	// ShutDown is called when the throttler is shutting down
	ShutDown()
	// limitFactor returns a factor between 0 and 1 that is used to multiply the limit
	LimitFactor() float64
}

const (
	throttlingAdaptiveAlgoTypeCounter = "counter"
)

func newAdaptiveAlgorithm(config *config.Config) adaptiveAlgorithm {
	name := config.GetString("Router.throttler.adaptive.algorithm", "")
	switch name {
	case throttlingAdaptiveAlgoTypeCounter:
		return adaptiveAlgorithmCounter.New(config)
	default:
		return &noopAdaptiveAlgorithm{}
	}
}
