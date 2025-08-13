package adaptive

import "github.com/rudderlabs/rudder-go-kit/logger"

type Algorithm interface {
	// ResponseCodeReceived is called when a response is received from the destination
	ResponseCodeReceived(code int)
	// Shutdown is called when the throttler is shutting down
	Shutdown()
	// limitFactor returns a factor that is used to multiply the limit, a number between 0 and 1
	LimitFactor() float64
}

type Logger interface {
	Warnn(msg string, fields ...logger.Field)
}

const DefaultMaxThrottlingLimit = 1000 // 1000 requests per second
