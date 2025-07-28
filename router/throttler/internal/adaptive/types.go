package adaptive

type Algorithm interface {
	// ResponseCodeReceived is called when a response is received from the destination
	ResponseCodeReceived(code int)
	// Shutdown is called when the throttler is shutting down
	Shutdown()
	// limitFactor returns a factor that is used to multiply the limit, a number between 0 and 1
	LimitFactor() float64
}

const DefaultMaxLimit = 1000 // 1000 requests per second
