package throttler

type noopAdaptiveAlgorithm struct{}

func (ba *noopAdaptiveAlgorithm) ResponseCodeReceived(code int) {
	// no-op
}

func (ba *noopAdaptiveAlgorithm) ShutDown() {
	// no-op
}

func (ba *noopAdaptiveAlgorithm) LimitFactor() float64 {
	return 1.0
}
