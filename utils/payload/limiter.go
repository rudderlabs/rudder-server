package payload

import (
	"context"
	"sync"
	"time"
)

// LimiterState represents the LimiterState of the adaptive payload limiter algorithm
type LimiterState int

const (
	// LimiterStateNormal is the default state and the state when free memory is above the threshold
	LimiterStateNormal LimiterState = iota
	// LimiterStateThreshold is the state when free memory is below the LimiterStateThreshold but above the critical LimiterStateThreshold
	LimiterStateThreshold
	// LimiterStateCritical is the state when free memory is below the LimiterStateCritical threshold
	LimiterStateCritical
)

type LimiterStats struct {
	State           LimiterState
	ThresholdFactor int
}

// Limit is a function that returns the current payload limit in bytes
type Limiter interface {
	RunLoop(ctx context.Context, frequency func() <-chan time.Time)
	Limit(maxLimit int64) int64
	Stats() LimiterStats
	tick()
}

// NewAdaptiveLimiter creates a PayloadLimit function following an adaptive payload limiting algorithm
func NewAdaptiveLimiter(config AdaptiveLimiterConfig) Limiter {
	config.parse()
	algo := adaptivePayloadLimitAlgorithm{
		config:          config,
		thresholdFactor: 1,
	}
	algo.tick()
	return &algo
}

type adaptivePayloadLimitAlgorithm struct {
	state           LimiterState
	config          AdaptiveLimiterConfig
	thresholdFactor int
	freeMem         float64
	mu              sync.Mutex
}

func (r *adaptivePayloadLimitAlgorithm) RunLoop(ctx context.Context, frequency func() <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-frequency():
			r.tick()
		}
	}
}

func (r *adaptivePayloadLimitAlgorithm) Limit(maxLimit int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.state {
	case LimiterStateNormal:
		// during normal state we return the max limit
		return maxLimit
	case LimiterStateThreshold:
		// during threshold state we return the max limit decremented by 10% times the threshold factor
		return int64(float64(maxLimit) * (1.0 - (0.1 * float64(r.thresholdFactor))))
	default:
		// during critical state we return 1 byte as a limit, since 0 bytes is interpreted as unlimited
		return 1
	}
}

func (r *adaptivePayloadLimitAlgorithm) Stats() LimiterStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return LimiterStats{
		State:           r.state,
		ThresholdFactor: r.thresholdFactor,
	}
}

func (r *adaptivePayloadLimitAlgorithm) tick() {
	freeMem, err := r.config.FreeMemory()
	if err != nil {
		r.config.Log.Warnf("failed to get free memory: %v", err)
		freeMem = 100
	}
	var newState LimiterState = LimiterStateCritical
	if freeMem > r.config.FreeMemThresholdLimit {
		newState = LimiterStateNormal
	} else if freeMem > r.config.FreeMemCriticalLimit {
		newState = LimiterStateThreshold
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.freeMem = freeMem
	if r.state != newState {
		r.state = newState
		r.stateChanged(newState)
	}
}

func (r *adaptivePayloadLimitAlgorithm) stateChanged(newState LimiterState) {
	switch newState {
	case LimiterStateNormal:
		r.thresholdFactor = 1
	case LimiterStateCritical:
		r.config.Log.Warnf("critical memory state, free memory percentage: %f %", r.freeMem)
		r.thresholdFactor = r.thresholdFactor + 1
		if r.thresholdFactor > r.config.MaxThresholdFactor {
			r.thresholdFactor = r.config.MaxThresholdFactor
		}
	}
}
