package circuitbreaker

import (
	"errors"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/sony/gobreaker"
)

type CircuitBreaker interface {
	IsOpen() bool
	Success()
	Failure()
}

type Opt func(*cfg)

func WithMaxRequests(maxRequests int) Opt {
	return func(cfg *cfg) {
		cfg.maxRequests = maxRequests
	}
}
func WithTimeout(timeout time.Duration) Opt {
	return func(cfg *cfg) {
		cfg.timeout = timeout
	}
}
func WithConsecutiveFailures(consecutiveFailures int) Opt {
	return func(cfg *cfg) {
		cfg.consecutiveFailures = consecutiveFailures
	}
}
func WithLogger(logger logger.Logger) Opt {
	return func(cfg *cfg) {
		cfg.logger = logger
	}
}
func NewCircuitBreaker(name string, opts ...Opt) CircuitBreaker {
	cfg := &cfg{
		name:                name,
		maxRequests:         1,
		timeout:             5 * time.Second,
		consecutiveFailures: 3,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return &circuitBreaker{
		cb: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        name,
			MaxRequests: uint32(cfg.maxRequests), // Allow 1 request to pass through when in half-open state
			Interval:    0,                       // Doesn't count failures when time between requests > interval
			Timeout:     cfg.timeout,             // Time after which to transition from Open to Half-Open
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures >= uint32(cfg.consecutiveFailures)
			},
			OnStateChange: func(name string, from, to gobreaker.State) {
				if cfg.logger != nil {
					cfg.logger.Infon("circuit breaker state changed",
						logger.NewStringField("name", name),
						logger.NewStringField("from", from.String()),
						logger.NewStringField("to", to.String()),
					)
				}
			},
		}),
	}
}

type circuitBreaker struct {
	cb *gobreaker.CircuitBreaker
}

func (cb *circuitBreaker) IsOpen() bool {
	return cb.cb.State() == gobreaker.StateOpen
}
func (cb *circuitBreaker) Success() {
	_, _ = cb.cb.Execute(func() (any, error) {
		return true, nil
	})
}
func (cb *circuitBreaker) Failure() {
	_, _ = cb.cb.Execute(func() (any, error) {
		return nil, errors.New("failure")
	})
}

type cfg struct {
	name                string
	maxRequests         int
	timeout             time.Duration
	consecutiveFailures int
	logger              logger.Logger
}
