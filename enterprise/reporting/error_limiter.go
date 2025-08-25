package reporting

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/lcs"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

//go:generate mockgen -destination=../../../mocks/enterprise/reporting/mock_error_limiter.go -package=mocks github.com/rudderlabs/rudder-server/enterprise/reporting ErrorLimiter

// ErrorLimiter defines the interface for canonicalizing error messages
// By canonicalizing errors, we can reduce the number of unique errors that are reported,
// This interface allows for easier testing by enabling mock implementations
type ErrorLimiter interface {
	// CanonicalizeError returns the canonical error message for the given error
	// Returns "UnknownError" if rate limited, or the canonical error message if should be reported
	CanonicalizeError(ctx context.Context, connectionKey, errorMessage string) string

	// StartCleanup starts the periodic cleanup routine
	StartCleanup(ctx context.Context) error
}

type errorCounter struct {
	messages    map[string]time.Time
	lastUpdated time.Time
	lastBlocked time.Time
}

type errorLimiter struct {
	mu       sync.Mutex
	log      logger.Logger
	counters map[string]*errorCounter

	similarityThreshold config.ValueLoader[float64]
	maxErrorsPerMinute  config.ValueLoader[int]
	maxCounters         config.ValueLoader[int]
	cleanupInterval     config.ValueLoader[time.Duration]
}

func NewErrorLimiter(log logger.Logger, conf *config.Config) ErrorLimiter {
	return &errorLimiter{
		log:                 log,
		counters:            make(map[string]*errorCounter),
		similarityThreshold: conf.GetReloadableFloat64Var(0.75, "Reporting.errorReporting.rateLimit.similarityThreshold"),
		maxErrorsPerMinute:  conf.GetReloadableIntVar(20, 1, "Reporting.errorReporting.rateLimit.maxErrorsPerMinute"),
		maxCounters:         conf.GetReloadableIntVar(10000, 1, "Reporting.errorReporting.rateLimit.maxCounters"),
		cleanupInterval:     conf.GetReloadableDurationVar(1, time.Second, "Reporting.errorReporting.rateLimit.cleanupInterval"),
	}
}

// CanonicalizeError canonicalizes the error message for the given connection key
// If the error message is similar to an existing error message, it returns the existing error message
// If the error message is not similar to any existing error message, it returns the error message
// If the number of unique error messages exceeds the maxErrorsPerMinute, it returns "UnknownError"
// If the number of unique error messages exceeds the maxCounters, it returns "UnknownError"
func (e *errorLimiter) CanonicalizeError(ctx context.Context, key, msg string) string {
	now := time.Now()

	e.mu.Lock()
	defer e.mu.Unlock()

	c, ok := e.counters[key]
	if !ok {
		if len(e.counters) >= e.maxCounters.Load() {
			return "UnknownError"
		}
		c = &errorCounter{lastUpdated: now, messages: make(map[string]time.Time)}
		e.counters[key] = c
	}

	similarityThreshold := e.similarityThreshold.Load()
	maxErrorsPerMinute := e.maxErrorsPerMinute.Load()
	// Similarity
	for existing := range c.messages {
		if lcs.Similarity(msg, existing) >= similarityThreshold {
			c.messages[existing] = now
			return existing
		}
	}

	// Rate limit
	if len(c.messages) >= maxErrorsPerMinute {
		c.lastBlocked = now
		return "UnknownError"
	}

	// Insert
	c.messages[msg] = now
	c.lastUpdated = now
	return msg
}

func (e *errorLimiter) cleanup() {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-time.Minute)

	maxErrorsPerMinute := e.maxErrorsPerMinute.Load()

	for k, c := range e.counters {
		e.dropStaleMessages(c, cutoff)

		// Drop if no messages left OR no new unique error for >1m
		if e.shouldDropCounter(c, now, maxErrorsPerMinute) {
			delete(e.counters, k)
		}
	}
}

func (e *errorLimiter) dropStaleMessages(c *errorCounter, cutoff time.Time) {
	for msg, t := range c.messages {
		if t.Before(cutoff) {
			delete(c.messages, msg)
		}
	}
}

func (e *errorLimiter) shouldDropCounter(c *errorCounter, now time.Time, maxErrorsPerMinute int) bool {
	// Drop if no messages left
	if len(c.messages) == 0 {
		return true
	}
	// Drop if counter has reached maxErrorsPerMinute and no changes to errors for >1m and
	// an error has been blocked in the last minute, means this counter is starving other errors
	if len(c.messages) == maxErrorsPerMinute &&
		now.Sub(c.lastUpdated) > time.Minute &&
		now.Sub(c.lastBlocked) < time.Minute {
		return true
	}
	return false
}

func (e *errorLimiter) StartCleanup(ctx context.Context) error {
	ticker := time.NewTicker(e.cleanupInterval.Load())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			e.cleanup()
		}
	}
}
