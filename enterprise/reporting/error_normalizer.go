package reporting

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/lcs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

//go:generate mockgen -destination=../../../mocks/enterprise/reporting/mock_error_normalizer.go -package=mocks github.com/rudderlabs/rudder-server/enterprise/reporting ErrorNormalizer

// ErrorNormalizer defines the interface for normalizing error messages
// By normalizing errors, we can reduce the number of unique errors that are reported,
// This interface allows for easier testing by enabling mock implementations
type ErrorNormalizer interface {
	// NormalizeError returns the normalized error message for the given error
	// Returns "UnknownError" if rate limited, or the normalized error message if should be reported
	NormalizeError(ctx context.Context, errorDetailGroupKey types.ErrorDetailGroupKey, errorMessage string) string

	// StartCleanup starts the periodic cleanup routine
	StartCleanup(ctx context.Context) error
}

type errorCounter struct {
	messages    map[string]time.Time
	lastUpdated time.Time
	lastBlocked time.Time
}

type errorNormalizer struct {
	mu       sync.Mutex
	log      logger.Logger
	counters map[types.ErrorDetailGroupKey]*errorCounter

	similarityThreshold config.ValueLoader[float64]
	maxErrorsPerMinute  config.ValueLoader[int]
	maxCounters         config.ValueLoader[int]
	cleanupInterval     config.ValueLoader[time.Duration]
}

func NewErrorNormalizer(log logger.Logger, conf *config.Config) ErrorNormalizer {
	return &errorNormalizer{
		log:                 log,
		counters:            make(map[types.ErrorDetailGroupKey]*errorCounter),
		similarityThreshold: conf.GetReloadableFloat64Var(0.75, "Reporting.errorReporting.normalizer.similarityThreshold"),
		maxErrorsPerMinute:  conf.GetReloadableIntVar(20, 1, "Reporting.errorReporting.normalizer.maxErrorsPerMinute"),
		maxCounters:         conf.GetReloadableIntVar(10000, 1, "Reporting.errorReporting.normalizer.maxCounters"),
		cleanupInterval:     conf.GetReloadableDurationVar(1, time.Second, "Reporting.errorReporting.normalizer.cleanupInterval"),
	}
}

// NormalizeError normalizes the error message for the given connection key
// If the error message is similar to an existing error message, it returns the existing error message
// If the error message is not similar to any existing error message, it returns the error message
// If the number of unique error messages exceeds the maxErrorsPerMinute, it returns "UnknownError"
// If the number of unique error messages exceeds the maxCounters, it returns "UnknownError"
func (e *errorNormalizer) NormalizeError(ctx context.Context, errorDetailGroupKey types.ErrorDetailGroupKey, msg string) string {
	// Convert struct key to string for internal use
	now := time.Now()

	e.mu.Lock()
	defer e.mu.Unlock()

	c, ok := e.counters[errorDetailGroupKey]
	if !ok {
		if len(e.counters) >= e.maxCounters.Load() {
			return "UnknownError"
		}
		c = &errorCounter{lastUpdated: now, messages: make(map[string]time.Time)}
		e.counters[errorDetailGroupKey] = c
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

func (e *errorNormalizer) cleanup() {
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

func (e *errorNormalizer) dropStaleMessages(c *errorCounter, cutoff time.Time) {
	for msg, t := range c.messages {
		if t.Before(cutoff) {
			delete(c.messages, msg)
		}
	}
}

func (e *errorNormalizer) shouldDropCounter(c *errorCounter, now time.Time, maxErrorsPerMinute int) bool {
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

func (e *errorNormalizer) StartCleanup(ctx context.Context) error {
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
