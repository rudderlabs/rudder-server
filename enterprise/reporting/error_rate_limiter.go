package reporting

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/lcs"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

//go:generate mockgen -destination=../../../mocks/enterprise/reporting/mock_error_rate_limiter.go -package=mocks github.com/rudderlabs/rudder-server/enterprise/reporting ErrorRateLimiter

// ErrorRateLimiter defines the interface for error rate limiting functionality
// This interface allows for easier testing by enabling mock implementations
type ErrorRateLimiter interface {
	// CanonicalizeError returns the canonical error message for the given error
	// Returns "UnknownError" if rate limited, or the canonical error message if should be reported
	CanonicalizeError(ctx context.Context, connectionKey, errorMessage string) string

	// StartCleanup starts the periodic cleanup routine
	StartCleanup(ctx context.Context) error
}

// cleanupStrategy defines the interface for different cleanup strategies
type cleanupStrategy interface {
	// shouldCleanupCounter determines if a counter should be cleaned up
	shouldCleanupCounter(counter *errorCounter, now time.Time) bool

	// cleanupMessages cleans up individual messages within a counter
	cleanupMessages(counter *errorCounter, now time.Time, minCleanupInterval time.Duration)

	// handleRateLimit handles what happens when rate limit is reached
	// Returns true if the error should be allowed (after eviction), false if it should be rejected
	handleRateLimit(counter *errorCounter, newMessage string, now time.Time) bool
}

// errorCounter tracks errors for a specific combination
type errorCounter struct {
	windowEnd          time.Time
	lastMessageCleanup time.Time // Track when messages were last cleaned from this counter
	// Track error messages and when each was last used (for LRU strategy)
	messageTimestamps map[string]time.Time // error message -> last used timestamp
}

// errorRateLimiter provides error rate limiting functionality with configurable strategies
type errorRateLimiter struct {
	mu                sync.RWMutex
	log               logger.Logger
	lastCleanup       time.Time
	cleanupInProgress int32

	// Configuration
	similarityThreshold      config.ValueLoader[float64]
	maxErrorsPerMinute       config.ValueLoader[int]
	maxCounters              config.ValueLoader[int]
	enabled                  config.ValueLoader[bool]
	maxCountersCheckInterval config.ValueLoader[time.Duration]
	minCleanupInterval       config.ValueLoader[time.Duration]

	// State management
	counters map[string]*errorCounter
	strategy cleanupStrategy
}

// newErrorRateLimiter creates a new error rate limiter with the specified strategy
func newErrorRateLimiter(log logger.Logger, conf *config.Config, strategy cleanupStrategy) *errorRateLimiter {
	return &errorRateLimiter{
		log: log,

		enabled:            conf.GetReloadableBoolVar(true, "Reporting.errorReporting.rateLimit.enabled"),
		maxErrorsPerMinute: conf.GetReloadableIntVar(20, 1, "Reporting.errorReporting.rateLimit.maxErrorsPerMinute"),
		maxCounters:        conf.GetReloadableIntVar(1000, 1, "Reporting.errorReporting.rateLimit.maxCounters"),

		similarityThreshold:      conf.GetReloadableFloat64Var(0.75, "Reporting.errorReporting.rateLimit.similarityThreshold"),
		maxCountersCheckInterval: conf.GetReloadableDurationVar(1, time.Second, "Reporting.errorReporting.rateLimit.maxCountersCheckInterval"),
		minCleanupInterval:       conf.GetReloadableDurationVar(5, time.Minute, "Reporting.errorReporting.rateLimit.minCleanupInterval"),

		counters: make(map[string]*errorCounter),
		strategy: strategy,
	}
}

// CanonicalizeError returns the canonical error message for the given error
func (e *errorRateLimiter) CanonicalizeError(ctx context.Context, connectionKey, errorMessage string) string {
	if !e.enabled.Load() {
		return errorMessage
	}

	if atomic.LoadInt32(&e.cleanupInProgress) == 1 {
		return "UnknownError"
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	counter, exists := e.counters[connectionKey]

	if !exists {
		maxCounters := e.maxCounters.Load()
		if len(e.counters) >= maxCounters {
			return "UnknownError"
		}

		counter = &errorCounter{
			windowEnd:          now.Add(time.Minute),
			lastMessageCleanup: now, // Initialize to current time
			messageTimestamps:  make(map[string]time.Time),
		}
		e.counters[connectionKey] = counter
	}

	// lastMessageCleanup is only updated when messages are actually cleaned in cleanupMessages

	// Check for similar errors first (before rate limit check)
	canonicalError := findCanonicalError(errorMessage, counter.messageTimestamps, e.similarityThreshold.Load())
	if canonicalError != "" {
		e.log.Debugn("Similar error found",
			logger.NewStringField("connectionKey", connectionKey),
			logger.NewStringField("errorMessage", errorMessage),
			logger.NewStringField("canonicalError", canonicalError),
		)
		counter.messageTimestamps[canonicalError] = now
		return canonicalError
	}

	// Check rate limit (common logic for all strategies)
	maxErrors := e.maxErrorsPerMinute.Load()
	if len(counter.messageTimestamps) >= maxErrors {
		// Strategy-specific handling when rate limit is reached
		if e.strategy.handleRateLimit(counter, errorMessage, now) {
			return errorMessage // Strategy allowed the error after eviction
		}
		return "UnknownError" // Strategy rejected the error
	}

	// Add new error message
	counter.messageTimestamps[errorMessage] = now

	return errorMessage
}

// cleanup removes expired data using the strategy-specific logic
func (e *errorRateLimiter) cleanup() {
	atomic.StoreInt32(&e.cleanupInProgress, 1)
	defer atomic.StoreInt32(&e.cleanupInProgress, 0)

	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	initialCount := len(e.counters)

	for key, counter := range e.counters {
		// Use strategy-specific cleanup logic
		e.strategy.cleanupMessages(counter, now, e.minCleanupInterval.Load())

		if e.strategy.shouldCleanupCounter(counter, now) {
			delete(e.counters, key)
		}
	}

	finalCount := len(e.counters)
	if initialCount != finalCount {
		e.log.Debugn("Cleaned up expired error counters",
			logger.NewIntField("removed", int64(initialCount-finalCount)),
			logger.NewIntField("remaining", int64(finalCount)),
		)
	}

	e.lastCleanup = now
}

// StartCleanup starts the periodic cleanup routine
func (e *errorRateLimiter) StartCleanup(ctx context.Context) error {
	ticker := time.NewTicker(e.maxCountersCheckInterval.Load())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Since(e.lastCleanup) >= e.minCleanupInterval.Load() || len(e.counters) >= e.maxCounters.Load() {
				e.cleanup()
			}
		}
	}
}

// fixedWindowStrategy implements fixed window cleanup strategy
type fixedWindowStrategy struct{}

func newFixedWindowStrategy() *fixedWindowStrategy {
	return &fixedWindowStrategy{}
}

func (f *fixedWindowStrategy) shouldCleanupCounter(counter *errorCounter, now time.Time) bool {
	return now.After(counter.windowEnd)
}

func (f *fixedWindowStrategy) cleanupMessages(counter *errorCounter, now time.Time, minCleanupInterval time.Duration) {
	// Fixed window strategy doesn't clean individual messages
	// It only cleans entire counters when the window expires
}

func (f *fixedWindowStrategy) handleRateLimit(counter *errorCounter, newMessage string, now time.Time) bool {
	// Fixed window: when limit is reached, reject all new errors until window expires
	// No eviction - just reject
	return false
}

// lruStrategy implements least recently used cleanup strategy
type lruStrategy struct{}

func newLRUStrategy() *lruStrategy {
	return &lruStrategy{}
}

func (l *lruStrategy) shouldCleanupCounter(counter *errorCounter, now time.Time) bool {
	// Delete counter if no messages remain OR it hasn't been used recently
	// Note: Using 1 minute as a reasonable default since we don't have access to minCleanupInterval here
	return len(counter.messageTimestamps) == 0 || now.Sub(counter.lastMessageCleanup) > time.Minute
}

func (l *lruStrategy) cleanupMessages(counter *errorCounter, now time.Time, minCleanupInterval time.Duration) {
	// Clean up individual error messages that haven't been used recently
	messagesCleaned := 0
	for msg, lastUsed := range counter.messageTimestamps {
		if now.Sub(lastUsed) > time.Minute {
			delete(counter.messageTimestamps, msg)
			messagesCleaned++
		}
	}

	// If we cleaned any messages, update the lastMessageCleanup timestamp
	if messagesCleaned > 0 {
		counter.lastMessageCleanup = now
	}
}

func (l *lruStrategy) handleRateLimit(counter *errorCounter, newMessage string, now time.Time) bool {
	// LRU: when limit is reached, evict the least recently used error that's older than 1 minute
	// Find the oldest message that's older than 1 minute

	// Current approach: O(n) scan through all messages
	// Alternative: Could use a min-heap for O(1) oldest message access
	// Trade-off: Heap adds complexity and memory overhead, but better for large message counts
	// For typical error rate limiting (small limits), current approach is simpler and sufficient

	var oldestMessage string
	oldestTime := now
	oneMinuteAgo := now.Add(-time.Minute)

	for msg, timestamp := range counter.messageTimestamps {
		if timestamp.Before(oneMinuteAgo) && timestamp.Before(oldestTime) {
			oldestMessage = msg
			oldestTime = timestamp
		}
	}

	if oldestMessage != "" {
		delete(counter.messageTimestamps, oldestMessage)
		// Add the new message
		counter.messageTimestamps[newMessage] = now
		// Update lastMessageCleanup since we cleaned an old message
		counter.lastMessageCleanup = now
		return true // Allow the new error
	}

	return false // No old message found to evict, reject the new error
}

// findCanonicalError finds a canonical error message for similar errors
func findCanonicalError(newMessage string, messageTimestamps map[string]time.Time, threshold float64) string {
	for message := range messageTimestamps {
		similarity := lcs.Similarity(newMessage, message)
		if similarity >= threshold {
			return message
		}
	}
	return ""
}

// NewErrorRateLimiter creates a new error rate limiter based on configuration
func NewErrorRateLimiter(log logger.Logger, conf *config.Config) ErrorRateLimiter {
	strategyType := conf.GetReloadableStringVar("lru", "Reporting.errorReporting.rateLimit.strategy").Load()

	var strategy cleanupStrategy
	switch strategyType {
	case "fixed_window":
		strategy = newFixedWindowStrategy()
		log = log.Child("fixed-window-error-rate-limiter")
	case "lru":
		fallthrough
	default:
		strategy = newLRUStrategy()
		log = log.Child("lru-error-rate-limiter")
	}

	return newErrorRateLimiter(log, conf, strategy)
}
