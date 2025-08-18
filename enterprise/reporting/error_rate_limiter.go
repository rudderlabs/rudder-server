package reporting

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/lcs"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// ErrorRateLimiter manages error message rate limiting
// per sourceId, destinationId, eventType, reportedBy combination
type ErrorRateLimiter struct {
	mu                sync.RWMutex
	counters          map[string]*errorCounter
	log               logger.Logger
	lastCleanup       time.Time // Track when last cleanup happened
	cleanupInProgress int32     // Atomic flag to track if cleanup is running

	similarityThreshold      config.ValueLoader[float64]
	maxErrorsPerMinute       config.ValueLoader[int]
	maxCounters              config.ValueLoader[int]
	enabled                  config.ValueLoader[bool]
	cleanupInterval          config.ValueLoader[time.Duration]
	maxCountersCheckInterval config.ValueLoader[time.Duration]
	minCleanupInterval       config.ValueLoader[time.Duration]
}

// errorCounter tracks errors for a specific combination
type errorCounter struct {
	count     int
	windowEnd time.Time
	messages  []string // Store truncated error messages
}

// NewErrorRateLimiter creates a new error rate limiter instance
func NewErrorRateLimiter(log logger.Logger, conf *config.Config) *ErrorRateLimiter {
	return &ErrorRateLimiter{
		counters: make(map[string]*errorCounter),
		log:      log.Child("error-rate-limiter"),

		enabled:                  conf.GetReloadableBoolVar(true, "Reporting.errorReporting.rateLimit.enabled"),
		maxErrorsPerMinute:       conf.GetReloadableIntVar(20, 1, "Reporting.errorReporting.rateLimit.maxErrorsPerMinute"),
		maxCounters:              conf.GetReloadableIntVar(1000, 1, "Reporting.errorReporting.rateLimit.maxCounters"),
		similarityThreshold:      conf.GetReloadableFloat64Var(0.75, "Reporting.errorReporting.rateLimit.similarityThreshold"),
		cleanupInterval:          conf.GetReloadableDurationVar(5, time.Minute, "Reporting.errorReporting.rateLimit.cleanupInterval"),
		maxCountersCheckInterval: conf.GetReloadableDurationVar(1, time.Second, "Reporting.errorReporting.rateLimit.maxCountersCheckInterval"),
		minCleanupInterval:       conf.GetReloadableDurationVar(5, time.Minute, "Reporting.errorReporting.rateLimit.minCleanupInterval"),
	}
}

// generateKey creates a unique key for the combination of identifiers
func (erl *ErrorRateLimiter) generateKey(sourceID, destinationID, eventType, reportedBy string) string {
	return fmt.Sprintf("%s:%s:%s:%s", sourceID, destinationID, eventType, reportedBy)
}

// isSimilarToExisting checks if a new message is similar to any existing messages
func (erl *ErrorRateLimiter) isSimilarToExisting(newMessage string, existingMessages []string) bool {
	threshold := erl.similarityThreshold.Load()
	return lcs.SimilarMessageExistsWithOptions(newMessage, existingMessages, threshold, lcs.DefaultOptions())
}

// IsBucketFull checks if the rate limit bucket is already full for the given key
// This can be used to skip error extraction entirely if the rate limit has been exceeded
func (erl *ErrorRateLimiter) IsBucketFull(ctx context.Context, sourceID, destinationID, eventType, reportedBy string) bool {
	if !erl.enabled.Load() {
		return false
	}

	// If cleanup is in progress, return true to avoid blocking
	if atomic.LoadInt32(&erl.cleanupInProgress) == 1 {
		return true
	}

	key := erl.generateKey(sourceID, destinationID, eventType, reportedBy)

	erl.mu.RLock()
	defer erl.mu.RUnlock()

	// Check if we've reached the maximum number of counters
	maxCounters := erl.maxCounters.Load()
	if len(erl.counters) >= maxCounters {
		// If the key doesn't exist, bucket is effectively full
		if _, exists := erl.counters[key]; !exists {
			return true
		}
		// If the key exists, we can still process it, so check the actual bucket
	}

	now := time.Now()
	counter, exists := erl.counters[key]

	// If no counter exists or window has expired, bucket is not full
	if !exists || now.After(counter.windowEnd) {
		return false
	}

	// Check if we've exceeded the rate limit
	maxErrors := erl.maxErrorsPerMinute.Load()
	return counter.count >= maxErrors
}

// ShouldReport determines if an error should be reported based on rate limiting rules
func (erl *ErrorRateLimiter) ShouldReport(ctx context.Context, sourceID, destinationID, eventType, reportedBy, errorMessage string) bool {
	if !erl.enabled.Load() {
		return true
	}

	// Skip empty error messages as they don't provide useful information
	if strings.TrimSpace(errorMessage) == "" {
		return false
	}

	// If cleanup is in progress, return false to avoid blocking
	if atomic.LoadInt32(&erl.cleanupInProgress) == 1 {
		return false
	}

	key := erl.generateKey(sourceID, destinationID, eventType, reportedBy)

	erl.mu.Lock()
	defer erl.mu.Unlock()

	now := time.Now()
	counter, exists := erl.counters[key]

	// Initialize counter if it doesn't exist or window has expired
	if !exists || now.After(counter.windowEnd) {
		// Check if we're at the max counters limit and need to create a new counter
		maxCounters := erl.maxCounters.Load()
		if !exists && len(erl.counters) >= maxCounters {
			// Can't create new counter when at limit
			return false
		}

		counter = &errorCounter{
			count:     1,
			windowEnd: now.Add(time.Minute),
			messages:  []string{errorMessage},
		}
		erl.counters[key] = counter
		return true
	}

	// Check if we've exceeded the rate limit
	maxErrors := erl.maxErrorsPerMinute.Load()
	if counter.count >= maxErrors {
		return false
	}

	// Check if this is a similar error to existing ones
	if erl.isSimilarToExisting(errorMessage, counter.messages) {
		return false
	}

	// Add the new error message and increment counter
	counter.count++
	counter.messages = append(counter.messages, errorMessage)

	return true
}

// cleanup removes expired counters to prevent memory leaks
func (erl *ErrorRateLimiter) cleanup() {
	// Set cleanup in progress flag
	atomic.StoreInt32(&erl.cleanupInProgress, 1)
	defer atomic.StoreInt32(&erl.cleanupInProgress, 0)

	erl.mu.Lock()
	defer erl.mu.Unlock()

	now := time.Now()
	initialCount := len(erl.counters)

	for key, counter := range erl.counters {
		if now.After(counter.windowEnd) {
			delete(erl.counters, key)
		}
	}

	finalCount := len(erl.counters)
	if initialCount != finalCount {
		erl.log.Debugn("Cleaned up expired error counters",
			logger.NewIntField("removed", int64(initialCount-finalCount)),
			logger.NewIntField("remaining", int64(finalCount)),
		)
	}

	// Update last cleanup time
	erl.lastCleanup = now
}

// StartCleanup starts the periodic cleanup routine
func (erl *ErrorRateLimiter) StartCleanup(ctx context.Context) {
	go func() {
		// Use a faster ticker for max counters check
		maxCountersTicker := time.NewTicker(erl.maxCountersCheckInterval.Load())
		defer maxCountersTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-maxCountersTicker.C:
				// Check if we're at max counters
				erl.mu.RLock()
				maxCounters := erl.maxCounters.Load()
				currentCount := len(erl.counters)
				erl.mu.RUnlock()

				if currentCount >= maxCounters || time.Since(erl.lastCleanup) >= erl.minCleanupInterval.Load() {
					erl.log.Debugn("Triggering cleanup",
						logger.NewIntField("currentCount", int64(currentCount)),
						logger.NewIntField("maxCounters", int64(maxCounters)),
						logger.NewStringField("timeSinceLastCleanup", time.Since(erl.lastCleanup).String()),
					)
					erl.cleanup()
				}
			}
		}
	}()
}
