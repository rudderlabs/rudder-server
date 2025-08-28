package reporting

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/lcs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/types"
)

//go:generate mockgen -destination=../../../mocks/enterprise/reporting/mock_error_normalizer.go -package=mocks github.com/rudderlabs/rudder-server/enterprise/reporting ErrorNormalizer

// ErrorNormalizer defines the interface for normalizing error messages
// By normalizing errors, we can reduce the number of unique errors that are reported,
// This interface allows for easier testing by enabling mock implementations
type ErrorNormalizer interface {
	// NormalizeError returns the normalized error message for the given error
	// Returns "RedactedError" if rate limited, or the normalized error message if should be reported
	NormalizeError(ctx context.Context, errorDetailGroupKey types.ErrorDetailGroupKey, errorMessage string) string

	// StartCleanup starts the periodic cleanup routine
	StartCleanup(ctx context.Context) error
}

const (
	RedactedError = "RedactedError"
)

type errorGroup struct {
	errors map[string]time.Time
	// lastUpdated is the time when the group's error set last changed (new error added or old error removed)
	lastUpdated time.Time
	// lastBlocked is the time when the error group was last blocked due to rate limiting
	lastBlocked time.Time
}

type errorNormalizer struct {
	mu     sync.Mutex
	log    logger.Logger
	groups map[types.ErrorDetailGroupKey]*errorGroup
	stats  stats.Stats

	similarityThreshold config.ValueLoader[float64]
	maxErrorsPerGroup   config.ValueLoader[int]
	maxGroups           config.ValueLoader[int]
	cleanupInterval     config.ValueLoader[time.Duration]
}

func NewErrorNormalizer(log logger.Logger, conf *config.Config, stats stats.Stats) ErrorNormalizer {
	return &errorNormalizer{
		log:                 log,
		groups:              make(map[types.ErrorDetailGroupKey]*errorGroup),
		stats:               stats,
		similarityThreshold: conf.GetReloadableFloat64Var(0.75, "Reporting.errorReporting.normalizer.similarityThreshold"),
		maxErrorsPerGroup:   conf.GetReloadableIntVar(20, 1, "Reporting.errorReporting.normalizer.maxErrorsPerGroup"),
		maxGroups:           conf.GetReloadableIntVar(10000, 1, "Reporting.errorReporting.normalizer.maxGroups"),
		cleanupInterval:     conf.GetReloadableDurationVar(5, time.Second, "Reporting.errorReporting.normalizer.cleanupInterval"),
	}
}

// NormalizeError normalizes the error message for the given connection key
func (e *errorNormalizer) NormalizeError(ctx context.Context, errorDetailGroupKey types.ErrorDetailGroupKey, msg string) string {
	// Convert struct key to string for internal use
	now := time.Now()

	e.mu.Lock()
	defer e.mu.Unlock()

	c, ok := e.groups[errorDetailGroupKey]
	if !ok {
		if len(e.groups) >= e.maxGroups.Load() {
			return RedactedError
		}
		c = &errorGroup{lastUpdated: now, errors: make(map[string]time.Time)}
		e.groups[errorDetailGroupKey] = c
	}

	// Exact match
	if _, ok := c.errors[msg]; ok {
		c.errors[msg] = now
		return msg
	}

	// Similarity match
	similarityThreshold := e.similarityThreshold.Load()
	for existing := range c.errors {
		if lcs.Similarity(msg, existing) >= similarityThreshold {
			c.errors[existing] = now
			return existing
		}
	}

	// Rate limit the new error message
	maxErrorsPerGroup := e.maxErrorsPerGroup.Load()
	if len(c.errors) >= maxErrorsPerGroup {
		c.lastBlocked = now
		return RedactedError
	}

	// Insert the new error message
	c.errors[msg] = now
	c.lastUpdated = now
	return msg
}

func (e *errorNormalizer) cleanup() {
	start := time.Now()
	defer func() {
		e.stats.NewStat("error_normalizer_cleanup_time", stats.TimerType).Since(start)
	}()

	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-time.Minute)

	maxErrorsPerGroup := e.maxErrorsPerGroup.Load()

	for k, c := range e.groups {
		e.dropStaleMessages(c, cutoff)

		// Drop if no messages left OR no new unique error for >1m
		if e.shouldDropCounter(c, now, maxErrorsPerGroup) {
			delete(e.groups, k)
		}
	}
}

func (e *errorNormalizer) dropStaleMessages(c *errorGroup, cutoff time.Time) {
	for msg, t := range c.errors {
		if t.Before(cutoff) {
			delete(c.errors, msg)
			c.lastUpdated = time.Now()
		}
	}
}

func (e *errorNormalizer) shouldDropCounter(c *errorGroup, now time.Time, maxErrorsPerGroup int) bool {
	// Drop if no messages left
	if len(c.errors) == 0 {
		return true
	}
	// Drop if counter has reached maxErrorsPerGroup and no changes to errors for >1m and
	// an error has been blocked in the last minute, means this counter is starving other errors
	if len(c.errors) == maxErrorsPerGroup &&
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
