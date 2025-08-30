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

type errorEntry struct {
	message string
	time    time.Time
}

type boundedErrorSet struct {
	entries []*errorEntry          // oldest → newest
	index   map[string]*errorEntry // message → entry
}

func newBoundedErrorSet(max int) *boundedErrorSet {
	return &boundedErrorSet{
		entries: make([]*errorEntry, 0, max),
		index:   make(map[string]*errorEntry, max),
	}
}

// GetExact looks up exact match
func (s *boundedErrorSet) GetExact(msg string, now time.Time) (*errorEntry, bool) {
	if entry, ok := s.index[msg]; ok {
		entry.time = now
		s.moveToEnd(entry)
		return entry, true
	}
	return nil, false
}

// GetSimilar scans for a near match (caller supplies similarity fn)
func (s *boundedErrorSet) GetSimilar(msg string, now time.Time, similar func(a, b string) bool) (*errorEntry, bool) {
	for i, entry := range s.entries {
		if similar(msg, entry.message) {
			entry.time = now
			s.moveToEndAt(i)
			return entry, true
		}
	}
	return nil, false
}

// Add inserts a new entry, evicting oldest if full
func (s *boundedErrorSet) Add(msg string, now time.Time) *errorEntry {
	entry := &errorEntry{message: msg, time: now}
	s.entries = append(s.entries, entry)
	s.index[msg] = entry
	return entry
}

// DropStale removes messages older than cutoff
func (s *boundedErrorSet) DropStale(cutoff time.Time) {
	var drop int
	for _, entry := range s.entries {
		if entry.time.Before(cutoff) {
			delete(s.index, entry.message)
			drop++
		} else {
			break
		}
	}
	if drop > 0 {
		s.entries = s.entries[drop:]
	}
}

// --- internal helpers ---

func (s *boundedErrorSet) moveToEnd(entry *errorEntry) {
	for i, en := range s.entries {
		if en == entry {
			s.moveToEndAt(i)
			return
		}
	}
}

func (s *boundedErrorSet) moveToEndAt(i int) {
	entry := s.entries[i]
	s.entries = append(s.entries[:i], s.entries[i+1:]...)
	s.entries = append(s.entries, entry)
}

type errorGroup struct {
	errors boundedErrorSet
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
	staleTime           config.ValueLoader[time.Duration]

	// Stats manager reference
	statsManager *ErrorReportingStats
}

func NewErrorNormalizer(log logger.Logger, conf *config.Config, statsInstance stats.Stats, statsManager *ErrorReportingStats) ErrorNormalizer {
	return &errorNormalizer{
		log:                 log,
		groups:              make(map[types.ErrorDetailGroupKey]*errorGroup),
		stats:               statsInstance,
		similarityThreshold: conf.GetReloadableFloat64Var(0.75, "Reporting.errorReporting.normalizer.similarityThreshold"),
		maxErrorsPerGroup:   conf.GetReloadableIntVar(20, 1, "Reporting.errorReporting.normalizer.maxErrorsPerGroup"),
		maxGroups:           conf.GetReloadableIntVar(10000, 1, "Reporting.errorReporting.normalizer.maxGroups"),
		cleanupInterval:     conf.GetReloadableDurationVar(5, time.Second, "Reporting.errorReporting.normalizer.cleanupInterval"),
		staleTime:           conf.GetReloadableDurationVar(1, time.Minute, "Reporting.errorReporting.normalizer.staleTime"),
		statsManager:        statsManager,
	}
}

// NormalizeError normalizes the error message for the given connection key
func (e *errorNormalizer) NormalizeError(ctx context.Context, errorDetailGroupKey types.ErrorDetailGroupKey, msg string) string {
	now := time.Now()

	e.mu.Lock()
	defer e.mu.Unlock()

	maxErrorsPerGroup := e.maxErrorsPerGroup.Load()
	currentGroup, ok := e.groups[errorDetailGroupKey]
	if !ok {
		if len(e.groups) >= e.maxGroups.Load() {
			return RedactedError
		}
		currentGroup = &errorGroup{
			lastUpdated: now,
			errors:      *newBoundedErrorSet(maxErrorsPerGroup),
		}
		e.groups[errorDetailGroupKey] = currentGroup
	}

	// Exact match
	if _, ok := currentGroup.errors.GetExact(msg, now); ok {
		return msg
	}

	// Similarity match
	similarityThreshold := e.similarityThreshold.Load()
	similarityFunc := func(a, b string) bool {
		return lcs.Similarity(a, b) >= similarityThreshold
	}

	if entry, ok := currentGroup.errors.GetSimilar(msg, now, similarityFunc); ok {
		return entry.message
	}

	// Rate limit the new error message
	if len(currentGroup.errors.entries) >= maxErrorsPerGroup {
		currentGroup.lastBlocked = now
		return RedactedError
	}

	// Insert the new error message
	currentGroup.lastUpdated = now
	currentGroup.errors.Add(msg, now)
	return msg
}

func (e *errorNormalizer) cleanup() {
	start := time.Now()
	defer func() {
		e.statsManager.NormalizerCleanupTime.Since(start)
	}()

	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-e.staleTime.Load())

	maxErrorsPerGroup := e.maxErrorsPerGroup.Load()

	for k, currentGroup := range e.groups {
		currentGroup.errors.DropStale(cutoff)

		// Drop if no messages left OR no new unique error for >staleTime
		if e.shouldDropCounter(currentGroup, now, maxErrorsPerGroup) {
			delete(e.groups, k)
		}
	}
}

func (e *errorNormalizer) shouldDropCounter(currentGroup *errorGroup, now time.Time, maxErrorsPerGroup int) bool {
	// Drop if no messages left
	if len(currentGroup.errors.entries) == 0 {
		return true
	}
	// Drop if counter has reached maxErrorsPerGroup and no changes to errors for >staleTime and
	// an error has been blocked in the last staleTime, means this counter is starving other errors
	staleTime := e.staleTime.Load()
	if len(currentGroup.errors.entries) == maxErrorsPerGroup &&
		now.Sub(currentGroup.lastUpdated) > staleTime &&
		now.Sub(currentGroup.lastBlocked) < staleTime {
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
