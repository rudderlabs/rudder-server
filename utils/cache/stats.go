package cache

import (
	"sync"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

// StatsCacheKey interface defines the requirements for cache keys
type StatsCacheKey interface {
	comparable
	ToStatTags() stats.Tags
}

// StatsCache is a generic cache for stats measurements using sync.Map
type StatsCache[T StatsCacheKey] struct {
	cache    sync.Map // stores map[T]stats.Measurement
	producer func(T) stats.Measurement
}

// NewStatsCache creates a new stats cache instance
func NewStatsCache[T StatsCacheKey](producer func(T) stats.Measurement) *StatsCache[T] {
	return &StatsCache[T]{
		producer: producer,
	}
}

// Get retrieves a measurement from cache, creating it if it doesn't exist
func (c *StatsCache[T]) Get(key T) stats.Measurement {
	// Try to load the value from the map
	if value, ok := c.cache.Load(key); ok {
		return value.(stats.Measurement)
	}
	// Value not found—create it
	measurement := c.producer(key)
	// Store and possibly get actual value (if stored by another goroutine in the meantime)
	actual, _ := c.cache.LoadOrStore(key, measurement)
	return actual.(stats.Measurement)
}
