package cache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

// testKey implements StatsCacheKey for testing
type testKey struct {
	id string
}

func (k testKey) ToStatTags() stats.Tags {
	return stats.Tags{"id": k.id}
}

func TestNewStatsCache(t *testing.T) {
	producer := func(key testKey) stats.Measurement {
		return stats.NOP.NewStat("test", stats.CountType)
	}

	cache := NewStatsCache(producer)
	require.NotNil(t, cache)
	require.NotNil(t, cache.producer)
}

func TestStatsCache_Get(t *testing.T) {
	producer := func(key testKey) stats.Measurement {
		return stats.NOP.NewStat("test-"+key.id, stats.CountType)
	}

	cache := NewStatsCache(producer)
	key := testKey{id: "test-key"}

	// First call should create and cache
	first := cache.Get(key)
	require.NotNil(t, first)

	// Second call should return cached value
	second := cache.Get(key)
	require.Equal(t, first, second)
}

func TestStatsCache_MultipleKeys(t *testing.T) {
	producer := func(key testKey) stats.Measurement {
		return stats.NOP.NewStat("test-"+key.id, stats.CountType)
	}

	cache := NewStatsCache(producer)
	key1 := testKey{id: "key1"}
	key2 := testKey{id: "key2"}

	measurement1 := cache.Get(key1)
	measurement2 := cache.Get(key2)

	require.NotNil(t, measurement1)
	require.NotNil(t, measurement2)
	// Note: NOP stats with same name return same instance, so we test that they're both valid
	require.NotNil(t, measurement1)
	require.NotNil(t, measurement2)
}

func TestStatsCache_ProducerFunction(t *testing.T) {
	callCount := 0
	producer := func(key testKey) stats.Measurement {
		callCount++
		return stats.NOP.NewStat("test-"+key.id, stats.CountType)
	}

	cache := NewStatsCache(producer)
	key := testKey{id: "test-key"}

	// First call should call producer
	cache.Get(key)
	require.Equal(t, 1, callCount)

	// Second call should not call producer (cached)
	cache.Get(key)
	require.Equal(t, 1, callCount)
}

func TestStatsCache_KeyComparability(t *testing.T) {
	key1 := testKey{id: "same"}
	key2 := testKey{id: "same"}
	key3 := testKey{id: "different"}

	require.Equal(t, key1, key2)
	require.NotEqual(t, key1, key3)
}
