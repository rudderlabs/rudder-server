package event_sampler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestBadger(t *testing.T) {
	ctx := context.Background()
	conf := config.New()
	ttl := conf.GetReloadableDurationVar(3000, time.Millisecond, "Reporting.eventSampling.durationInMinutes")
	eventSamplerType := conf.GetReloadableStringVar("badger", "Reporting.eventSampling.type")
	eventSamplingCardinality := conf.GetReloadableIntVar(10, 1, "Reporting.eventSampling.cardinality")
	log := logger.NewLogger()

	t.Run("should put and get keys", func(t *testing.T) {
		assert.Equal(t, 3000*time.Millisecond, ttl.Load())
		es, _ := NewEventSampler(ctx, ttl, eventSamplerType, eventSamplingCardinality, conf, log)
		_ = es.Put("key1")
		_ = es.Put("key2")
		_ = es.Put("key3")
		val1, _ := es.Get("key1")
		val2, _ := es.Get("key2")
		val3, _ := es.Get("key3")
		val4, _ := es.Get("key4")

		assert.True(t, val1, "Expected key1 to be present")
		assert.True(t, val2, "Expected key2 to be present")
		assert.True(t, val3, "Expected key3 to be present")
		assert.False(t, val4, "Expected key4 to not be present")
		es.Close()
	})

	t.Run("should not get evicted keys", func(t *testing.T) {
		conf.Set("Reporting.eventSampling.durationInMinutes", 100)
		assert.Equal(t, 100*time.Millisecond, ttl.Load())

		es, _ := NewEventSampler(ctx, ttl, eventSamplerType, eventSamplingCardinality, conf, log)
		defer es.Close()

		_ = es.Put("key1")

		require.Eventually(t, func() bool {
			val1, _ := es.Get("key1")
			return !val1
		}, 1*time.Second, 50*time.Millisecond)
	})
}

func TestInMemoryCache(t *testing.T) {
	ctx := context.Background()
	conf := config.New()
	eventSamplerType := conf.GetReloadableStringVar("in_memory_cache", "Reporting.eventSampling.type")
	eventSamplingCardinality := conf.GetReloadableIntVar(3, 1, "Reporting.eventSampling.cardinality")
	ttl := conf.GetReloadableDurationVar(3000, time.Millisecond, "Reporting.eventSampling.durationInMinutes")
	log := logger.NewLogger()

	t.Run("should put and get keys", func(t *testing.T) {
		assert.Equal(t, 3000*time.Millisecond, ttl.Load())
		es, _ := NewEventSampler(ctx, ttl, eventSamplerType, eventSamplingCardinality, conf, log)
		_ = es.Put("key1")
		_ = es.Put("key2")
		_ = es.Put("key3")
		val1, _ := es.Get("key1")
		val2, _ := es.Get("key2")
		val3, _ := es.Get("key3")
		val4, _ := es.Get("key4")

		assert.True(t, val1, "Expected key1 to be present")
		assert.True(t, val2, "Expected key2 to be present")
		assert.True(t, val3, "Expected key3 to be present")
		assert.False(t, val4, "Expected key4 to not be present")
	})

	t.Run("should not get evicted keys", func(t *testing.T) {
		conf.Set("Reporting.eventSampling.durationInMinutes", 100)
		assert.Equal(t, 100*time.Millisecond, ttl.Load())
		es, _ := NewEventSampler(ctx, ttl, eventSamplerType, eventSamplingCardinality, conf, log)
		_ = es.Put("key1")

		require.Eventually(t, func() bool {
			val1, _ := es.Get("key1")
			return !val1
		}, 1*time.Second, 50*time.Millisecond)
	})

	t.Run("should not add keys if length exceeds", func(t *testing.T) {
		conf.Set("Reporting.eventSampling.durationInMinutes", 3000)
		assert.Equal(t, 3000*time.Millisecond, ttl.Load())
		es, _ := NewEventSampler(ctx, ttl, eventSamplerType, eventSamplingCardinality, conf, log)
		_ = es.Put("key1")
		_ = es.Put("key2")
		_ = es.Put("key3")
		_ = es.Put("key4")
		_ = es.Put("key5")

		val1, _ := es.Get("key1")
		val2, _ := es.Get("key2")
		val3, _ := es.Get("key3")
		val4, _ := es.Get("key4")
		val5, _ := es.Get("key5")

		assert.True(t, val1, "Expected key1 to be present")
		assert.True(t, val2, "Expected key2 to be present")
		assert.True(t, val3, "Expected key3 to be present")
		assert.False(t, val4, "Expected key4 to not be added")
		assert.False(t, val5, "Expected key5 to not be added")
	})
}

func BenchmarkEventSampler(b *testing.B) {
	testCases := []struct {
		name             string
		eventSamplerType string
	}{
		{
			name:             "Badger",
			eventSamplerType: "badger",
		},
		{
			name:             "InMemoryCache",
			eventSamplerType: "in_memory_cache",
		},
	}

	ctx := context.Background()
	conf := config.New()
	ttl := conf.GetReloadableDurationVar(1, time.Minute, "Reporting.eventSampling.durationInMinutes")
	eventSamplerType := conf.GetReloadableStringVar("default", "Reporting.eventSampling.type")
	eventSamplingCardinality := conf.GetReloadableIntVar(10, 1, "Reporting.eventSampling.cardinality")
	log := logger.NewLogger()

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			conf.Set("Reporting.eventSampling.type", tc.eventSamplerType)

			eventSampler, err := NewEventSampler(
				ctx,
				ttl,
				eventSamplerType,
				eventSamplingCardinality,
				conf,
				log,
			)
			require.NoError(b, err)

			b.Run("Put", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					key := uuid.New().String()
					err := eventSampler.Put(key)
					require.NoError(b, err)
				}
			})

			b.Run("Get", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					key := uuid.New().String()

					err := eventSampler.Put(key)
					require.NoError(b, err)

					_, err = eventSampler.Get(key)
					require.NoError(b, err)
				}
			})

			eventSampler.Close()
		})
	}
}
