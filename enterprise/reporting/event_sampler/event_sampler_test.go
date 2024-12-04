package event_sampler

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestBadger(t *testing.T) {
	conf := config.New()
	ttl := conf.GetReloadableDurationVar(500, time.Millisecond, "Reporting.eventSampling.durationInMinutes")
	eventSamplerType := conf.GetReloadableStringVar("badger", "Reporting.eventSampling.type")
	eventSamplingCardinality := conf.GetReloadableIntVar(10, 1, "Reporting.eventSampling.cardinality")
	log := logger.NewLogger()

	t.Run("should put and get keys", func(t *testing.T) {
		es, _ := NewEventSampler(ttl, eventSamplerType, eventSamplingCardinality, conf, log)
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
		es, _ := NewEventSampler(ttl, eventSamplerType, eventSamplingCardinality, conf, log)
		_ = es.Put("key1")

		time.Sleep(600 * time.Millisecond)
		val1, _ := es.Get("key1")

		assert.False(t, val1, "Expected key1 to be evicted")
		es.Close()
	})
}

func TestInMemoryCache(t *testing.T) {
	conf := config.New()
	ttl := conf.GetReloadableDurationVar(500, time.Millisecond, "Reporting.eventSampling.durationInMinutes")
	eventSamplerType := conf.GetReloadableStringVar("in_memory_cache", "Reporting.eventSampling.type")
	eventSamplingCardinality := conf.GetReloadableIntVar(3, 1, "Reporting.eventSampling.cardinality")
	log := logger.NewLogger()

	t.Run("should put and get keys", func(t *testing.T) {
		es, _ := NewEventSampler(ttl, eventSamplerType, eventSamplingCardinality, conf, log)
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
		es, _ := NewEventSampler(ttl, eventSamplerType, eventSamplingCardinality, conf, log)
		_ = es.Put("key1")

		time.Sleep(600 * time.Millisecond)

		val1, _ := es.Get("key1")

		assert.False(t, val1, "Expected key1 to be evicted")
	})

	t.Run("should not add keys if length exceeds", func(t *testing.T) {
		es, _ := NewEventSampler(ttl, eventSamplerType, eventSamplingCardinality, conf, log)
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

	conf := config.New()
	ttl := conf.GetReloadableDurationVar(1, time.Minute, "Reporting.eventSampling.durationInMinutes")
	eventSamplerType := conf.GetReloadableStringVar("default", "Reporting.eventSampling.type")
	eventSamplingCardinality := conf.GetReloadableIntVar(10, 1, "Reporting.eventSampling.cardinality")
	log := logger.NewLogger()

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			conf.Set("Reporting.eventSampling.type", tc.eventSamplerType)

			eventSampler, err := NewEventSampler(
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
