package reporting

import (
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/stretchr/testify/assert"
)

func TestPutAndGet(t *testing.T) {
	conf := config.New()
	ttl := conf.GetReloadableDurationVar(1, time.Second, "Reporting.eventSamplingDurationInMinutes")
	limit := conf.GetReloadableIntVar(3, 1, "Reporting.eventSamplingCardinality")

	es := NewEventSampler[string](ttl, limit)

	es.Put("key1")
	es.Put("key2")
	es.Put("key3")

	assert.True(t, es.Get("key1"), "Expected key1 to be present")
	assert.True(t, es.Get("key2"), "Expected key2 to be present")
	assert.True(t, es.Get("key3"), "Expected key3 to be present")

	es.Put("key4")
	assert.False(t, es.Get("key4"), "Expected key4 not to be added as limit is reached")
}

func TestLimitEnforcement(t *testing.T) {
	conf := config.New()
	ttl := conf.GetReloadableDurationVar(1, time.Second, "Reporting.eventSamplingDurationInMinutes")
	limit := conf.GetReloadableIntVar(2, 1, "Reporting.eventSamplingCardinality")

	es := NewEventSampler[string](ttl, limit)

	es.Put("key1")
	es.Put("key2")
	es.Put("key3")

	assert.Equal(t, 2, es.length, "Expected length to be 2 due to limit")
	assert.True(t, es.Get("key1"), "Expected key1 to be present")
	assert.True(t, es.Get("key2"), "Expected key2 to be present")
	assert.False(t, es.Get("key3"), "Expected key3 not to be present as limit was reached")
}

func TestEvictionOnTTLExpiry(t *testing.T) {
	conf := config.New()
	ttl := conf.GetReloadableDurationVar(500, time.Millisecond, "Reporting.eventSamplingDurationInMinutes")
	limit := conf.GetReloadableIntVar(3, 1, "Reporting.eventSamplingCardinality")

	es := NewEventSampler[string](ttl, limit)

	es.Put("key1")
	time.Sleep(600 * time.Millisecond)

	assert.False(t, es.Get("key1"), "Expected key1 to be evicted after TTL expiry")
	assert.Equal(t, 0, es.length, "Expected length to be 0 after TTL expiry eviction")
}
