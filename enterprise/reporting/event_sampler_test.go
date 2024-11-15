package reporting

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestPutAndGet(t *testing.T) {
	conf := config.New()
	ttl := conf.GetReloadableDurationVar(1, time.Second, "Reporting.eventSamplingDurationInMinutes")
	log := logger.NewLogger()

	es, _ := NewEventSampler("/test-reporting-badger", ttl, conf, log)

	_ = es.Put([]byte("key1"))
	_ = es.Put([]byte("key2"))
	_ = es.Put([]byte("key3"))
	val1, _ := es.Get([]byte("key1"))
	val2, _ := es.Get([]byte("key2"))
	val3, _ := es.Get([]byte("key3"))

	assert.True(t, val1, "Expected key1 to be present")
	assert.True(t, val2, "Expected key2 to be present")
	assert.True(t, val3, "Expected key3 to be present")

	es.Close()
}

func TestEvictionOnTTLExpiry(t *testing.T) {
	conf := config.New()
	ttl := conf.GetReloadableDurationVar(500, time.Millisecond, "Reporting.eventSamplingDurationInMinutes")
	log := logger.NewLogger()

	es, _ := NewEventSampler("/test-reporting-badger", ttl, conf, log)

	_ = es.Put([]byte("key1"))

	time.Sleep(600 * time.Millisecond)

	val1, _ := es.Get([]byte("key1"))

	assert.False(t, val1, "Expected key1 to be evicted")

	es.Close()
}
