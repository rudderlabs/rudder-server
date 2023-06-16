package forwarder

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
)

func TestSampler(t *testing.T) {
	key := &proto.EventSchemaKey{
		WriteKey:        "writeKey",
		EventType:       "eventType",
		EventIdentifier: "eventIdentifier",
	}
	now := time.Now()
	sampler := newSampler[string](time.Second, 10000)
	sampler.now = func() time.Time { return now }

	require.True(t, sampler.Sample(key.String()), "should sample first time")
	require.False(t, sampler.Sample(key.String()), "should not sample if the sampling period has not elapsed")
	now = now.Add(time.Second)
	require.True(t, sampler.Sample(key.String()), "should sample if the sampling period has elapsed")

	now = now.Add(time.Second).Add(-1)
	require.False(t, sampler.Sample(key.String()), "should not sample if the sampling period has not elapsed")

	t.Run("panic scenarios", func(t *testing.T) {
		require.Panics(t, func() { newSampler[string](time.Second, 0) }, "should panic if cache size is 0")
		require.Panics(t, func() { newSampler[string](time.Second, -1) }, "should panic if cache size is negative")
	})
}
