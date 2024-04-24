package reporting

import (
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/stretchr/testify/assert"
)

func TestEventSampler(t *testing.T) {
	resetDuration := config.Default.GetReloadableDurationVar(10, time.Second)
	es := NewEventSampler(resetDuration)

	t.Run("MarkSampleEventAsCollected and IsSampleEventCollected", func(t *testing.T) {
		groupingColumnsHash := "test"
		assert.False(t, es.IsSampleEventCollected(groupingColumnsHash))
		es.MarkSampleEventAsCollected(groupingColumnsHash)
		assert.True(t, es.IsSampleEventCollected(groupingColumnsHash))
		assert.False(t, es.IsSampleEventCollected("new hash"))
	})
}
