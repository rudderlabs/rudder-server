package throttling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGCRA(t *testing.T) {
	// @TODO finish this test
	g := gcra{
		getterSetter: &inMemoryGetterSetter{
			m:  make(map[string]interface{}),
			ex: make(map[string]time.Time),
		},
	}
	ticker := time.NewTicker(time.Second)
	for {
		allowed, _, _, _, err := g.limit("test", 1, 1, 1, 5)
		require.NoError(t, err)
		t.Log(allowed)
		<-ticker.C
	}
}
