package throttling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TODO add more test scenarios
func TestGCRA(t *testing.T) {
	g := gcra{
		m:  make(map[string]interface{}),
		ex: make(map[string]time.Time),
	}

	var (
		passed  int64
		rate    int64 = 3
		window        = time.Duration(1)
		endTest       = time.NewTimer(window*time.Second + (50 * time.Millisecond))
	)
loop:
	for {
		select {
		case <-endTest.C:
			break loop
		default:
			allowed, _, _, _, err := g.limit("test", 1, rate, rate, int64(window))
			require.NoError(t, err)
			passed += allowed
			time.Sleep(time.Millisecond)
		}
	}

	t.Log("PASSED:", passed)
}
