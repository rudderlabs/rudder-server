package throttling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TODO add more test scenarios
func TestGCRA(t *testing.T) {
	delta := 50 * time.Millisecond
	for _, tc := range []testCase{
		{name: "1 token each 1s", rate: 1, window: 1, expected: 1},
		{name: "2 tokens each 2s", rate: 2, window: 2, expected: 2},
		{name: "100 tokens each 1s", rate: 100, window: 1, expected: 100, errorMargin: 2},
		{name: "100 tokens each 3s", rate: 100, window: 3, expected: 100, errorMargin: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				g       gcra
				passed  int64
				endTest = time.NewTimer(time.Duration(tc.window)*time.Second + delta)
			)
		loop:
			for {
				select {
				case <-endTest.C:
					break loop
				default:
					allowed, _, _, _, err := g.limit(tc.name, 1, 1, tc.rate, tc.window)
					require.NoError(t, err)
					passed += allowed
					time.Sleep(time.Millisecond)
				}
			}
			// expected +1 because of burst which is the initial number of tokens in the bucket
			require.InDeltaf(
				t, tc.expected+1, passed, float64(tc.errorMargin),
				"Expected %d, got %d", tc.expected, passed,
			)
		})
	}
}

type testCase struct {
	name string
	rate,
	window,
	expected,
	errorMargin int64
}
