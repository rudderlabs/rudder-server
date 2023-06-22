package suppression

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestIsSuppressedConcurrency(t *testing.T) {
	log := &tLog{Logger: logger.NOP}
	h := newHandler(&fakeSuppresser{}, log)

	var wg sync.WaitGroup
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			require.False(t, h.IsSuppressedUser("workspaceID", "userID", "sourceID"))
		}()
	}
	wg.Wait()
	require.Less(t, log.times, 1000)
}

func TestGetCreatedAtConcurrency(t *testing.T) {
	log := &tLog{Logger: logger.NOP}
	h := newHandler(&fakeSuppresser{}, log)

	var wg sync.WaitGroup
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			require.True(t, h.GetCreatedAt("workspaceID", "userID", "sourceID").IsZero())
		}()
	}
	wg.Wait()
	require.Less(t, log.times, 1000)
}

type fakeSuppresser struct {
	Repository
}

func (*fakeSuppresser) Suppressed(_, _, _ string) (bool, error) {
	// random failures, but always returning false
	if rand.Intn(2)%2 == 0 { // skipcq: GSC-G404
		return false, fmt.Errorf("some error")
	} else {
		return false, nil
	}
}

func (*fakeSuppresser) GetCreatedAt(_, _, _ string) (time.Time, error) {
	// random failures, but always returning a zero time
	if rand.Intn(2)%2 == 0 { // skipcq: GSC-G404
		return time.Time{}, fmt.Errorf("some error")
	} else {
		return time.Time{}, nil
	}
}

type tLog struct {
	times int
	logger.Logger
}

func (t *tLog) Errorf(_ string, _ ...interface{}) {
	t.times++
}
