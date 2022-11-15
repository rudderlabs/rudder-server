package suppression

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
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

type tLog struct {
	times int
	logger.Logger
}

func (t *tLog) Warn(_ ...interface{}) {
	t.times++
}
