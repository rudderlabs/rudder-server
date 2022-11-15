package suppression

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

func TestDebounceLogConcurrency(t *testing.T) {
	ctx := context.Background()
	log := &tLog{Logger: logger.NOP}
	h := newHandler(ctx, &fakeSuppresser{}, log)

	var wg sync.WaitGroup
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			_ = h.IsSuppressedUser("workspaceID", "userID", "sourceID")
		}()
	}
	wg.Wait()
	require.Less(t, log.times, 1000)
}

type fakeSuppresser struct {
	Repository
}

func (*fakeSuppresser) Suppressed(_, _, _ string) (bool, error) {
	return false, fmt.Errorf("some error")
}

type tLog struct {
	times int
	logger.Logger
}

func (t *tLog) Warn(_ ...interface{}) {
	t.times++
}
