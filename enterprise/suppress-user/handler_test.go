package suppression

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
)

func TestIsSuppressedConcurrency(t *testing.T) {
	log := &tLog{Logger: logger.NOP}
	h := newHandler(&fakeSuppresser{}, log)

	var wg sync.WaitGroup
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			h.GetSuppressedUser("workspaceID", "userID", "sourceID")
		}()
	}
	wg.Wait()
	require.Less(t, log.times, 1000)
}

type fakeSuppresser struct {
	Repository
}

func (*fakeSuppresser) Suppressed(_, _, _ string) (*model.Metadata, error) {
	// random failures, but always returning false
	if rand.New(rand.NewSource(time.Now().UnixNano())).Intn(2)%2 == 0 { // skipcq: GSC-G404
		return nil, fmt.Errorf("some error")
	} else {
		return &model.Metadata{}, nil
	}
}

type tLog struct {
	times int
	logger.Logger
}

func (t *tLog) Errorf(_ string, _ ...interface{}) {
	t.times++
}
