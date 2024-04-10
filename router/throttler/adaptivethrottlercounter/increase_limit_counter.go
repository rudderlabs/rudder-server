package adaptivethrottlercounter

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type increaseLimitCounter struct {
	window             func() time.Duration
	increasePercentage config.ValueLoader[int64]
	limitFactor        *limitFactor
	throttledCountMu   sync.Mutex
	throttledCount     int64
}

func (c *increaseLimitCounter) ResponseCodeReceived(code int) {
	if code == http.StatusTooManyRequests {
		c.throttledCountMu.Lock()
		c.throttledCount++
		c.throttledCountMu.Unlock()
	}
}

func (c *increaseLimitCounter) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.window()):
			c.throttledCountMu.Lock()
			if c.throttledCount == 0 {
				c.limitFactor.Add(float64(c.increasePercentage.Load()) / 100)
			}
			c.throttledCount = 0
			c.throttledCountMu.Unlock()
		}
	}
}
