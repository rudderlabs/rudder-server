package adaptivethrottlercounter

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

type decreaseLimitCounter struct {
	window                      func() time.Duration
	waitWindow                  func() time.Duration
	decreasePercentage          config.ValueLoader[int64]
	throttleTolerancePercentage func() int64
	limitFactor                 *limitFactor

	counterMu      sync.Mutex
	throttledCount int64
	totalCount     int64
}

func (c *decreaseLimitCounter) ResponseCodeReceived(code int) {
	c.counterMu.Lock()
	defer c.counterMu.Unlock()
	if code == http.StatusTooManyRequests {
		c.throttledCount++
	}
	c.totalCount++
}

func (c *decreaseLimitCounter) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.window()):
			c.counterMu.Lock()
			var throttledRate float64
			if c.totalCount > 0 {
				throttledRate = float64(c.throttledCount) / float64(c.totalCount)
				c.throttledCount = 0
				c.totalCount = 0
			}
			c.counterMu.Unlock()
			if throttledRate > float64(c.throttleTolerancePercentage())/100 {
				c.limitFactor.Add(-float64(c.decreasePercentage.Load()) / 100)
				if err := c.wait(ctx); err != nil {
					return
				}
			}
		}
	}
}

// wait waits for the waitWindow duration and resets the throttledCount and totalCount to 0
func (c *decreaseLimitCounter) wait(ctx context.Context) error {
	if err := misc.SleepCtx(ctx, c.waitWindow()); err != nil {
		return err
	}
	c.counterMu.Lock()
	c.throttledCount = 0
	c.totalCount = 0
	c.counterMu.Unlock()
	return nil
}
