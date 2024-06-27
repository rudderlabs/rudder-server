package safeguard

import (
	"context"
	"sync"
	"time"
)

var defaultGuard = &Guard{}

type Guard struct {
	Go   func(func())
	once sync.Once
}

func (g *Guard) MustStop(ctx context.Context, timeout time.Duration) func() {
	g.once.Do(func() {
		if g.Go == nil {
			g.Go = func(fn func()) {
				go fn()
			}
		}
	})

	inCtx, cancel := context.WithCancel(context.Background())

	g.Go(func() {
		select {
		case <-inCtx.Done():
			return
		case <-ctx.Done():
		}

		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			panic("timeout waiting for method stop")
		case <-inCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
		}
	})

	return cancel
}

func MustStop(ctx context.Context, timeout time.Duration) func() {
	return defaultGuard.MustStop(ctx, timeout)
}
