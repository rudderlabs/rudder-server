package waitgroup

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// New returns a WaitGroup with context support on Wait().
// WARNING: IT SHOULD NOT BE USED unless for testing or edge cases with legacy code.
// The preferable way is to have the go routines honour a context and have those timeout, instead of having
// Wait() to timeout.
func New() *waitGroup {
	return &waitGroup{
		sem: semaphore.NewWeighted(1),
	}
}

type waitGroup struct {
	delta int
	sem   *semaphore.Weighted
}

func (wg *waitGroup) Add(delta int) {
	err := wg.sem.Acquire(context.Background(), 1)
	if err != nil {
		panic(err)
	}

	defer wg.sem.Release(1)
	wg.delta += delta
	if wg.delta < 0 {
		panic("negative delta")
	}
}

func (wg *waitGroup) Done() {
	wg.Add(-1)
}

func (wg *waitGroup) Wait(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := wg.sem.Acquire(ctx, 1); err != nil {
			return err // semaphore is left unchanged on error, no need to release
		}
		v := wg.delta
		wg.sem.Release(1)
		if v == 0 {
			return nil
		}
	}
}
