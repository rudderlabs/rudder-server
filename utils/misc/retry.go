package misc

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Notify func(attempt int)

func RetryWith(parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) error) error {
	return RetryWithNotify(parentContext, timeout, maxAttempts, f, nil)
}

// RetryWithNotify retries a function f with a timeout and a maximum number of attempts & calls notify on each failure.
func RetryWithNotify(parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) error, notify Notify) error {
	if parentContext.Err() != nil {
		return parentContext.Err()
	}
	attempt := 1
	if maxAttempts < 1 {
		return fmt.Errorf("invalid parameter maxAttempts: %d", maxAttempts)
	}
	var err error
	for attempt <= maxAttempts {
		ctx, cancel := context.WithTimeout(parentContext, timeout)
		defer cancel()
		err = f(ctx)
		if err == nil {
			return nil
		}
		// only retry if the child context's deadline was exceeded
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) || parentContext.Err() != nil {
			return err
		}
		if notify != nil {
			notify(attempt)
		}
		attempt++
	}
	return err
}

func QueryWithRetries[T any](parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) (T, error)) (T, error) {
	return QueryWithRetriesAndNotify(parentContext, timeout, maxAttempts, f, nil)
}

func QueryWithRetriesAndNotify[T any](parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) (T, error), notify Notify) (T, error) {
	var res T
	if parentContext.Err() != nil {
		return res, parentContext.Err()
	}
	attempt := 1
	if maxAttempts < 1 {
		return res, fmt.Errorf("invalid parameter maxAttempts: %d", maxAttempts)
	}
	var err error
	for attempt <= maxAttempts {
		ctx, cancel := context.WithTimeout(parentContext, timeout)
		defer cancel()

		res, err = f(ctx)
		if err == nil {
			return res, nil
		}
		// only retry if the child context's deadline was exceeded
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) || parentContext.Err() != nil {
			return res, err
		}
		if notify != nil {
			notify(attempt)
		}
		attempt++
	}
	return res, err
}
