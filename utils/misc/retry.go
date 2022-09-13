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
		attempt++

		if notify != nil {
			notify(attempt)
		}
	}
	return err
}

func QueryWithRetries(parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) (interface{}, error)) (interface{}, error) {
	return QueryWithRetriesAndNotify(parentContext, timeout, maxAttempts, f, nil)
}

func QueryWithRetriesAndNotify(parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) (interface{}, error), notify Notify) (interface{}, error) {
	if parentContext.Err() != nil {
		return nil, parentContext.Err()
	}
	attempt := 1
	if maxAttempts < 1 {
		return nil, fmt.Errorf("invalid parameter maxAttempts: %d", maxAttempts)
	}
	var err error
	var res interface{}
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
		attempt++

		if notify != nil {
			notify(attempt)
		}
	}
	return res, err
}
