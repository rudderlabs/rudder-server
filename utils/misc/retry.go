package misc

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func RetryWith(parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) error) error {
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
	}
	return err
}

func QueryWithRetries(parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) (interface{}, error)) (interface{}, error) {
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
	}
	return res, err
}
