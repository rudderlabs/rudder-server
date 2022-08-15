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
		if !errors.Is(err, context.DeadlineExceeded) || parentContext.Err() != nil {
			return err
		}
		attempt++
	}
	return err
}
