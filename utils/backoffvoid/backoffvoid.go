package backoffvoid

import (
	"context"

	"github.com/cenkalti/backoff/v5"
)

// Retry retries the given function according to the provided backoff options.
func Retry(ctx context.Context, fn func() error, opts ...backoff.RetryOption) error {
	_, err := backoff.Retry(ctx,
		func() (struct{}, error) {
			return struct{}{}, fn()
		},
		opts...,
	)
	return err
}
