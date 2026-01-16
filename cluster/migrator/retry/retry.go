package retry

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v5"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/backoffvoid"
)

// PerpetualExponentialBackoff retries the given function with an exponential backoff strategy
// until it succeeds or the context is cancelled.
func PerpetualExponentialBackoffWithNotify(ctx context.Context, config *config.Config, fn func() error, notify func(error, time.Duration)) error {
	return backoffvoid.Retry(ctx,
		fn,
		backoff.WithBackOff(&backoff.ExponentialBackOff{
			InitialInterval:     config.GetDurationVar(int64(backoff.DefaultInitialInterval), time.Nanosecond, "PartitionMigration.Retry.InitialInterval"),
			RandomizationFactor: config.GetFloat64Var(float64(backoff.DefaultRandomizationFactor), "PartitionMigration.Retry.RandomizationFactor"),
			Multiplier:          config.GetFloat64Var(float64(backoff.DefaultMultiplier), "PartitionMigration.Retry.Multiplier"),
			MaxInterval:         config.GetDurationVar(int64(backoff.DefaultMaxInterval), time.Nanosecond, "PartitionMigration.Retry.MaxInterval"),
		}),
		backoff.WithNotify(notify),
		backoff.WithMaxElapsedTime(0), // perpetual retries
	)
}
