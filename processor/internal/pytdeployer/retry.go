package pytdeployer

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// retrySettings holds the exponential-backoff parameters for k8s API calls. They
// are read once at startup (not per call) and reused for every retry.
type retrySettings struct {
	initialInterval     time.Duration
	maxInterval         time.Duration
	maxElapsedTime      time.Duration
	multiplier          float64
	randomizationFactor float64
}

func newRetrySettings(conf *config.Config) retrySettings {
	return retrySettings{
		initialInterval:     conf.GetDurationVar(200, time.Millisecond, "Processor.pytDeployer.retry.initialInterval"),
		maxInterval:         conf.GetDurationVar(3, time.Second, "Processor.pytDeployer.retry.maxInterval"),
		maxElapsedTime:      conf.GetDurationVar(30, time.Second, "Processor.pytDeployer.retry.maxElapsedTime"),
		multiplier:          conf.GetFloat64Var(1.5, "Processor.pytDeployer.retry.multiplier"),
		randomizationFactor: conf.GetFloat64Var(0.5, "Processor.pytDeployer.retry.randomizationFactor"),
	}
}

// withRetry runs fn with exponential backoff, retrying only transient Kubernetes
// API errors (429, timeouts, internal/unavailable). Any other error is permanent
// and returned immediately.
func withRetry[T any](ctx context.Context, rs retrySettings, fn func() (T, error)) (T, error) {
	return backoff.Retry(
		ctx,
		func() (T, error) {
			v, err := fn()
			if err == nil || isTransient(err) {
				return v, err
			}
			return v, backoff.Permanent(err)
		},
		backoff.WithBackOff(&backoff.ExponentialBackOff{
			InitialInterval:     rs.initialInterval,
			RandomizationFactor: rs.randomizationFactor,
			Multiplier:          rs.multiplier,
			MaxInterval:         rs.maxInterval,
		}),
		backoff.WithMaxElapsedTime(rs.maxElapsedTime),
	)
}

func isTransient(err error) bool {
	return apierrors.IsTooManyRequests(err) ||
		apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsInternalError(err) ||
		apierrors.IsServiceUnavailable(err)
}
