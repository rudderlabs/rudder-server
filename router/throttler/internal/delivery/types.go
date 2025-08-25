package delivery

import (
	"context"
	"time"
)

type Limiter interface {
	AllowAfter(ctx context.Context, cost, rate, window int64, key string) (bool, time.Duration, func(context.Context) error, error)
}
