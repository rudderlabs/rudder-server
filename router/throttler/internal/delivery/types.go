package delivery

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

type Logger interface {
	Warnn(msg string, fields ...logger.Field)
}

type Limiter interface {
	AllowAfter(ctx context.Context, cost, rate, window int64, key string) (bool, time.Duration, func(context.Context) error, error)
}
