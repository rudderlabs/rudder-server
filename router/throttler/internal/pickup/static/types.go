package static

import (
	"context"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

type Logger interface {
	Warnn(msg string, fields ...logger.Field)
}

type Limiter interface {
	Allow(ctx context.Context, cost, rate, window int64, key string) (bool, func(context.Context) error, error)
}
