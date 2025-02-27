//go:generate mockgen -destination=./aggregator_mock.go -package=aggregator -source=./aggregator.go Aggregator
package aggregator

import (
	"context"
	"time"
)

type Aggregator interface {
	Aggregate(ctx context.Context, start, end time.Time) (aggregates []Aggregate, err error)
}
