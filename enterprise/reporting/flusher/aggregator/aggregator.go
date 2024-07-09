//go:generate mockgen -destination=./aggregator_mock.go -package=aggregator -source=./aggregator.go Aggregator
package aggregator

import (
	"context"
	"encoding/json"
	"time"
)

type Aggregator interface {
	Aggregate(ctx context.Context, start, end time.Time) (jsonReports []json.RawMessage, err error)
}
