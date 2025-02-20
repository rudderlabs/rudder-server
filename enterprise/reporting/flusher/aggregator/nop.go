package aggregator

import (
	"context"
	"time"
)

type NOP struct{}

func (n *NOP) Aggregate(ctx context.Context, start, end time.Time) (jsonReports []Aggregate, err error) {
	return nil, nil
}
