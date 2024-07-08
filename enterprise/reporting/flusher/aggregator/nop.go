package aggregator

import (
	"context"
	"encoding/json"
	"time"
)

type NOP struct{}

func (n *NOP) Aggregate(ctx context.Context, start, end time.Time) (jsonReports []json.RawMessage, err error) {
	return nil, nil
}
