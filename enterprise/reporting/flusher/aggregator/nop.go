package aggregator

import (
	"context"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/report"
)

type NOP struct{}

func (n *NOP) Aggregate(ctx context.Context, aggReport, report report.DecodedReport) ([]interface{}, error) {
	return nil, nil
}
