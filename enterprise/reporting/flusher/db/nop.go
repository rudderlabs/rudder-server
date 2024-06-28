package db

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/report"
)

type NOP struct{}

func (n *NOP) InitDB() error {
	return nil
}

func (n *NOP) GetStart(ctx context.Context, table string) (time.Time, error) {
	return time.Time{}, nil
}

func (n *NOP) FetchBatch(ctx context.Context, table string, start, end time.Time, limit, offset int) ([]report.RawReport, error) {
	return nil, nil
}

func (n *NOP) Delete(ctx context.Context, table string, start, end time.Time) error {
	return nil
}

func (n *NOP) CloseDB() error {
	return nil
}
