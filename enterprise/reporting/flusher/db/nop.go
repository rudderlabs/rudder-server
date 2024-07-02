package db

import (
	"context"
	"time"
)

type NOP struct{}

func (n *NOP) GetStart(ctx context.Context, table string) (time.Time, error) {
	return time.Time{}, nil
}

func (n *NOP) Delete(ctx context.Context, table string, start, end time.Time) error {
	return nil
}

func (n *NOP) Close() error {
	return nil
}
