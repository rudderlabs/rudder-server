// +build go1.8

package clickhouse

import (
	"context"
	"database/sql/driver"
)

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	values, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return s.query(ctx, values)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	values, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return s.exec(ctx, values)
}
