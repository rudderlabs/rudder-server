package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

type PostgresDB struct {
	db *sql.DB
}

func NewPostgresDB(connStr string, maxOpenConns int) (*PostgresDB, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpenConns)
	return &PostgresDB{db: db}, nil
}

func (p *PostgresDB) GetStart(ctx context.Context, table string) (time.Time, error) {
	var start sql.NullTime
	query := fmt.Sprintf("SELECT MIN(reported_at) FROM %s", table)
	err := p.db.QueryRowContext(ctx, query).Scan(&start)
	if err != nil {
		return time.Time{}, err
	}

	return start.Time, nil
}

func (p *PostgresDB) FetchBatch(ctx context.Context, table string, start, end time.Time, limit, offset int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE reported_at >= $1 AND reported_at < $2 ORDER BY reported_at LIMIT $3 OFFSET $4`, table)
	return p.fetch(ctx, query, start, end, limit, offset)
}

func (p *PostgresDB) Fetch(ctx context.Context, table string, start, end time.Time) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE reported_at >= $1 AND reported_at < $2 ORDER BY reported_at`, table)
	return p.fetch(ctx, query, start, end)
}

func (p *PostgresDB) fetch(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}
	return result, nil
}

func (p *PostgresDB) Delete(ctx context.Context, tableName string, minReportedAt, maxReportedAt time.Time) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE reported_at >= $1 AND reported_at < $2", tableName)
	_, err := p.db.ExecContext(ctx, query, minReportedAt, maxReportedAt)
	return err
}

func (p *PostgresDB) Close() error {
	return p.db.Close()
}
