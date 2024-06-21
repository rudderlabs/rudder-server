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

func (p *PostgresDB) GetMinReportedAt(ctx context.Context, table string) (time.Time, error) {
	var minReportedAt sql.NullTime
	query := fmt.Sprintf("SELECT MIN(reported_at) FROM %s", table)
	err := p.db.QueryRowContext(ctx, query).Scan(&minReportedAt)
	if err != nil {
		return time.Time{}, err
	}

	return minReportedAt.Time, nil
}

func (p *PostgresDB) FetchReports(ctx context.Context, tableName string, minReportedAt, maxReportedAt time.Time, limit, offset int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE reported_at >= $1 AND reported_at < $2 ORDER BY reported_at LIMIT $3 OFFSET $4`, tableName)
	rows, err := p.db.QueryContext(ctx, query, minReportedAt, maxReportedAt, limit, offset)
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

func (p *PostgresDB) DeleteReports(ctx context.Context, tableName string, minReportedAt, maxReportedAt time.Time) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE reported_at >= $1 AND reported_at < $2", tableName)
	_, err := p.db.ExecContext(ctx, query, minReportedAt, maxReportedAt)
	return err
}

func (p *PostgresDB) Close() error {
    return p.db.Close()
}
