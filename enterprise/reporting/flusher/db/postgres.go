package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

type PostgresDB struct {
	DB *sql.DB
}

func New(connStr string, maxOpenConns int) (*PostgresDB, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpenConns)

	p := &PostgresDB{
		DB: db,
	}
	return p, nil
}

func (p *PostgresDB) GetStart(ctx context.Context, table string) (time.Time, error) {
	var start sql.NullTime
	query := fmt.Sprintf("SELECT MIN(reported_at) FROM %s", table)
	err := p.DB.QueryRowContext(ctx, query).Scan(&start)
	if err != nil {
		return time.Time{}, err
	}

	return start.Time, nil
}

func (p *PostgresDB) Delete(ctx context.Context, tableName string, minReportedAt, maxReportedAt time.Time) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE reported_at >= $1 AND reported_at < $2", tableName)
	_, err := p.DB.ExecContext(ctx, query, minReportedAt, maxReportedAt)
	return err
}

func (p *PostgresDB) Close() error {
	return p.DB.Close()
}
