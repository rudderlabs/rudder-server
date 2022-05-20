package rsources

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
)

type extension interface {
	getReadDB() *sql.DB
	createStatsTable(ctx context.Context, jobRunId string) error
	dropTables(ctx context.Context, jobRunId string) error
	cleanupLoop(ctx context.Context) error
	doCleanupTables(ctx context.Context) error
}

type defaultExtension struct {
	localDB *sql.DB
}

func (r *defaultExtension) getReadDB() *sql.DB {
	return r.localDB
}

func (r *defaultExtension) createStatsTable(ctx context.Context, jobRunId string) error {
	dbHost := config.GetEnv("JOBS_DB_HOST", "localhost")
	jobRunIdStatTableName := fmt.Sprintf("%s%s", tablePrefix, jobRunId)
	sqlStatement := fmt.Sprintf(`create table if not exists "%s" (
		db_name text not null default '%s',
		source_id text not null,
		destination_id text not null,
		task_run_id text not null,
		in_count integer not null default 0,
		out_count integer not null default 0,
		failed_count integer not null default 0,
		ts timestamp not null default NOW(),
		primary key (db_name, source_id, destination_id, task_run_id)
	)`, jobRunIdStatTableName, dbHost)
	_, err := r.localDB.ExecContext(ctx, sqlStatement)
	return err
}

func (r *defaultExtension) cleanupLoop(ctx context.Context) error {
	err := r.doCleanupTables(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(sourcesStatCleanUpSleepTime):
			err := r.doCleanupTables(ctx)
			if err != nil {
				return err
			}
		}
	}
}

func (r *defaultExtension) doCleanupTables(ctx context.Context) error {
	db := r.localDB
	tables, err := getAllTableNames(ctx, db)
	if err != nil {
		return err
	}

	before := time.Now().Add(-24 * time.Hour)
	for _, table := range tables {
		noRowsAfter, err := latestTableEntryBefore(ctx, db, table, before)
		if err != nil {
			var e *pq.Error
			if errors.As(err, &e) && e.Code == "42P01" {
				continue // if table does not exist resume
			}
			return err
		}
		if noRowsAfter {
			err := r.dropTables(ctx, strings.TrimPrefix(table, tablePrefix))
			if err != nil {
				return err
			}
			tables = append(tables, table)
		}
	}
	return nil
}

func (r *defaultExtension) dropTables(ctx context.Context, jobRunId string) error {
	table := fmt.Sprintf("%s%s", tablePrefix, jobRunId)
	sqlStatement := fmt.Sprintf(`drop table if exists %s`, table)
	_, err := r.localDB.ExecContext(ctx, sqlStatement)
	return err
}

func getAllTableNames(ctx context.Context, db *sql.DB) ([]string, error) {
	var tables []string
	statTableNameLike := tablePrefix + `%`
	sqlStatement := fmt.Sprintf(`
	select table_name from information_schema.tables
	where table_schema='public' and table_name ilike '%s'`, statTableNameLike)
	rows, err := db.QueryContext(ctx, sqlStatement)
	if err != nil {
		return tables, err
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return tables, err
		}
		tables = append(tables, table)
	}
	return tables, rows.Err()
}

func latestTableEntryBefore(ctx context.Context, db *sql.DB, table string, before time.Time) (bool, error) {
	sqlStatement := fmt.Sprintf(`select max(ts) from "%s"`, table)
	var t time.Time
	if err := db.QueryRowContext(ctx, sqlStatement).Scan(&t); err != nil {
		return false, err
	}
	return t.Before(before), nil
}
