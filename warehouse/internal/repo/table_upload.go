package repo

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	tableUploadTableName            = warehouseutils.WarehouseTableUploadsTable
	tableUploadUniqueConstraintName = "unique_table_upload_wh_upload"
)

const tableUploadColumns = `
	id,
    wh_upload_id,
    table_name,
    status,
    error,
    last_exec_time,
    total_events,
    created_at,
    updated_at,
    location
`

// TableUploads is a repository for table uploads
type TableUploads repo

func NewTableUploads(db *sql.DB, opts ...Opt) *TableUploads {
	r := &TableUploads{
		db:  db,
		now: timeutil.Now,
	}
	for _, opt := range opts {
		opt((*repo)(r))
	}
	return r
}

func (repo *TableUploads) Insert(ctx context.Context, uploadID int64, tableNames []string) error {
	var (
		txn  *sql.Tx
		stmt *sql.Stmt
		err  error
	)

	if txn, err = repo.db.Begin(); err != nil {
		return fmt.Errorf(`begin transaction: %w`, err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
		}
	}()

	stmt, err = txn.PrepareContext(ctx, `
		INSERT INTO `+tableUploadTableName+` (
		  wh_upload_id, table_name, status,
		  error, created_at, updated_at
		)
		VALUES
		  ($1, $2, $3, $4, $5, $6)
		ON CONFLICT
		ON CONSTRAINT `+tableUploadUniqueConstraintName+`
		DO NOTHING;
`)
	if err != nil {
		return fmt.Errorf(`prepared statement: %w`, err)
	}
	defer func() { _ = stmt.Close() }()

	for _, tableName := range tableNames {
		_, err = stmt.ExecContext(ctx, uploadID, tableName, model.TableUploadWaiting, "{}", repo.now(), repo.now())
		if err != nil {
			return fmt.Errorf(`stmt exec: %w`, err)
		}
	}

	if _, err = stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf(`final stmt exec: %w`, err)
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf(`commit: %w`, err)
	}

	return nil
}

func (repo *TableUploads) SetStatus(ctx context.Context, status string, uploadId int64, tableName string) error {
	if status == model.TableUploadExecuting {
		return repo.setExecutingStatus(ctx, status, uploadId, tableName)
	}

	query := `
		UPDATE
			` + tableUploadTableName + `
		SET
		  status = $1,
		  updated_at = $2
		WHERE
		  wh_upload_id = $3 AND
		  table_name = $4;
`
	queryArgs := []any{
		status,
		repo.now(),
		uploadId,
		tableName,
	}
	result, err := repo.db.ExecContext(
		ctx,
		query,
		queryArgs...,
	)
	if err != nil {
		return fmt.Errorf(`set status: %w`, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(`rows affected: %w`, err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf(`no rows affected`)
	}

	return nil
}

func (repo *TableUploads) SetTotalEvents(ctx context.Context, uploadId int64, tableName string, stagingFileIDs []int64) error {
	subQuery := `
		WITH row_numbered_load_files as (
		  SELECT
			total_events,
			row_number() OVER (
			  PARTITION BY staging_file_id,
			  table_name
			  ORDER BY
				id DESC
			) AS row_number
		  FROM
			` + loadTableName + `
		  WHERE
			staging_file_id = ANY($3)
			AND table_name = $2
		)
		SELECT
		  sum(total_events) as total
		FROM
		  row_numbered_load_files
		WHERE
		  row_number = 1
`
	query := `
		UPDATE
			` + tableUploadTableName + `
		SET
		  total_events = subquery.total
		FROM
		  (` + subQuery + `) AS subquery
		WHERE
		  wh_upload_id = $1 AND
		  table_name = $2;
`
	queryArgs := []any{
		uploadId,
		tableName,
		stagingFileIDs,
	}
	result, err := repo.db.ExecContext(
		ctx,
		query,
		queryArgs...,
	)
	if err != nil {
		return fmt.Errorf(`set total events: %w`, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(`rows affected: %w`, err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf(`no rows affected`)
	}

	return nil
}

func (repo *TableUploads) setExecutingStatus(ctx context.Context, status string, uploadId int64, tableName string) error {
	query := `
		UPDATE
			` + tableUploadTableName + `
		SET
		  status = $1,
		  updated_at = $2,
		  last_exec_time = $3
		WHERE
		  wh_upload_id = $4 AND
		  table_name = $5;
`
	queryArgs := []any{
		status,
		repo.now(),
		repo.now(),
		uploadId,
		tableName,
	}
	result, err := repo.db.ExecContext(
		ctx,
		query,
		queryArgs...,
	)
	if err != nil {
		return fmt.Errorf(`set status: %w`, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(`rows affected: %w`, err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf(`no rows affected`)
	}

	return nil
}

func (repo *TableUploads) SetError(ctx context.Context, status string, statusError error, uploadId int64, tableName string) error {
	query := `
		UPDATE
			` + tableUploadTableName + `
		SET
		  status = $1,
		  updated_at = $2,
		  error = $3
		WHERE
		  wh_upload_id = $4 AND
		  table_name = $5;
`
	queryArgs := []any{
		status,
		repo.now(),
		misc.QuoteLiteral(statusError.Error()),
		uploadId,
		tableName,
	}
	result, err := repo.db.ExecContext(
		ctx,
		query,
		queryArgs...,
	)
	if err != nil {
		return fmt.Errorf(`set error: %w`, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(`rows affected: %w`, err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf(`no rows affected`)
	}

	return nil
}

func (repo *TableUploads) GetTotalEventsPerTable(ctx context.Context, uploadId int64, tableName string) (int64, error) {
	var (
		count int64
		err   error
	)
	err = repo.db.QueryRowContext(ctx,
		`
			SELECT
				total_events
			FROM
				`+tableUploadTableName+`
			WHERE
				wh_upload_id = $1 AND
				table_name = $2;
`,
		uploadId, tableName).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf(`counting total events per table: %w`, err)
	}

	return count, nil
}

func (repo *TableUploads) GetLocation(ctx context.Context, uploadId int64, tableName string) (string, error) {
	var (
		location sql.NullString
		err      error
	)
	err = repo.db.QueryRowContext(ctx,
		`
			SELECT
				location
			FROM
				`+tableUploadTableName+`
			WHERE
				wh_upload_id = $1 AND
				table_name = $2
			LIMIT 1;
`,
		uploadId, tableName).Scan(&location)
	if err != nil {
		return "", fmt.Errorf(`getting location: %w`, err)
	}
	if !location.Valid {
		return "", fmt.Errorf(`location is null`)
	}

	return location.String, nil
}

func (repo *TableUploads) GetTotalExportedEvents(ctx context.Context, uploadId int64, skipTables []string) (int64, error) {
	var (
		count int64
		err   error
	)

	if skipTables == nil {
		skipTables = []string{}
	}

	err = repo.db.QueryRowContext(ctx, `
			SELECT
				sum(total_events)
			FROM
				`+tableUploadTableName+`
			WHERE
				wh_upload_id = $1 AND
				status = $2 AND
				table_name NOT ANY($3);
`,
		uploadId, model.ExportedData, skipTables).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf(`counting total exported events: %w`, err)
	}

	return count, nil
}

func (repo *TableUploads) ExistsForUpload(ctx context.Context, uploadId int64) (bool, error) {
	var (
		count int64
		err   error
	)
	err = repo.db.QueryRowContext(ctx,
		`
			SELECT
				COUNT(*)
			FROM
				`+tableUploadTableName+`
			WHERE
				wh_upload_id = $1;
`,
		uploadId).Scan(&count)
	if err != nil {
		return false, fmt.Errorf(`is created: %w`, err)
	}

	return count > 0, nil
}
