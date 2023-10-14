package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	tableUploadTableName            = warehouseutils.WarehouseTableUploadsTable
	tableUploadUniqueConstraintName = "unique_table_upload_wh_upload"

	tableUploadColumns = `
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
)

// TableUploads is a repository for table uploads
type TableUploads repo

type TableUploadSetOptions struct {
	Status       *string
	Error        *string
	LastExecTime *time.Time
	Location     *string
	TotalEvents  *int64
}

func NewTableUploads(db *sqlmiddleware.DB, opts ...Opt) *TableUploads {
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
		txn  *sqlmiddleware.Tx
		stmt *sql.Stmt
		err  error
	)

	if txn, err = repo.db.BeginTx(ctx, &sql.TxOptions{}); err != nil {
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
	if err = txn.Commit(); err != nil {
		return fmt.Errorf(`commit: %w`, err)
	}

	return nil
}

func (repo *TableUploads) GetByUploadID(ctx context.Context, uploadID int64) ([]model.TableUpload, error) {
	query := `SELECT ` + tableUploadColumns + ` FROM ` + tableUploadTableName + `
	WHERE
		wh_upload_id = $1;`

	rows, err := repo.db.QueryContext(ctx, query, uploadID)
	if err != nil {
		return nil, fmt.Errorf("querying table uploads: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tableUploads []model.TableUpload
	for rows.Next() {
		var tableUpload model.TableUpload
		err := scanTableUpload(rows.Scan, &tableUpload)
		if err != nil {
			return nil, fmt.Errorf("parsing rows: %w", err)
		}
		tableUploads = append(tableUploads, tableUpload)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tableUploads, nil
}

func (repo *TableUploads) GetByUploadIDAndTableName(ctx context.Context, uploadID int64, tableName string) (model.TableUpload, error) {
	query := `SELECT ` + tableUploadColumns + ` FROM ` + tableUploadTableName + `
	WHERE
		wh_upload_id = $1 AND
		table_name = $2
	LIMIT 1;
`

	row := repo.db.QueryRowContext(ctx, query, uploadID, tableName)

	var tableUpload model.TableUpload
	err := scanTableUpload(row.Scan, &tableUpload)
	if errors.Is(err, sql.ErrNoRows) {
		return tableUpload, fmt.Errorf("no table upload found with uploadID: %d, tableName: %s", uploadID, tableName)
	}
	if err != nil {
		return tableUpload, fmt.Errorf("parsing rows: %w", err)
	}

	return tableUpload, err
}

func scanTableUpload(scan scanFn, tableUpload *model.TableUpload) error {
	var (
		locationRaw     sql.NullString
		lastExecTimeRaw sql.NullTime
		totalEvents     sql.NullInt64
	)
	err := scan(
		&tableUpload.ID,
		&tableUpload.UploadID,
		&tableUpload.TableName,
		&tableUpload.Status,
		&tableUpload.Error,
		&lastExecTimeRaw,
		&totalEvents,
		&tableUpload.CreatedAt,
		&tableUpload.UpdatedAt,
		&locationRaw,
	)
	if err != nil {
		return fmt.Errorf("scanning row: %w", err)
	}

	tableUpload.CreatedAt = tableUpload.CreatedAt.UTC()
	tableUpload.UpdatedAt = tableUpload.UpdatedAt.UTC()

	if lastExecTimeRaw.Valid {
		tableUpload.LastExecTime = lastExecTimeRaw.Time.UTC()
	}
	if locationRaw.Valid {
		tableUpload.Location = locationRaw.String
	}
	if totalEvents.Valid {
		tableUpload.TotalEvents = totalEvents.Int64
	}
	return nil
}

func (repo *TableUploads) PopulateTotalEventsFromStagingFileIDs(ctx context.Context, uploadId int64, tableName string, stagingFileIDs []int64) error {
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
		pq.Array(stagingFileIDs),
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

func (repo *TableUploads) TotalExportedEvents(ctx context.Context, uploadId int64, skipTables []string) (int64, error) {
	var (
		count sql.NullInt64
		err   error
	)

	if skipTables == nil {
		skipTables = []string{}
	}

	err = repo.db.QueryRowContext(ctx, `
			SELECT
				COALESCE(sum(total_events), 0) AS total
			FROM
				`+tableUploadTableName+`
			WHERE
				wh_upload_id = $1 AND
				status = $2 AND
				table_name != ALL($3);
`,
		uploadId,
		model.ExportedData,
		pq.Array(skipTables),
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf(`counting total exported events: %w`, err)
	}
	if count.Valid {
		return count.Int64, nil
	}

	return 0, errors.New(`count is not valid`)
}

func (repo *TableUploads) Set(ctx context.Context, uploadId int64, tableName string, options TableUploadSetOptions) error {
	var (
		query     string
		queryArgs []any
		setQuery  strings.Builder
		err       error
	)

	queryArgs = []any{
		uploadId,
		tableName,
	}

	if options.Status != nil {
		setQuery.WriteString(fmt.Sprintf(`status = $%d,`, len(queryArgs)+1))
		queryArgs = append(queryArgs, *options.Status)
	}
	if options.Error != nil {
		setQuery.WriteString(fmt.Sprintf(`error = $%d,`, len(queryArgs)+1))
		sanitizedError := warehouseutils.SanitizeString(*options.Error)
		queryArgs = append(queryArgs, sanitizedError)
	}
	if options.LastExecTime != nil {
		setQuery.WriteString(fmt.Sprintf(`last_exec_time = $%d,`, len(queryArgs)+1))
		queryArgs = append(queryArgs, *options.LastExecTime)
	}
	if options.Location != nil {
		setQuery.WriteString(fmt.Sprintf(`location = $%d,`, len(queryArgs)+1))
		queryArgs = append(queryArgs, *options.Location)
	}
	if options.TotalEvents != nil {
		setQuery.WriteString(fmt.Sprintf(`total_events = $%d,`, len(queryArgs)+1))
		queryArgs = append(queryArgs, *options.TotalEvents)
	}

	if setQuery.Len() == 0 {
		return fmt.Errorf(`no set options provided`)
	}

	setQuery.WriteString(fmt.Sprintf(`updated_at = $%d,`, len(queryArgs)+1))
	queryArgs = append(queryArgs, repo.now())

	// remove trailing comma
	setQueryString := strings.TrimSuffix(setQuery.String(), ",")

	query = `
		UPDATE
			` + tableUploadTableName + `
		SET
			` + setQueryString + `
		WHERE
		  wh_upload_id = $1 AND
		  table_name = $2;
`
	result, err := repo.db.ExecContext(
		ctx,
		query,
		queryArgs...,
	)
	if err != nil {
		return fmt.Errorf(`set: %w`, err)
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

func (repo *TableUploads) ExistsForUploadID(ctx context.Context, uploadId int64) (bool, error) {
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
		return false, fmt.Errorf("checking if table upload exists: %w", err)
	}

	return count > 0, nil
}

func (repo *TableUploads) SyncsInfo(ctx context.Context, uploadID int64) ([]model.TableUploadInfo, error) {
	tableUploads, err := repo.GetByUploadID(ctx, uploadID)
	if err != nil {
		return nil, fmt.Errorf("table uploads for upload id: %w", err)
	}

	tableUploadInfos := lo.Map(tableUploads, func(item model.TableUpload, index int) model.TableUploadInfo {
		return model.TableUploadInfo{
			ID:         item.ID,
			UploadID:   item.UploadID,
			Name:       item.TableName,
			Status:     item.Status,
			Error:      item.Error,
			LastExecAt: item.LastExecTime,
			Count:      item.TotalEvents,
			Duration:   int64(item.UpdatedAt.Sub(item.LastExecTime) / time.Second),
		}
	})
	return tableUploadInfos, nil
}

func (repo *TableUploads) GetByJobRunTaskRun(
	ctx context.Context,
	sourceID,
	destinationID,
	jobRunID,
	taskRunID string,
) ([]model.TableUpload, error) {
	rows, err := repo.db.QueryContext(ctx, `
		SELECT
			`+tableUploadColumns+`
		FROM
			`+tableUploadTableName+`
		WHERE
			wh_upload_id IN (
				SELECT
					id
				FROM
					`+uploadsTableName+`
				WHERE
					source_id=$1 AND
					destination_id=$2 AND
					metadata->>'source_job_run_id'=$3 AND
					metadata->>'source_task_run_id'=$4
			);
	`,
		sourceID,
		destinationID,
		jobRunID,
		taskRunID,
	)
	if err != nil {
		return nil, fmt.Errorf("getting table uploads: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tableUploads []model.TableUpload
	for rows.Next() {
		var tableUpload model.TableUpload
		err := scanTableUpload(rows.Scan, &tableUpload)
		if err != nil {
			return nil, err
		}
		tableUploads = append(tableUploads, tableUpload)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tableUploads, nil
}
