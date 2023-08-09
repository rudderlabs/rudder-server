package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	jsoniter "github.com/json-iterator/go"
	"github.com/lib/pq"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	uploadsTableName = warehouseutils.WarehouseUploadsTable

	uploadColumns = `
		id,
		status,
		schema,
		namespace,
		workspace_id,
		source_id,
		destination_id,
		destination_type,
		start_staging_file_id,
		end_staging_file_id,
		start_load_file_id,
		end_load_file_id,
		error,
		metadata,
		timings->0 as firstTiming,
		timings->-1 as lastTiming,
		timings,
		COALESCE(metadata->>'priority', '100')::int,
		first_event_at,
		last_event_at
	`
)

type Uploads repo

type scanFn func(dest ...any) error

type ProcessOptions struct {
	SkipIdentifiers                   []string
	SkipWorkspaces                    []string
	AllowMultipleSourcesForJobsPickup bool
}

type UploadMetadata struct {
	UseRudderStorage bool      `json:"use_rudder_storage"`
	SourceTaskRunID  string    `json:"source_task_run_id"`
	SourceJobID      string    `json:"source_job_id"`
	SourceJobRunID   string    `json:"source_job_run_id"`
	LoadFileType     string    `json:"load_file_type"`
	Retried          bool      `json:"retried"`
	Priority         int       `json:"priority"`
	NextRetryTime    time.Time `json:"nextRetryTime"`
}

func NewUploads(db *sqlmiddleware.DB, opts ...Opt) *Uploads {
	u := &Uploads{
		db:  db,
		now: timeutil.Now,
	}

	for _, opt := range opts {
		opt((*repo)(u))
	}

	return u
}

func ExtractUploadMetadata(upload model.Upload) UploadMetadata {
	return UploadMetadata{
		UseRudderStorage: upload.UseRudderStorage,
		SourceTaskRunID:  upload.SourceTaskRunID,
		SourceJobID:      upload.SourceJobID,
		SourceJobRunID:   upload.SourceJobRunID,
		LoadFileType:     upload.LoadFileType,
		Retried:          upload.Retried,
		Priority:         upload.Priority,
		NextRetryTime:    upload.NextRetryTime,
	}
}

func (uploads *Uploads) CreateWithStagingFiles(ctx context.Context, upload model.Upload, files []*model.StagingFile) (int64, error) {
	startJSONID := files[0].ID
	endJSONID := files[len(files)-1].ID

	stagingFileIDs := make([]int64, len(files))
	for i, file := range files {
		stagingFileIDs[i] = file.ID
	}

	var firstEventAt, lastEventAt time.Time
	if !files[0].FirstEventAt.IsZero() {
		firstEventAt = files[0].FirstEventAt
	}
	if !files[len(files)-1].LastEventAt.IsZero() {
		lastEventAt = files[len(files)-1].LastEventAt
	}

	metadataMap := UploadMetadata{
		UseRudderStorage: files[0].UseRudderStorage,
		SourceTaskRunID:  files[0].SourceTaskRunID,
		SourceJobID:      files[0].SourceJobID,
		SourceJobRunID:   files[0].SourceJobRunID,
		LoadFileType:     warehouseutils.GetLoadFileType(upload.DestinationType),
		Retried:          upload.Retried,
		Priority:         upload.Priority,
		NextRetryTime:    upload.NextRetryTime,
	}

	metadata, err := json.Marshal(metadataMap)
	if err != nil {
		return 0, err
	}

	tx, err := uploads.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	var uploadID int64
	err = tx.QueryRow(
		`INSERT INTO `+uploadsTableName+` (
			source_id, namespace, workspace_id, destination_id,
			destination_type, start_staging_file_id,
			end_staging_file_id, start_load_file_id,
			end_load_file_id, status, schema,
			error, metadata, first_event_at,
			last_event_at, created_at, updated_at
		)
		VALUES
			(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17
			) RETURNING id;`,

		upload.SourceID,
		upload.Namespace,
		upload.WorkspaceID,
		upload.DestinationID,
		upload.DestinationType,
		startJSONID,
		endJSONID,
		0,
		0,
		upload.Status,
		"{}",
		"{}",
		metadata,
		firstEventAt,
		lastEventAt,
		uploads.now(),
		uploads.now(),
	).Scan(&uploadID)
	if err != nil {
		return 0, err
	}

	result, err := tx.Exec(
		`UPDATE `+stagingTableName+` SET upload_id = $1 WHERE id = ANY($2)`,
		uploadID, pq.Array(stagingFileIDs),
	)
	if err != nil {
		return 0, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if affected != int64(len(stagingFileIDs)) {
		return 0, fmt.Errorf("failed to update staging files %d != %d", affected, len(stagingFileIDs))
	}

	rollback = false
	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return uploadID, nil
}

type FilterBy struct {
	Key       string
	Value     interface{}
	NotEquals bool
}

func (uploads *Uploads) Count(ctx context.Context, filters ...FilterBy) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE 1=1", uploadsTableName)

	args := make([]interface{}, 0)
	for i, filter := range filters {

		if filter.NotEquals {
			query += fmt.Sprintf(" AND %s!=$%d", filter.Key, i+1)
		} else {
			query += fmt.Sprintf(" AND %s=$%d", filter.Key, i+1)
		}

		args = append(args, filter.Value)
	}

	var count int64
	if err := uploads.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning count into local variable: %w", err)
	}

	return count, nil
}

func (uploads *Uploads) Get(ctx context.Context, id int64) (model.Upload, error) {
	row := uploads.db.QueryRowContext(ctx, `
		SELECT
		`+uploadColumns+`
		FROM
		`+uploadsTableName+`
		WHERE
			id = $1
	`, id)

	var upload model.Upload
	err := scanUpload(row.Scan, &upload)
	if err == sql.ErrNoRows {
		return model.Upload{}, model.ErrUploadNotFound
	}
	if err != nil {
		return model.Upload{}, err
	}

	return upload, nil
}

func (uploads *Uploads) GetToProcess(ctx context.Context, destType string, limit int, opts ProcessOptions) ([]model.Upload, error) {
	var skipIdentifiersSQL string
	partitionIdentifierSQL := `destination_id, namespace`

	if len(opts.SkipIdentifiers) > 0 {
		skipIdentifiersSQL = `AND ((destination_id || '_' || namespace)) != ALL($5)`
	}

	if opts.AllowMultipleSourcesForJobsPickup {
		if len(opts.SkipIdentifiers) > 0 {
			skipIdentifiersSQL = `AND ((source_id || '_' || destination_id || '_' || namespace)) != ALL($5)`
		}
		partitionIdentifierSQL = fmt.Sprintf(`%s, %s`, "source_id", partitionIdentifierSQL)
	}

	sqlStatement := fmt.Sprintf(`
			SELECT
			`+uploadColumns+`
			FROM (
				SELECT
					ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COALESCE(metadata->>'priority', '100')::int ASC, COALESCE(first_event_at, NOW()) ASC, id ASC) AS row_number,
					t.*
				FROM
					`+uploadsTableName+` t
				WHERE
					t.destination_type = $1 AND
					t.in_progress=false AND
					t.status != $2 AND
					t.status != $3 %s AND
					COALESCE(metadata->>'nextRetryTime', NOW()::text)::timestamptz <= NOW() AND
          			workspace_id <> ALL ($4)
			) grouped_uploads
			WHERE
				grouped_uploads.row_number = 1
			ORDER BY
				COALESCE(metadata->>'priority', '100')::int ASC,
				COALESCE(first_event_at, NOW()) ASC,
				id ASC
			LIMIT %d;
`,
		partitionIdentifierSQL,
		skipIdentifiersSQL,
		limit,
	)

	var (
		rows *sqlmiddleware.Rows
		err  error
	)
	if opts.SkipWorkspaces == nil {
		opts.SkipWorkspaces = []string{}
	}

	args := []interface{}{
		destType,
		model.ExportedData,
		model.Aborted,
		pq.Array(opts.SkipWorkspaces),
	}

	if len(opts.SkipIdentifiers) > 0 {
		args = append(args, pq.Array(opts.SkipIdentifiers))
	}

	rows, err = uploads.db.QueryContext(
		ctx,
		sqlStatement,
		args...,
	)

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	defer func() { _ = rows.Close() }()

	var uploadJobs []model.Upload
	for rows.Next() {
		var upload model.Upload
		err := scanUpload(rows.Scan, &upload)
		if err != nil {
			return nil, err
		}
		uploadJobs = append(uploadJobs, upload)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return uploadJobs, nil
}

func (uploads *Uploads) UploadJobsStats(ctx context.Context, destType string, opts ProcessOptions) (model.UploadJobsStats, error) {
	query := `
		SELECT
			COALESCE(COUNT(*), 0) AS pending_jobs,

			COALESCE(EXTRACT(EPOCH FROM(AGE($1::TIMESTAMPTZ, MIN(COALESCE((metadata->>'nextRetryTime')::TIMESTAMPTZ, $1::TIMESTAMPTZ)::timestamptz)))), 0) AS pickup_lag_in_seconds,
			COALESCE(SUM(EXTRACT(EPOCH FROM AGE($1::TIMESTAMPTZ, COALESCE((metadata->>'nextRetryTime')::TIMESTAMPTZ, $1::TIMESTAMPTZ)::timestamptz))), 0) AS pickup_wait_time_in_seconds

		FROM
			` + uploadsTableName + `
		WHERE
			destination_type = $2 AND
			in_progress = false AND
			status != $3 AND
			status != $4  AND
			COALESCE((metadata->>'nextRetryTime')::TIMESTAMPTZ, $1::TIMESTAMPTZ) <= $1::TIMESTAMPTZ AND
			workspace_id <> ALL ($5)`

	if opts.SkipWorkspaces == nil {
		opts.SkipWorkspaces = []string{}
	}

	args := []any{
		uploads.now(),
		destType,
		model.ExportedData,
		model.Aborted,
		pq.Array(opts.SkipWorkspaces),
	}

	if len(opts.SkipIdentifiers) > 0 {
		query += `AND ((destination_id || '_' || namespace)) != ALL($6)`
		args = append(args, pq.Array(opts.SkipIdentifiers))
	}

	var stats model.UploadJobsStats
	var (
		pickupLag      sql.NullFloat64
		pickupWaitTime sql.NullFloat64
	)

	err := uploads.db.QueryRowContext(
		ctx,
		query,
		args...,
	).Scan(
		&stats.PendingJobs,
		&pickupLag,
		&pickupWaitTime,
	)
	if err != nil {
		return stats, fmt.Errorf("count pending jobs: %w", err)
	}

	stats.PickupLag = time.Duration(pickupLag.Float64) * time.Second
	stats.PickupWaitTime = time.Duration(pickupWaitTime.Float64) * time.Second

	return stats, nil
}

// UploadTimings returns the timings for an upload.
func (uploads *Uploads) UploadTimings(ctx context.Context, uploadID int64) (model.Timings, error) {
	var (
		rawJSON jsoniter.RawMessage
		timings model.Timings
	)

	err := uploads.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(timings, '[]')::JSONB
		FROM
			`+uploadsTableName+`
		WHERE
			id = $1;
	`, uploadID).Scan(&rawJSON)
	if err == sql.ErrNoRows {
		return timings, model.ErrUploadNotFound
	}
	if err != nil {
		return timings, err
	}

	err = json.Unmarshal(rawJSON, &timings)
	if err != nil {
		return timings, err
	}

	return timings, nil
}

func (uploads *Uploads) DeleteWaiting(ctx context.Context, uploadID int64) error {
	_, err := uploads.db.ExecContext(ctx,
		`DELETE FROM `+uploadsTableName+` WHERE id = $1 AND status = $2`,
		uploadID, model.Waiting,
	)
	if err != nil {
		return fmt.Errorf("delete waiting upload: %w", err)
	}

	return nil
}

func scanUpload(scan scanFn, upload *model.Upload) error {
	var (
		schema                    []byte
		firstTiming               sql.NullString
		lastTiming                sql.NullString
		firstEventAt, lastEventAt sql.NullTime
		metadataRaw               []byte
		timingsRaw                []byte
	)

	err := scan(
		&upload.ID,
		&upload.Status,
		&schema,
		&upload.Namespace,
		&upload.WorkspaceID,
		&upload.SourceID,
		&upload.DestinationID,
		&upload.DestinationType,
		&upload.StagingFileStartID,
		&upload.StagingFileEndID,
		&upload.LoadFileStartID,
		&upload.LoadFileEndID,
		&upload.Error,
		&metadataRaw,
		&firstTiming,
		&lastTiming,
		&timingsRaw,
		&upload.Priority,
		&firstEventAt,
		&lastEventAt,
	)
	if err != nil {
		return err
	}
	upload.FirstEventAt = firstEventAt.Time.UTC()
	upload.LastEventAt = lastEventAt.Time.UTC()

	if err := json.Unmarshal(schema, &upload.UploadSchema); err != nil {
		return fmt.Errorf("unmarshal upload schema: %w", err)
	}
	var metadata UploadMetadata
	if err := json.Unmarshal(metadataRaw, &metadata); err != nil {
		return fmt.Errorf("unmarshal metadata: %w", err)
	}

	if len(timingsRaw) > 0 {
		if err := json.Unmarshal(timingsRaw, &upload.Timings); err != nil {
			return fmt.Errorf("unmarshal timings: %w", err)
		}
	}
	upload.SourceTaskRunID = metadata.SourceTaskRunID
	upload.SourceJobID = metadata.SourceJobID
	upload.SourceJobRunID = metadata.SourceJobRunID
	upload.LoadFileType = metadata.LoadFileType
	upload.NextRetryTime = metadata.NextRetryTime
	upload.Priority = metadata.Priority
	upload.Retried = metadata.Retried
	upload.UseRudderStorage = metadata.UseRudderStorage

	_, upload.FirstAttemptAt = warehouseutils.TimingFromJSONString(firstTiming)
	var lastStatus string
	lastStatus, upload.LastAttemptAt = warehouseutils.TimingFromJSONString(lastTiming)
	upload.Attempts = gjson.Get(string(upload.Error), fmt.Sprintf(`%s.attempt`, lastStatus)).Int()

	return nil
}

// InterruptedDestinations returns a list of destination IDs, which have uploads was interrupted.
//
//	Interrupted upload might require cleanup of intermediate upload tables.
func (uploads *Uploads) InterruptedDestinations(ctx context.Context, destinationType string) ([]string, error) {
	destinationIDs := make([]string, 0)
	rows, err := uploads.db.QueryContext(ctx, `
		SELECT
			destination_id
		FROM
			`+uploadsTableName+`
		WHERE
			in_progress = TRUE
			AND destination_type = $1
			AND (
				status = $2
				OR status = $3
			);
	`,
		destinationType,
		model.ExportingData,
		model.ExportingDataFailed,
	)
	if err != nil {
		return nil, fmt.Errorf("query for interrupted destinations: %w", err)
	}

	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var destID string
		err := rows.Scan(&destID)
		if err != nil {
			return nil, fmt.Errorf("scanning: %w", err)
		}
		destinationIDs = append(destinationIDs, destID)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("iterating rows: %w", rows.Err())
	}

	return destinationIDs, nil
}

// PendingTableUploads returns a list of pending table uploads for a given upload.
func (uploads *Uploads) PendingTableUploads(ctx context.Context, namespace string, uploadID int64, destID string) ([]model.PendingTableUpload, error) {
	pendingTableUploads := make([]model.PendingTableUpload, 0)

	rows, err := uploads.db.QueryContext(ctx, `
		SELECT
		  UT.id,
		  UT.destination_id,
		  UT.namespace,
		  TU.table_name,
		  TU.status,
		  TU.error
		FROM
			`+uploadsTableName+` UT
		INNER JOIN
			`+tableUploadTableName+` TU
		ON
			UT.id = TU.wh_upload_id
		WHERE
		  	UT.id <= $1 AND
			UT.destination_id = $2 AND
		   	UT.namespace = $3 AND
		  	UT.status != $4 AND
		  	UT.status != $5 AND
		  	TU.table_name in (
				SELECT
				  table_name
				FROM
				  `+tableUploadTableName+` TU1
				WHERE
				  TU1.wh_upload_id = $1
		  )
		ORDER BY
		  UT.id ASC;
`,
		uploadID,
		destID,
		namespace,
		model.ExportedData,
		model.Aborted,
	)
	if err != nil {
		return nil, fmt.Errorf("pending table uploads: %w", err)
	}

	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var pendingTableUpload model.PendingTableUpload

		err := rows.Scan(
			&pendingTableUpload.UploadID,
			&pendingTableUpload.DestinationID,
			&pendingTableUpload.Namespace,
			&pendingTableUpload.TableName,
			&pendingTableUpload.Status,
			&pendingTableUpload.Error,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		pendingTableUploads = append(pendingTableUploads, pendingTableUpload)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	return pendingTableUploads, nil
}

func (uploads *Uploads) ResetInProgress(ctx context.Context, destType string) error {
	_, err := uploads.db.ExecContext(ctx, `
		UPDATE
			`+uploadsTableName+`
		SET
			in_progress = FALSE
		WHERE
			destination_type = $1 AND
			in_progress = TRUE;
	`,
		destType,
	)
	if err != nil {
		return fmt.Errorf("reset in progress: %w", err)
	}
	return nil
}

func (uploads *Uploads) LastCreatedAt(ctx context.Context, sourceID, destinationID string) (time.Time, error) {
	row := uploads.db.QueryRowContext(ctx, `
		SELECT
			created_at
		FROM
		`+uploadsTableName+`
		WHERE
			source_id = $1 AND
			destination_id = $2
		ORDER BY
			id DESC
		LIMIT 1;
	`,
		sourceID,
		destinationID,
	)

	var createdAt sql.NullTime

	err := row.Scan(&createdAt)
	if err != nil && err != sql.ErrNoRows {
		return time.Time{}, fmt.Errorf("last created at: %w", err)
	}
	if err == sql.ErrNoRows || !createdAt.Valid {
		return time.Time{}, nil
	}

	return createdAt.Time, nil
}
