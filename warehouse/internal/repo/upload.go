package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	jsoniter "github.com/json-iterator/go"
	"github.com/lib/pq"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var syncStatusMap = map[string]string{
	"success": model.ExportedData,
	"waiting": model.Waiting,
	"aborted": model.Aborted,
	"failed":  "%failed%",
}

const (
	uploadsTableName = warehouseutils.WarehouseUploadsTable
	uploadColumns    = `
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

var (
	UploadFieldStatus          UpdateField = func(v any) UpdateKeyValue { return keyValue{"status", v} }
	UploadFieldStartLoadFileID UpdateField = func(v any) UpdateKeyValue { return keyValue{"start_load_file_id", v} }
	UploadFieldEndLoadFileID   UpdateField = func(v any) UpdateKeyValue { return keyValue{"end_load_file_id", v} }
	UploadFieldUpdatedAt       UpdateField = func(v any) UpdateKeyValue { return keyValue{"updated_at", v} }
	UploadFieldTimings         UpdateField = func(v any) UpdateKeyValue { return keyValue{"timings", v} }
	UploadFieldSchema          UpdateField = func(v any) UpdateKeyValue { return keyValue{"schema", v} }
	UploadFieldLastExecAt      UpdateField = func(v any) UpdateKeyValue { return keyValue{"last_exec_at", v} }
	UploadFieldInProgress      UpdateField = func(v any) UpdateKeyValue { return keyValue{"in_progress", v} }
	UploadFieldMetadata        UpdateField = func(v any) UpdateKeyValue { return keyValue{"metadata", v} }
	UploadFieldError           UpdateField = func(v any) UpdateKeyValue { return keyValue{"error", v} }
	UploadFieldErrorCategory   UpdateField = func(v any) UpdateKeyValue { return keyValue{"error_category", v} }
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

func (u *Uploads) CreateWithStagingFiles(ctx context.Context, upload model.Upload, files []*model.StagingFile) (int64, error) {
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

	tx, err := u.db.BeginTx(ctx, &sql.TxOptions{})
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
		u.now(),
		u.now(),
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
	Value     any
	NotEquals bool
}

func (u *Uploads) Count(ctx context.Context, filters ...FilterBy) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE 1=1", uploadsTableName)

	args := make([]any, 0)
	for i, filter := range filters {

		if filter.NotEquals {
			query += fmt.Sprintf(" AND %s!=$%d", filter.Key, i+1)
		} else {
			query += fmt.Sprintf(" AND %s=$%d", filter.Key, i+1)
		}

		args = append(args, filter.Value)
	}

	var count int64
	if err := u.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning count into local variable: %w", err)
	}

	return count, nil
}

func (u *Uploads) Get(ctx context.Context, id int64) (model.Upload, error) {
	row := u.db.QueryRowContext(ctx,
		`SELECT
		`+uploadColumns+`
		FROM
		`+uploadsTableName+`
		WHERE
			id = $1`,
		id,
	)

	var upload model.Upload
	err := scanUpload(row.Scan, &upload)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Upload{}, model.ErrUploadNotFound
	}
	if err != nil {
		return model.Upload{}, err
	}

	return upload, nil
}

func (u *Uploads) GetToProcess(ctx context.Context, destType string, limit int, opts ProcessOptions) ([]model.Upload, error) {
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

	args := []any{
		destType,
		model.ExportedData,
		model.Aborted,
		pq.Array(opts.SkipWorkspaces),
	}

	if len(opts.SkipIdentifiers) > 0 {
		args = append(args, pq.Array(opts.SkipIdentifiers))
	}

	rows, err = u.db.QueryContext(
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

func (u *Uploads) UploadJobsStats(ctx context.Context, destType string, opts ProcessOptions) (model.UploadJobsStats, error) {
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
		u.now(),
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

	err := u.db.QueryRowContext(
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
func (u *Uploads) UploadTimings(ctx context.Context, uploadID int64) (model.Timings, error) {
	var (
		rawJSON jsoniter.RawMessage
		timings model.Timings
	)

	err := u.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(timings, '[]')::JSONB
		FROM
			`+uploadsTableName+`
		WHERE
			id = $1;
	`, uploadID).Scan(&rawJSON)
	if errors.Is(err, sql.ErrNoRows) {
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

func (u *Uploads) DeleteWaiting(ctx context.Context, uploadID int64) error {
	_, err := u.db.ExecContext(ctx,
		`DELETE FROM `+uploadsTableName+` WHERE id = $1 AND status = $2;`,
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

// PendingTableUploads returns a list of pending table uploads for a given upload.
func (u *Uploads) PendingTableUploads(ctx context.Context, namespace string, uploadID int64, destID string) ([]model.PendingTableUpload, error) {
	pendingTableUploads := make([]model.PendingTableUpload, 0)

	rows, err := u.db.QueryContext(ctx, `
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

func (u *Uploads) ResetInProgress(ctx context.Context, destType string) error {
	_, err := u.db.ExecContext(ctx, `
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

func (u *Uploads) LastCreatedAt(ctx context.Context, sourceID, destinationID string) (time.Time, error) {
	row := u.db.QueryRowContext(ctx, `
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
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, fmt.Errorf("last created at: %w", err)
	}
	if errors.Is(err, sql.ErrNoRows) || !createdAt.Valid {
		return time.Time{}, nil
	}

	return createdAt.Time, nil
}

func (u *Uploads) SyncsInfoForMultiTenant(ctx context.Context, limit, offset int, opts model.SyncUploadOptions) ([]model.UploadInfo, int64, error) {
	syncUploadInfos, totalUploads, err := u.syncsInfo(ctx, limit, offset, opts, true)
	if err != nil {
		return nil, 0, fmt.Errorf("syncs upload info for multi tenant: %w", err)
	}
	if len(syncUploadInfos) != 0 {
		return syncUploadInfos, totalUploads, nil
	}

	// Getting the syncs count in case, we were not able to get the count
	totalUploads, err = u.syncsCount(ctx, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("syncs upload count: %w", err)
	}

	return syncUploadInfos, totalUploads, nil
}

func (u *Uploads) SyncsInfoForNonMultiTenant(ctx context.Context, limit, offset int, opts model.SyncUploadOptions) ([]model.UploadInfo, int64, error) {
	syncUploadInfos, _, err := u.syncsInfo(ctx, limit, offset, opts, false)
	if err != nil {
		return nil, 0, fmt.Errorf("syncs upload info for multi tenant: %w", err)
	}

	totalUploads, err := u.syncsCount(ctx, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("syncs upload count: %w", err)
	}

	return syncUploadInfos, totalUploads, nil
}

func (u *Uploads) syncsInfo(ctx context.Context, limit, offset int, opts model.SyncUploadOptions, countOver bool) ([]model.UploadInfo, int64, error) {
	filterQuery, filterArgs := syncUploadQueryArgs(&opts)

	countOverClause := "0 AS total_uploads"
	if countOver {
		countOverClause = "COUNT(*) OVER() AS total_uploads"
	}

	stmt := fmt.Sprintf(`
	WITH filtered_uploads as (
		SELECT
			id,
			source_id,
			destination_id,
			destination_type,
			namespace,
			status,
			error,
			created_at,
			first_event_at,
			last_event_at,
			last_exec_at,
			updated_at,
			timings,
			metadata->>'nextRetryTime',
			metadata->>'archivedStagingAndLoadFiles',
			%s
		FROM
			`+uploadsTableName+`
		WHERE
			1 = 1 %s )
	SELECT * from filtered_uploads
		ORDER BY
		  	id DESC
		LIMIT
			$%d
		OFFSET
			$%d;
	`,
		countOverClause,
		filterQuery,
		len(filterArgs)+1,
		len(filterArgs)+2,
	)
	stmtArgs := append([]any{}, append(filterArgs, limit, offset)...)

	rows, err := u.db.QueryContext(ctx, stmt, stmtArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("syncs upload info: %w", err)
	}

	var uploadInfos []model.UploadInfo
	var totalUploads int64

	for rows.Next() {
		var uploadInfo model.UploadInfo

		var timings model.Timings
		var timingsRaw []byte
		var nextRetryTime sql.NullString
		var archivedStagingAndLoadFiles sql.NullBool
		var firstEventAt, lastEventAt, lastExecAt, updatedAt sql.NullTime

		err := rows.Scan(
			&uploadInfo.ID,
			&uploadInfo.SourceID,
			&uploadInfo.DestinationID,
			&uploadInfo.DestinationType,
			&uploadInfo.Namespace,
			&uploadInfo.Status,
			&uploadInfo.Error,
			&uploadInfo.CreatedAt,
			&firstEventAt,
			&lastEventAt,
			&lastExecAt,
			&updatedAt,
			&timingsRaw,
			&nextRetryTime,
			&archivedStagingAndLoadFiles,
			&totalUploads,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("syncs upload info: scanning row: %w", err)
		}
		if firstEventAt.Valid {
			uploadInfo.FirstEventAt = firstEventAt.Time
		}
		if lastEventAt.Valid {
			uploadInfo.LastEventAt = lastEventAt.Time
		}
		if lastExecAt.Valid {
			uploadInfo.LastExecAt = lastExecAt.Time
		}
		if updatedAt.Valid {
			uploadInfo.UpdatedAt = updatedAt.Time
		}
		if archivedStagingAndLoadFiles.Valid {
			uploadInfo.IsArchivedUpload = archivedStagingAndLoadFiles.Bool
		}
		gjson.Parse(uploadInfo.Error).ForEach(func(key, value gjson.Result) bool {
			uploadInfo.Attempt += gjson.Get(value.String(), "attempt").Int()
			return true
		})
		if uploadInfo.Status != model.ExportedData && uploadInfo.Status != model.Aborted && nextRetryTime.Valid {
			if nextRetryTime, err := time.Parse(time.RFC3339, nextRetryTime.String); err == nil {
				uploadInfo.NextRetryTime = nextRetryTime
			}
		}
		if lastExecAt.Valid {
			// set duration as time between updatedAt and lastExec recorded timings for ongoing/retrying uploads
			// set diff between lastExec and current time
			if uploadInfo.Status == model.ExportedData || uploadInfo.Status == model.Aborted {
				uploadInfo.Duration = uploadInfo.UpdatedAt.Sub(lastExecAt.Time) / time.Second
			} else {
				uploadInfo.Duration = u.now().Sub(lastExecAt.Time) / time.Second
			}
		}

		// set error only for failed uploads. skip for retried and then successful uploads
		if uploadInfo.Status != model.ExportedData && len(timingsRaw) > 0 {
			_ = json.Unmarshal(timingsRaw, &timings)

			errs := gjson.Get(
				uploadInfo.Error,
				fmt.Sprintf("%s.errors", model.GetLastFailedStatus(timings)),
			).Array()
			if len(errs) > 0 {
				uploadInfo.Error = errs[len(errs)-1].String()
			}
		}

		uploadInfos = append(uploadInfos, uploadInfo)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("syncs upload info: iterating rows: %w", err)
	}

	return uploadInfos, totalUploads, nil
}

func syncUploadQueryArgs(suo *model.SyncUploadOptions) (string, []any) {
	var queryFilters string
	var queryArgs []any

	if suo.SourceIDs != nil {
		queryFilters += fmt.Sprintf(" AND source_id = ANY($%d)", len(queryArgs)+1)
		queryArgs = append(queryArgs, pq.Array(suo.SourceIDs))
	}
	if suo.DestinationID != "" {
		queryFilters += fmt.Sprintf(" AND destination_id = $%d", len(queryArgs)+1)
		queryArgs = append(queryArgs, suo.DestinationID)
	}
	if suo.DestinationType != "" {
		queryFilters += fmt.Sprintf(" AND destination_type = $%d", len(queryArgs)+1)
		queryArgs = append(queryArgs, suo.DestinationType)
	}
	if suo.Status != "" {
		queryFilters += fmt.Sprintf(" AND status LIKE $%d", len(queryArgs)+1)
		queryArgs = append(queryArgs, syncStatusMap[suo.Status])
	}
	if suo.UploadID != 0 {
		queryFilters += fmt.Sprintf(" AND id = $%d", len(queryArgs)+1)
		queryArgs = append(queryArgs, suo.UploadID)
	}
	if suo.WorkspaceID != "" {
		queryFilters += fmt.Sprintf(" AND workspace_id = $%d", len(queryArgs)+1)
		queryArgs = append(queryArgs, suo.WorkspaceID)
	}
	return queryFilters, queryArgs
}

func (u *Uploads) syncsCount(ctx context.Context, opts model.SyncUploadOptions) (int64, error) {
	filterQuery, filterArgs := syncUploadQueryArgs(&opts)

	var totalUploads int64
	err := u.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
		  	COUNT(*)
		FROM
			`+uploadsTableName+`
		WHERE
			1 = 1 %s
	`,
		filterQuery,
	),
		filterArgs...,
	).Scan(&totalUploads)
	if err != nil {
		return 0, fmt.Errorf("syncs upload info count: %w", err)
	}
	return totalUploads, nil
}

func (u *Uploads) TriggerUpload(ctx context.Context, uploadID int64) error {
	r, err := u.db.ExecContext(ctx, `
		UPDATE
			`+uploadsTableName+`
		SET
			metadata = metadata || '{"retried": true, "priority": 50}' || jsonb_build_object('nextRetryTime', NOW() - INTERVAL '1 HOUR'),
			status = $1,
			updated_at = $2
		WHERE
			id = $3;
`,
		model.Waiting,
		u.now(),
		uploadID,
	)
	if err != nil {
		return fmt.Errorf("trigger uploads: update: %w", err)
	}

	rowsAffected, err := r.RowsAffected()
	if err != nil {
		return fmt.Errorf("trigger uploads: rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("trigger uploads: no rows affected")
	}
	return nil
}

func (u *Uploads) Retry(ctx context.Context, opts model.RetryOptions) (int64, error) {
	filterQuery, filterArgs := retryQueryArgs(&opts)

	stmt := fmt.Sprintf(`
		UPDATE
			`+uploadsTableName+`
		SET
			metadata = metadata || '{"retried": true, "priority": 50}' || jsonb_build_object('nextRetryTime', NOW() - INTERVAL '1 HOUR'),
			status = $%d,
			updated_at = $%d
		WHERE
			1 = 1 %s;
`,
		len(filterArgs)+1,
		len(filterArgs)+2,
		filterQuery,
	)
	stmtArgs := append([]any{}, append(filterArgs, model.Waiting, u.now())...)

	r, err := u.db.ExecContext(ctx, stmt, stmtArgs...)
	if err != nil {
		return 0, fmt.Errorf("trigger uploads: update: %w", err)
	}
	return r.RowsAffected()
}

func retryQueryArgs(ro *model.RetryOptions) (string, []any) {
	var queryFilters string
	var queryArgs []any

	if ro.WorkspaceID != "" {
		queryFilters += fmt.Sprintf(" AND workspace_id = $%d", len(queryArgs)+1)
		queryArgs = append(queryArgs, ro.WorkspaceID)
	}
	if ro.SourceIDs != nil {
		queryFilters += fmt.Sprintf(" AND source_id = ANY($%d)", len(queryArgs)+1)
		queryArgs = append(queryArgs, pq.Array(ro.SourceIDs))
	}
	if ro.DestinationID != "" {
		queryFilters += fmt.Sprintf(" AND destination_id = $%d", len(queryArgs)+1)
		queryArgs = append(queryArgs, ro.DestinationID)
	}
	if ro.DestinationType != "" {
		queryFilters += fmt.Sprintf(" AND destination_type = $%d", len(queryArgs)+1)
		queryArgs = append(queryArgs, ro.DestinationType)
	}
	if !ro.ForceRetry {
		queryFilters += fmt.Sprintf(" AND status = $%d", len(queryArgs)+1)
		queryArgs = append(queryArgs, model.Aborted)
	}
	if len(ro.UploadIds) != 0 {
		queryFilters += fmt.Sprintf(" AND id = ANY($%d)", len(queryArgs)+1)
		queryArgs = append(queryArgs, pq.Array(ro.UploadIds))
	} else {
		queryFilters += fmt.Sprintf(" AND created_at > NOW() - $%d * INTERVAL '1 HOUR'", len(queryArgs)+1)
		queryArgs = append(queryArgs, ro.IntervalInHours)
	}
	return queryFilters, queryArgs
}

func (u *Uploads) RetryCount(ctx context.Context, opts model.RetryOptions) (int64, error) {
	filterQuery, filterArgs := retryQueryArgs(&opts)

	stmt := fmt.Sprintf(`
		SELECT COUNT(*) FROM
			`+uploadsTableName+`
		WHERE
			1 = 1 %s;
`,
		filterQuery,
	)

	var totalUploads int64
	err := u.db.QueryRowContext(ctx, stmt, filterArgs...).Scan(&totalUploads)
	if err != nil {
		return 0, fmt.Errorf("count uploads to retry: %w", err)
	}
	return totalUploads, nil
}

func (u *Uploads) GetLatestUploadInfo(ctx context.Context, sourceID, destinationID string) (*model.LatestUploadInfo, error) {
	var latestUploadInfo model.LatestUploadInfo
	err := u.db.QueryRowContext(ctx, `
		SELECT
		  	id,
		  	status,
		  	COALESCE(metadata ->> 'priority', '100')::int
		FROM
			`+uploadsTableName+`
		WHERE
			source_id = $1 AND destination_id = $2
		ORDER BY
			id DESC
		LIMIT 1;
`,
		sourceID,
		destinationID,
	).Scan(
		&latestUploadInfo.ID,
		&latestUploadInfo.Status,
		&latestUploadInfo.Priority,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, model.ErrNoUploadsFound
	}
	if err != nil {
		return nil, fmt.Errorf("get latest upload info: %w", err)
	}
	return &latestUploadInfo, nil
}

func (u *Uploads) RetrieveFailedBatches(
	ctx context.Context,
	req model.RetrieveFailedBatchesRequest,
) ([]model.RetrieveFailedBatchesResponse, error) {
	stmt := `
		WITH table_uploads_result AS (
		  -- Getting entries from table uploads where total_events got populated
		  SELECT
			u.error_category,
			u.source_id,
			CASE WHEN u.status = 'waiting' THEN 'syncing' WHEN u.status LIKE '%failed%' THEN 'failed' ELSE u.status END AS status,
			u.id,
			tu.total_events,
			u.updated_at,
			u.error,
			u.timings
		  FROM
			wh_uploads u
			LEFT JOIN wh_table_uploads tu ON u.id = tu.wh_upload_id
		  WHERE
			u.destination_id = $1
			AND u.workspace_id = $2
			AND u.created_at >= $3
			AND u.created_at <= $4
			AND u.status ~~ ANY($5)
			AND u.error IS NOT NULL
			AND u.error != '{}'
			AND tu.table_name NOT ILIKE $6
			AND tu.total_events IS NOT NULL
		),
		staging_files_result AS (
		  -- Getting entries from staging files
		  SELECT
			u.error_category,
			u.source_id,
			CASE WHEN u.status = 'waiting' THEN 'syncing' WHEN u.status LIKE '%failed%' THEN 'failed' ELSE u.status END AS status,
			u.id,
			s.total_events,
			u.updated_at,
			u.error,
			u.timings
		  FROM
			wh_uploads u
			LEFT JOIN wh_staging_files s ON u.id = s.upload_id
		  WHERE
			u.destination_id = $1
			AND u.workspace_id = $2
			AND u.created_at >= $3
			AND u.created_at <= $4
			AND u.status ~~ ANY($5)
			AND u.error IS NOT NULL
			AND u.error != '{}'
		),
		combined_result AS (
		  -- Select everything from table_uploads_result
		  SELECT
			tu.error_category,
			tu.source_id,
			tu.status,
			tu.id,
			tu.total_events,
			tu.updated_at,
			tu.error,
			tu.timings
		  FROM
			table_uploads_result tu
		  UNION ALL
			-- Select everything from staging_files_result which is not present in table_uploads_result
		  SELECT
			s.error_category,
			s.source_id,
			s.status,
			s.id,
			s.total_events,
			s.updated_at,
			s.error,
			s.timings
		  FROM
			staging_files_result s
		  WHERE
			NOT EXISTS (
			  SELECT
				1
			  FROM
				table_uploads_result tu
			  WHERE
				tu.id = s.id
			)
		)
		SELECT
		  error_category,
		  source_id,
		  status,
		  COALESCE (total_events, 0) AS total_events,
		  COALESCE(
			(
			  SELECT
				COUNT (DISTINCT combined_result.id)
			  FROM
				combined_result
			  WHERE
				combined_result.error_category = q.error_category
				AND combined_result.source_id = q.source_id
				AND combined_result.status = q.status
			),
			0
		  ) AS total_syncs,
		  last_happened_at,
		  first_happened_at,
		  error,
		  timings
		FROM
		  (
			SELECT
			  error_category,
			  source_id,
			  status,
			  SUM(total_events) OVER (
				PARTITION BY error_category, source_id, status
			  ) AS total_events,
			  MAX(updated_at) OVER (
				PARTITION BY error_category, source_id, status
			  ) AS last_happened_at,
			  MIN(updated_at) OVER (
				PARTITION BY error_category, source_id, status
			  ) AS first_happened_at,
			  error,
			  timings,
			  ROW_NUMBER() OVER (
				PARTITION BY error_category, source_id, status
				ORDER BY updated_at DESC
			  ) AS row_number
			FROM
			  combined_result
		  ) q
		WHERE
		  row_number = 1;
	`
	start, end := req.Start, req.End
	if req.End.IsZero() {
		end = u.now()
	}

	stmtArgs := []any{
		req.DestinationID, req.WorkspaceID, start, end,
		pq.Array([]string{model.Waiting, "%" + model.Failed + "%", model.Aborted}),
		warehouseutils.DiscardsTable,
	}

	rows, err := u.db.QueryContext(ctx, stmt, stmtArgs...)
	if err != nil {
		return nil, fmt.Errorf("querying failed batches: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var failedBatches []model.RetrieveFailedBatchesResponse
	for rows.Next() {
		var failedBatch model.RetrieveFailedBatchesResponse
		var timingsRaw []byte
		var timings model.Timings

		err := rows.Scan(
			&failedBatch.ErrorCategory,
			&failedBatch.SourceID,
			&failedBatch.Status,
			&failedBatch.TotalEvents,
			&failedBatch.TotalSyncs,
			&failedBatch.LastHappenedAt,
			&failedBatch.FirstHappenedAt,
			&failedBatch.Error,
			&timingsRaw,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning failed batches: %w", err)
		}

		failedBatch.LastHappenedAt = failedBatch.LastHappenedAt.UTC()
		failedBatch.FirstHappenedAt = failedBatch.FirstHappenedAt.UTC()

		if len(timingsRaw) > 0 {
			_ = json.Unmarshal(timingsRaw, &timings)

			errs := gjson.Get(
				failedBatch.Error,
				fmt.Sprintf("%s.errors", model.GetLastFailedStatus(timings)),
			).Array()
			if len(errs) > 0 {
				failedBatch.Error = errs[len(errs)-1].String()
			}
		}

		failedBatches = append(failedBatches, failedBatch)
	}

	return failedBatches, nil
}

func (u *Uploads) RetryFailedBatches(
	ctx context.Context,
	req model.RetryFailedBatchesRequest,
) (int64, error) {
	stmt := `
		UPDATE
			` + uploadsTableName + `
		SET
			metadata = metadata || '{"retried": true, "priority": 50}' || jsonb_build_object('nextRetryTime', NOW() - INTERVAL '1 HOUR'),
			status = $5,
			updated_at = $6
		WHERE
			destination_id = $1
			AND workspace_id = $2
			AND created_at >= $3
			AND created_at <= $4
			AND error != '{}'
	`
	start, end := req.Start, req.End
	if req.End.IsZero() {
		end = u.now()
	}

	stmtArgs := []any{
		req.DestinationID, req.WorkspaceID, start, end,
		model.Waiting, u.now(),
	}

	if req.ErrorCategory != "" {
		stmt += fmt.Sprintf(" AND error_category = $%d", len(stmtArgs)+1)
		stmtArgs = append(stmtArgs, req.ErrorCategory)
	}
	if req.SourceID != "" {
		stmt += fmt.Sprintf(" AND source_id = $%d", len(stmtArgs)+1)
		stmtArgs = append(stmtArgs, req.SourceID)
	}
	if req.Status != "" {
		stmt += fmt.Sprintf(" AND status LIKE $%d", len(stmtArgs)+1)
		stmtArgs = append(stmtArgs, "%"+req.Status+"%")
	} else {
		stmt += fmt.Sprintf(" AND status ~~ ANY($%d)", len(stmtArgs)+1)
		stmtArgs = append(stmtArgs, pq.Array([]string{model.Waiting, "%" + model.Failed + "%", model.Aborted}))
	}

	r, err := u.db.ExecContext(ctx, stmt, stmtArgs...)
	if err != nil {
		return 0, fmt.Errorf("retrying failed batches: %w", err)
	}

	return r.RowsAffected()
}

func (u *Uploads) WithTx(ctx context.Context, f func(tx *sqlmiddleware.Tx) error) error {
	return (*repo)(u).WithTx(ctx, f)
}

func (u *Uploads) Update(ctx context.Context, id int64, fields []UpdateKeyValue) error {
	return u.update(ctx, u.db.ExecContext, id, fields)
}

func (u *Uploads) UpdateWithTx(ctx context.Context, tx *sqlmiddleware.Tx, id int64, fields []UpdateKeyValue) error {
	return u.update(ctx, tx.ExecContext, id, fields)
}

func (u *Uploads) update(
	ctx context.Context,
	exec func(context.Context, string, ...any) (sql.Result, error),
	id int64,
	fields []UpdateKeyValue,
) error {
	if len(fields) == 0 {
		return fmt.Errorf("no fields to update")
	}

	filters := strings.Join(lo.Map(fields, func(item UpdateKeyValue, index int) string {
		return fmt.Sprintf(" %s = $%d ", item.key(), index+1)
	}), ", ")
	filtersArgs := lo.Map(fields, func(item UpdateKeyValue, index int) any {
		return item.value()
	})

	query := `UPDATE ` + uploadsTableName + ` SET ` + filters + ` WHERE id = $` + strconv.Itoa(len(filtersArgs)+1)
	filtersArgs = append(filtersArgs, id)

	_, err := exec(ctx, query, filtersArgs...)
	if err != nil {
		return fmt.Errorf("updating uploads: %w", err)
	}
	return nil
}

func (u *Uploads) GetFirstAbortedUploadInContinuousAbortsByDestination(ctx context.Context, workspaceID string, start time.Time) ([]model.FirstAbortedUploadResponse, error) {
	outputColumns := "id, source_id, destination_id, created_at, first_event_at, last_event_at"

	stmt := fmt.Sprintf(`
	WITH wh_uploads_with_last_successful_upload AS (
		SELECT
			%[1]s,
			status,
        	MAX(CASE WHEN status = 'exported_data' THEN created_at ELSE NULL END) OVER (PARTITION BY destination_id) AS last_successful_upload
		FROM
			`+uploadsTableName+`
		WHERE workspace_id = $1
		AND created_at >= $2
	)
	SELECT %[1]s
	FROM (
		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY destination_id ORDER BY created_at) AS row_number
		FROM wh_uploads_with_last_successful_upload
		WHERE status = 'aborted'
		AND (last_successful_upload IS NULL OR created_at > last_successful_upload)
	) AS q
	WHERE q.row_number = 1;
	`, outputColumns)

	rows, err := u.db.QueryContext(ctx, stmt, workspaceID, start)
	if err != nil {
		return nil, fmt.Errorf("first aborted upload in a series of continues aborts info: %w", err)
	}

	defer func() { _ = rows.Close() }()

	var abortedUploadsInfo []model.FirstAbortedUploadResponse

	for rows.Next() {
		var abortedUpload model.FirstAbortedUploadResponse
		var firstEventAt, lastEventAt sql.NullTime

		err := rows.Scan(
			&abortedUpload.ID,
			&abortedUpload.SourceID,
			&abortedUpload.DestinationID,
			&abortedUpload.CreatedAt,
			&firstEventAt,
			&lastEventAt,
		)
		if err != nil {
			return nil, fmt.Errorf("first aborted upload in a series of continues aborts info: scanning row: %w", err)
		}

		if firstEventAt.Valid {
			abortedUpload.FirstEventAt = firstEventAt.Time
		}
		if lastEventAt.Valid {
			abortedUpload.LastEventAt = lastEventAt.Time
		}

		abortedUploadsInfo = append(abortedUploadsInfo, abortedUpload)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("first aborted upload in series info: iterating rows: %w", err)
	}

	return abortedUploadsInfo, nil
}

func (u *Uploads) GetSyncLatencies(ctx context.Context, request model.SyncLatencyRequest) ([]model.LatencyTimeSeriesDataPoint, error) {
	aggregationSQL := getLatencyAggregationSQL(request.AggregationType)

	var filterSQL strings.Builder
	filterSQL.WriteString(`destination_id = $1 AND workspace_id = $2 AND status = $3 AND created_at >= $4`)
	filterParams := []any{request.DestinationID, request.WorkspaceID, model.ExportedData, request.StartTime}

	if request.SourceID != "" {
		filterParams = append(filterParams, request.SourceID)
		filterSQL.WriteString(` AND source_id = $` + strconv.Itoa(len(filterParams)))
	}

	filterParams = append(filterParams, request.AggregationMinutes)
	intervalParamIndex := len(filterParams)

	query := fmt.Sprintf(`
		SELECT
			FLOOR(EXTRACT(EPOCH FROM created_at) / ($%[1]d * 60)) * ($%[1]d * 60) * 1000 AS bucket,
			%s
		FROM wh_uploads
		WHERE %s
		GROUP BY bucket
		ORDER BY bucket;
	`,
		intervalParamIndex,
		aggregationSQL,
		filterSQL.String(),
	)

	rows, err := u.db.QueryContext(ctx, query, filterParams...)
	if err != nil {
		return nil, fmt.Errorf("querying sync latencies: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var syncLatencies []model.LatencyTimeSeriesDataPoint
	for rows.Next() {
		var bucket, latency float64
		if err := rows.Scan(&bucket, &latency); err != nil {
			return nil, fmt.Errorf("scanning sync latencies: %w", err)
		}

		syncLatencies = append(syncLatencies, model.LatencyTimeSeriesDataPoint{
			TimestampMillis: bucket,
			LatencySeconds:  latency,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sync latencies: iterating rows: %w", err)
	}
	return syncLatencies, nil
}

func getLatencyAggregationSQL(aggType model.LatencyAggregationType) string {
	switch aggType {
	case model.P90Latency:
		return "PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_at - created_at)))"
	case model.P95Latency:
		return "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_at - created_at)))"
	case model.AvgLatency:
		return "AVG(EXTRACT(EPOCH FROM (updated_at - created_at)))"
	default:
		return "MAX(EXTRACT(EPOCH FROM (updated_at - created_at)))" // Default to max
	}
}
