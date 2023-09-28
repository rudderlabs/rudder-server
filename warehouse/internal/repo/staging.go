package repo

import (
	"context"
	"database/sql"
	jsonstd "encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const stagingTableName = warehouseutils.WarehouseStagingFilesTable

const stagingTableColumns = `
	id,
    location,
    source_id,
    destination_id,
    error,
    status,
    first_event_at,
    last_event_at,
    total_events,
	total_bytes,
    created_at,
    updated_at,
    metadata,
    workspace_id
`

// StagingFiles is a repository for inserting and querying staging files.
type StagingFiles repo

type metadataSchema struct {
	UseRudderStorage      bool   `json:"use_rudder_storage"`
	SourceTaskRunID       string `json:"source_task_run_id"`
	SourceJobID           string `json:"source_job_id"`
	SourceJobRunID        string `json:"source_job_run_id"`
	TimeWindowYear        int    `json:"time_window_year"`
	TimeWindowMonth       int    `json:"time_window_month"`
	TimeWindowDay         int    `json:"time_window_day"`
	TimeWindowHour        int    `json:"time_window_hour"`
	DestinationRevisionID string `json:"destination_revision_id"`
}

func StagingFileIDs(stagingFiles []*model.StagingFile) []int64 {
	stagingFileIDs := make([]int64, len(stagingFiles))
	for i, stagingFile := range stagingFiles {
		stagingFileIDs[i] = stagingFile.ID
	}
	return stagingFileIDs
}

func metadataFromStagingFile(stagingFile *model.StagingFile) metadataSchema {
	return metadataSchema{
		UseRudderStorage:      stagingFile.UseRudderStorage,
		SourceTaskRunID:       stagingFile.SourceTaskRunID,
		SourceJobID:           stagingFile.SourceJobID,
		SourceJobRunID:        stagingFile.SourceJobRunID,
		TimeWindowYear:        stagingFile.TimeWindow.Year(),
		TimeWindowMonth:       int(stagingFile.TimeWindow.Month()),
		TimeWindowDay:         stagingFile.TimeWindow.Day(),
		TimeWindowHour:        stagingFile.TimeWindow.Hour(),
		DestinationRevisionID: stagingFile.DestinationRevisionID,
	}
}

func NewStagingFiles(db *sqlmiddleware.DB, opts ...Opt) *StagingFiles {
	r := &StagingFiles{
		db:  db,
		now: timeutil.Now,
	}
	for _, opt := range opts {
		opt((*repo)(r))
	}
	return r
}

func (m *metadataSchema) SetStagingFile(stagingFile *model.StagingFile) {
	stagingFile.UseRudderStorage = m.UseRudderStorage
	stagingFile.SourceTaskRunID = m.SourceTaskRunID
	stagingFile.SourceJobID = m.SourceJobID
	stagingFile.SourceJobRunID = m.SourceJobRunID
	stagingFile.TimeWindow = time.Date(m.TimeWindowYear, time.Month(m.TimeWindowMonth), m.TimeWindowDay, m.TimeWindowHour, 0, 0, 0, time.UTC)
	stagingFile.DestinationRevisionID = m.DestinationRevisionID
}

// Insert inserts a staging file into the staging files table. It returns the ID of the inserted staging file.
//
// NOTE: The following fields are ignored and set by the database:
// - ID
// - Error
// - CreatedAt
// - UpdatedAt
func (repo *StagingFiles) Insert(ctx context.Context, stagingFile *model.StagingFileWithSchema) (int64, error) {
	var (
		id                        int64
		firstEventAt, lastEventAt interface{}
	)

	firstEventAt = stagingFile.FirstEventAt.UTC()
	if stagingFile.FirstEventAt.IsZero() {
		firstEventAt = nil
	}

	lastEventAt = stagingFile.LastEventAt.UTC()
	if stagingFile.LastEventAt.IsZero() {
		lastEventAt = nil
	}

	m := metadataFromStagingFile(&stagingFile.StagingFile)
	rawMetadata, err := json.Marshal(&m)
	if err != nil {
		return id, fmt.Errorf("marshaling metadata: %w", err)
	}
	now := repo.now()

	schemaPayload, err := json.Marshal(stagingFile.Schema)
	if err != nil {
		return id, fmt.Errorf("marshaling schema: %w", err)
	}

	err = repo.db.QueryRowContext(ctx,
		`INSERT INTO `+stagingTableName+` (
			location,
			schema,
			workspace_id,
			source_id,
			destination_id,
			status,
			total_events,
			total_bytes,
			first_event_at,
			last_event_at,
			created_at,
			updated_at,
			metadata
		)
		VALUES
		 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING id`,

		stagingFile.Location,
		schemaPayload,
		stagingFile.WorkspaceID,
		stagingFile.SourceID,
		stagingFile.DestinationID,
		warehouseutils.StagingFileWaitingState,
		stagingFile.TotalEvents,
		stagingFile.TotalBytes,
		firstEventAt,
		lastEventAt,
		now.UTC(),
		now.UTC(),
		rawMetadata,
	).Scan(&id)
	if err != nil {
		return id, fmt.Errorf("inserting staging file: %w", err)
	}

	return id, nil
}

// praseRow is a helper for mapping a row of tableColumns to a model.StagingFile.
func (*StagingFiles) parseRows(rows *sqlmiddleware.Rows) ([]*model.StagingFile, error) {
	var stagingFiles []*model.StagingFile

	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			stagingFile model.StagingFile
			metadataRaw []byte

			firstEventAt, lastEventAt sql.NullTime

			errorRaw sql.NullString
		)
		err := rows.Scan(
			&stagingFile.ID,
			&stagingFile.Location,
			&stagingFile.SourceID,
			&stagingFile.DestinationID,
			&errorRaw,
			&stagingFile.Status,
			&firstEventAt,
			&lastEventAt,
			&stagingFile.TotalEvents,
			&stagingFile.TotalBytes,
			&stagingFile.CreatedAt,
			&stagingFile.UpdatedAt,
			&metadataRaw,
			&stagingFile.WorkspaceID,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		stagingFile.CreatedAt = stagingFile.CreatedAt.UTC()
		stagingFile.UpdatedAt = stagingFile.UpdatedAt.UTC()

		if firstEventAt.Valid {
			stagingFile.FirstEventAt = firstEventAt.Time.UTC()
		}
		if lastEventAt.Valid {
			stagingFile.LastEventAt = lastEventAt.Time.UTC()
		}

		if errorRaw.Valid {
			stagingFile.Error = errors.New(errorRaw.String)
		}

		var m metadataSchema
		err = json.Unmarshal(metadataRaw, &m)
		if err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}

		m.SetStagingFile(&stagingFile)
		stagingFiles = append(stagingFiles, &stagingFile)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return stagingFiles, nil
}

// GetByID returns staging file with the given ID.
func (repo *StagingFiles) GetByID(ctx context.Context, ID int64) (model.StagingFile, error) {
	query := `SELECT ` + stagingTableColumns + ` FROM ` + stagingTableName + ` WHERE id = $1`

	rows, err := repo.db.QueryContext(ctx, query, ID)
	if err != nil {
		return model.StagingFile{}, fmt.Errorf("querying staging files: %w", err)
	}

	entries, err := repo.parseRows(rows)
	if err != nil {
		return model.StagingFile{}, fmt.Errorf("parsing rows: %w", err)
	}
	if len(entries) == 0 {
		return model.StagingFile{}, fmt.Errorf("no staging file found with id: %d", ID)
	}

	return *entries[0], err
}

// GetSchemasByIDs returns staging file schemas for the given IDs.
func (repo *StagingFiles) GetSchemasByIDs(ctx context.Context, ids []int64) ([]model.Schema, error) {
	query := `SELECT schema FROM ` + stagingTableName + ` WHERE id = ANY ($1);`

	rows, err := repo.db.QueryContext(ctx, query, pq.Array(ids))
	if err != nil {
		return nil, fmt.Errorf("querying schemas: %w", err)
	}
	defer func() { _ = rows.Close() }()

	schemas := make([]model.Schema, 0, len(ids))

	for rows.Next() {
		var (
			rawSchema jsonstd.RawMessage
			schema    model.Schema
		)

		if err := rows.Scan(&rawSchema); err != nil {
			return nil, fmt.Errorf("cannot get schemas by ids: scanning row: %w", err)
		}
		if err := json.Unmarshal(rawSchema, &schema); err != nil {
			return nil, fmt.Errorf("cannot get schemas by ids: unmarshal staging schema: %w", err)
		}

		schemas = append(schemas, schema)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot get schemas by ids: iterating rows: %w", err)
	}
	if len(schemas) != len(ids) {
		return nil, fmt.Errorf("cannot get schemas by ids: not all schemas were found")
	}

	return schemas, nil
}

// GetForUploadID returns all the staging files for that uploadID
func (repo *StagingFiles) GetForUploadID(ctx context.Context, sourceID, destinationID string, uploadId int64) ([]*model.StagingFile, error) {
	query := `SELECT ` + stagingTableColumns + ` FROM ` + stagingTableName + ` ST
	WHERE
		upload_id = $1
		AND source_id = $2
		AND destination_id = $3
	ORDER BY
		id ASC;`
	rows, err := repo.db.QueryContext(ctx, query, uploadId, sourceID, destinationID)
	if err != nil {
		return nil, fmt.Errorf("querying staging files: %w", err)
	}

	return repo.parseRows(rows)
}

func (repo *StagingFiles) GetForUpload(ctx context.Context, upload model.Upload) ([]*model.StagingFile, error) {
	return repo.GetForUploadID(ctx, upload.SourceID, upload.DestinationID, upload.ID)
}

func (repo *StagingFiles) Pending(ctx context.Context, sourceID, destinationID string) ([]*model.StagingFile, error) {
	var (
		uploadID               int64
		lastStartStagingFileID int64
	)
	err := repo.db.QueryRowContext(ctx, `
		SELECT
			id,
			start_staging_file_id
		FROM
		`+uploadsTableName+`
		WHERE
			source_id = $1 AND destination_id = $2
		ORDER BY
			id DESC
		LIMIT 1;
	`, sourceID, destinationID,
	).Scan(
		&uploadID,
		&lastStartStagingFileID,
	)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("querying uploads: %w", err)
	}
	// lastStartStagingFileID is used as an optimization to avoid scanning the whole table.
	query := `SELECT ` + stagingTableColumns + ` FROM ` + stagingTableName + `
	WHERE
		id > $1
		AND source_id = $2
		AND destination_id = $3
		AND upload_id IS NULL
	ORDER BY
		id ASC;`

	rows, err := repo.db.QueryContext(ctx, query, lastStartStagingFileID, sourceID, destinationID)
	if err != nil {
		return nil, fmt.Errorf("querying staging files: %w", err)
	}

	return repo.parseRows(rows)
}

func (repo *StagingFiles) CountPendingForSource(ctx context.Context, sourceID string) (int64, error) {
	return repo.countPending(ctx, `source_id = $1`, sourceID)
}

func (repo *StagingFiles) CountPendingForDestination(ctx context.Context, destinationID string) (int64, error) {
	return repo.countPending(ctx, `destination_id = $1`, destinationID)
}

func (repo *StagingFiles) countPending(ctx context.Context, query string, value interface{}) (int64, error) {
	var count int64
	err := repo.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+stagingTableName+` WHERE `+query+` AND id > (SELECT COALESCE(MAX(end_staging_file_id), 0) FROM `+uploadsTableName+` WHERE `+query+`)`,
		value,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting staging files: %w", err)
	}

	return count, nil
}

func (repo *StagingFiles) TotalEventsForUpload(ctx context.Context, upload model.Upload) (int64, error) {
	var total sql.NullInt64

	err := repo.db.QueryRowContext(ctx, `
		SELECT
			sum(total_events)
		FROM
			`+stagingTableName+`
		WHERE
			id >= $1
			AND id <= $2
			AND source_id = $3
			AND destination_id = $4;
		`,
		upload.StagingFileStartID,
		upload.StagingFileEndID,
		upload.SourceID,
		upload.DestinationID,
	).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("querying total rows for upload: %w", err)
	}

	return total.Int64, nil
}

func (repo *StagingFiles) FirstEventForUpload(ctx context.Context, upload model.Upload) (time.Time, error) {
	var firstEvent sql.NullTime
	err := repo.db.QueryRowContext(ctx, `
		SELECT
			first_event_at
		FROM
			`+stagingTableName+`
		WHERE
			id = $1;`,
		upload.StagingFileStartID,
	).Scan(&firstEvent)
	if err != nil {
		return time.Time{}, fmt.Errorf("querying first event for upload: %w", err)
	}

	return firstEvent.Time, nil
}

func (repo *StagingFiles) DestinationRevisionIDs(ctx context.Context, upload model.Upload) ([]string, error) {
	sqlStatement := `
		SELECT
		  DISTINCT metadata ->> 'destination_revision_id' AS destination_revision_id
		FROM
		  ` + stagingTableName + `
		WHERE
		  id >= $1
		  AND id <= $2
		  AND source_id = $3
		  AND destination_id = $4
		  AND metadata ->> 'destination_revision_id' <> '';
	`
	rows, err := repo.db.QueryContext(ctx, sqlStatement,
		upload.StagingFileStartID,
		upload.StagingFileEndID,
		upload.SourceID,
		upload.DestinationID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query destination revisionID: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var revisionIDs []string
	for rows.Next() {
		var revisionID string
		err = rows.Scan(&revisionID)
		if err != nil {
			return nil, fmt.Errorf("scan destination revisionID: %w", err)
		}
		revisionIDs = append(revisionIDs, revisionID)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate destination revisionID: %w", err)
	}

	return revisionIDs, nil
}

func (repo *StagingFiles) SetStatuses(ctx context.Context, ids []int64, status string) error {
	if len(ids) == 0 {
		return fmt.Errorf("no staging files to update")
	}

	sqlStatement := `
		UPDATE
		` + stagingTableName + `
		SET
		  status = $1,
		  updated_at = $2
		WHERE
		  id = ANY($3);
`
	result, err := repo.db.ExecContext(ctx, sqlStatement, status, repo.now(), pq.Array(ids))
	if err != nil {
		return fmt.Errorf("update ids status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rowsAffected != int64(len(ids)) {
		return fmt.Errorf("not all rows were updated: %d != %d", rowsAffected, len(ids))
	}

	return nil
}

func (repo *StagingFiles) SetErrorStatus(ctx context.Context, stagingFileID int64, stageFileErr error) error {
	sqlStatement := `
		UPDATE
		` + stagingTableName + `
		SET
			status = $1,
			error = $2,
			updated_at = $3
		WHERE
			id = $4;`

	result, err := repo.db.ExecContext(
		ctx,
		sqlStatement,
		warehouseutils.StagingFileFailedState,
		stageFileErr.Error(),
		repo.now(),
		stagingFileID,
	)
	if err != nil {
		return fmt.Errorf("update staging file with error: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("no rows affected")
	}
	return nil
}
