package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
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
type StagingFiles struct {
	DB  *sql.DB
	Now func() time.Time
}

type metadataSchema struct {
	UseRudderStorage      bool   `json:"use_rudder_storage"`
	SourceBatchID         string `json:"source_batch_id"`
	SourceTaskID          string `json:"source_task_id"`
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
	var stagingFileIDs []int64
	for _, stagingFile := range stagingFiles {
		stagingFileIDs = append(stagingFileIDs, stagingFile.ID)
	}
	return stagingFileIDs
}

func metadataFromStagingFile(stagingFile *model.StagingFile) metadataSchema {
	return metadataSchema{
		UseRudderStorage:      stagingFile.UseRudderStorage,
		SourceBatchID:         stagingFile.SourceBatchID,
		SourceTaskID:          stagingFile.SourceTaskID,
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

func NewStagingFiles(db *sql.DB) *StagingFiles {
	return &StagingFiles{
		DB:  db,
		Now: time.Now,
	}
}

func (m *metadataSchema) SetStagingFile(stagingFile *model.StagingFile) {
	stagingFile.UseRudderStorage = m.UseRudderStorage
	stagingFile.SourceBatchID = m.SourceBatchID
	stagingFile.SourceTaskID = m.SourceTaskID
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
	now := repo.Now()

	schemaPayload, err := json.Marshal(stagingFile.Schema)
	if err != nil {
		return id, fmt.Errorf("marshaling schema: %w", err)
	}

	err = repo.DB.QueryRowContext(ctx,
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
		stagingFile.Status,
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
func (*StagingFiles) parseRows(rows *sql.Rows) ([]model.StagingFile, error) {
	var stagingFiles []model.StagingFile

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
		stagingFiles = append(stagingFiles, stagingFile)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return stagingFiles, nil
}

// GetByID returns staging file with the given ID.
func (repo *StagingFiles) GetByID(ctx context.Context, ID int64) (model.StagingFile, error) {
	query := `SELECT ` + stagingTableColumns + ` FROM ` + stagingTableName + ` WHERE id = $1`

	rows, err := repo.DB.QueryContext(ctx, query, ID)
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

	return entries[0], err
}

// GetSchemaByID returns staging file schema field the given ID.
func (repo *StagingFiles) GetSchemaByID(ctx context.Context, ID int64) (json.RawMessage, error) {
	query := `SELECT schema FROM ` + stagingTableName + ` WHERE id = $1`

	row := repo.DB.QueryRowContext(ctx, query, ID)
	if row.Err() != nil {
		return nil, fmt.Errorf("querying staging files: %w", row.Err())
	}

	var schema json.RawMessage
	err := row.Scan(&schema)
	if err != nil {
		return nil, fmt.Errorf("parsing rows: %w", err)
	}

	return schema, err
}

// GetInRange returns staging files in [startID, endID] range inclusive.
func (repo *StagingFiles) GetInRange(ctx context.Context, sourceID, destinationID string, startID, endID int64) ([]model.StagingFile, error) {
	query := `SELECT ` + stagingTableColumns + ` FROM ` + stagingTableName + ` ST
	WHERE
		id >= $1 AND id <= $2
		AND source_id = $3
		AND destination_id = $4
	ORDER BY
		id ASC;`

	rows, err := repo.DB.QueryContext(ctx, query, startID, endID, sourceID, destinationID)
	if err != nil {
		return nil, fmt.Errorf("querying staging files: %w", err)
	}

	return repo.parseRows(rows)
}

// GetAfterID returns staging files in (startID, +Inf) range.
func (repo *StagingFiles) GetAfterID(ctx context.Context, sourceID, destinationID string, startID int64) ([]model.StagingFile, error) {
	query := `SELECT ` + stagingTableColumns + ` FROM ` + stagingTableName + `
	WHERE
		id > $1
		AND source_id = $2
		AND destination_id = $3
	ORDER BY
		id ASC;`

	rows, err := repo.DB.QueryContext(ctx, query, startID, sourceID, destinationID)
	if err != nil {
		return nil, fmt.Errorf("querying staging files: %w", err)
	}

	return repo.parseRows(rows)
}

func (repo *StagingFiles) SetStatuses(ctx context.Context, ids []int64, status string) (err error) {
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
	result, err := repo.DB.ExecContext(ctx, sqlStatement, status, repo.Now(), pq.Array(ids))
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

	return
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

	result, err := repo.DB.ExecContext(
		ctx,
		sqlStatement,
		warehouseutils.StagingFileFailedState,
		stageFileErr.Error(),
		repo.Now(),
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
