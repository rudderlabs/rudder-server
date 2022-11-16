package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const tableName = warehouseutils.WarehouseStagingFilesTable

const tableColumns = `
	id,
    location,
    source_id,
    destination_id,
    schema,
    error,
    status,
    first_event_at,
    last_event_at,
    total_events,
    created_at,
    updated_at,
    metadata,
    workspace_id 
`

type StagingFiles struct {
	DB  *sql.DB
	Now func() time.Time

	once sync.Once
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

func metadataFromStagingFile(stagingFile model.StagingFile) metadataSchema {
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

func (repo *StagingFiles) init() {
	repo.once.Do(func() {
		if repo.Now == nil {
			repo.Now = timeutil.Now
		}
	})
}

// Insert inserts a staging file into the staging files table.
//
// NOTE: The following fields are ignored and set by the database:
// - ID
// - Error
// - CreatedAt
// - UpdatedAt
func (repo *StagingFiles) Insert(ctx context.Context, stagingFile model.StagingFile) (int64, error) {
	repo.init()

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

	m := metadataFromStagingFile(stagingFile)
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
		`INSERT INTO `+tableName+` (
			location,
			schema,
			workspace_id,
			source_id,
			destination_id,
			status,
			total_events,
			first_event_at,
			last_event_at,
			created_at,
			updated_at,
			metadata
		)
		VALUES
		 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) 
		RETURNING id`,

		stagingFile.Location,
		schemaPayload,
		stagingFile.WorkspaceID,
		stagingFile.SourceID,
		stagingFile.DestinationID,
		stagingFile.Status,
		stagingFile.TotalEvents,
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

func (_ *StagingFiles) parseRows(rows *sql.Rows) ([]model.StagingFile, error) {
	var stagingFiles []model.StagingFile
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
			&stagingFile.Schema,
			&errorRaw,
			&stagingFile.Status,
			&firstEventAt,
			&lastEventAt,
			&stagingFile.TotalEvents,
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

	return stagingFiles, nil
}

// GetInRange returns staging file with the given ID.
func (repo *StagingFiles) GetByID(ctx context.Context, ID int64) (model.StagingFile, error) {
	repo.init()

	query := `SELECT ` + tableColumns + ` FROM ` + tableName + ` WHERE id = $1`

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

// GetInRange returns staging files in [startID, endID] range.
func (repo *StagingFiles) GetInRange(ctx context.Context, sourceID, destinationID string, startID, endID int64) ([]model.StagingFile, error) {
	repo.init()

	query := `SELECT ` + tableColumns + ` FROM ` + tableName + ` ST
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
	repo.init()

	query := `SELECT ` + tableColumns + ` FROM ` + tableName + `
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
