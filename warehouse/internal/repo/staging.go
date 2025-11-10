package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"

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
    workspace_id,
    bytes_per_table
`

// StagingFiles is a repository for inserting and querying staging files.
type StagingFiles struct {
	*repo
	conf *config.Config
}

type metadataSchema struct {
	UseRudderStorage              bool     `json:"use_rudder_storage"`
	SourceTaskRunID               string   `json:"source_task_run_id"`
	SourceJobID                   string   `json:"source_job_id"`
	SourceJobRunID                string   `json:"source_job_run_id"`
	TimeWindowYear                int      `json:"time_window_year"`
	TimeWindowMonth               int      `json:"time_window_month"`
	TimeWindowDay                 int      `json:"time_window_day"`
	TimeWindowHour                int      `json:"time_window_hour"`
	DestinationRevisionID         string   `json:"destination_revision_id"`
	ServerInstanceID              string   `json:"server_instance_id"`
	SnapshotPatchSize             *int     `json:"snapshot_patch_size,omitempty"`
	SnapshotPatchCompressionRatio *float64 `json:"snapshot_patch_compression_ratio,omitempty"`
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
		ServerInstanceID:      stagingFile.ServerInstanceID,
	}
}

func NewStagingFiles(db *sqlmiddleware.DB, conf *config.Config, opts ...Opt) *StagingFiles {
	r := &StagingFiles{
		repo: &repo{
			db:           db,
			now:          timeutil.Now,
			statsFactory: stats.NOP,
			repoType:     stagingTableName,
		},
		conf: conf,
	}
	for _, opt := range opts {
		opt(r.repo)
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
	stagingFile.ServerInstanceID = m.ServerInstanceID
}

// Insert inserts a staging file into the staging files table. It returns the ID of the inserted staging file.
//
// NOTE: The following fields are ignored and set by the database:
// - ID
// - Error
// - CreatedAt
// - UpdatedAt
func (sf *StagingFiles) Insert(ctx context.Context, stagingFile *model.StagingFileWithSchema) (int64, error) {
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
	if len(stagingFile.SnapshotPatch) > 0 {
		m.SnapshotPatchSize = lo.ToPtr[int](len(stagingFile.SnapshotPatch))
		m.SnapshotPatchCompressionRatio = lo.ToPtr[float64](float64(len(stagingFile.SnapshotPatch)) / float64(len(stagingFile.Schema)))
	}
	rawMetadata, err := jsonrs.Marshal(&m)
	if err != nil {
		return id, fmt.Errorf("marshaling metadata: %w", err)
	}
	now := sf.now()

	var bytesPerTablePayload interface{}
	if stagingFile.BytesPerTable != nil {
		marshalled, err := jsonrs.Marshal(stagingFile.BytesPerTable)
		if err != nil {
			return id, fmt.Errorf("marshaling bytes per table: %w", err)
		}
		bytesPerTablePayload = marshalled
	} else {
		bytesPerTablePayload = nil
	}

	defer sf.TimerStat("insert", stats.Tags{
		"destId":      stagingFile.DestinationID,
		"workspaceId": stagingFile.WorkspaceID,
	})()

	var schemaSnapshotID interface{}
	if stagingFile.SnapshotID != uuid.Nil {
		schemaSnapshotID = stagingFile.SnapshotID.String()
	}
	var schemaPatchPayload interface{}
	if len(stagingFile.SnapshotPatch) > 0 {
		schemaPatchPayload = stagingFile.SnapshotPatch
	}

	err = sf.db.QueryRowContext(ctx,
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
			metadata,
			bytes_per_table,
			schema_snapshot_id,
			schema_snapshot_patch
		)
		VALUES
		 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		RETURNING id`,
		stagingFile.Location,
		"{}", // schema is not used for now
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
		bytesPerTablePayload,
		schemaSnapshotID,
		schemaPatchPayload,
	).Scan(&id)
	if err != nil {
		return id, fmt.Errorf("inserting staging file: %w", err)
	}

	return id, nil
}

// praseRow is a helper for mapping a row of tableColumns to a model.StagingFile.
func parseStagingFiles(rows *sqlmiddleware.Rows) ([]*model.StagingFile, error) {
	var stagingFiles []*model.StagingFile

	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			stagingFile      model.StagingFile
			metadataRaw      []byte
			bytesPerTableRaw []byte

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
			&bytesPerTableRaw,
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
		err = jsonrs.Unmarshal(metadataRaw, &m)
		if err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}

		if bytesPerTableRaw != nil {
			err = jsonrs.Unmarshal(bytesPerTableRaw, &stagingFile.BytesPerTable)
			if err != nil {
				return nil, fmt.Errorf("unmarshal bytes per table: %w", err)
			}
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
func (sf *StagingFiles) GetByID(ctx context.Context, ID int64) (model.StagingFile, error) {
	defer sf.TimerStat("get_by_id", nil)()

	query := `SELECT ` + stagingTableColumns + ` FROM ` + stagingTableName + ` WHERE id = $1`

	rows, err := sf.db.QueryContext(ctx, query, ID)
	if err != nil {
		return model.StagingFile{}, fmt.Errorf("querying staging files: %w", err)
	}

	entries, err := parseStagingFiles(rows)
	if err != nil {
		return model.StagingFile{}, fmt.Errorf("parsing rows: %w", err)
	}
	if len(entries) == 0 {
		return model.StagingFile{}, fmt.Errorf("no staging file found with id: %d", ID)
	}

	return *entries[0], err
}

// GetSchemasByIDs returns staging file schemas for the given IDs.
func (sf *StagingFiles) GetSchemasByIDs(ctx context.Context, ids []int64) ([]model.Schema, error) {
	defer sf.TimerStat("get_schemas_by_ids", nil)()

	query := `
			SELECT sf.schema AS schema, sf.schema_snapshot_patch as schema_snapshot_patch, ss.schema AS schema_snapshot
			FROM ` + stagingTableName + ` sf
			LEFT JOIN ` + stagingFileSchemaSnapshotTableName + ` ss ON sf.schema_snapshot_id = ss.id
			WHERE sf.id = ANY($1);
		`
	rows, err := sf.db.QueryContext(ctx, query, pq.Array(ids))
	if err != nil {
		return nil, fmt.Errorf("querying schemas: %w", err)
	}
	defer func() { _ = rows.Close() }()

	schemas := make([]model.Schema, 0, len(ids))
	for rows.Next() {
		var (
			rawSchema      []byte
			schemaSnapshot []byte
			schemaPatch    []byte
		)
		if err := rows.Scan(&rawSchema, &schemaPatch, &schemaSnapshot); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		// If both snapshot and patch are present, use them instead of the regular schema
		if len(schemaPatch) > 0 && len(schemaSnapshot) > 0 {
			originalSchemaBytes, err := warehouseutils.ApplyPatchToJSON(schemaSnapshot, schemaPatch)
			if err != nil {
				return nil, fmt.Errorf("cannot get schemas by ids: applying patch: %w", err)
			}
			var originalSchema model.Schema
			if err := jsonrs.Unmarshal(originalSchemaBytes, &originalSchema); err != nil {
				return nil, fmt.Errorf("cannot get schemas by ids: unmarshal staging schema: %w", err)
			}
			if len(originalSchema) == 0 {
				return nil, fmt.Errorf("cannot get schemas by ids: staging snapshot schema is empty")
			}
			schemas = append(schemas, originalSchema)
		} else {
			// Fall back to regular schema only if snapshot or patch is missing
			var schema model.Schema
			if err := jsonrs.Unmarshal(rawSchema, &schema); err != nil {
				return nil, fmt.Errorf("unmarshal staging schema: %w", err)
			}
			if len(schema) == 0 {
				return nil, fmt.Errorf("cannot get schemas by ids: staging fallback schema is empty")
			}
			schemas = append(schemas, schema)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	if len(schemas) != len(ids) {
		return nil, fmt.Errorf("cannot get schemas by ids: not all schemas were found")
	}
	return schemas, nil
}

// GetForUploadID retrieves all the staging files associated with a specific upload ID.
func (sf *StagingFiles) GetForUploadID(ctx context.Context, uploadID int64) ([]*model.StagingFile, error) {
	defer sf.TimerStat("get_for_upload_id", nil)()

	query := `SELECT ` + stagingTableColumns + ` FROM ` + stagingTableName + ` WHERE upload_id = $1 ORDER BY id ASC;`

	rows, err := sf.db.QueryContext(ctx, query, uploadID)
	if err != nil {
		return nil, fmt.Errorf("querying staging files for upload %d: %w", uploadID, err)
	}

	return parseStagingFiles(rows)
}

func (sf *StagingFiles) Pending(ctx context.Context, sourceID, destinationID string) ([]*model.StagingFile, error) {
	defer sf.TimerStat("pending", stats.Tags{
		"destId": destinationID,
	})()

	var (
		uploadID               int64
		lastStartStagingFileID int64
	)
	err := sf.db.QueryRowContext(ctx, `
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

	rows, err := sf.db.QueryContext(ctx, query, lastStartStagingFileID, sourceID, destinationID)
	if err != nil {
		return nil, fmt.Errorf("querying staging files: %w", err)
	}

	return parseStagingFiles(rows)
}

func (sf *StagingFiles) CountPendingForSource(ctx context.Context, sourceID string) (int64, error) {
	defer sf.TimerStat("count_pending_for_source", nil)()

	return sf.countPending(ctx, `source_id = $1`, sourceID)
}

func (sf *StagingFiles) CountPendingForDestination(ctx context.Context, destinationID string) (int64, error) {
	defer sf.TimerStat("count_pending_for_destination", stats.Tags{
		"destId": destinationID,
	})()

	return sf.countPending(ctx, `destination_id = $1`, destinationID)
}

func (sf *StagingFiles) countPending(ctx context.Context, query string, value interface{}) (int64, error) {
	var count int64
	err := sf.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+stagingTableName+` WHERE `+query+` AND id > (SELECT COALESCE(MAX(end_staging_file_id), 0) FROM `+uploadsTableName+` WHERE `+query+`)`,
		value,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting staging files: %w", err)
	}

	return count, nil
}

func (sf *StagingFiles) TotalEventsForUploadID(ctx context.Context, uploadID int64) (int64, error) {
	defer sf.TimerStat("total_events_for_upload_id", nil)()

	var total sql.NullInt64

	query := `SELECT SUM(total_events) FROM ` + stagingTableName + ` WHERE upload_id = $1`
	err := sf.db.QueryRowContext(ctx, query, uploadID).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("querying total events for upload %d: %w", uploadID, err)
	}

	return total.Int64, nil
}

func (sf *StagingFiles) GetEventTimeRangesByUploadID(ctx context.Context, uploadID int64) ([]model.EventTimeRange, error) {
	defer sf.TimerStat("get_event_time_ranges_by_upload_id", nil)()

	rows, err := sf.db.QueryContext(ctx, `
		SELECT
			first_event_at, last_event_at
		FROM
			`+stagingTableName+`
		WHERE
			upload_id = $1;`,
		uploadID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query event time ranges: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var eventTimeRanges []model.EventTimeRange
	for rows.Next() {
		var firstEventAt, lastEventAt time.Time
		err = rows.Scan(&firstEventAt, &lastEventAt)
		if err != nil {
			return nil, fmt.Errorf("scan event time range: %w", err)
		}
		eventTimeRanges = append(eventTimeRanges, model.EventTimeRange{
			FirstEventAt: firstEventAt,
			LastEventAt:  lastEventAt,
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate event time range: %w", err)
	}

	return eventTimeRanges, nil
}

func (sf *StagingFiles) DestinationRevisionIDsForUploadID(ctx context.Context, uploadID int64) ([]string, error) {
	defer sf.TimerStat("destination_revision_ids_for_upload_id", nil)()

	query := `
		SELECT DISTINCT metadata ->> 'destination_revision_id' AS destination_revision_id
		FROM ` + stagingTableName + `
		WHERE upload_id = $1
		  AND metadata ->> 'destination_revision_id' <> '';
	`

	rows, err := sf.db.QueryContext(ctx, query, uploadID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("querying destination revision IDs for upload %d: %w", uploadID, err)
	}
	defer func() { _ = rows.Close() }()

	var destinationRevisionIDs []string
	for rows.Next() {
		var revisionID string
		if err := rows.Scan(&revisionID); err != nil {
			return nil, fmt.Errorf("scanning destination revision IDs for upload %d: %w", uploadID, err)
		}
		destinationRevisionIDs = append(destinationRevisionIDs, revisionID)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows for destination revision IDs for upload %d: %w", uploadID, err)
	}

	return destinationRevisionIDs, nil
}

func (sf *StagingFiles) SetStatuses(ctx context.Context, ids []int64, status string) error {
	defer sf.TimerStat("set_statuses", nil)()

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
	result, err := sf.db.ExecContext(ctx, sqlStatement, status, sf.now(), pq.Array(ids))
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
