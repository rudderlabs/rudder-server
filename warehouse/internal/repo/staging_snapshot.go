// Package repo provides repository implementations for warehouse entities.
package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	stagingFileSchemaSnapshotTableName    = whutils.WarehouseStagingFileSchemaSnapshotTable
	stagingFileSchemaSnapshotTableColumns = `id, schema, source_id, destination_id, workspace_id, created_at`
)

var ErrNoSchemaSnapshot = errors.New("no schema snapshot found")

type StagingFileSchemaSnapshots repo

// NewStagingFileSchemaSnapshots creates a new StagingFileSchemaSnapshots using the given DB connection.
func NewStagingFileSchemaSnapshots(db *sqlmiddleware.DB, opts ...Opt) *StagingFileSchemaSnapshots {
	r := &StagingFileSchemaSnapshots{
		db:           db,
		now:          timeutil.Now,
		statsFactory: stats.NOP,
		repoType:     stagingFileSchemaSnapshotTableName,
	}
	for _, opt := range opts {
		opt((*repo)(r))
	}
	return r
}

// Insert inserts a new schema snapshot into the database and returns its auto-generated ID.
func (r *StagingFileSchemaSnapshots) Insert(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
	defer (*repo)(r).TimerStat("insert", stats.Tags{
		"destId":      destinationID,
		"workspaceId": workspaceID,
	})()

	query := `
		INSERT INTO ` + stagingFileSchemaSnapshotTableName + ` (
			id, schema, source_id, destination_id, workspace_id, created_at
		) VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id;
	`
	id := uuid.New()
	now := r.now()
	_, err := r.db.ExecContext(ctx, query,
		id,
		schemaBytes,
		sourceID,
		destinationID,
		workspaceID,
		now,
	)
	if err != nil {
		return uuid.Nil, fmt.Errorf("inserting schema snapshot: %w", err)
	}
	return id, nil
}

// GetLatest returns the most recent schema snapshot for the given source and destination.
// Returns ErrNoSchemaSnapshot if not found.
func (r *StagingFileSchemaSnapshots) GetLatest(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error) {
	defer (*repo)(r).TimerStat("get_latest", stats.Tags{
		"destId": destinationID,
	})()

	query := `
		SELECT ` + stagingFileSchemaSnapshotTableColumns + `
		FROM ` + stagingFileSchemaSnapshotTableName + `
		WHERE source_id = $1 AND destination_id = $2
		ORDER BY created_at DESC
		LIMIT 1;
	`
	row := r.db.QueryRowContext(ctx, query, sourceID, destinationID)
	var snapshot model.StagingFileSchemaSnapshot
	var schemaRaw []byte
	if err := row.Scan(&snapshot.ID, &schemaRaw, &snapshot.SourceID, &snapshot.DestinationID, &snapshot.WorkspaceID, &snapshot.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoSchemaSnapshot
		}
		return nil, fmt.Errorf("scanning latest schema snapshot by source/dest: %w", err)
	}
	snapshot.Schema = schemaRaw
	snapshot.CreatedAt = snapshot.CreatedAt.UTC()
	return &snapshot, nil
}
