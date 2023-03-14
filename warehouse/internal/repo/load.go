package repo

import (
	"context"
	"database/sql"
	jsonstd "encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	loadTableName    = warehouseutils.WarehouseLoadFilesTable
	loadTableColumns = `
		id,
		staging_file_id,
		location,
		source_id,
		destination_id,
		destination_type,
		table_name,
		total_events,
		metadata
`
)

type LoadFiles repo

func NewLoadFiles(db *sql.DB, opts ...Opt) *LoadFiles {
	r := &LoadFiles{
		db:  db,
		now: time.Now,
	}

	for _, opt := range opts {
		opt((*repo)(r))
	}
	return r
}

// DeleteByStagingFiles deletes load files associated with stagingFileIDs.
func (repo *LoadFiles) DeleteByStagingFiles(ctx context.Context, stagingFileIDs []int64) error {
	sqlStatement := `
		DELETE FROM
		  ` + loadTableName + `
		WHERE
		  staging_file_id = ANY($1);`

	_, err := repo.db.ExecContext(ctx, sqlStatement, pq.Array(stagingFileIDs))
	if err != nil {
		return fmt.Errorf(`deleting load files: %w`, err)
	}

	return nil
}

// Insert loadFiles into the database.
func (repo *LoadFiles) Insert(ctx context.Context, loadFiles []model.LoadFile) (err error) {
	// Using transactions for bulk copying
	txn, err := repo.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return
	}

	stmt, err := txn.PrepareContext(ctx, pq.CopyIn("wh_load_files", "staging_file_id", "location", "source_id", "destination_id", "destination_type", "table_name", "total_events", "created_at", "metadata"))
	if err != nil {
		return fmt.Errorf(`inserting load files: CopyIn: %w`, err)
	}
	defer func() { _ = stmt.Close() }()

	for _, loadFile := range loadFiles {
		metadata := fmt.Sprintf(`{"content_length": %d, "destination_revision_id": %q, "use_rudder_storage": %t}`, loadFile.ContentLength, loadFile.DestinationRevisionID, loadFile.UseRudderStorage)
		_, err = stmt.ExecContext(ctx, loadFile.StagingFileID, loadFile.Location, loadFile.SourceID, loadFile.DestinationID, loadFile.DestinationType, loadFile.TableName, loadFile.TotalRows, timeutil.Now(), metadata)
		if err != nil {
			_ = txn.Rollback()
			return fmt.Errorf(`inserting load files: CopyIn exec: %w`, err)
		}
	}

	_, err = stmt.ExecContext(ctx)
	if err != nil {
		_ = txn.Rollback()
		return fmt.Errorf(`inserting load files: CopyIn final exec: %w`, err)
	}
	err = txn.Commit()
	if err != nil {
		return fmt.Errorf(`inserting load files: commit: %w`, err)
	}
	return
}

// GetByStagingFiles returns all load files matching the staging file ids.
//
//	Ordered by id ascending.
func (repo *LoadFiles) GetByStagingFiles(ctx context.Context, stagingFileIDs []int64) ([]model.LoadFile, error) {
	sqlStatement := `
		WITH row_numbered_load_files as (
		SELECT
			` + loadTableColumns + `,
			row_number() OVER (
			PARTITION BY staging_file_id,
			table_name
			ORDER BY
				id DESC
			) AS row_number
		FROM
			` + loadTableName + `
		WHERE
			staging_file_id = ANY($1)
		)
		SELECT
		` + loadTableColumns + `
		FROM
		row_numbered_load_files
		WHERE
		row_number = 1
		ORDER BY id ASC
	`

	rows, err := repo.db.QueryContext(ctx, sqlStatement, pq.Array(stagingFileIDs))
	if err != nil {
		return nil, fmt.Errorf("query staging ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type metadataSchema struct {
		DestinationRevisionID string `json:"destination_revision_id"`
		ContentLength         int64  `json:"content_length"`
		UseRudderStorage      bool   `json:"use_rudder_storage"`
	}

	var loadFiles []model.LoadFile
	for rows.Next() {
		var loadFile model.LoadFile

		var metadataRaw jsonstd.RawMessage
		err := rows.Scan(
			&loadFile.ID,
			&loadFile.StagingFileID,
			&loadFile.Location,
			&loadFile.SourceID,
			&loadFile.DestinationID,
			&loadFile.DestinationType,
			&loadFile.TableName,
			&loadFile.TotalRows,
			&metadataRaw,
		)
		if err != nil {
			return nil, fmt.Errorf(`scanning load files: %w`, err)
		}

		var metadata metadataSchema
		if err := json.Unmarshal(metadataRaw, &metadata); err != nil {
			return nil, fmt.Errorf(`un-marshalling load files metadata: %w`, err)
		}

		loadFile.ContentLength = metadata.ContentLength
		loadFile.DestinationRevisionID = metadata.DestinationRevisionID
		loadFile.UseRudderStorage = metadata.UseRudderStorage

		loadFiles = append(loadFiles, loadFile)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("querying load files: %w", err)
	}

	return loadFiles, nil
}
