package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
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

type LoadFiles struct {
	DB  *sql.DB
	Now func() time.Time

	once sync.Once
}

func (repo *LoadFiles) init() {
	repo.once.Do(func() {
		if repo.Now == nil {
			repo.Now = timeutil.Now
		}
	})
}

func (repo *LoadFiles) DeleteByStaging(ctx context.Context, stagingFileIDs []int64) error {
	repo.init()

	sqlStatement := `
		DELETE FROM
		  ` + loadTableName + `
		WHERE
		  staging_file_id = ANY($1);`

	_, err := repo.DB.ExecContext(ctx, sqlStatement, pq.Array(stagingFileIDs))
	if err != nil {
		return fmt.Errorf(`deleting load files: %w`, err)
	}

	return nil
}

func (repo *LoadFiles) Insert(ctx context.Context, loadFiles []model.LoadFile) (err error) {
	repo.init()

	// Using transactions for bulk copying
	txn, err := repo.DB.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn("wh_load_files", "staging_file_id", "location", "source_id", "destination_id", "destination_type", "table_name", "total_events", "created_at", "metadata"))
	if err != nil {
		return fmt.Errorf(`inserting load files: CopyIn: %w`, err)
	}
	defer stmt.Close()

	for _, loadFile := range loadFiles {
		metadata := fmt.Sprintf(`{"content_length": %d, "destination_revision_id": %q, "use_rudder_storage": %t}`, loadFile.ContentLength, loadFile.DestinationRevisionID, loadFile.UseRudderStorage)
		_, err = stmt.Exec(loadFile.StagingFileID, loadFile.Location, loadFile.SourceID, loadFile.DestinationID, loadFile.DestinationType, loadFile.TableName, loadFile.TotalRows, timeutil.Now(), metadata)
		if err != nil {
			_ = txn.Rollback()
			return fmt.Errorf(`inserting load files: CopyIn exec: %w`, err)
		}
	}

	_, err = stmt.Exec()
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

func (repo *LoadFiles) GetByStagingIDs(ctx context.Context, stagingFileIDs []int64) ([]model.LoadFile, error) {
	repo.init()

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
		row_number = 1;
	`

	rows, err := repo.DB.Query(sqlStatement, pq.Array(stagingFileIDs))
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

		var metadataRaw json.RawMessage
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

		// loadFile.ContentLength, loadFile.DestinationRevisionID, loadFile.UseRudderStorage
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("querying load files: %w", err)
	}

	return loadFiles, nil
}
