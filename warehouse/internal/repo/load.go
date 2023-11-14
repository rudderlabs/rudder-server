package repo

import (
	"context"
	"database/sql"
	jsonstd "encoding/json"
	"errors"
	"fmt"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
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
		metadata,
		created_at
`
)

type LoadFiles repo

func NewLoadFiles(db *sqlmiddleware.DB, opts ...Opt) *LoadFiles {
	r := &LoadFiles{
		db:  db,
		now: timeutil.Now,
	}

	for _, opt := range opts {
		opt((*repo)(r))
	}
	return r
}

// DeleteByStagingFiles deletes load files associated with stagingFileIDs.
func (lf *LoadFiles) DeleteByStagingFiles(ctx context.Context, stagingFileIDs []int64) error {
	sqlStatement := `
		DELETE FROM
		  ` + loadTableName + `
		WHERE
		  staging_file_id = ANY($1);`

	_, err := lf.db.ExecContext(ctx, sqlStatement, pq.Array(stagingFileIDs))
	if err != nil {
		return fmt.Errorf(`deleting load files: %w`, err)
	}

	return nil
}

// Insert loadFiles into the database.
func (lf *LoadFiles) Insert(ctx context.Context, loadFiles []model.LoadFile) error {
	return (*repo)(lf).WithTx(ctx, func(tx *sqlmiddleware.Tx) error {
		stmt, err := tx.PrepareContext(
			ctx,
			pq.CopyIn(
				"wh_load_files",
				"staging_file_id",
				"location",
				"source_id",
				"destination_id",
				"destination_type",
				"table_name",
				"total_events",
				"created_at",
				"metadata",
			),
		)
		if err != nil {
			return fmt.Errorf(`inserting load files: CopyIn: %w`, err)
		}
		defer func() { _ = stmt.Close() }()

		for _, loadFile := range loadFiles {
			metadata := fmt.Sprintf(`{"content_length": %d, "destination_revision_id": %q, "use_rudder_storage": %t}`, loadFile.ContentLength, loadFile.DestinationRevisionID, loadFile.UseRudderStorage)
			_, err = stmt.ExecContext(ctx, loadFile.StagingFileID, loadFile.Location, loadFile.SourceID, loadFile.DestinationID, loadFile.DestinationType, loadFile.TableName, loadFile.TotalRows, lf.now(), metadata)
			if err != nil {
				return fmt.Errorf(`inserting load files: CopyIn exec: %w`, err)
			}
		}
		_, err = stmt.ExecContext(ctx)
		if err != nil {
			return fmt.Errorf(`inserting load files: CopyIn final exec: %w`, err)
		}
		return nil
	})
}

// GetByStagingFiles returns all load files matching the staging file ids.
//
//	Ordered by id ascending.
func (lf *LoadFiles) GetByStagingFiles(ctx context.Context, stagingFileIDs []int64) ([]model.LoadFile, error) {
	sqlStatement := `
		WITH row_numbered_load_files AS (
		SELECT
			` + loadTableColumns + `,
			row_number() OVER (
				PARTITION BY
					staging_file_id,
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
		ORDER BY
			id ASC;
	`

	rows, err := lf.db.QueryContext(ctx, sqlStatement, pq.Array(stagingFileIDs))
	if err != nil {
		return nil, fmt.Errorf("query staging ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	loadFiles, err := scanLoadFiles(rows)
	if err != nil {
		return nil, fmt.Errorf("scanning load files: %w", err)
	}
	return loadFiles, nil
}

func scanLoadFiles(rows *sqlmiddleware.Rows) ([]model.LoadFile, error) {
	var loadFiles []model.LoadFile
	for rows.Next() {
		var loadFile model.LoadFile
		err := scanLoadFile(rows.Scan, &loadFile)
		if err != nil {
			return nil, fmt.Errorf("scanning load file: %w", err)
		}
		loadFiles = append(loadFiles, loadFile)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return loadFiles, nil
}

func scanLoadFile(scan scanFn, loadFile *model.LoadFile) error {
	type metadataSchema struct {
		DestinationRevisionID string `json:"destination_revision_id"`
		ContentLength         int64  `json:"content_length"`
		UseRudderStorage      bool   `json:"use_rudder_storage"`
	}

	var metadataRaw jsonstd.RawMessage

	err := scan(
		&loadFile.ID,
		&loadFile.StagingFileID,
		&loadFile.Location,
		&loadFile.SourceID,
		&loadFile.DestinationID,
		&loadFile.DestinationType,
		&loadFile.TableName,
		&loadFile.TotalRows,
		&metadataRaw,
		&loadFile.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf(`scanning row: %w`, err)
	}

	var metadata metadataSchema
	if err := json.Unmarshal(metadataRaw, &metadata); err != nil {
		return fmt.Errorf(`un-marshalling load file metadata: %w`, err)
	}

	loadFile.ContentLength = metadata.ContentLength
	loadFile.DestinationRevisionID = metadata.DestinationRevisionID
	loadFile.UseRudderStorage = metadata.UseRudderStorage
	loadFile.CreatedAt = loadFile.CreatedAt.UTC()

	return nil
}

// GetByID returns the load file matching the id.
func (lf *LoadFiles) GetByID(ctx context.Context, id int64) (*model.LoadFile, error) {
	row := lf.db.QueryRowContext(ctx, `
		SELECT
		`+loadTableColumns+`
		FROM
			`+loadTableName+`
		WHERE
			id = $1;
	`,
		id,
	)
	var loadFile model.LoadFile
	err := scanLoadFile(row.Scan, &loadFile)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, model.ErrLoadFileNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scanning load file: %w", err)
	}
	return &loadFile, nil
}

// TotalExportedEvents returns the total number of events exported by the corresponding staging files.
// It excludes the tables present in skipTables.
func (lf *LoadFiles) TotalExportedEvents(
	ctx context.Context,
	stagingFileIDs []int64,
	skipTables []string,
) (int64, error) {
	var (
		count sql.NullInt64
		err   error
	)

	if skipTables == nil {
		skipTables = []string{}
	}

	sqlStatement := `
		WITH row_numbered_load_files AS (
		SELECT
			total_events,
			table_name,
			row_number() OVER (
				PARTITION BY
					staging_file_id,
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
			COALESCE(sum(total_events), 0) AS total_events
		FROM
			row_numbered_load_files
		WHERE
			row_number = 1
		AND
			table_name != ALL($2);`

	err = lf.db.QueryRowContext(ctx, sqlStatement, pq.Array(stagingFileIDs), pq.Array(skipTables)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf(`counting total exported events: %w`, err)
	}
	if !count.Valid {
		return 0, errors.New(`count is not valid`)
	}
	return count.Int64, nil
}

// DistinctTableName returns the distinct table names for the given parameters.
func (lf *LoadFiles) DistinctTableName(
	ctx context.Context,
	sourceID string,
	destinationID string,
	startID int64,
	endID int64,
) ([]string, error) {
	rows, err := lf.db.QueryContext(ctx, `
		SELECT
		  distinct table_name
		FROM
		  `+loadTableName+`
		WHERE
			source_id = $1
			AND destination_id = $2
			AND id >= $3
			AND id <= $4;`,
		sourceID,
		destinationID,
		startID,
		endID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying load files: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tableNames []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf(`scanning table names: %w`, err)
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("querying table names: %w", err)
	}
	return tableNames, nil
}
