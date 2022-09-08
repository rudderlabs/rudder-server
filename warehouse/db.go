package warehouse

import (
	"context"
	"database/sql"
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// DB encapsulate interactions of warehouse operations
// with the database.
type DB struct {
	handle *sql.DB
}

func NewWarehouseDB(handle *sql.DB) *DB {
	return &DB{handle}
}

func (db *DB) GetLatestUploadStatus(ctx context.Context, destType, sourceID, destinationID string) (int64, string, int, error) {
	pkgLogger.Debugf("Fetching latest upload status for: destType: %s, sourceID: %s, destID: %s", destType, sourceID, destinationID)

	query := fmt.Sprintf(`
		SELECT
			id,
			status,
			COALESCE(metadata->>'priority', '100')::int
		FROM %[1]s
		WHERE
				%[1]s.destination_type='%[2]s' AND
				%[1]s.source_id='%[3]s' AND
				%[1]s.destination_id='%[4]s'
		ORDER BY id DESC LIMIT 1`, warehouseutils.WarehouseUploadsTable,
		destType, sourceID, destinationID)

	var (
		uploadID int64
		status   string
		priority int
	)

	err := db.handle.QueryRowContext(ctx, query).Scan(&uploadID, &status, &priority)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf(`Error getting latest upload status for warehouse: %v`, err)
		return 0, "", 0, fmt.Errorf("unable to get latest upload status for warehouse: %w", err)
	}

	return uploadID, status, priority, nil
}

func (db *DB) RetryUploads(ctx context.Context, clausesQuery string, clausesArgs []interface{}) (rowsAffected int64, err error) {
	// Preparing the prepared statement
	preparedStatement := fmt.Sprintf(`
		UPDATE wh_uploads
		SET
			metadata = metadata || '{"retried": true, "priority": 50}' || jsonb_build_object('nextRetryTime', NOW() - INTERVAL '1 HOUR'),
			status = 'waiting',
			updated_at = NOW()
		WHERE %[1]s`,
		clausesQuery,
	)
	pkgLogger.Debugf("[RetryUploads] sqlStatement: %s", preparedStatement)

	// Executing the statement
	result, err := db.handle.ExecContext(ctx, preparedStatement, clausesArgs...)
	if err != nil {
		return
	}

	// Getting rows affected
	rowsAffected, err = result.RowsAffected()
	return
}

func (db *DB) CountUploadsToRetry(ctx context.Context, clausesQuery string, clausesArgs []interface{}) (count int64, err error) {
	// Preparing the prepared statement
	preparedStatement := fmt.Sprintf(`
		SELECT
		  count(*)
		FROM
		  wh_uploads
		WHERE %[1]s`,
		clausesQuery,
	)
	pkgLogger.Debugf("[CountUploadsToRetry] sqlStatement: %s", preparedStatement)

	// Executing the statement
	err = db.handle.QueryRowContext(ctx, preparedStatement, clausesArgs...).Scan(&count)
	return
}
