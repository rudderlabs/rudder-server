package warehouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	queryPlaceHolder = "<noop>"
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
		  COALESCE(metadata ->> 'priority', '100'):: int 
		FROM 
		  %[1]s UT
		WHERE 
		  UT.destination_type = '%[2]s' 
		  AND UT.source_id = '%[3]s' 
		  AND UT.destination_id = '%[4]s' 
		ORDER BY 
		  id DESC 
		LIMIT 
		  1;
`,
		warehouseutils.WarehouseUploadsTable,
		destType,
		sourceID,
		destinationID,
	)

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

func (db *DB) RetryUploads(ctx context.Context, filterClauses ...FilterClause) (rowsAffected int64, err error) {
	if len(filterClauses) == 0 {
		return 0, errors.New("no filter clauses are present to retry uploads")
	}

	var (
		clausesQuery      string
		clausesArgs       []interface{}
		preparedStatement string
	)

	clausesQuery, clausesArgs = ClauseQueryArgs(filterClauses...)
	preparedStatement = fmt.Sprintf(`
		UPDATE wh_uploads
		SET
			metadata = metadata || '{"retried": true, "priority": 50}' || jsonb_build_object('nextRetryTime', NOW() - INTERVAL '1 HOUR'),
			status = 'waiting',
			updated_at = NOW()
		WHERE %[1]s;`,
		clausesQuery,
	)
	pkgLogger.Debugf("[RetryUploads] sqlStatement: %s", preparedStatement)

	result, err := db.handle.ExecContext(ctx, preparedStatement, clausesArgs...)
	if err != nil {
		return
	}

	rowsAffected, err = result.RowsAffected()
	return
}

func (db *DB) GetUploadsCount(ctx context.Context, filterClauses ...FilterClause) (count int64, err error) {
	var (
		clausesQuery      string
		clausesArgs       []interface{}
		whereClausesQuery string
		preparedStatement string
	)

	clausesQuery, clausesArgs = ClauseQueryArgs(filterClauses...)
	if len(clausesArgs) > 0 {
		whereClausesQuery = fmt.Sprintf(`
			WHERE %[1]s`,
			clausesQuery,
		)
	}

	preparedStatement = fmt.Sprintf(`
		SELECT
		  count(*)
		FROM
		  wh_uploads
		%[1]s;`,
		whereClausesQuery,
	)
	pkgLogger.Debugf("[GetUploadsCount] sqlStatement: %s", preparedStatement)

	err = db.handle.QueryRowContext(ctx, preparedStatement, clausesArgs...).Scan(&count)
	return
}

func ClauseQueryArgs(filterClauses ...FilterClause) (string, []interface{}) {
	var (
		clausesQuery string
		clausesArgs  []interface{}
	)
	for i, fc := range filterClauses {
		clausesArgs = append(clausesArgs, fc.ClauseArg)
		clausesQuery = clausesQuery + strings.Replace(fc.Clause, queryPlaceHolder, fmt.Sprintf("$%d", i+1), 1)
		if i != len(filterClauses)-1 {
			clausesQuery += " AND "
		}
	}
	return clausesQuery, clausesArgs
}
