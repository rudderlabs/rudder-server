//go:generate mockgen -source=db.go -destination=../../mocks/warehouse/mock_db.go -package=mock_warehouse github.com/rudderlabs/rudder-server/warehouse/db DB

package db

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
type DB interface {
	GetLatestUploadStatus(ctx context.Context, destType, sourceID, destinationID string) (int64, string, int, error)
	RetryUploads(ctx context.Context, filterClauses ...warehouseutils.FilterClause) (rowsAffected int64, err error)
	GetUploadsCount(ctx context.Context, filterClauses ...warehouseutils.FilterClause) (count int64, err error)
	SetUploadColumns(uploadID int64, opts warehouseutils.UploadColumnsOpts) (err error)
}

type DBImpl struct {
	handle *sql.DB
}

func NewWarehouseDB(handle *sql.DB) DB {
	return &DBImpl{handle}
}

func (db *DBImpl) GetLatestUploadStatus(ctx context.Context, destType, sourceID, destinationID string) (int64, string, int, error) {
	//warehouse.pkgLogger.Debugf("Fetching latest upload status for: destType: %s, sourceID: %s, destID: %s", destType, sourceID, destinationID)

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
		//warehouse.pkgLogger.Errorf(`Error getting latest upload status for warehouse: %v`, err)
		return 0, "", 0, fmt.Errorf("unable to get latest upload status for warehouse: %w", err)
	}

	return uploadID, status, priority, nil
}

func (db *DBImpl) RetryUploads(ctx context.Context, filterClauses ...warehouseutils.FilterClause) (rowsAffected int64, err error) {
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
	//warehouse.pkgLogger.Debugf("[RetryUploads] sqlStatement: %s", preparedStatement)

	result, err := db.handle.ExecContext(ctx, preparedStatement, clausesArgs...)
	if err != nil {
		return
	}

	rowsAffected, err = result.RowsAffected()
	return
}

func (db *DBImpl) GetUploadsCount(ctx context.Context, filterClauses ...warehouseutils.FilterClause) (count int64, err error) {
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
	//warehouse.pkgLogger.Debugf("[GetUploadsCount] sqlStatement: %s", preparedStatement)

	err = db.handle.QueryRowContext(ctx, preparedStatement, clausesArgs...).Scan(&count)
	return
}

func (db *DBImpl) SetUploadColumns(uploadID int64, opts warehouseutils.UploadColumnsOpts) (err error) {
	var columns string
	values := []interface{}{uploadID}
	// setting values using syntax $n since Exec can correctly format time.Time strings
	for idx, f := range opts.Fields {
		// start with $2 as $1 is upload.ID
		columns += fmt.Sprintf(`%s=$%d`, f.Column, idx+2)
		if idx < len(opts.Fields)-1 {
			columns += ","
		}
		values = append(values, f.Value)
	}
	sqlStatement := fmt.Sprintf(`
		UPDATE
		  %s
		SET
		  %s
		WHERE
		  id = $1;
`,
		warehouseutils.WarehouseUploadsTable,
		columns,
	)
	if opts.Txn != nil {
		_, err = opts.Txn.Exec(sqlStatement, values...)
	} else {
		_, err = db.handle.Exec(sqlStatement, values...)
	}

	return err
}

func ClauseQueryArgs(filterClauses ...warehouseutils.FilterClause) (string, []interface{}) {
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
