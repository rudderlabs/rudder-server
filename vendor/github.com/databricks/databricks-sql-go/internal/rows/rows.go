package rows

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlclient "github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerr_int "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows/arrowbased"
	"github.com/databricks/databricks-sql-go/internal/rows/columnbased"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
)

// rows implements the following interfaces from database.sql.driver
// Rows
// RowsColumnTypeScanType
// RowsColumnTypeDatabaseTypeName
// RowsColumnTypeNullable
// RowsColumnTypeLength
type rows struct {
	// The RowScanner is responsible for handling the different
	// formats in which the query results can be returned
	rowscanner.RowScanner

	// Handle for the associated database operation.
	opHandle *cli_service.TOperationHandle

	client cli_service.TCLIService

	// Maximum number of rows to return from a single fetch operation
	maxPageSize int64

	location *time.Location

	// Metadata for result set
	resultSetMetadata *cli_service.TGetResultSetMetadataResp
	schema            *cli_service.TTableSchema

	hasMoreRows bool

	config *config.Config

	// connId and correlationId are used for creating a context
	// when accessing the server and when logging
	connId        string
	correlationId string

	// Index in the current page of rows
	nextRowIndex int64

	// Row number within the overall result set
	nextRowNumber int64

	// starting row number of the current results page
	pageStartingRowNum int64

	// If the server returns an entire result set
	// in the direct results it may have already
	// closed the operation.
	closedOnServer bool

	logger_ *dbsqllog.DBSQLLogger

	ctx context.Context
}

var _ driver.Rows = (*rows)(nil)
var _ driver.RowsColumnTypeScanType = (*rows)(nil)
var _ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
var _ driver.RowsColumnTypeNullable = (*rows)(nil)
var _ driver.RowsColumnTypeLength = (*rows)(nil)

func NewRows(
	connId string,
	correlationId string,
	opHandle *cli_service.TOperationHandle,
	client cli_service.TCLIService,
	config *config.Config,
	directResults *cli_service.TSparkDirectResults,
) (driver.Rows, dbsqlerr.DBError) {

	var logger *dbsqllog.DBSQLLogger
	var ctx context.Context
	if opHandle != nil {
		logger = dbsqllog.WithContext(connId, correlationId, dbsqlclient.SprintGuid(opHandle.OperationId.GUID))
		ctx = driverctx.NewContextWithQueryId(driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), connId), correlationId), dbsqlclient.SprintGuid(opHandle.OperationId.GUID))
	} else {
		logger = dbsqllog.WithContext(connId, correlationId, "")
		ctx = driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), connId), correlationId)
	}

	if client == nil {
		logger.Error().Msg(errRowsNoClient)
		return nil, dbsqlerr_int.NewDriverError(ctx, errRowsNoClient, nil)
	}

	var pageSize int64 = 10000
	var location *time.Location = time.UTC
	if config != nil {
		pageSize = int64(config.MaxRows)

		if config.Location != nil {
			location = config.Location
		}
	}

	logger.Debug().Msgf("databricks: creating Rows, pageSize: %d, location: %v", pageSize, location)

	r := &rows{
		client:        client,
		opHandle:      opHandle,
		connId:        connId,
		correlationId: correlationId,
		maxPageSize:   pageSize,
		location:      location,
		config:        config,
		hasMoreRows:   true,
		logger_:       logger,
		ctx:           ctx,
	}

	// if we already have results for the query do some additional initialization
	if directResults != nil {
		logger.Debug().Msgf("databricks: creating Rows with direct results")
		// set the result set metadata
		if directResults.ResultSetMetadata != nil {
			r.resultSetMetadata = directResults.ResultSetMetadata
			r.schema = directResults.ResultSetMetadata.Schema
		}

		// If the entire query result set fits in direct results the server closes
		// the operations.
		if directResults.CloseOperation != nil {
			logger.Debug().Msgf("databricks: creating Rows with server operation closed")
			r.closedOnServer = true
		}

		// initialize the row scanner
		err := r.makeRowScanner(directResults.ResultSet)
		if err != nil {
			return r, err
		}
	}

	return r, nil
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	err := isValidRows(r)
	if err != nil {
		return []string{}
	}

	schema, err := r.getResultSetSchema()
	if err != nil {
		return []string{}
	}

	tColumns := schema.GetColumns()
	colNames := make([]string, len(tColumns))

	for i := range tColumns {
		colNames[i] = tColumns[i].ColumnName
	}

	return colNames
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	if r == nil {
		return nil
	}

	if r.RowScanner != nil {
		// make sure the row scanner frees up any resources
		r.RowScanner.Close()
	}

	if !r.closedOnServer {
		r.logger().Debug().Msgf("databricks: closing Rows operation")

		// if the operation hasn't already been closed on the server we
		// need to do that now
		r.closedOnServer = true

		req := cli_service.TCloseOperationReq{
			OperationHandle: r.opHandle,
		}

		_, err1 := r.client.CloseOperation(r.ctx, &req)
		if err1 != nil {
			r.logger().Err(err1).Msg(errRowsCloseFailed)
			return dbsqlerr_int.NewRequestError(r.ctx, errRowsCloseFailed, err1)
		}
	}

	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the number of columns.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *rows) Next(dest []driver.Value) error {
	err := isValidRows(r)
	if err != nil {
		return err
	}

	// if the next row is not in the current result page
	// fetch the containing page
	var b bool
	var e error
	if b, e = r.isNextRowInPage(); !b && e == nil {
		err := r.fetchResultPage()
		if err != nil {
			return err
		}
	}

	if e != nil {
		return e
	}

	// Put values into the destination slice
	err = r.ScanRow(dest, r.nextRowIndex)
	if err != nil {
		return err
	}

	r.nextRowIndex++
	r.nextRowNumber++

	return nil
}

// ColumnTypeScanType returns column's native type
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	err := isValidRows(r)
	if err != nil {
		return nil
	}

	column, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		return nil
	}

	scanType := getScanType(column)
	return scanType
}

// ColumnTypeDatabaseTypeName returns column's database type name
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	err := isValidRows(r)
	if err != nil {
		return ""
	}

	column, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		return ""
	}

	dbtype := rowscanner.GetDBTypeName(column)

	return dbtype
}

// ColumnTypeNullable returns a flag indicating whether the column is nullable
// and an ok value of true if the status of the column is known. Otherwise
// a value of false is returned for ok.
func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return false, false
}

func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	columnInfo, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		return 0, false
	}

	typeName := rowscanner.GetDBTypeID(columnInfo)

	switch typeName {
	case cli_service.TTypeId_STRING_TYPE,
		cli_service.TTypeId_VARCHAR_TYPE,
		cli_service.TTypeId_BINARY_TYPE,
		cli_service.TTypeId_ARRAY_TYPE,
		cli_service.TTypeId_MAP_TYPE,
		cli_service.TTypeId_STRUCT_TYPE:
		return math.MaxInt64, true
	default:
		return 0, false
	}
}

var (
	scanTypeNull     = reflect.TypeOf(nil)
	scanTypeBoolean  = reflect.TypeOf(true)
	scanTypeFloat32  = reflect.TypeOf(float32(0))
	scanTypeFloat64  = reflect.TypeOf(float64(0))
	scanTypeInt8     = reflect.TypeOf(int8(0))
	scanTypeInt16    = reflect.TypeOf(int16(0))
	scanTypeInt32    = reflect.TypeOf(int32(0))
	scanTypeInt64    = reflect.TypeOf(int64(0))
	scanTypeString   = reflect.TypeOf("")
	scanTypeDateTime = reflect.TypeOf(time.Time{})
	scanTypeRawBytes = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown  = reflect.TypeOf(new(any))
)

func getScanType(column *cli_service.TColumnDesc) reflect.Type {

	// Currently all types are returned from the thrift server using
	// the primitive entry
	entry := column.TypeDesc.Types[0].PrimitiveEntry

	switch entry.Type {
	case cli_service.TTypeId_BOOLEAN_TYPE:
		return scanTypeBoolean
	case cli_service.TTypeId_TINYINT_TYPE:
		return scanTypeInt8
	case cli_service.TTypeId_SMALLINT_TYPE:
		return scanTypeInt16
	case cli_service.TTypeId_INT_TYPE:
		return scanTypeInt32
	case cli_service.TTypeId_BIGINT_TYPE:
		return scanTypeInt64
	case cli_service.TTypeId_FLOAT_TYPE:
		return scanTypeFloat32
	case cli_service.TTypeId_DOUBLE_TYPE:
		return scanTypeFloat64
	case cli_service.TTypeId_NULL_TYPE:
		return scanTypeNull
	case cli_service.TTypeId_STRING_TYPE:
		return scanTypeString
	case cli_service.TTypeId_CHAR_TYPE:
		return scanTypeString
	case cli_service.TTypeId_VARCHAR_TYPE:
		return scanTypeString
	case cli_service.TTypeId_DATE_TYPE, cli_service.TTypeId_TIMESTAMP_TYPE:
		return scanTypeDateTime
	case cli_service.TTypeId_DECIMAL_TYPE, cli_service.TTypeId_BINARY_TYPE, cli_service.TTypeId_ARRAY_TYPE,
		cli_service.TTypeId_STRUCT_TYPE, cli_service.TTypeId_MAP_TYPE, cli_service.TTypeId_UNION_TYPE:
		return scanTypeRawBytes
	case cli_service.TTypeId_USER_DEFINED_TYPE:
		return scanTypeUnknown
	case cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE, cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE:
		return scanTypeString
	default:
		return scanTypeUnknown
	}
}

// isValidRows checks that the row instance is not nil
// and that it has a client
func isValidRows(r *rows) dbsqlerr.DBError {
	var err dbsqlerr.DBError
	if r == nil {
		err = dbsqlerr_int.NewDriverError(context.Background(), errRowsNilRows, nil)
	} else if r.client == nil {
		err = dbsqlerr_int.NewDriverError(r.ctx, errRowsNoClient, nil)
		r.logger().Err(err).Msg(errRowsNoClient)
	}

	return err
}

func (r *rows) getColumnMetadataByIndex(index int) (*cli_service.TColumnDesc, dbsqlerr.DBError) {
	err := isValidRows(r)
	if err != nil {
		return nil, err
	}

	schema, err := r.getResultSetSchema()
	if err != nil {
		return nil, err
	}

	columns := schema.GetColumns()
	if index < 0 || index >= len(columns) {
		err = dbsqlerr_int.NewDriverError(r.ctx, errRowsInvalidColumnIndex(index), nil)
		r.logger().Err(err).Msg(err.Error())
		return nil, err
	}

	return columns[index], nil
}

// isNextRowInPage returns a boolean flag indicating whether
// the next result set row is in the current result set page
func (r *rows) isNextRowInPage() (bool, dbsqlerr.DBError) {
	if r == nil || r.RowScanner == nil {
		return false, nil
	}

	nRowsInPage := r.NRows()
	if nRowsInPage == 0 {
		return false, nil
	}

	startRowOffset := r.pageStartingRowNum
	return r.nextRowNumber >= startRowOffset && r.nextRowNumber < (startRowOffset+nRowsInPage), nil
}

// getResultMetadata does a one time fetch of the result set schema
func (r *rows) getResultSetSchema() (*cli_service.TTableSchema, dbsqlerr.DBError) {
	if r.schema == nil {
		err := isValidRows(r)
		if err != nil {
			return nil, err
		}

		req := cli_service.TGetResultSetMetadataReq{
			OperationHandle: r.opHandle,
		}
		ctx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), r.connId), r.correlationId)

		resp, err2 := r.client.GetResultSetMetadata(ctx, &req)
		if err2 != nil {
			r.logger().Err(err2).Msg(err2.Error())
			return nil, dbsqlerr_int.NewRequestError(r.ctx, errRowsMetadataFetchFailed, err)
		}

		r.resultSetMetadata = resp
		r.schema = resp.Schema

	}

	return r.schema, nil
}

// fetchResultPage will fetch the result page containing the next row, if necessary
func (r *rows) fetchResultPage() error {
	var err dbsqlerr.DBError = isValidRows(r)
	if err != nil {
		return err
	}

	r.logger().Debug().Msgf("databricks: fetching result page for row %d", r.nextRowNumber)

	var b bool
	var e dbsqlerr.DBError
	for b, e = r.isNextRowInPage(); !b && e == nil; b, e = r.isNextRowInPage() {

		// determine the direction of page fetching. Currently we only handle
		// TFetchOrientation_FETCH_PRIOR and TFetchOrientation_FETCH_NEXT
		var direction cli_service.TFetchOrientation = r.getPageFetchDirection()
		if direction == cli_service.TFetchOrientation_FETCH_PRIOR {
			// can't fetch rows previous to the start
			if r.pageStartingRowNum == 0 {
				return dbsqlerr_int.NewDriverError(r.ctx, errRowsFetchPriorToStart, nil)
			}
		} else if direction == cli_service.TFetchOrientation_FETCH_NEXT {
			// can't fetch past the end of the query results
			if !r.hasMoreRows {
				return io.EOF
			}
		} else {
			r.logger().Error().Msgf(errRowsUnandledFetchDirection(direction.String()))
			return dbsqlerr_int.NewDriverError(r.ctx, errRowsUnandledFetchDirection(direction.String()), nil)
		}

		r.logger().Debug().Msgf("fetching next batch of up to %d rows, %s", r.maxPageSize, direction.String())

		var includeResultSetMetadata = true
		req := cli_service.TFetchResultsReq{
			OperationHandle:          r.opHandle,
			MaxRows:                  r.maxPageSize,
			Orientation:              direction,
			IncludeResultSetMetadata: &includeResultSetMetadata,
		}
		ctx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), r.connId), r.correlationId)

		fetchResult, err := r.client.FetchResults(ctx, &req)
		if err != nil {
			r.logger().Err(err).Msg("databricks: Rows instance failed to retrieve results")
			return dbsqlerr_int.NewRequestError(r.ctx, errRowsResultFetchFailed, err)
		}

		err1 := r.makeRowScanner(fetchResult)
		if err1 != nil {
			return err1
		}

		r.logger().Debug().Msgf("databricks: new result page startRow: %d, nRows: %v, hasMoreRows: %v", fetchResult.Results.StartRowOffset, r.NRows(), fetchResult.HasMoreRows)
	}

	if e != nil {
		return e
	}

	// don't assume the next row is the first row in the page
	r.nextRowIndex = r.nextRowNumber - r.pageStartingRowNum

	return nil
}

// getPageFetchDirection returns the cli_service.TFetchOrientation
// necessary to fetch a result page containing the next row number.
// Note: if the next row number is in the current page TFetchOrientation_FETCH_NEXT
// is returned. Use rows.nextRowInPage to determine if a fetch is necessary
func (r *rows) getPageFetchDirection() cli_service.TFetchOrientation {
	if r == nil {
		return cli_service.TFetchOrientation_FETCH_NEXT
	}

	if r.nextRowNumber < r.pageStartingRowNum {
		return cli_service.TFetchOrientation_FETCH_PRIOR
	}

	return cli_service.TFetchOrientation_FETCH_NEXT
}

// makeRowScanner creates the embedded RowScanner instance based on the format
// of the returned query results
func (r *rows) makeRowScanner(fetchResults *cli_service.TFetchResultsResp) dbsqlerr.DBError {

	schema, err1 := r.getResultSetSchema()
	if err1 != nil {
		return err1
	}

	if fetchResults == nil {
		return nil
	}

	var rs rowscanner.RowScanner
	var err dbsqlerr.DBError
	if fetchResults.Results != nil {
		if fetchResults.Results.Columns != nil {
			rs, err = columnbased.NewColumnRowScanner(schema, fetchResults.Results, r.config, r.logger(), r.ctx)
		} else if fetchResults.Results.ArrowBatches != nil {
			rs, err = arrowbased.NewArrowRowScanner(r.resultSetMetadata, fetchResults.Results, r.config, r.logger(), r.ctx)
		} else if fetchResults.Results.ResultLinks != nil {
			rs, err = arrowbased.NewArrowRowScanner(r.resultSetMetadata, fetchResults.Results, r.config, r.logger(), r.ctx)
		} else {
			r.logger().Error().Msg(errRowsUnknowRowType)
			err = dbsqlerr_int.NewDriverError(r.ctx, errRowsUnknowRowType, nil)
		}

		r.pageStartingRowNum = fetchResults.Results.StartRowOffset
	} else {
		r.logger().Error().Msg(errRowsUnknowRowType)
		err = dbsqlerr_int.NewDriverError(r.ctx, errRowsUnknowRowType, nil)
	}

	r.RowScanner = rs
	if fetchResults.HasMoreRows != nil {
		r.hasMoreRows = *fetchResults.HasMoreRows
	} else {
		r.hasMoreRows = false
	}

	return err
}

func (r *rows) logger() *dbsqllog.DBSQLLogger {
	if r.logger_ == nil {
		if r.opHandle != nil {
			r.logger_ = dbsqllog.WithContext(r.connId, r.correlationId, dbsqlclient.SprintGuid(r.opHandle.OperationId.GUID))
		} else {
			r.logger_ = dbsqllog.WithContext(r.connId, r.correlationId, "")
		}
	}
	return r.logger_
}
