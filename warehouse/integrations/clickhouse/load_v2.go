package clickhouse

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sort"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/misc"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	errObjectStorageNotSupported = errors.New("objectStorage not supported for loading using S3 engine")
	errCSVColumnsMismatch        = errors.New("csv columns mismatch")
)

func (ch *ClickhouseV2) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	var (
		preLoadTableCount int64
		err               error
	)
	if !ch.config.disableLoadTableStats(ch.Warehouse.WorkspaceID) {
		preLoadTableCount, err = ch.totalCountIntable(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("counting rows before load: %w", err)
		}
	}

	err = ch.loadTable(ctx, tableName, ch.Uploader.GetTableSchemaInUpload(tableName))
	if err != nil {
		return nil, fmt.Errorf("loading table: %w", err)
	}

	var postLoadTableCount int64
	if !ch.config.disableLoadTableStats(ch.Warehouse.WorkspaceID) {
		postLoadTableCount, err = ch.totalCountIntable(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("counting rows after load: %w", err)
		}
	}

	return &types.LoadTableStats{
		RowsInserted: postLoadTableCount - preLoadTableCount,
	}, nil
}

func (ch *ClickhouseV2) totalCountIntable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM "%[1]s"."%[2]s";
	`,
		ch.Namespace,
		tableName,
	)
	err = ch.DB.QueryRowContext(ctx, sqlStatement).Scan(&total)
	return total, err
}

func (ch *ClickhouseV2) LoadUserTables(ctx context.Context) (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	err := ch.loadTable(ctx, warehouseutils.IdentifiesTable, ch.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable))
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return errorMap
	}

	if len(ch.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return errorMap
	}
	errorMap[warehouseutils.UsersTable] = nil
	err = ch.loadTable(ctx, warehouseutils.UsersTable, ch.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable))
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return errorMap
	}
	return errorMap
}

func (ch *ClickhouseV2) loadTable(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema) (err error) {
	if delay := ch.config.randomLoadDelay(ch.Warehouse.WorkspaceID); delay > 0 {
		if err = misc.SleepCtx(ctx, delay); err != nil {
			return err
		}
	}
	if ch.UseS3CopyEngineForLoading() {
		return ch.loadByCopyCommand(ctx, tableName, tableSchemaInUpload)
	}
	return ch.loadByDownloadingLoadFiles(ctx, tableName, tableSchemaInUpload)
}

func (ch *ClickhouseV2) UseS3CopyEngineForLoading() bool {
	if !slices.Contains(ch.config.s3EngineEnabledWorkspaceIDs, ch.Warehouse.WorkspaceID) {
		return false
	}
	return ch.ObjectStorage == warehouseutils.S3 || ch.ObjectStorage == warehouseutils.MINIO
}

func (ch *ClickhouseV2) loadByCopyCommand(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema) error {
	log := ch.logger.Withn(
		logger.NewStringField(logfield.SourceID, ch.Warehouse.Source.ID),
		logger.NewStringField(logfield.SourceType, ch.Warehouse.Source.SourceDefinition.Name),
		logger.NewStringField(logfield.DestinationID, ch.Warehouse.Destination.ID),
		logger.NewStringField(logfield.DestinationType, ch.Warehouse.Destination.DestinationDefinition.Name),
		logger.NewStringField(logfield.WorkspaceID, ch.Warehouse.WorkspaceID),
		logger.NewStringField(logfield.Namespace, ch.Namespace),
		logger.NewStringField(logfield.TableName, tableName),
	)
	log.Infon("Starting load by copy command")

	strKeys := warehouseutils.GetColumnsFromTableSchema(tableSchemaInUpload)
	sort.Strings(strKeys)
	sortedColumnNames := strings.Join(strKeys, ",")
	sortedColumnNamesWithDataTypes := warehouseutils.JoinWithFormatting(strKeys, func(idx int, name string) string {
		return fmt.Sprintf(`%s %s`, name, rudderDataTypesMapToClickHouse[tableSchemaInUpload[name]])
	}, ",")

	csvObjectLocation, err := ch.Uploader.GetSampleLoadFileLocation(ctx, tableName)
	if err != nil {
		return fmt.Errorf("getting sample load file location: %w", err)
	}
	loadFolderDir, _ := path.Split(csvObjectLocation)
	loadFolder := loadFolderDir + "*.csv.gz"

	accessKeyID, secretAccessKey, err := ch.credentials()
	if err != nil {
		return fmt.Errorf("getting auth credentials: %w", err)
	}

	sqlStatement := fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (
			%[3]s
		)
		SELECT
		  *
		FROM
		  s3(
			'%[4]s',
		  	'%[5]s',
		  	'%[6]s',
			'CSV',
			'%[7]s',
			'gz'
		  )
			settings
				date_time_input_format = 'best_effort',
				input_format_csv_arrays_as_nested_csv = 1;
		`,
		ch.Namespace,                   // 1
		tableName,                      // 2
		sortedColumnNames,              // 3
		loadFolder,                     // 4
		accessKeyID,                    // 5
		secretAccessKey,                // 6
		sortedColumnNamesWithDataTypes, // 7
	)
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
	if err != nil {
		return fmt.Errorf("executing insert query: %w", err)
	}

	log.Infon("Completed load by copy command")
	return nil
}

func (ch *ClickhouseV2) loadByDownloadingLoadFiles(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema) error {
	log := ch.logger.Withn(
		logger.NewStringField(logfield.SourceID, ch.Warehouse.Source.ID),
		logger.NewStringField(logfield.SourceType, ch.Warehouse.Source.SourceDefinition.Name),
		logger.NewStringField(logfield.DestinationID, ch.Warehouse.Destination.ID),
		logger.NewStringField(logfield.DestinationType, ch.Warehouse.Destination.DestinationDefinition.Name),
		logger.NewStringField(logfield.WorkspaceID, ch.Warehouse.WorkspaceID),
		logger.NewStringField(logfield.Namespace, ch.Namespace),
		logger.NewStringField(logfield.TableName, tableName),
	)
	log.Infon("Starting load by downloading load files")

	fileNames, err := ch.LoadFileDownloader.Download(ctx, tableName)
	if err != nil {
		return fmt.Errorf("downloading load files: %w", err)
	}
	defer func() {
		misc.RemoveFilePaths(fileNames...)
	}()

	if err := ch.loadTableFromFiles(ctx, log, tableName, tableSchemaInUpload, fileNames); err != nil {
		return fmt.Errorf("loading table from files: %w", err)
	}
	log.Infon("Completed load by downloading load files")
	return nil
}

func (ch *ClickhouseV2) credentials() (accessKeyID, secretAccessKey string, err error) {
	if ch.ObjectStorage == warehouseutils.S3 {
		return ch.Warehouse.GetStringDestinationConfig(ch.conf, model.AWSAccessSecretSetting), ch.Warehouse.GetStringDestinationConfig(ch.conf, model.AWSAccessKeySetting), nil
	}
	if ch.ObjectStorage == warehouseutils.MINIO {
		return ch.Warehouse.GetStringDestinationConfig(ch.conf, model.MinioAccessKeyIDSetting), ch.Warehouse.GetStringDestinationConfig(ch.conf, model.MinioSecretAccessKeySetting), nil
	}
	return "", "", errObjectStorageNotSupported
}

func (ch *ClickhouseV2) TestLoadTable(ctx context.Context, _, tableName string, payloadMap map[string]any, _ string) error {
	// One pass over the map. Collecting keys and values separately means two
	// iterations, and Go randomises map order, so a value can end up paired with
	// the wrong column.
	columns := make([]string, 0, len(payloadMap))
	values := make([]any, 0, len(payloadMap))
	for column, value := range payloadMap {
		columns = append(columns, column)
		values = append(values, value)
	}

	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`,
		ch.Namespace,
		tableName,
		strings.Join(columns, ","),
		generateArgumentString(len(columns)),
	)
	txn, err := ch.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
		}
	}()

	stmt, err := txn.PrepareContext(ctx, sqlStatement)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	if _, err = stmt.ExecContext(ctx, values...); err != nil {
		return fmt.Errorf("executing statement: %w", err)
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

// loadTableFromFiles inserts every row of every load file into tableName, in
// blocks of commitEvery rows.
//
// The load files are read as one stream rather than one at a time: they are
// headerless CSV in separate gzip members, so concatenating them and running a
// single gzip reader over the lot yields one row sequence for the table. Blocks
// therefore span load files instead of ending early at each one, which is what
// keeps the number of inserts — and so the number of parts ClickHouse has to
// merge — proportional to the rows loaded rather than to the file count.
//
// ExecContext only appends to a client-side batch and the single wire write
// happens on commit, so commitEvery is what bounds how much of the upload is
// held in memory at once.
func (ch *ClickhouseV2) loadTableFromFiles(
	ctx context.Context,
	log logger.Logger,
	tableName string,
	tableSchemaInUpload model.TableSchema,
	fileNames []string,
) error {
	if len(fileNames) == 0 {
		return nil
	}

	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
	insertSQL := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`,
		ch.Namespace,
		tableName,
		warehouseutils.DoubleQuoteAndJoinByComma(sortedColumnKeys),
		generateArgumentString(len(sortedColumnKeys)),
	)

	files := make([]*os.File, 0, len(fileNames))
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()

	readers := make([]io.Reader, 0, len(fileNames))
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			return fmt.Errorf("opening load file %s: %w", fileName, err)
		}
		files = append(files, file)
		readers = append(readers, file)
	}

	gzipReader, err := gzip.NewReader(io.MultiReader(readers...))
	if err != nil {
		return fmt.Errorf("creating gzip reader: %w", err)
	}
	defer func() { _ = gzipReader.Close() }()

	var rows int
	csvReader := csv.NewReader(gzipReader)
	for {
		inserted, err := ch.insertBlock(ctx, insertSQL, csvReader, sortedColumnKeys, tableSchemaInUpload)
		if err != nil {
			return fmt.Errorf("inserting block after %d rows: %w", rows, err)
		}

		rows += inserted
		// A short block means insertBlock hit EOF, so the stream is done. Only a
		// row count that is an exact multiple of commitEvery costs one more call.
		if inserted < ch.config.commitEvery {
			break
		}
	}

	log.Debugn("Load files processed",
		logger.NewIntField("files", int64(len(fileNames))),
		logger.NewIntField("rows", int64(rows)),
	)
	return nil
}

// insertBlock reads up to commitEvery rows from csvReader and sends them as one
// batch — one transaction per block, not per row. It reports how many rows it
// sent, and zero means the stream is exhausted.
//
// A return below commitEvery means EOF was reached, which is how the caller
// knows to stop. When the row count is an exact multiple of commitEvery the
// caller makes one more call that reads EOF and commits nothing: that costs a
// prepare and a query close but no data frame, since batch.Send skips the send
// entirely for a zero-row block.
//
// "Transaction" is the driver's word, not ClickHouse's: BeginTx sends nothing
// and only health-checks the connection, Commit is batch.Send, and Rollback
// discards an unsent batch and closes the connection. Preparing a batch and
// sending it is the only thing the transaction is for. A block is therefore
// atomic only until it is sent, and a failure part-way through a load leaves
// the blocks before it already in the table.
func (ch *ClickhouseV2) insertBlock(
	ctx context.Context,
	insertSQL string,
	csvReader *csv.Reader,
	columnKeys []string,
	schema model.TableSchema,
) (int, error) {
	var inserted int

	err := ch.DB.WithTx(ctx, func(txn *sqlmw.Tx) error {
		// A *sql.Stmt belongs to its transaction, and sending a batch is what
		// ends one, so every block prepares the same SQL again on a fresh
		// transaction.
		stmt, err := txn.PrepareContext(ctx, insertSQL)
		if err != nil {
			return fmt.Errorf("preparing statement %s: %w", insertSQL, err)
		}
		defer func() { _ = stmt.Close() }()

		for inserted < ch.config.commitEvery {
			record, err := csvReader.Read()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("reading csv: %w", err)
			}
			if len(columnKeys) != len(record) {
				return fmt.Errorf("%w: columns in row: %d, columns in upload schema: %d",
					errCSVColumnsMismatch, len(record), len(columnKeys),
				)
			}

			values := make([]any, 0, len(record))
			for index, value := range record {
				values = append(values, ch.bindValue(value, schema[columnKeys[index]]))
			}
			if _, err := stmt.ExecContext(ctx, values...); err != nil {
				return fmt.Errorf("executing statement: %w", err)
			}
			inserted++
		}
		return nil
	})
	return inserted, err
}
