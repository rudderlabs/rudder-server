package clickhouse

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/rudderlabs/rudder-go-kit/logger"

	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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
