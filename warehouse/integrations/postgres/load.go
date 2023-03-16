package postgres

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"golang.org/x/exp/slices"
)

// load table transaction stages
const (
	createStagingTable       = "staging_table_creation"
	copyInSchemaStagingTable = "staging_table_copy_in_schema"
	openLoadFiles            = "load_files_opening"
	readGzipLoadFiles        = "load_files_gzip_reading"
	readCsvLoadFiles         = "load_files_csv_reading"
	csvColumnCountMismatch   = "csv_column_count_mismatch"
	loadStagingTable         = "staging_table_loading"
	stagingTableLoadStage    = "staging_table_load_stage"
	deleteDedup              = "dedup_deletion"
	insertDedup              = "dedup_insertion"
	dedupStage               = "dedup_stage"
)

type LoadTable struct {
	Logger             logger.Logger
	DB                 *sql.DB
	Namespace          string
	Warehouse          *model.Warehouse
	Stats              stats.Stats
	Config             *config.Config
	LoadFileDownloader downloader.Downloader
}

type LoadUsersTable struct {
	Logger             logger.Logger
	DB                 *sql.DB
	Namespace          string
	Warehouse          *model.Warehouse
	Stats              stats.Stats
	Config             *config.Config
	LoadFileDownloader downloader.Downloader
}

func (lt *LoadTable) Load(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema) (string, error) {
	var (
		err                     error
		query                   string
		stagingTableName        string
		txn                     *sql.Tx
		result                  sql.Result
		stmt                    *sql.Stmt
		gzFile                  *os.File
		gzReader                *gzip.Reader
		csvRowsProcessedCount   int
		rowsAffected            int64
		stage                   string
		csvReader               *csv.Reader
		loadFiles               []string
		sortedColumnKeys        []string
		diagnostic              Diagnostic
		skipDedupDestinationIDs []string
	)

	skipDedupDestinationIDs = lt.Config.GetStringSlice("Warehouse.postgres.skipDedupDestinationIDs", nil)
	diagnostic = Diagnostic{
		Logger:    lt.Logger,
		Stats:     lt.Stats,
		Config:    lt.Config,
		Namespace: lt.Namespace,
		Warehouse: lt.Warehouse,
	}

	query = fmt.Sprintf(`SET search_path TO %q`, lt.Namespace)
	if _, err = lt.DB.ExecContext(ctx, query); err != nil {
		return "", fmt.Errorf("setting search path: %w", err)
	}

	lt.Logger.Infow("started loading",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.Namespace, lt.Namespace,
		logfield.TableName, tableName,
	)

	loadFiles, err = lt.LoadFileDownloader.Download(ctx, tableName)
	defer misc.RemoveFilePaths(loadFiles...)
	if err != nil {
		return "", fmt.Errorf("downloading load files: %w", err)
	}

	txn, err = lt.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("beginning transaction: %w", err)
	}

	defer func() {
		if err != nil {
			diagnostic.TxnRollback(txn, tableName, stage)
		}
	}()

	// Creating staging table
	sortedColumnKeys = warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
	stagingTableName = warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	query = fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[2]s (LIKE %[1]q.%[3]q)
		ON COMMIT PRESERVE ROWS;
`,
		lt.Namespace,
		stagingTableName,
		tableName,
	)
	lt.Logger.Infow("creating temporary table",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.Namespace, lt.Namespace,
		logfield.TableName, tableName,
		logfield.StagingTableName, stagingTableName,
		logfield.Query, query,
	)

	if _, err = txn.ExecContext(ctx, query); err != nil {
		stage = createStagingTable
		return "", fmt.Errorf("creating temporary table: %w", err)
	}

	stmt, err = txn.Prepare(pq.CopyIn(stagingTableName, sortedColumnKeys...))
	if err != nil {
		stage = copyInSchemaStagingTable
		return "", fmt.Errorf("preparing statement for copy in: %w", err)
	}

	for _, objectFileName := range loadFiles {
		gzFile, err = os.Open(objectFileName)
		if err != nil {
			stage = openLoadFiles
			return "", fmt.Errorf("opening load file: %w", err)
		}

		gzReader, err = gzip.NewReader(gzFile)
		if err != nil {
			_ = gzFile.Close()

			stage = readGzipLoadFiles
			return "", fmt.Errorf("reading gzip load file: %w", err)
		}

		csvReader = csv.NewReader(gzReader)

		for {
			var (
				record          []string
				recordInterface []interface{}
			)

			record, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}

				stage = readCsvLoadFiles
				return "", fmt.Errorf("reading csv file: %w", err)
			}

			if len(sortedColumnKeys) != len(record) {
				stage = csvColumnCountMismatch
				err = fmt.Errorf("number of columns in files: %d, upload schema: %d, processed rows until now: %d", len(record), len(sortedColumnKeys), csvRowsProcessedCount)
				lt.Logger.Warnw("mismatch in number of columns in csv file",
					logfield.SourceID, lt.Warehouse.Source.ID,
					logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
					logfield.DestinationID, lt.Warehouse.Destination.ID,
					logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
					logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
					logfield.Namespace, lt.Namespace,
					logfield.TableName, tableName,
					logfield.StagingTableName, stagingTableName,
					logfield.LoadFile, objectFileName,
					logfield.Error, err.Error(),
				)
				return "", fmt.Errorf("missing columns in csv file: %w", err)
			}

			for _, value := range record {
				if strings.TrimSpace(value) == "" {
					recordInterface = append(recordInterface, nil)
				} else {
					recordInterface = append(recordInterface, value)
				}
			}

			_, err = stmt.ExecContext(ctx, recordInterface...)
			if err != nil {
				stage = loadStagingTable
				return "", fmt.Errorf("exec statement: %w", err)
			}

			csvRowsProcessedCount++
		}

		_ = gzReader.Close()
		_ = gzFile.Close()
	}

	if _, err = stmt.ExecContext(ctx); err != nil {
		stage = stagingTableLoadStage
		return "", fmt.Errorf("exec statement: %w", err)
	}

	var (
		primaryKey   = "id"
		partitionKey = "id"

		additionalJoinClause string
	)
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}
	if tableName == warehouseutils.DiscardsTable {
		additionalJoinClause = fmt.Sprintf(
			`AND _source.%[3]s = %[1]q.%[2]q.%[3]q AND _source.%[4]s = %[1]q.%[2]q.%[4]q`,
			lt.Namespace,
			tableName,
			"table_name",
			"column_name",
		)
	}

	// Deduplication
	// Delete rows from the table which are already present in the staging table
	query = fmt.Sprintf(`
		DELETE FROM
		  %[1]q.%[2]q USING %[3]q AS _source
		WHERE
		  (
			_source.%[4]s = %[1]q.%[2]q.%[4]q %[5]s
		  );
	`,
		lt.Namespace,
		tableName,
		stagingTableName,
		primaryKey,
		additionalJoinClause,
	)

	if !slices.Contains(skipDedupDestinationIDs, lt.Warehouse.Destination.ID) {
		lt.Logger.Infow("deduplication",
			logfield.SourceID, lt.Warehouse.Source.ID,
			logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, lt.Warehouse.Destination.ID,
			logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
			logfield.Namespace, lt.Namespace,
			logfield.TableName, tableName,
			logfield.StagingTableName, stagingTableName,
			logfield.Query, query,
		)

		if result, err = txn.Exec(query); err != nil {
			stage = deleteDedup
			return "", fmt.Errorf("deleting from original table for dedup: %w", err)
		}
		if rowsAffected, err = result.RowsAffected(); err != nil {
			stage = deleteDedup
			return "", fmt.Errorf("getting rows affected for dedup: %w", err)
		}

		lt.Stats.NewTaggedStat("dedup_rows", stats.CountType, stats.Tags{
			"sourceID":     lt.Warehouse.Source.ID,
			"sourceType":   lt.Warehouse.Source.SourceDefinition.Name,
			"destID":       lt.Warehouse.Destination.ID,
			"destType":     lt.Warehouse.Destination.DestinationDefinition.Name,
			"workspaceId":  lt.Warehouse.WorkspaceID,
			"namespace":    lt.Namespace,
			"tableName":    tableName,
			"rowsAffected": fmt.Sprintf("%d", rowsAffected),
		})
	}

	// Insert rows from staging table to the original table
	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(sortedColumnKeys)
	query = fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (%[3]s)
		SELECT
		  %[3]s
		FROM
		  (
			SELECT
			  *,
			  ROW_NUMBER() OVER (
				PARTITION BY %[5]s
				ORDER BY
				  received_at DESC
			  ) AS _rudder_staging_row_number
			FROM
			  %[4]q
		  ) AS _
		WHERE
		  _rudder_staging_row_number = 1;
	`,
		lt.Namespace,
		tableName,
		quotedColumnNames,
		stagingTableName,
		partitionKey,
	)

	lt.Logger.Infow("inserting records",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.Namespace, lt.Namespace,
		logfield.TableName, tableName,
		logfield.StagingTableName, stagingTableName,
		logfield.Query, query,
	)
	err = diagnostic.TxnExecute(ctx, txn, tableName, query)
	if err != nil {
		stage = insertDedup
		return "", fmt.Errorf("inserting into original table: %w", err)
	}
	if err = txn.Commit(); err != nil {
		stage = dedupStage
		return "", fmt.Errorf("commit transaction: %w", err)
	}

	lt.Logger.Infow("completed loading",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.Namespace, lt.Namespace,
		logfield.TableName, tableName,
		logfield.StagingTableName, stagingTableName,
	)
	return stagingTableName, nil
}

func (lut *LoadUsersTable) Load(ctx context.Context, identifiesSchemaInUpload, usersSchemaInUpload, usersSchemaInWarehouse model.TableSchema) map[string]error {
	var (
		err                        error
		query                      string
		identifiesStagingTableName string
		txn                        *sql.Tx
	)

	query = fmt.Sprintf(`SET search_path TO %q`, lut.Namespace)
	if _, err = lut.DB.ExecContext(ctx, query); err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: fmt.Errorf("setting search path: %w", err),
		}
	}

	lut.Logger.Infow("started loading for identifies and users tables",
		logfield.SourceID, lut.Warehouse.Source.ID,
		logfield.SourceType, lut.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lut.Warehouse.Destination.ID,
		logfield.DestinationType, lut.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lut.Warehouse.WorkspaceID,
		logfield.Namespace, lut.Namespace,
	)

	lt := LoadTable{
		Logger:             lut.Logger,
		DB:                 lut.DB,
		Namespace:          lut.Namespace,
		Warehouse:          lut.Warehouse,
		Stats:              lut.Stats,
		Config:             lut.Config,
		LoadFileDownloader: lut.LoadFileDownloader,
	}
	diagnostic := Diagnostic{
		Logger:    lut.Logger,
		Stats:     lut.Stats,
		Config:    lut.Config,
		Namespace: lut.Namespace,
		Warehouse: lut.Warehouse,
	}

	if identifiesStagingTableName, err = lt.Load(ctx, warehouseutils.IdentifiesTable, identifiesSchemaInUpload); err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: fmt.Errorf("loading identifies table: %w", err),
		}
	}

	if len(usersSchemaInUpload) == 0 {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
		}
	}

	var (
		skipComputingUserLatestTraits             = lut.Config.GetBool("Warehouse.postgres.skipComputingUserLatestTraits", false)
		skipComputingUserLatestTraitsWorkspaceIDs = lut.Config.GetStringSlice("Warehouse.postgres.SkipComputingUserLatestTraitsWorkspaceIDs", nil)
		canSkipComputingLatestUserTraits          = skipComputingUserLatestTraits || slices.Contains(skipComputingUserLatestTraitsWorkspaceIDs, lut.Warehouse.WorkspaceID)
	)

	if canSkipComputingLatestUserTraits {
		if _, err = lt.Load(ctx, warehouseutils.UsersTable, usersSchemaInUpload); err != nil {
			return map[string]error{
				warehouseutils.IdentifiesTable: nil,
				warehouseutils.UsersTable:      fmt.Errorf("loading users table: %w", err),
			}
		}
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      nil,
		}
	}

	var (
		primaryKey = "id"
		stage      string

		unionStagingTableName = warehouseutils.StagingTableName(provider, "users_identifies_union", tableNameLimit)
		usersStagingTableName = warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)

		userColNames, firstValProps []string
	)

	txn, err = lut.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("beginning transaction: %w", err),
		}
	}

	defer func() {
		if err != nil {
			diagnostic.TxnRollback(txn, warehouseutils.UsersTable, stage)
		}
	}()

	for colName := range usersSchemaInWarehouse {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, fmt.Sprintf(`%q`, colName))
		caseSubQuery := fmt.Sprintf(`
			CASE WHEN (
			  SELECT
				true
			) THEN (
			  SELECT
				%[1]q
			  FROM
				%[2]q AS staging_table
			  WHERE
				x.id = staging_table.id AND
				%[1]q IS NOT NULL
			  ORDER BY
				received_at DESC
			  LIMIT
				1
			) END AS %[1]q
`,
			colName,
			unionStagingTableName,
		)
		firstValProps = append(firstValProps, caseSubQuery)
	}

	query = fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[5]s AS (
		  (
			SELECT
			  id,
			  %[4]s
			FROM
			  %[1]q.%[2]q
			WHERE
			  id IN (
				SELECT
				  user_id
				FROM
				  %[3]q
				WHERE
				  user_id IS NOT NULL
			  )
		  )
		  UNION
			(
			  SELECT
				user_id,
				%[4]s
			  FROM
				%[3]q
			  WHERE
				user_id IS NOT NULL
			)
		);
`,
		lut.Namespace,
		warehouseutils.UsersTable,
		identifiesStagingTableName,
		strings.Join(userColNames, ","),
		unionStagingTableName,
	)

	lut.Logger.Infow("creating union staging users table",
		logfield.SourceID, lut.Warehouse.Source.ID,
		logfield.SourceType, lut.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lut.Warehouse.Destination.ID,
		logfield.DestinationType, lut.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lut.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, unionStagingTableName,
		logfield.Namespace, lut.Namespace,
		logfield.Query, query,
	)
	if _, err = txn.ExecContext(ctx, query); err != nil {
		stage = createStagingTable
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("creating union staging users table: %w", err),
		}
	}

	query = fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[1]s AS (
		  SELECT
			DISTINCT *
		  FROM
			(
			  SELECT
				x.id,
				%[2]s
			  FROM
				%[3]s AS x
			) AS xyz
		);
`,
		usersStagingTableName,
		strings.Join(firstValProps, ","),
		unionStagingTableName,
	)

	lut.Logger.Debugw("creating temporary users table",
		logfield.SourceID, lut.Warehouse.Source.ID,
		logfield.SourceType, lut.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lut.Warehouse.Destination.ID,
		logfield.DestinationType, lut.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lut.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, usersStagingTableName,
		logfield.Query, query,
	)

	if _, err = txn.ExecContext(ctx, query); err != nil {
		stage = createStagingTable
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("creating temporary users table: %w", err),
		}
	}

	// Deduplication
	// Delete from users table if the id is present in the staging table
	query = fmt.Sprintf(`
		DELETE FROM
		  %[1]q.%[2]q using %[3]q _source
		WHERE
		  (
			_source.%[4]s = %[1]s.%[2]s.%[4]s
		  );
`,
		lut.Namespace,
		warehouseutils.UsersTable,
		usersStagingTableName,
		primaryKey,
	)

	lut.Logger.Infow("deduplication for users table",
		logfield.SourceID, lut.Warehouse.Source.ID,
		logfield.SourceType, lut.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lut.Warehouse.Destination.ID,
		logfield.DestinationType, lut.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lut.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, usersStagingTableName,
		logfield.Namespace, lut.Namespace,
		logfield.Query, query,
	)
	err = diagnostic.TxnExecute(ctx, txn, warehouseutils.UsersTable, query)
	if err != nil {
		stage = deleteDedup
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("deleting from original users table for dedup: %w", err),
		}
	}

	// Insert rows from staging table to users table
	query = fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (%[4]s)
		SELECT
		  %[4]s
		FROM
		  %[3]q;
`,
		lut.Namespace,
		warehouseutils.UsersTable,
		usersStagingTableName,
		strings.Join(append([]string{"id"}, userColNames...), ","),
	)
	lut.Logger.Infow("inserting records to users table",
		logfield.SourceID, lut.Warehouse.Source.ID,
		logfield.SourceType, lut.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lut.Warehouse.Destination.ID,
		logfield.DestinationType, lut.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lut.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, usersStagingTableName,
		logfield.Namespace, lut.Namespace,
		logfield.Query, query,
	)
	err = diagnostic.TxnExecute(ctx, txn, warehouseutils.UsersTable, query)
	if err != nil {
		stage = insertDedup
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("inserting records to users table: %w", err),
		}
	}

	if err = txn.Commit(); err != nil {
		stage = dedupStage
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("commit transaction: %w", err),
		}
	}

	lut.Logger.Infow("completed loading for users and identities table",
		logfield.SourceID, lut.Warehouse.Source.ID,
		logfield.SourceType, lut.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lut.Warehouse.Destination.ID,
		logfield.DestinationType, lut.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lut.Warehouse.WorkspaceID,
		logfield.Namespace, lut.Namespace,
	)
	return map[string]error{
		warehouseutils.IdentifiesTable: nil,
		warehouseutils.UsersTable:      nil,
	}
}
