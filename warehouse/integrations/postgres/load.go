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
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/utils/load_file_downloader"
	"golang.org/x/exp/slices"
)

// load table transaction stages
const (
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
	Warehouse          *warehouseutils.Warehouse
	Stats              stats.Stats
	config             *config.Config
	LoadFileDownloader load_file_downloader.LoadFileDownloader
}

type LoadUsersTable struct {
	Logger             logger.Logger
	DB                 *sql.DB
	Namespace          string
	Warehouse          *warehouseutils.Warehouse
	Stats              stats.Stats
	config             *config.Config
	LoadFileDownloader load_file_downloader.LoadFileDownloader
}

func (lt *LoadTable) Load(ctx context.Context, tableName string, tableSchemaInUpload warehouseutils.TableSchemaT) error {
	var (
		err                   error
		sqlStatement          string
		stagingTableName      string
		txn                   *sql.Tx
		stmt                  *sql.Stmt
		gzFile                *os.File
		gzReader              *gzip.Reader
		csvRowsProcessedCount int
		stage                 string
		csvReader             *csv.Reader
	)

	sqlStatement = fmt.Sprintf(`SET search_path to %q`, lt.Namespace)
	if _, err = lt.DB.Exec(sqlStatement); err != nil {
		return fmt.Errorf("setting search path: %w", err)
	}

	lt.Logger.Infow("started loading",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.TableName, tableName,
	)

	fileNames, err := lt.LoadFileDownloader.Download(ctx, tableName)
	defer misc.RemoveFilePaths(fileNames...)

	if err != nil {
		return fmt.Errorf("downloading files: %w", err)
	}

	// Creating staging table
	stagingTableName = warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	sqlStatement = fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[2]s (LIKE "%[1]s"."%[3]s");
`,
		lt.Namespace,
		stagingTableName,
		tableName,
	)
	lt.Logger.Debugw("creating temporary table",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.TableName, tableName,
		logfield.StagingTableName, stagingTableName,
		logfield.Query, sqlStatement,
	)

	if _, err = lt.DB.Exec(sqlStatement); err != nil {
		return fmt.Errorf("creating temporary table: %w", err)
	}

	txn, err = lt.DB.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	defer func() {
		if err != nil {
			lt.onTxnRollback(txn, tableName, stage)
		}
	}()

	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
	stmt, err = txn.Prepare(pq.CopyIn(stagingTableName, sortedColumnKeys...))
	if err != nil {
		stage = copyInSchemaStagingTable
		return fmt.Errorf("preparing statement for copy in: %w", err)
	}

	for _, objectFileName := range fileNames {
		gzFile, err = os.Open(objectFileName)
		if err != nil {
			stage = openLoadFiles
			return fmt.Errorf("opening load file: %w", err)
		}

		gzReader, err = gzip.NewReader(gzFile)
		if err != nil {
			_ = gzFile.Close()

			stage = readGzipLoadFiles
			return fmt.Errorf("reading gzip load file: %w", err)
		}

		csvReader = csv.NewReader(gzReader)

		for {
			var (
				record          []string
				recordInterface []interface{}
			)

			record, err = csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}

				stage = readCsvLoadFiles
				return fmt.Errorf("reading csv file: %w", err)
			}

			if len(sortedColumnKeys) != len(record) {
				stage = csvColumnCountMismatch
				return fmt.Errorf(
					"mismatch in number of columns in csv file: %d, number of columns in upload schema: %d, processed rows until now: %d",
					len(record),
					len(sortedColumnKeys),
					csvRowsProcessedCount,
				)
			}

			for _, value := range record {
				if strings.TrimSpace(value) == "" {
					recordInterface = append(recordInterface, nil)
				} else {
					recordInterface = append(recordInterface, value)
				}
			}

			_, err = stmt.Exec(recordInterface...)
			if err != nil {
				stage = loadStagingTable
				return fmt.Errorf("exec statement: %w", err)
			}

			csvRowsProcessedCount++
		}

		_ = gzReader.Close()
		_ = gzFile.Close()
	}

	if _, err = stmt.Exec(); err != nil {
		stage = stagingTableLoadStage
		return fmt.Errorf("exec statement: %w", err)
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
			`AND _source.%[3]s = "%[1]s"."%[2]s"."%[3]s" AND _source.%[4]s = "%[1]s"."%[2]s"."%[4]s"`,
			lt.Namespace,
			tableName,
			"table_name",
			"column_name",
		)
	}

	// Deduplication
	// Delete rows from the table which are already present in the staging table
	sqlStatement = fmt.Sprintf(`
		DELETE FROM
		  "%[1]s"."%[2]s" USING "%[3]s" AS _source
		WHERE
		  (
			_source.%[4]s = "%[1]s"."%[2]s"."%[4]s" %[5]s
		  );
	`,
		lt.Namespace,
		tableName,
		stagingTableName,
		primaryKey,
		additionalJoinClause,
	)

	lt.Logger.Infow("deduplication",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.TableName, tableName,
		logfield.StagingTableName, stagingTableName,
		logfield.Query, sqlStatement,
	)
	err = lt.handleExec(txn, tableName, sqlStatement)
	if err != nil {
		stage = deleteDedup
		return fmt.Errorf("deleting from original table for dedup: %w", err)
	}

	// Deduplication
	// Insert rows from staging table to the original table
	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(sortedColumnKeys)
	sqlStatement = fmt.Sprintf(`
		INSERT INTO "%[1]s"."%[2]s" (%[3]s)
		SELECT
		  %[3]s
		FROM
		  (
			SELECT
			  *,
			  row_number() OVER (
				PARTITION BY %[5]s
				ORDER BY
				  received_at DESC
			  ) AS _rudder_staging_row_number
			FROM
			  "%[4]s"
		  ) AS _
		where
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
		logfield.TableName, tableName,
		logfield.StagingTableName, stagingTableName,
		logfield.Query, sqlStatement,
	)
	err = lt.handleExec(txn, tableName, sqlStatement)
	if err != nil {
		stage = insertDedup
		return fmt.Errorf("inserting into original table: %w", err)
	}
	if err = txn.Commit(); err != nil {
		stage = dedupStage
		return fmt.Errorf("commit transaction: %w", err)
	}

	lt.Logger.Infow("completed loading",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.TableName, tableName,
	)
	return nil
}

func (lt *LoadTable) onTxnRollback(txn *sql.Tx, tableName, stage string) {
	c := make(chan struct{})
	go func() {
		defer close(c)

		if err := txn.Rollback(); err != nil {
			lt.Logger.Errorf("PG: Error while rolling back transaction: %v", err)
		}
	}()

	txnRollbackTimeout := lt.config.GetDuration("Warehouse.postgres.txnRollbackTimeout", 30, time.Second)

	select {
	case <-c:
	case <-time.After(txnRollbackTimeout):
		tags := stats.Tags{
			"workspaceId":   lt.Warehouse.WorkspaceID,
			"namespace":     lt.Namespace,
			"destinationID": lt.Warehouse.Destination.ID,
			"tableName":     tableName,
			"stage":         stage,
		}
		lt.Stats.NewTaggedStat("pg_rollback_timeout", stats.CountType, tags).Count(1)
	}
}

// handleExec
// Print execution plan if enableWithQueryPlan is set to true else return result set.
// Currently, these statements are supported by EXPLAIN
// Any INSERT, UPDATE, DELETE whose execution plan you wish to see.
func (lt *LoadTable) handleExec(txn *sql.Tx, tableName, query string) error {
	enableSQLStatementExecutionPlanWorkspaceIDs := lt.config.GetStringSlice("Warehouse.postgres.EnableSQLStatementExecutionPlanWorkspaceIDs", nil)
	enableSQLStatementExecutionPlan := lt.config.GetBool("Warehouse.postgres.enableSQLStatementExecutionPlan", false)

	canUseQueryPlanner := enableSQLStatementExecutionPlan || slices.Contains(enableSQLStatementExecutionPlanWorkspaceIDs, lt.Warehouse.WorkspaceID)
	if !canUseQueryPlanner {
		_, err := txn.Exec(query)
		return fmt.Errorf("executing query: %w", err)
	}

	var (
		err      error
		rows     *sql.Rows
		response []string
		s        string
	)

	explainQuery := fmt.Sprintf("EXPLAIN %s", query)
	if rows, err = txn.Query(explainQuery); err != nil {
		return fmt.Errorf("executing explain query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		if err = rows.Scan(&s); err != nil {
			return fmt.Errorf("scanning explain query: %w", err)
		}

		response = append(response, s)
	}
	lt.Logger.Infow("execution query plan",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.TableName, tableName,
		logfield.Query, explainQuery,
		logfield.QueryPlanner, strings.Join(response, "\n"),
	)

	_, err = txn.Exec(query)
	return fmt.Errorf("executing query: %w", err)
}

func (lut *LoadUsersTable) Load(ctx context.Context, identifiesSchema, usersSchema warehouseutils.TableSchemaT) map[string]error {
	var (
		err          error
		sqlStatement string
		txn          *sql.Tx
	)

	sqlStatement = fmt.Sprintf(`SET search_path to %q`, lut.Namespace)
	if _, err = lut.DB.Exec(sqlStatement); err != nil {
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
	)

	lt := LoadTable{
		Logger:             lut.Logger,
		DB:                 lut.DB,
		Namespace:          lut.Namespace,
		Warehouse:          lut.Warehouse,
		Stats:              lut.Stats,
		config:             lut.config,
		LoadFileDownloader: lut.LoadFileDownloader,
	}

	if err = lt.Load(ctx, warehouseutils.IdentifiesTable, identifiesSchema); err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: fmt.Errorf("loading identifies table: %w", err),
		}
	}

	if len(usersSchema) == 0 {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
		}
	}

	skipComputingUserLatestTraits := lt.config.GetBool("Warehouse.postgres.skipComputingUserLatestTraits", false)
	skipComputingUserLatestTraitsWorkspaceIDs := lt.config.GetStringSlice("Warehouse.postgres.SkipComputingUserLatestTraitsWorkspaceIDs", nil)
	canSkipComputingLatestUserTraits := skipComputingUserLatestTraits || slices.Contains(skipComputingUserLatestTraitsWorkspaceIDs, lut.Warehouse.WorkspaceID)

	if canSkipComputingLatestUserTraits {
		if err = lt.Load(ctx, warehouseutils.UsersTable, usersSchema); err != nil {
			return map[string]error{
				warehouseutils.IdentifiesTable: nil,
				warehouseutils.UsersTable:      fmt.Errorf("loading users table: %w", err),
			}
		}
	}

	var (
		unionStagingTableName      = warehouseutils.StagingTableName(provider, "users_identifies_union", tableNameLimit)
		identifiesStagingTableName = warehouseutils.StagingTableName(provider, warehouseutils.IdentifiesTable, tableNameLimit)
		usersStagingTableName      = warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)

		userColNames, firstValProps []string
	)

	for colName := range usersSchema {
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
				"%[1]s"
			  FROM
				"%[2]s" AS staging_table
			  WHERE
				x.id = staging_table.id AND
				"%[1]s" IS NOT NULL
			  ORDER BY
				received_at DESC
			  LIMIT
				1
			) END AS "%[1]s"
`,
			colName,
			unionStagingTableName,
		)
		firstValProps = append(firstValProps, caseSubQuery)
	}

	sqlStatement = fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[5]s as (
		  (
			SELECT
			  id,
			  %[4]s
			FROM
			  "%[1]s"."%[2]s"
			WHERE
			  id in (
				SELECT
				  user_id
				FROM
				  "%[3]s"
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
				"%[3]s"
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

	lt.Logger.Infow("creating union staging users table",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, unionStagingTableName,
		logfield.Query, sqlStatement,
	)
	if _, err = lut.DB.Exec(sqlStatement); err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("creating union staging users table: %w", err),
		}
	}

	sqlStatement = fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[1]s AS (
		  SELECT
			DISTINCT *
		  FROM
			(
			  SELECT
				x.id,
				%[2]s
			  FROM
				%[3]s as x
			) as xyz
		);
`,
		usersStagingTableName,
		strings.Join(firstValProps, ","),
		unionStagingTableName,
	)

	lt.Logger.Debugw("creating temporary users table",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, usersStagingTableName,
		logfield.Query, sqlStatement,
	)

	if _, err = lut.DB.Exec(sqlStatement); err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("creating temporary users table: %w", err),
		}
	}

	var (
		primaryKey = "id"
		stage      string
	)

	txn, err = lut.DB.Begin()
	if err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("beginning transaction: %w", err),
		}
	}

	defer func() {
		if err != nil {
			lt.onTxnRollback(txn, warehouseutils.UsersTable, stage)
		}
	}()

	// Deduplication
	// Delete from users table if the id is present in the staging table
	sqlStatement = fmt.Sprintf(`
		DELETE FROM
		  "%[1]s"."%[2]s" using "%[3]s" _source
		where
		  (
			_source.%[4]s = %[1]s.%[2]s.%[4]s
		  );
`,
		lut.Namespace,
		warehouseutils.UsersTable,
		usersStagingTableName,
		primaryKey,
	)
	lt.Logger.Infow("deduplication for users table",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, usersStagingTableName,
		logfield.Query, sqlStatement,
	)
	err = lt.handleExec(txn, warehouseutils.UsersTable, sqlStatement)
	if err != nil {
		stage = deleteDedup
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("deleting from original users table for dedup: %w", err),
		}
	}

	// Deduplication
	// Insert rows from staging table to users table
	sqlStatement = fmt.Sprintf(`
		INSERT INTO "%[1]s"."%[2]s" (%[4]s)
		SELECT
		  %[4]s
		FROM
		  "%[3]s";
`,
		lut.Namespace,
		warehouseutils.UsersTable,
		usersStagingTableName,
		strings.Join(append([]string{"id"}, userColNames...), ","),
	)
	lt.Logger.Infow("inserting records to users table",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, usersStagingTableName,
		logfield.Query, sqlStatement,
	)
	err = lt.handleExec(txn, warehouseutils.UsersTable, sqlStatement)
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

	lt.Logger.Infow("completed loading for users and identities table",
		logfield.SourceID, lt.Warehouse.Source.ID,
		logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, lt.Warehouse.Destination.ID,
		logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
	)
	return map[string]error{
		warehouseutils.IdentifiesTable: nil,
		warehouseutils.UsersTable:      nil,
	}
}
