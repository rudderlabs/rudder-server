package postgres

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/safeguard"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type loadUsersTableResponse struct {
	identifiesError error
	usersError      error
}

func (pg *Postgres) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	var loadTableStats *types.LoadTableStats
	cancel := safeguard.MustStop(ctx, 5*time.Minute)
	defer cancel()

	err := pg.DB.WithTx(ctx, func(tx *sqlmiddleware.Tx) error {
		var err error
		loadTableStats, _, err = pg.loadTable(
			ctx,
			tx,
			tableName,
			pg.Uploader.GetTableSchemaInUpload(tableName),
		)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("loading table: %w", err)
	}

	return loadTableStats, err
}

func (pg *Postgres) loadTable(
	ctx context.Context,
	txn *sqlmiddleware.Tx,
	tableName string,
	tableSchemaInUpload model.TableSchema,
) (*types.LoadTableStats, string, error) {
	log := pg.logger.With(
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.Namespace, pg.Namespace,
		logfield.TableName, tableName,
		logfield.ShouldMerge, pg.shouldMerge(tableName),
	)
	log.Infow("started loading")
	defer log.Infow("completed loading")

	log.Debugw("setting search path")
	searchPathStmt := fmt.Sprintf(`SET search_path TO %q;`,
		pg.Namespace,
	)
	if _, err := txn.ExecContext(ctx, searchPathStmt); err != nil {
		return nil, "", fmt.Errorf("setting search path: %w", err)
	}

	loadFiles, err := pg.LoadFileDownloader.Download(ctx, tableName)
	if err != nil {
		return nil, "", fmt.Errorf("downloading load files: %w", err)
	}
	defer func() {
		misc.RemoveFilePaths(loadFiles...)
	}()

	stagingTableName := warehouseutils.StagingTableName(
		provider,
		tableName,
		tableNameLimit,
	)

	log.Debugw("creating staging table")
	createStagingTableStmt := fmt.Sprintf(
		`CREATE TEMPORARY TABLE %[2]s (LIKE %[1]q.%[3]q)
		ON COMMIT PRESERVE ROWS;`,
		pg.Namespace,
		stagingTableName,
		tableName,
	)
	if _, err := txn.ExecContext(ctx, createStagingTableStmt); err != nil {
		return nil, "", fmt.Errorf("creating temporary table: %w", err)
	}

	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(
		tableSchemaInUpload,
	)

	log.Debugw("creating prepared stmt for loading data")
	copyInStmt := pq.CopyIn(stagingTableName, sortedColumnKeys...)
	stmt, err := txn.PrepareContext(ctx, copyInStmt)
	if err != nil {
		return nil, "", fmt.Errorf("preparing statement for copy in: %w", err)
	}

	log.Infow("loading data into staging table")
	for _, fileName := range loadFiles {
		err = pg.loadDataIntoStagingTable(
			ctx, stmt,
			fileName, sortedColumnKeys,
		)
		if err != nil {
			return nil, "", fmt.Errorf("loading data into staging table: %w", err)
		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return nil, "", fmt.Errorf("executing copyIn statement: %w", err)
	}

	var rowsDeleted int64
	if pg.shouldMerge(tableName) {
		log.Infow("deleting from load table")
		rowsDeleted, err = pg.deleteFromLoadTable(
			ctx, txn, tableName,
			stagingTableName,
		)
		if err != nil {
			return nil, "", fmt.Errorf("delete from load table: %w", err)
		}
	}

	log.Infow("inserting into load table")
	rowsInserted, err := pg.insertIntoLoadTable(
		ctx, txn, tableName,
		stagingTableName, sortedColumnKeys,
	)
	if err != nil {
		return nil, "", fmt.Errorf("insert into: %w", err)
	}

	return &types.LoadTableStats{
		RowsInserted: rowsInserted - rowsDeleted,
		RowsUpdated:  rowsDeleted,
	}, stagingTableName, nil
}

func (pg *Postgres) loadDataIntoStagingTable(
	ctx context.Context,
	stmt *sql.Stmt,
	fileName string,
	sortedColumnKeys []string,
) error {
	gzipFile, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("opening load file: %w", err)
	}
	defer func() {
		_ = gzipFile.Close()
	}()

	gzReader, err := gzip.NewReader(gzipFile)
	if err != nil {
		return fmt.Errorf("reading gzip load file: %w", err)
	}
	defer func() {
		_ = gzReader.Close()
	}()

	csvReader := csv.NewReader(gzReader)

	for {
		record, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reading file: %w", err)
		}
		if len(sortedColumnKeys) != len(record) {
			return fmt.Errorf("mismatch in number of columns: actual count: %d, expected count: %d",
				len(record),
				len(sortedColumnKeys),
			)
		}

		recordInterface := make([]interface{}, 0, len(record))
		for _, value := range record {
			if strings.TrimSpace(value) == "" {
				recordInterface = append(recordInterface, nil)
			} else {
				recordInterface = append(recordInterface, value)
			}
		}

		_, err = stmt.ExecContext(ctx, recordInterface...)
		if err != nil {
			return fmt.Errorf("exec statement: %w", err)
		}
	}
	return nil
}

func (pg *Postgres) deleteFromLoadTable(
	ctx context.Context,
	txn *sqlmiddleware.Tx,
	tableName string,
	stagingTableName string,
) (int64, error) {
	primaryKey := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}

	var additionalJoinClause string
	if tableName == warehouseutils.DiscardsTable {
		additionalJoinClause = fmt.Sprintf(
			`AND _source.%[3]s = %[1]q.%[2]q.%[3]q AND _source.%[4]s = %[1]q.%[2]q.%[4]q`,
			pg.Namespace,
			tableName,
			"table_name",
			"column_name",
		)
	}

	deleteStmt := fmt.Sprintf(`
		DELETE FROM
		  %[1]q.%[2]q USING %[3]q AS _source
		WHERE
		  (
			_source.%[4]s = %[1]q.%[2]q.%[4]q %[5]s
		  );`,
		pg.Namespace,
		tableName,
		stagingTableName,
		primaryKey,
		additionalJoinClause,
	)

	result, err := txn.ExecContext(ctx, deleteStmt)
	if err != nil {
		return 0, fmt.Errorf("deleting from main table for dedup: %w", err)
	}
	return result.RowsAffected()
}

func (pg *Postgres) insertIntoLoadTable(
	ctx context.Context,
	txn *sqlmiddleware.Tx,
	tableName string,
	stagingTableName string,
	sortedColumnKeys []string,
) (int64, error) {
	partitionKey := "id"
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(
		sortedColumnKeys,
	)

	insertStmt := fmt.Sprintf(`
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
		  _rudder_staging_row_number = 1;`,
		pg.Namespace,
		tableName,
		quotedColumnNames,
		stagingTableName,
		partitionKey,
	)

	r, err := txn.ExecContext(ctx, insertStmt)
	if err != nil {
		return 0, fmt.Errorf("inserting into main table: %w", err)
	}
	return r.RowsAffected()
}

func (pg *Postgres) LoadUserTables(ctx context.Context) map[string]error {
	pg.logger.Infow("started loading for identifies and users tables",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.Namespace, pg.Namespace,
	)

	identifiesSchemaInUpload := pg.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable)
	usersSchemaInUpload := pg.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)
	usersSchemaInWarehouse := pg.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)

	var loadingError loadUsersTableResponse
	_ = pg.DB.WithTx(ctx, func(tx *sqlmiddleware.Tx) error {
		loadingError = pg.loadUsersTable(ctx, tx, identifiesSchemaInUpload, usersSchemaInUpload, usersSchemaInWarehouse)
		if loadingError.identifiesError != nil || loadingError.usersError != nil {
			return errors.New("loading users and identifies table")
		}

		return nil
	})
	if loadingError.identifiesError != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: loadingError.identifiesError,
		}
	}
	if len(usersSchemaInUpload) == 0 {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
		}
	}
	if loadingError.usersError != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      loadingError.usersError,
		}
	}

	pg.logger.Infow("completed loading for users and identities table",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.Namespace, pg.Namespace,
	)

	return map[string]error{
		warehouseutils.IdentifiesTable: nil,
		warehouseutils.UsersTable:      nil,
	}
}

func (pg *Postgres) loadUsersTable(
	ctx context.Context,
	tx *sqlmiddleware.Tx,
	identifiesSchemaInUpload,
	usersSchemaInUpload,
	usersSchemaInWarehouse model.TableSchema,
) loadUsersTableResponse {
	_, identifyStagingTable, err := pg.loadTable(ctx, tx, warehouseutils.IdentifiesTable, identifiesSchemaInUpload)
	if err != nil {
		return loadUsersTableResponse{
			identifiesError: fmt.Errorf("loading identifies table: %w", err),
		}
	}

	if len(usersSchemaInUpload) == 0 {
		return loadUsersTableResponse{}
	}

	canSkipComputingLatestUserTraits := pg.config.skipComputingUserLatestTraits ||
		slices.Contains(pg.config.skipComputingUserLatestTraitsWorkspaceIDs, pg.Warehouse.WorkspaceID)
	if canSkipComputingLatestUserTraits {
		if _, _, err = pg.loadTable(ctx, tx, warehouseutils.UsersTable, usersSchemaInUpload); err != nil {
			return loadUsersTableResponse{
				usersError: fmt.Errorf("loading users table: %w", err),
			}
		}
		return loadUsersTableResponse{}
	}

	unionStagingTableName := warehouseutils.StagingTableName(provider, "users_identifies_union", tableNameLimit)
	usersStagingTableName := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)

	var userColNames, firstValProps []string
	for colName := range usersSchemaInWarehouse {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, fmt.Sprintf(`%q`, colName))
		caseSubQuery := fmt.Sprintf(
			`CASE WHEN (
			  SELECT true
			) THEN (
			  SELECT %[1]q
			  FROM %[2]q AS staging_table
			  WHERE x.id = staging_table.id AND %[1]q IS NOT NULL
			  ORDER BY received_at DESC
			  LIMIT 1
			) END AS %[1]q`,
			colName,
			unionStagingTableName,
		)
		firstValProps = append(firstValProps, caseSubQuery)
	}

	query := fmt.Sprintf(
		`CREATE TEMPORARY TABLE %[5]s AS (
			(
				SELECT id, %[4]s
				FROM %[1]q.%[2]q
				WHERE id IN (
					SELECT user_id
					FROM %[3]q
					WHERE user_id IS NOT NULL
				)
			)
			UNION
			(
				SELECT user_id, %[4]s
				FROM %[3]q
				WHERE user_id IS NOT NULL
			)
		);`,
		pg.Namespace,
		warehouseutils.UsersTable,
		identifyStagingTable,
		strings.Join(userColNames, ","),
		unionStagingTableName,
	)

	pg.logger.Infow("creating union staging users table",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, unionStagingTableName,
		logfield.Namespace, pg.Namespace,
		logfield.Query, query,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return loadUsersTableResponse{
			usersError: fmt.Errorf("creating union staging users table: %w", err),
		}
	}

	query = fmt.Sprintf(`
		CREATE INDEX users_identifies_union_id_idx ON %[1]s (id);`,
		unionStagingTableName,
	)
	pg.logger.Debugw("creating index on union staging users table",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, usersStagingTableName,
		logfield.Query, query,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return loadUsersTableResponse{
			usersError: fmt.Errorf("creating index on union staging users table: %w", err),
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

	pg.logger.Debugw("creating temporary users table",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, usersStagingTableName,
		logfield.Query, query,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return loadUsersTableResponse{
			usersError: fmt.Errorf("creating temporary users table: %w", err),
		}
	}

	// Deduplication
	// Delete from users table if the id is present in the staging table
	primaryKey := "id"
	query = fmt.Sprintf(`
		DELETE FROM %[1]q.%[2]q using %[3]q _source
		WHERE _source.%[4]s = %[1]s.%[2]s.%[4]s;`,
		pg.Namespace,
		warehouseutils.UsersTable,
		usersStagingTableName,
		primaryKey,
	)

	pg.logger.Infow("deduplication for users table",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, usersStagingTableName,
		logfield.Namespace, pg.Namespace,
		logfield.Query, query,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return loadUsersTableResponse{
			usersError: fmt.Errorf("deduplication for users table: %w", err),
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
		pg.Namespace,
		warehouseutils.UsersTable,
		usersStagingTableName,
		strings.Join(append([]string{"id"}, userColNames...), ","),
	)
	pg.logger.Infow("inserting records to users table",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, usersStagingTableName,
		logfield.Namespace, pg.Namespace,
		logfield.Query, query,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return loadUsersTableResponse{
			usersError: fmt.Errorf("inserting records to users table: %w", err),
		}
	}

	return loadUsersTableResponse{}
}

func (pg *Postgres) shouldMerge(tableName string) bool {
	if !pg.config.allowMerge {
		return false
	}
	if tableName == warehouseutils.UsersTable {
		// If we are here it's because canSkipComputingLatestUserTraits is true.
		// preferAppend doesn't apply to the users table, so we are just checking skipDedupDestinationIDs for
		// backwards compatibility.
		return !slices.Contains(pg.config.skipDedupDestinationIDs, pg.Warehouse.Destination.ID)
	}
	if !pg.Uploader.CanAppend() {
		return true
	}
	return !pg.Warehouse.GetPreferAppendSetting() &&
		!slices.Contains(pg.config.skipDedupDestinationIDs, pg.Warehouse.Destination.ID)
}
