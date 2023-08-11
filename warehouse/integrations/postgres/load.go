package postgres

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/lib/pq"
	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type loadTableResponse struct {
	StagingTableName string
}

type loadUsersTableResponse struct {
	identifiesError error
	usersError      error
}

func (pg *Postgres) LoadTable(ctx context.Context, tableName string) error {
	pg.logger.Infow("started loading",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.Namespace, pg.Namespace,
		logfield.TableName, tableName,
	)

	err := pg.DB.WithTx(ctx, func(tx *sqlmiddleware.Tx) error {
		tableSchemaInUpload := pg.Uploader.GetTableSchemaInUpload(tableName)

		_, err := pg.loadTable(ctx, tx, tableName, tableSchemaInUpload)
		return err
	})
	if err != nil {
		return fmt.Errorf("loading table: %w", err)
	}

	pg.logger.Infow("completed loading",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.Namespace, pg.Namespace,
		logfield.TableName, tableName,
	)

	return nil
}

func (pg *Postgres) loadTable(
	ctx context.Context,
	txn *sqlmiddleware.Tx,
	tableName string,
	tableSchemaInUpload model.TableSchema,
) (loadTableResponse, error) {
	query := fmt.Sprintf(`SET search_path TO %q;`, pg.Namespace)
	if _, err := txn.ExecContext(ctx, query); err != nil {
		return loadTableResponse{}, fmt.Errorf("setting search path: %w", err)
	}

	// Creating staging table
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
	stagingTableName := warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	query = fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[2]s (LIKE %[1]q.%[3]q)
		ON COMMIT PRESERVE ROWS;
`,
		pg.Namespace,
		stagingTableName,
		tableName,
	)
	pg.logger.Infow("creating temporary table",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.Namespace, pg.Namespace,
		logfield.TableName, tableName,
		logfield.StagingTableName, stagingTableName,
		logfield.Query, query,
	)
	if _, err := txn.ExecContext(ctx, query); err != nil {
		return loadTableResponse{}, fmt.Errorf("creating temporary table: %w", err)
	}

	stmt, err := txn.PrepareContext(ctx, pq.CopyIn(stagingTableName, sortedColumnKeys...))
	if err != nil {
		return loadTableResponse{}, fmt.Errorf("preparing statement for copy in: %w", err)
	}

	loadFiles, err := pg.LoadFileDownloader.Download(ctx, tableName)
	defer misc.RemoveFilePaths(loadFiles...)
	if err != nil {
		return loadTableResponse{}, fmt.Errorf("downloading load files: %w", err)
	}

	var csvRowsProcessedCount int64
	for _, objectFileName := range loadFiles {
		gzFile, err := os.Open(objectFileName)
		if err != nil {
			return loadTableResponse{}, fmt.Errorf("opening load file: %w", err)
		}

		gzReader, err := gzip.NewReader(gzFile)
		if err != nil {
			_ = gzFile.Close()

			return loadTableResponse{}, fmt.Errorf("reading gzip load file: %w", err)
		}

		csvReader := csv.NewReader(gzReader)

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

				return loadTableResponse{}, fmt.Errorf("reading csv file: %w", err)
			}

			if len(sortedColumnKeys) != len(record) {
				return loadTableResponse{}, fmt.Errorf("missing columns in csv file %s", objectFileName)
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
				return loadTableResponse{}, fmt.Errorf("exec statement: %w", err)
			}

			csvRowsProcessedCount++
		}

		_ = gzReader.Close()
		_ = gzFile.Close()
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return loadTableResponse{}, fmt.Errorf("exec statement: %w", err)
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
			pg.Namespace,
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
		pg.Namespace,
		tableName,
		stagingTableName,
		primaryKey,
		additionalJoinClause,
	)

	if !slices.Contains(pg.config.skipDedupDestinationIDs, pg.Warehouse.Destination.ID) {
		pg.logger.Infow("deduplication",
			logfield.SourceID, pg.Warehouse.Source.ID,
			logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, pg.Warehouse.Destination.ID,
			logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
			logfield.Namespace, pg.Namespace,
			logfield.TableName, tableName,
			logfield.StagingTableName, stagingTableName,
			logfield.Query, query,
		)

		result, err := txn.ExecContext(ctx, query)
		if err != nil {
			return loadTableResponse{}, fmt.Errorf("deleting from original table for dedup: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return loadTableResponse{}, fmt.Errorf("getting rows affected for dedup: %w", err)
		}

		pg.stats.NewTaggedStat("dedup_rows", stats.CountType, stats.Tags{
			"sourceID":       pg.Warehouse.Source.ID,
			"sourceType":     pg.Warehouse.Source.SourceDefinition.Name,
			"sourceCategory": pg.Warehouse.Source.SourceDefinition.Category,
			"destID":         pg.Warehouse.Destination.ID,
			"destType":       pg.Warehouse.Destination.DestinationDefinition.Name,
			"workspaceId":    pg.Warehouse.WorkspaceID,
			"tableName":      tableName,
			"rowsAffected":   fmt.Sprintf("%d", rowsAffected),
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
		pg.Namespace,
		tableName,
		quotedColumnNames,
		stagingTableName,
		partitionKey,
	)

	pg.logger.Infow("inserting records",
		logfield.SourceID, pg.Warehouse.Source.ID,
		logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, pg.Warehouse.Destination.ID,
		logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
		logfield.Namespace, pg.Namespace,
		logfield.TableName, tableName,
		logfield.StagingTableName, stagingTableName,
		logfield.Query, query,
	)
	if _, err := txn.ExecContext(ctx, query); err != nil {
		return loadTableResponse{}, fmt.Errorf("executing query: %w", err)
	}

	response := loadTableResponse{
		StagingTableName: stagingTableName,
	}
	return response, nil
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
	identifiesTableResponse, err := pg.loadTable(ctx, tx, warehouseutils.IdentifiesTable, identifiesSchemaInUpload)
	if err != nil {
		return loadUsersTableResponse{
			identifiesError: fmt.Errorf("loading identifies table: %w", err),
		}
	}

	if len(usersSchemaInUpload) == 0 {
		return loadUsersTableResponse{}
	}

	canSkipComputingLatestUserTraits := pg.config.skipComputingUserLatestTraits || slices.Contains(pg.config.skipComputingUserLatestTraitsWorkspaceIDs, pg.Warehouse.WorkspaceID)
	if canSkipComputingLatestUserTraits {
		if _, err = pg.loadTable(ctx, tx, warehouseutils.UsersTable, usersSchemaInUpload); err != nil {
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

	query := fmt.Sprintf(`
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
		pg.Namespace,
		warehouseutils.UsersTable,
		identifiesTableResponse.StagingTableName,
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
		DELETE FROM
		  %[1]q.%[2]q using %[3]q _source
		WHERE
		  (
			_source.%[4]s = %[1]s.%[2]s.%[4]s
		  );
`,
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
