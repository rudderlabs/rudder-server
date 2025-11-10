package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stringify"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const whSchemaTableName = warehouseutils.WarehouseSchemasTable

const whSchemaTableColumns = `
	id,
   	source_id,
	namespace,
   	destination_id,
	destination_type,
	schema,
   	created_at,
   	updated_at,
	expires_at
`

type WHSchema struct {
	*repo

	log    logger.Logger
	config struct {
		enableTableLevelSchema config.ValueLoader[bool]
	}
}

func NewWHSchemas(db *sqlmiddleware.DB, conf *config.Config, log logger.Logger, opts ...Opt) *WHSchema {
	r := &WHSchema{
		repo: &repo{
			db:           db,
			now:          timeutil.Now,
			statsFactory: stats.NOP,
			repoType:     whSchemaTableName,
		},
		log: log.Child("repo.wh_schemas"),
	}
	r.config.enableTableLevelSchema = conf.GetReloadableBoolVar(false, "Warehouse.enableTableLevelSchema")
	for _, opt := range opts {
		opt(r.repo)
	}
	return r
}

// Insert inserts a schema row in wh_schemas with the given schema.
// If Warehouse.enableTableLevelSchema is true in config, it also inserts/updates table-level schemas for each table in the schema.
func (sh *WHSchema) Insert(ctx context.Context, whSchema *model.WHSchema) error {
	defer sh.TimerStat("insert", stats.Tags{
		"destId":   whSchema.DestinationID,
		"destType": whSchema.DestinationType,
	})()

	now := sh.now()

	schemaPayload, err := jsonrs.Marshal(whSchema.Schema)
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}

	log := sh.log.Withn(
		obskit.SourceID(whSchema.SourceID),
		obskit.Namespace(whSchema.Namespace),
		obskit.DestinationID(whSchema.DestinationID),
		obskit.DestinationType(whSchema.DestinationType),
	)

	err = sh.WithTx(ctx, func(tx *sqlmiddleware.Tx) error {
		// update all schemas with the same destination_id and namespace but different source_id
		// this is to ensure all the connections for a destination have the same schema copy
		_, err = tx.ExecContext(ctx, `
			UPDATE `+whSchemaTableName+`
			SET
				schema = $1,
				updated_at = $2,
				expires_at = $3
			WHERE
				destination_id = $4 AND
				namespace = $5 AND
				source_id != $6 AND
				table_name = '';
		`,
			schemaPayload,
			now.UTC(),
			whSchema.ExpiresAt,
			whSchema.DestinationID,
			whSchema.Namespace,
			whSchema.SourceID,
		)
		if err != nil {
			log.Errorn("Failed to update related schemas",
				logger.NewStringField("schema", string(schemaPayload)),
				obskit.Error(err),
			)
			return fmt.Errorf("updating related schemas: %w", err)
		}

		// Then, insert/update the new schema using the unique constraint
		_, err = tx.ExecContext(ctx, `
			INSERT INTO `+whSchemaTableName+` (
			  source_id, namespace, destination_id,
			  destination_type, schema, created_at,
			  updated_at, expires_at
			)
			VALUES
			  ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (
				source_id, destination_id, namespace, table_name
			) DO
			UPDATE
			SET
			  schema = $5,
			  updated_at = $7,
			  expires_at = $8;
		`,
			whSchema.SourceID,
			whSchema.Namespace,
			whSchema.DestinationID,
			whSchema.DestinationType,
			schemaPayload,
			now.UTC(),
			now.UTC(),
			whSchema.ExpiresAt,
		)
		if err != nil {
			return fmt.Errorf("inserting schema: %w", err)
		}

		// If table-level schema is enabled, insert/update for each table
		if sh.config.enableTableLevelSchema.Load() {
			if len(whSchema.Schema) > 0 {
				// Delete orphaned table-level schemas for the current source_id + destination_id + namespace
				// This ensures consistency between raw schema and table-level schemas
				_, err = tx.ExecContext(ctx, `
				DELETE FROM `+whSchemaTableName+`
				WHERE destination_id = $1
				  AND namespace = $2
				  AND table_name != ''
				  AND table_name NOT IN (
					SELECT unnest($3::text[])
				  );
			`,
					whSchema.DestinationID,
					whSchema.Namespace,
					pq.Array(lo.Keys(whSchema.Schema)),
				)
				if err != nil {
					return fmt.Errorf("deleting orphaned table-level schemas: %w", err)
				}
			}

			for tableName, tableSchema := range whSchema.Schema {
				tableSchemaPayload, err := jsonrs.Marshal(tableSchema)
				if err != nil {
					return fmt.Errorf("marshaling table schema for table %s: %w", tableName, err)
				}

				_, err = tx.ExecContext(ctx, `
					UPDATE `+whSchemaTableName+`
					SET
						schema = $1,
						updated_at = $2,
						expires_at = $3
					WHERE
						destination_id = $4 AND
						namespace = $5 AND
						source_id != $6 AND
						table_name = $7;
	`,
					tableSchemaPayload,
					now.UTC(),
					whSchema.ExpiresAt,
					whSchema.DestinationID,
					whSchema.Namespace,
					whSchema.SourceID,
					tableName,
				)
				if err != nil {
					log.Errorn("Failed to update table-level related schemas",
						logger.NewStringField("tableName", tableName),
						logger.NewStringField("schema", string(schemaPayload)),
						logger.NewStringField("tableLevelSchema", string(tableSchemaPayload)),
						obskit.Error(err),
					)
					return fmt.Errorf("updating other table-level schemas for table %s: %w", tableName, err)
				}

				_, err = tx.ExecContext(ctx, `
					INSERT INTO `+whSchemaTableName+` (
					  source_id, namespace, destination_id,
					  destination_type, schema, created_at,
					  updated_at, expires_at, table_name
					)
					VALUES
					  ($1, $2, $3, $4, $5, $6, $7, $8, $9)
					ON CONFLICT (
						source_id, destination_id, namespace, table_name
					) DO
					UPDATE
					SET
					  schema = $5,
					  updated_at = $7,
					  expires_at = $8;
	`,
					whSchema.SourceID,
					whSchema.Namespace,
					whSchema.DestinationID,
					whSchema.DestinationType,
					tableSchemaPayload,
					now.UTC(),
					now.UTC(),
					whSchema.ExpiresAt,
					tableName,
				)
				if err != nil {
					return fmt.Errorf("inserting table-level schema: %w", err)
				}
			}
		}

		return nil
	})

	return err
}

// GetForNamespace fetches the schema for a namespace, supporting both legacy and table-level modes.
func (sh *WHSchema) GetForNamespace(ctx context.Context, destID, namespace string) (model.WHSchema, error) {
	defer sh.TimerStat("get_for_namespace", stats.Tags{"destId": destID})()

	if !sh.config.enableTableLevelSchema.Load() {
		return sh.getForNamespace(ctx, destID, namespace)
	}

	originalSchema, err := sh.getForNamespace(ctx, destID, namespace)
	if err != nil {
		return model.WHSchema{}, err
	}
	if len(originalSchema.Schema) == 0 {
		return originalSchema, nil
	}

	var tableLevelSchemas model.Schema
	err = sh.WithTx(ctx, func(tx *sqlmiddleware.Tx) error {
		err := sh.populateTableLevelSchemasWithTx(ctx, tx, destID, namespace)
		if err != nil {
			return fmt.Errorf("populating table-level schemas: %w", err)
		}

		tableLevelSchemas, err = sh.getTableLevelSchemasForNamespaceWithTx(ctx, tx, destID, namespace)
		if err != nil {
			return fmt.Errorf("getting table-level schemas: %w", err)
		}
		return nil
	})
	if err != nil {
		return model.WHSchema{}, err
	}
	diff := cmp.Diff(originalSchema.Schema, tableLevelSchemas)
	if len(diff) > 0 {
		sh.log.Warnn("Parent schema does not match",
			obskit.Namespace(namespace),
			obskit.DestinationID(destID),
			logger.NewStringField("schema", stringify.Any(originalSchema.Schema)),
			logger.NewStringField("tableLevelSchemas", stringify.Any(tableLevelSchemas)),
		)
		return model.WHSchema{}, fmt.Errorf("parent schema does not match: %s", diff)
	}
	return originalSchema, nil
}

func (sh *WHSchema) getForNamespace(ctx context.Context, destID, namespace string) (model.WHSchema, error) {
	query := `SELECT ` + whSchemaTableColumns + ` FROM ` + whSchemaTableName + `
	WHERE
		destination_id = $1 AND
		namespace = $2 AND
		table_name = ''
	ORDER BY
		source_id DESC
    LIMIT 1;
	`

	rows, err := sh.db.QueryContext(
		ctx,
		query,
		destID,
		namespace,
	)
	if err != nil {
		return model.WHSchema{}, fmt.Errorf("querying schemas: %w", err)
	}

	entries, err := parseWHSchemas(rows)
	if err != nil {
		return model.WHSchema{}, fmt.Errorf("parsing rows: %w", err)
	}
	if len(entries) == 0 {
		return model.WHSchema{}, nil
	}

	return *entries[0], err
}

// populateTableLevelSchemasWithTx inserts table-level schemas for each table in the parent schema
// that don't already exist as separate table-level schemas, using the provided transaction.
func (sh *WHSchema) populateTableLevelSchemasWithTx(ctx context.Context, tx *sqlmiddleware.Tx, destID, namespace string) error {
	now := sh.now()
	query := `
		INSERT INTO wh_schemas (
			source_id,
			namespace,
			destination_id,
			destination_type,
			schema,
			table_name,
			created_at,
			updated_at,
			expires_at
		)
		SELECT
			s.source_id,
			s.namespace,
			s.destination_id,
			s.destination_type,
			j.value AS schema,
			j.key AS table_name,
			$3,
			$3,
			s.expires_at
		FROM wh_schemas s
		CROSS JOIN LATERAL jsonb_each(s.schema::jsonb) AS j
		LEFT JOIN wh_schemas s2
		  ON s2.source_id = s.source_id
		  AND s2.namespace = s.namespace
		  AND s2.destination_id = s.destination_id
		  AND s2.table_name = j.key
		WHERE s.destination_id = $1
		  AND s.namespace = $2
		  AND s.table_name = ''
		  AND s2.id IS NULL
		ON CONFLICT (source_id, destination_id, namespace, table_name) DO NOTHING;
	`
	_, err := tx.ExecContext(ctx, query, destID, namespace, now.UTC())
	if err != nil {
		return fmt.Errorf("populating table-level schemas: %w", err)
	}
	return nil
}

// getTableLevelSchemasForNamespaceWithTx fetches the latest schema (by id) for each table in the given destID and namespace, regardless of source.
func (sh *WHSchema) getTableLevelSchemasForNamespaceWithTx(ctx context.Context, tx *sqlmiddleware.Tx, destID, namespace string) (model.Schema, error) {
	tableLevelQuery := `
		SELECT
			table_name,
			schema
		FROM (
			SELECT
				table_name,
				schema,
				ROW_NUMBER() OVER (
					PARTITION BY destination_id, namespace, table_name
					ORDER BY source_id DESC
				) as rn
			FROM ` + whSchemaTableName + `
			WHERE
				destination_id = $1 AND
				namespace = $2 AND
				table_name != ''
		) t
		WHERE rn = 1;
	`

	rows, err := tx.QueryContext(ctx, tableLevelQuery, destID, namespace)
	if err != nil {
		return nil, fmt.Errorf("querying table-level schemas: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tableLevelSchemas model.Schema
	for rows.Next() {
		var tableName string
		var schemaJSON []byte
		err := rows.Scan(&tableName, &schemaJSON)
		if err != nil {
			return nil, fmt.Errorf("scanning table-level schema row: %w", err)
		}

		var tableSchema model.TableSchema
		err = jsonrs.Unmarshal(schemaJSON, &tableSchema)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling table schema for table %s: %w", tableName, err)
		}
		if tableLevelSchemas == nil {
			tableLevelSchemas = make(model.Schema)
		}
		tableLevelSchemas[tableName] = tableSchema
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating table-level schema rows: %w", err)
	}
	return tableLevelSchemas, nil
}

func parseWHSchemas(rows *sqlmiddleware.Rows) ([]*model.WHSchema, error) {
	var whSchemas []*model.WHSchema

	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			whSchema            model.WHSchema
			schemaPayloadRawRaw []byte
			expiresAt           sql.NullTime
		)
		err := rows.Scan(
			&whSchema.ID,
			&whSchema.SourceID,
			&whSchema.Namespace,
			&whSchema.DestinationID,
			&whSchema.DestinationType,
			&schemaPayloadRawRaw,
			&whSchema.CreatedAt,
			&whSchema.UpdatedAt,
			&expiresAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		whSchema.CreatedAt = whSchema.CreatedAt.UTC()
		whSchema.UpdatedAt = whSchema.UpdatedAt.UTC()
		if expiresAt.Valid {
			whSchema.ExpiresAt = expiresAt.Time.UTC()
		}

		var schemaPayload model.Schema
		err = jsonrs.Unmarshal(schemaPayloadRawRaw, &schemaPayload)
		if err != nil {
			return nil, fmt.Errorf("unmarshal schemaPayload: %w", err)
		}

		whSchema.Schema = schemaPayload

		whSchemas = append(whSchemas, &whSchema)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return whSchemas, nil
}

func (sh *WHSchema) GetNamespace(ctx context.Context, sourceID, destID string) (string, error) {
	defer sh.TimerStat("get_namespace", stats.Tags{
		"destId": destID,
	})()

	query := `SELECT namespace FROM ` + whSchemaTableName + `
		WHERE
			source_id = $1 AND
			destination_id = $2 AND
			table_name = ''
		ORDER BY
			id DESC
		LIMIT 1;
	`

	row := sh.db.QueryRowContext(
		ctx,
		query,
		sourceID,
		destID,
	)
	if row.Err() != nil {
		return "", fmt.Errorf("querying schema: %w", row.Err())
	}

	var (
		namespace string
		err       error
	)

	err = row.Scan(&namespace)

	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("scanning row: %w", err)
	}

	return namespace, nil
}

func (sh *WHSchema) GetTablesForConnection(ctx context.Context, connections []warehouseutils.SourceIDDestinationID) ([]warehouseutils.FetchTableInfo, error) {
	defer sh.TimerStat("get_tables_for_connection", nil)()

	if len(connections) == 0 {
		return nil, fmt.Errorf("no source id and destination id pairs provided")
	}

	var parameters []interface{}
	sourceIDDestinationIDPairs := make([]string, len(connections))
	for idx, connection := range connections {
		sourceIDDestinationIDPairs[idx] = fmt.Sprintf("($%d,$%d)", 2*idx+1, 2*idx+2)
		parameters = append(parameters, connection.SourceID, connection.DestinationID)
	}

	// select all rows with max id for each source id and destination id pair
	query := `SELECT` + whSchemaTableColumns + `FROM ` + whSchemaTableName + `
		WHERE id IN (
			SELECT max(id) FROM ` + whSchemaTableName + `
			WHERE
				(source_id, destination_id) IN (` + strings.Join(sourceIDDestinationIDPairs, ", ") + `)
			AND
				table_name = ''
			AND
				schema::text <> '{}'::text
			GROUP BY source_id, destination_id
		)`
	rows, err := sh.db.QueryContext(
		ctx,
		query,
		parameters...)
	if err != nil {
		return nil, fmt.Errorf("querying schema: %w", err)
	}

	entries, err := parseWHSchemas(rows)
	if err != nil {
		return nil, fmt.Errorf("parsing rows: %w", err)
	}

	var tables []warehouseutils.FetchTableInfo
	for _, entry := range entries {
		var allTablesOfConnections []string
		for tableName := range entry.Schema {
			allTablesOfConnections = append(allTablesOfConnections, tableName)
		}
		tables = append(tables, warehouseutils.FetchTableInfo{
			SourceID:      entry.SourceID,
			DestinationID: entry.DestinationID,
			Namespace:     entry.Namespace,
			Tables:        allTablesOfConnections,
		})
	}

	return tables, nil
}

func (sh *WHSchema) SetExpiryForDestination(ctx context.Context, destinationID string, expiresAt time.Time) error {
	defer sh.TimerStat("set_expiry_for_destination", stats.Tags{"destId": destinationID})()

	query := `
		UPDATE ` + whSchemaTableName + `
		SET expires_at = $1, updated_at = $2
		WHERE destination_id = $3;
	`
	_, err := sh.db.ExecContext(ctx, query, expiresAt, sh.now(), destinationID)
	return err
}

// GetDestinationNamespaces returns the most recent namespace for each source for a given destination ID.
func (sh *WHSchema) GetDestinationNamespaces(ctx context.Context, destinationID string) ([]model.NamespaceMapping, error) {
	defer sh.TimerStat("get_destination_namespaces", stats.Tags{"destId": destinationID})()

	query := `
		SELECT DISTINCT ON (source_id)
			source_id,
			namespace
		FROM ` + whSchemaTableName + `
		WHERE destination_id = $1
		AND table_name = ''
		ORDER BY source_id, updated_at DESC;
	`
	rows, err := sh.db.QueryContext(ctx, query, destinationID)
	if err != nil {
		return nil, fmt.Errorf("querying destination namespaces: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var mappings []model.NamespaceMapping
	for rows.Next() {
		var mapping model.NamespaceMapping
		err := rows.Scan(&mapping.SourceID, &mapping.Namespace)
		if err != nil {
			return nil, fmt.Errorf("scanning namespace mapping: %w", err)
		}
		mappings = append(mappings, mapping)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating over namespace mappings: %w", err)
	}
	return mappings, nil
}
