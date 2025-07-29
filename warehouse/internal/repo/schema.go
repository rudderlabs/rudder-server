package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"

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

	config struct {
		enableTableLevelSchema config.ValueLoader[bool]
	}
}

func NewWHSchemas(db *sqlmiddleware.DB, conf *config.Config, opts ...Opt) *WHSchema {
	r := &WHSchema{
		repo: &repo{
			db:  db,
			now: timeutil.Now,
		},
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
	now := sh.now()

	schemaPayload, err := jsonrs.Marshal(whSchema.Schema)
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}

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
					sh.now().UTC(),
					whSchema.ExpiresAt,
					whSchema.DestinationID,
					whSchema.Namespace,
					whSchema.SourceID,
					tableName,
				)
				if err != nil {
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
					whSchema.CreatedAt.UTC(),
					whSchema.UpdatedAt.UTC(),
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
	if !sh.config.enableTableLevelSchema.Load() {
		return sh.getForNamespace(ctx, destID, namespace)
	}

	originalSchema, err := sh.getForNamespace(ctx, destID, namespace)
	if err != nil {
		return model.WHSchema{}, err
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
	if !reflect.DeepEqual(originalSchema.Schema, tableLevelSchemas) {
		return model.WHSchema{}, errors.New("parent schema does not match parent schema")
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
		id DESC;
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
			s.created_at,
			s.updated_at,
			s.expires_at
		FROM wh_schemas s
		CROSS JOIN LATERAL jsonb_each(s.schema::jsonb) AS j
		WHERE
			s.destination_id = $1 AND
			s.namespace = $2 AND
			s.table_name = '' AND
			NOT EXISTS (
				SELECT 1 FROM wh_schemas s2
				WHERE s2.source_id = s.source_id AND
					s2.namespace = s.namespace AND
					s2.destination_id = s.destination_id AND
					s2.table_name = j.key
			)
	`
	_, err := tx.ExecContext(ctx, query, destID, namespace)
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
					ORDER BY id DESC
				) as rn
			FROM ` + whSchemaTableName + `
			WHERE
				destination_id = $1
				AND namespace = $2
				AND table_name != ''
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
	query := `
		UPDATE ` + whSchemaTableName + `
		SET expires_at = $1, updated_at = $2
		WHERE destination_id = $3;
	`
	_, err := sh.db.ExecContext(ctx, query, expiresAt, sh.now(), destinationID)
	return err
}
