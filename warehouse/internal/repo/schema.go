package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const whSchemaTableName = warehouseutils.WarehouseSchemasTable

const whSchemaTableColumns = `
	id,
   	wh_upload_id,
   	source_id,
	namespace,
   	destination_id,
	destination_type,
	schema,
	schema_compressed,
   	created_at,
   	updated_at
`

type WHSchema repo

func NewWHSchemas(db *sqlmiddleware.DB, opts ...Opt) *WHSchema {
	r := &WHSchema{
		db:  db,
		now: timeutil.Now,
	}
	for _, opt := range opts {
		opt((*repo)(r))
	}
	return r
}

func (sh *WHSchema) Insert(ctx context.Context, whSchema *model.WHSchema) (int64, error) {
	var (
		id  int64
		now = sh.now()
	)

	schemaPayload, err := json.Marshal(whSchema.Schema)
	if err != nil {
		return id, fmt.Errorf("marshaling schema: %w", err)
	}

	schemaCompressed, err := warehouseutils.Compress(schemaPayload)
	if err != nil {
		return id, fmt.Errorf("compressing schema: %w", err)
	}

	err = sh.db.QueryRowContext(ctx, `
		INSERT INTO `+whSchemaTableName+` (
		  wh_upload_id, source_id, namespace, destination_id,
		  destination_type, schema, schema_compressed, created_at,
		  updated_at
		)
		VALUES
		  ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (
			source_id, destination_id, namespace
		  ) DO
		UPDATE
		SET
		  schema_compressed = $7,
		  updated_at = $8 RETURNING id;
`,
		whSchema.UploadID,
		whSchema.SourceID,
		whSchema.Namespace,
		whSchema.DestinationID,
		whSchema.DestinationType,
		"{}",
		schemaCompressed,
		now.UTC(),
		now.UTC(),
	).Scan(&id)
	if err != nil {
		return id, fmt.Errorf("inserting schema: %w", err)
	}
	return id, nil
}

func (sh *WHSchema) GetForNamespace(ctx context.Context, sourceID, destID, namespace string) (model.WHSchema, error) {
	query := `SELECT ` + whSchemaTableColumns + ` FROM ` + whSchemaTableName + `
	WHERE
		source_id = $1 AND
		destination_id = $2 AND
		namespace = $3
	ORDER BY
		id DESC;
	`

	rows, err := sh.db.QueryContext(
		ctx,
		query,
		sourceID,
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

func parseWHSchemas(rows *sqlmiddleware.Rows) ([]*model.WHSchema, error) {
	var whSchemas []*model.WHSchema

	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			whSchema                   model.WHSchema
			schemaPayloadRaw           []byte
			schemaCompressedPayloadRaw []byte
		)
		err := rows.Scan(
			&whSchema.ID,
			&whSchema.UploadID,
			&whSchema.SourceID,
			&whSchema.Namespace,
			&whSchema.DestinationID,
			&whSchema.DestinationType,
			&schemaPayloadRaw,
			&schemaCompressedPayloadRaw,
			&whSchema.CreatedAt,
			&whSchema.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		whSchema.CreatedAt = whSchema.CreatedAt.UTC()
		whSchema.UpdatedAt = whSchema.UpdatedAt.UTC()

		var schemaPayload model.Schema
		if len(schemaCompressedPayloadRaw) > 0 {
			var schemaCompressedPayload []byte
			schemaCompressedPayload, err = warehouseutils.Decompress(schemaCompressedPayloadRaw)
			if err != nil {
				return nil, fmt.Errorf("decompressing schema: %w", err)
			}

			err = json.Unmarshal(schemaCompressedPayload, &schemaPayload)
			if err != nil {
				return nil, fmt.Errorf("unmarshal schemaPayload: %w", err)
			}
		} else {
			err = json.Unmarshal(schemaPayloadRaw, &schemaPayload)
			if err != nil {
				return nil, fmt.Errorf("unmarshal schemaPayload: %w", err)
			}
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
			destination_id = $2
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
				(schema::text <> '{}'::text OR schema_compressed IS NOT NULL)

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
