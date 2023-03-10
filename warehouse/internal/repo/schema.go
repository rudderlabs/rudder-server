package repo

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
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
   	created_at,
   	updated_at
`

type WHSchema repo

func NewWHSchemas(db *sql.DB, opts ...Opt) *WHSchema {
	r := &WHSchema{
		db:  db,
		now: timeutil.Now,
	}
	for _, opt := range opts {
		opt((*repo)(r))
	}
	return r
}

func (repo *WHSchema) Insert(ctx context.Context, whSchema *model.WHSchema) (int64, error) {
	var (
		id  int64
		now = repo.now()
	)

	schemaPayload, err := json.Marshal(whSchema.Schema)
	if err != nil {
		return id, fmt.Errorf("marshaling schema: %w", err)
	}

	err = repo.db.QueryRowContext(ctx, `
		INSERT INTO `+whSchemaTableName+` (
		  wh_upload_id, source_id, namespace, destination_id,
		  destination_type, schema, created_at,
		  updated_at
		)
		VALUES
		  ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (
			source_id, destination_id, namespace
		  ) DO
		UPDATE
		SET
		  schema = $6,
		  updated_at = $7 RETURNING id;
`,
		whSchema.UploadID,
		whSchema.SourceID,
		whSchema.Namespace,
		whSchema.DestinationID,
		whSchema.DestinationType,
		schemaPayload,
		now.UTC(),
		now.UTC(),
	).Scan(&id)
	if err != nil {
		return id, fmt.Errorf("inserting schema: %w", err)
	}

	return id, nil
}

func (repo *WHSchema) GetForNamespace(ctx context.Context, sourceID, destID, namespace string) (model.WHSchema, error) {
	query := `SELECT ` + whSchemaTableColumns + ` FROM ` + whSchemaTableName + `
	WHERE
		source_id = $1 AND
		destination_id = $2 AND
		namespace = $3
	ORDER BY
		id DESC;
	`

	rows, err := repo.db.QueryContext(
		ctx,
		query,
		sourceID,
		destID,
		namespace,
	)
	if err != nil {
		return model.WHSchema{}, fmt.Errorf("querying schemas: %w", err)
	}

	entries, err := repo.parseRows(rows)
	if err != nil {
		return model.WHSchema{}, fmt.Errorf("parsing rows: %w", err)
	}
	if len(entries) == 0 {
		return model.WHSchema{}, nil
	}

	return *entries[0], err
}

func (*WHSchema) parseRows(rows *sql.Rows) ([]*model.WHSchema, error) {
	var whSchemas []*model.WHSchema

	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			whSchema            model.WHSchema
			schemaPayloadRawRaw []byte
		)
		err := rows.Scan(
			&whSchema.ID,
			&whSchema.UploadID,
			&whSchema.SourceID,
			&whSchema.Namespace,
			&whSchema.DestinationID,
			&whSchema.DestinationType,
			&schemaPayloadRawRaw,
			&whSchema.CreatedAt,
			&whSchema.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		whSchema.CreatedAt = whSchema.CreatedAt.UTC()
		whSchema.UpdatedAt = whSchema.UpdatedAt.UTC()

		var schemaPayload model.Schema
		err = json.Unmarshal(schemaPayloadRawRaw, &schemaPayload)
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

func (repo *WHSchema) GetNamespace(ctx context.Context, sourceID, destID string) (string, error) {
	query := `SELECT namespace FROM ` + whSchemaTableName + `
		WHERE
			source_id = $1 AND
			destination_id = $2
		ORDER BY
			id DESC
		LIMIT 1;
	`

	row := repo.db.QueryRowContext(
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

	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("scanning row: %w", err)
	}

	return namespace, nil
}
