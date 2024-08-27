package repo_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func TestWHSchemasRepo(t *testing.T) {
	const (
		sourceID        = "source_id"
		namespace       = "namespace"
		destinationID   = "destination_id"
		destinationType = "destination_type"
		notFound        = "not_found"
	)
	var (
		ctx = context.Background()
		now = time.Now().Truncate(time.Second).UTC()
	)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	var (
		schemaModel = model.Schema{
			"table_name_1": {
				"column_name_1": "string",
				"column_name_2": "int",
				"column_name_3": "boolean",
				"column_name_4": "float",
				"column_name_5": "bigint",
				"column_name_6": "json",
				"column_name_7": "text",
			},
			"table_name_2": {
				"column_name_1": "string",
				"column_name_2": "int",
				"column_name_3": "boolean",
				"column_name_4": "float",
				"column_name_5": "bigint",
				"column_name_6": "json",
				"column_name_7": "text",
			},
		}
		updatedSchemaModel = model.Schema{
			"table_name_3": {
				"column_name_1": "string",
				"column_name_2": "int",
				"column_name_3": "boolean",
				"column_name_4": "float",
				"column_name_5": "bigint",
				"column_name_6": "json",
				"column_name_7": "text",
			},
		}

		schema = model.WHSchema{
			UploadID:        1,
			SourceID:        sourceID,
			Namespace:       namespace,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			Schema:          schemaModel,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		updatedSchema = model.WHSchema{
			UploadID:        1,
			SourceID:        sourceID,
			Namespace:       namespace,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			Schema:          updatedSchemaModel,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
	)

	t.Run("Insert (Old schema)", func(t *testing.T) {
		db := setupDB(t)
		r := repo.NewWHSchemas(db, repo.WithNow(func() time.Time {
			return now
		}))

		_, err := db.ExecContext(ctx, `INSERT INTO wh_schemas (wh_upload_id, source_id, namespace, destination_id, destination_type, schema, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			1, sourceID, namespace, destinationID, destinationType, `{"key": "value"}`, now, now,
		)
		require.NoError(t, err)

		t.Log("new")
		id, err := r.Insert(ctx, &schema)
		require.NoError(t, err)

		r.GetForNamespace(ctx, sourceID, destinationID, namespace)

		schema.ID = id

		t.Log("duplicate")
		_, err = r.Insert(ctx, &schema)
		require.NoError(t, err)

		t.Log("cancelled context")
		_, err = r.Insert(cancelledCtx, &schema)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Insert", func(t *testing.T) {
		db := setupDB(t)
		r := repo.NewWHSchemas(db, repo.WithNow(func() time.Time {
			return now
		}))

		t.Run("duplicate", func(t *testing.T) {
			_, err := r.Insert(ctx, &schema)
			require.NoError(t, err)
			_, err = r.Insert(ctx, &schema)
			require.NoError(t, err)
		})
		t.Run("context cancelled", func(t *testing.T) {
			_, err := r.Insert(cancelledCtx, &schema)
			require.ErrorIs(t, err, context.Canceled)
		})
		t.Run("schema compressed", func(t *testing.T) {
			_, err := r.Insert(ctx, &schema)
			require.NoError(t, err)

			schema, schemaCompressed := schemaAndSchemaCompressed(t, db, sourceID, destinationID)
			require.Empty(t, schema)
			require.NotEmpty(t, schemaCompressed)
			require.Equal(t, schemaModel, schemaCompressed)
		})
		t.Run("old schema", func(t *testing.T) {
			_, err := db.ExecContext(ctx, `INSERT INTO wh_schemas (wh_upload_id, source_id, namespace, destination_id, destination_type, schema, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				1, sourceID, namespace, destinationID, destinationType, `{"key": "value"}`, now, now,
			)
			require.NoError(t, err)

			schema, schemaCompressed := schemaAndSchemaCompressed(t, db, sourceID, destinationID)
			require.NotEmpty(t, schema)
			require.Empty(t, schemaCompressed)
			require.Equal(t,, schema)

			_, err := r.Insert(ctx, &schema)
			require.NoError(t, err)
		})

		expectedSchema, err := r.GetForNamespace(ctx, sourceID, destinationID, namespace)
		require.NoError(t, err)
		require.Equal(t, expectedSchema, newSchema)

		t.Log("duplicate")
		id, err = r.Insert(ctx, &updatedSchema)
		require.NoError(t, err)

		newSchema = updatedSchema
		newSchema.ID = id

		expectedSchema, err = r.GetForNamespace(ctx, sourceID, destinationID, namespace)
		require.NoError(t, err)
		require.Equal(t, expectedSchema, newSchema)
	})

	t.Run("GetForNamespace", func(t *testing.T) {
		t.Run("using schema compressed", func(t *testing.T) {
			db := setupDB(t)
			r := repo.NewWHSchemas(db, repo.WithNow(func() time.Time {
				return now
			}))

			id, err := r.Insert(ctx, &schema)
			require.NoError(t, err)

			newSchema := schema
			newSchema.ID = id

			t.Run("existing", func(t *testing.T) {
				expectedSchema, err := r.GetForNamespace(ctx, sourceID, destinationID, namespace)
				require.NoError(t, err)
				require.Equal(t, expectedSchema, newSchema)
			})

			t.Run("cancelled context", func(t *testing.T) {
				_, err := r.GetForNamespace(cancelledCtx, sourceID, destinationID, namespace)
				require.ErrorIs(t, err, context.Canceled)
			})

			t.Run("not found", func(t *testing.T) {
				expectedSchema, err := r.GetForNamespace(ctx, notFound, notFound, notFound)
				require.NoError(t, err)
				require.Empty(t, expectedSchema)
			})
		})
		t.Run("using schema", func(t *testing.T) {
			db := setupDB(t)
			r := repo.NewWHSchemas(db, repo.WithNow(func() time.Time {
				return now
			}))

			marshalledSchema, err := json.Marshal(schemaModel)
			require.NoError(t, err)

			var id int64
			err = db.QueryRowContext(ctx, `INSERT INTO wh_schemas (wh_upload_id, source_id, namespace, destination_id, destination_type, schema, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
				1, sourceID, namespace, destinationID, destinationType, marshalledSchema, now, now,
			).Scan(&id)
			require.NoError(t, err)

			newSchema := schema
			newSchema.ID = id

			t.Run("existing", func(t *testing.T) {
				expectedSchema, err := r.GetForNamespace(ctx, sourceID, destinationID, namespace)
				require.NoError(t, err)
				require.Equal(t, expectedSchema, newSchema)
			})

			t.Run("cancelled context", func(t *testing.T) {
				_, err := r.GetForNamespace(cancelledCtx, sourceID, destinationID, namespace)
				require.ErrorIs(t, err, context.Canceled)
			})

			t.Run("not found", func(t *testing.T) {
				expectedSchema, err := r.GetForNamespace(ctx, notFound, notFound, notFound)
				require.NoError(t, err)
				require.Empty(t, expectedSchema)
			})
		})
	})

	t.Run("GetNamespace", func(t *testing.T) {
		db := setupDB(t)
		r := repo.NewWHSchemas(db, repo.WithNow(func() time.Time {
			return now
		}))

		_, err := r.Insert(ctx, &schema)
		require.NoError(t, err)

		t.Run("existing", func(t *testing.T) {
			expectedNamespace, err := r.GetNamespace(ctx, sourceID, destinationID)
			require.NoError(t, err)
			require.Equal(t, expectedNamespace, namespace)
		})

		t.Run("cancelled context", func(t *testing.T) {
			_, err = r.GetNamespace(cancelledCtx, sourceID, destinationID)
			require.ErrorIs(t, err, context.Canceled)
		})

		t.Run("not found", func(t *testing.T) {
			expectedNamespace, err := r.GetNamespace(ctx, notFound, notFound)
			require.NoError(t, err)
			require.Empty(t, expectedNamespace)
		})
	})

	t.Run("GetTablesForConnection", func(t *testing.T) {
		t.Run("using schema compressed", func(t *testing.T) {
			db := setupDB(t)
			r := repo.NewWHSchemas(db, repo.WithNow(func() time.Time {
				return now
			}))

			_, err := r.Insert(ctx, &schema)
			require.NoError(t, err)

			t.Run("existing", func(t *testing.T) {
				expectedTableNames, err := r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{
					{SourceID: sourceID, DestinationID: destinationID},
				})
				require.NoError(t, err)
				require.Len(t, expectedTableNames, 1)
				require.Equal(t, sourceID, expectedTableNames[0].SourceID)
				require.Equal(t, destinationID, expectedTableNames[0].DestinationID)
				require.Equal(t, namespace, expectedTableNames[0].Namespace)
				require.ElementsMatch(t, []string{"table_name_1", "table_name_2"}, expectedTableNames[0].Tables)
			})
			t.Run("cancelled context", func(t *testing.T) {
				_, err = r.GetTablesForConnection(cancelledCtx, []warehouseutils.SourceIDDestinationID{
					{SourceID: sourceID, DestinationID: destinationID},
				})
				require.ErrorIs(t, err, context.Canceled)
			})
			t.Run("not found", func(t *testing.T) {
				expectedTableNames, err := r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{
					{SourceID: notFound, DestinationID: notFound}},
				)
				require.NoError(t, err)
				require.Empty(t, expectedTableNames)
			})
			t.Run("empty", func(t *testing.T) {
				_, err = r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{})
				require.EqualError(t, err, errors.New("no source id and destination id pairs provided").Error())
			})
			t.Run("multiple", func(t *testing.T) {
				latestNamespace := "latest_namespace"
				latestSchema := model.WHSchema{
					UploadID:        2,
					SourceID:        sourceID,
					Namespace:       latestNamespace,
					DestinationID:   destinationID,
					DestinationType: destinationType,
					Schema:          schemaModel,
					CreatedAt:       now,
					UpdatedAt:       now,
				}

				_, err = r.Insert(ctx, &latestSchema)
				require.NoError(t, err)

				expectedTableNames, err := r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{
					{SourceID: sourceID, DestinationID: destinationID},
				})
				require.NoError(t, err)
				require.Len(t, expectedTableNames, 1)
				require.Equal(t, sourceID, expectedTableNames[0].SourceID)
				require.Equal(t, destinationID, expectedTableNames[0].DestinationID)
				require.Equal(t, latestNamespace, expectedTableNames[0].Namespace)
				require.ElementsMatch(t, []string{"table_name_1", "table_name_2"}, expectedTableNames[0].Tables)
			})
		})
		t.Run("using schema", func(t *testing.T) {
			db := setupDB(t)
			r := repo.NewWHSchemas(db, repo.WithNow(func() time.Time {
				return now
			}))

			marshalledSchema, err := json.Marshal(schemaModel)
			require.NoError(t, err)

			_, err = db.ExecContext(ctx, `INSERT INTO wh_schemas (wh_upload_id, source_id, namespace, destination_id, destination_type, schema, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				1, sourceID, namespace, destinationID, destinationType, marshalledSchema, now, now,
			)
			require.NoError(t, err)

			t.Run("existing", func(t *testing.T) {
				expectedTableNames, err := r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{
					{SourceID: sourceID, DestinationID: destinationID},
				})
				require.NoError(t, err)
				require.Len(t, expectedTableNames, 1)
				require.Equal(t, sourceID, expectedTableNames[0].SourceID)
				require.Equal(t, destinationID, expectedTableNames[0].DestinationID)
				require.Equal(t, namespace, expectedTableNames[0].Namespace)
				require.ElementsMatch(t, []string{"table_name_1", "table_name_2"}, expectedTableNames[0].Tables)
			})
			t.Run("cancelled context", func(t *testing.T) {
				_, err = r.GetTablesForConnection(cancelledCtx, []warehouseutils.SourceIDDestinationID{
					{SourceID: sourceID, DestinationID: destinationID},
				})
				require.ErrorIs(t, err, context.Canceled)
			})
			t.Run("not found", func(t *testing.T) {
				expectedTableNames, err := r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{
					{SourceID: notFound, DestinationID: notFound}},
				)
				require.NoError(t, err)
				require.Empty(t, expectedTableNames)
			})
			t.Run("empty", func(t *testing.T) {
				_, err = r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{})
				require.EqualError(t, err, errors.New("no source id and destination id pairs provided").Error())
			})
			t.Run("multiple", func(t *testing.T) {
				latestNamespace := "latest_namespace"
				latestSchema := model.WHSchema{
					UploadID:        2,
					SourceID:        sourceID,
					Namespace:       latestNamespace,
					DestinationID:   destinationID,
					DestinationType: destinationType,
					Schema:          schemaModel,
					CreatedAt:       now,
					UpdatedAt:       now,
				}

				_, err = r.Insert(ctx, &latestSchema)
				require.NoError(t, err)

				expectedTableNames, err := r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{
					{SourceID: sourceID, DestinationID: destinationID},
				})
				require.NoError(t, err)
				require.Len(t, expectedTableNames, 1)
				require.Equal(t, sourceID, expectedTableNames[0].SourceID)
				require.Equal(t, destinationID, expectedTableNames[0].DestinationID)
				require.Equal(t, latestNamespace, expectedTableNames[0].Namespace)
				require.ElementsMatch(t, []string{"table_name_1", "table_name_2"}, expectedTableNames[0].Tables)
			})
		})
	})
}

func schemaAndSchemaCompressed(t *testing.T, db *sqlmiddleware.DB, soureID, destinationID string) (schema, schemaCompressed model.Schema) {
	var (
		schemaRaw           []byte
		schemaCompressedRaw []byte
	)
	err := db.QueryRowContext(
		context.Background(),
		`SELECT schema, schema_compressed FROM wh_schemas WHERE source_id = $1 AND destination_id = $2 AND namespace = $3 ORDER BY id DESC LIMIT 1`,
		soureID, destinationID, "namespace",
	).Scan(
		&schemaRaw,
		&schemaCompressedRaw,
	)
	require.NoError(t, err)

	if len(schemaRaw) > 0 {
		err = json.Unmarshal(schemaRaw, &schema)
		require.NoError(t, err)
	}
	if len(schemaCompressedRaw) > 0 {
		schemaDecompressed, err := warehouseutils.Decompress(schemaCompressedRaw)
		require.NoError(t, err)
		err = json.Unmarshal(schemaDecompressed, &schemaCompressed)
		require.NoError(t, err)
	}
	return
}
