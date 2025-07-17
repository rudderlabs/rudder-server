package repo_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func TestWHSchemasRepo(t *testing.T) {
	var (
		ctx = context.Background()
		now = time.Now().Truncate(time.Second).UTC()
		db  = setupDB(t)
		r   = repo.NewWHSchemas(db, config.New(), repo.WithNow(func() time.Time {
			return now
		}))
	)

	const (
		sourceID        = "source_id"
		namespace       = "namespace"
		destinationID   = "destination_id"
		destinationType = "destination_type"
		notFound        = "not_found"
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
		schema = model.WHSchema{
			SourceID:        sourceID,
			Namespace:       namespace,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			Schema:          schemaModel,
			CreatedAt:       now,
			UpdatedAt:       now,
			ExpiresAt:       now.Add(time.Hour),
		}
	)

	t.Run("Insert", func(t *testing.T) {
		t.Log("new")
		err := r.Insert(ctx, &schema)
		require.NoError(t, err)

		t.Log("duplicate")
		err = r.Insert(ctx, &schema)
		require.NoError(t, err)

		t.Log("cancelled context")
		err = r.Insert(cancelledCtx, &schema)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("GetForNamespace", func(t *testing.T) {
		expectedSchema, err := r.GetForNamespace(ctx, destinationID, namespace)
		require.NoError(t, err)
		require.Equal(t, sourceID, expectedSchema.SourceID)
		require.Equal(t, namespace, expectedSchema.Namespace)
		require.Equal(t, destinationID, expectedSchema.DestinationID)
		require.Equal(t, destinationType, expectedSchema.DestinationType)
		require.Equal(t, schemaModel, expectedSchema.Schema)
		require.Equal(t, now, expectedSchema.CreatedAt)
		require.Equal(t, now, expectedSchema.UpdatedAt)
		require.Equal(t, now.Add(time.Hour), expectedSchema.ExpiresAt)

		t.Log("cancelled context")
		_, err = r.GetForNamespace(cancelledCtx, destinationID, namespace)
		require.ErrorIs(t, err, context.Canceled)

		t.Log("not found")
		expectedSchema, err = r.GetForNamespace(ctx, notFound, notFound)
		require.NoError(t, err)
		require.Empty(t, expectedSchema)
	})

	t.Run("GetNamespace", func(t *testing.T) {
		expectedNamespace, err := r.GetNamespace(ctx, sourceID, destinationID)
		require.NoError(t, err)
		require.Equal(t, expectedNamespace, namespace)

		t.Log("cancelled context")
		_, err = r.GetNamespace(cancelledCtx, sourceID, destinationID)
		require.ErrorIs(t, err, context.Canceled)

		t.Log("not found")
		expectedNamespace, err = r.GetNamespace(ctx, notFound, notFound)
		require.NoError(t, err)
		require.Empty(t, expectedNamespace, expectedNamespace)
	})

	t.Run("GetTablesForConnection", func(t *testing.T) {
		connection := warehouseutils.SourceIDDestinationID{SourceID: sourceID, DestinationID: destinationID}
		expectedTableNames, err := r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{connection})
		require.NoError(t, err)
		require.Len(t, expectedTableNames, 1)
		require.Equal(t, sourceID, expectedTableNames[0].SourceID)
		require.Equal(t, destinationID, expectedTableNames[0].DestinationID)
		require.Equal(t, namespace, expectedTableNames[0].Namespace)
		require.ElementsMatch(t, []string{"table_name_1", "table_name_2"}, expectedTableNames[0].Tables)

		t.Log("cancelled context")
		_, err = r.GetTablesForConnection(cancelledCtx, []warehouseutils.SourceIDDestinationID{connection})
		require.ErrorIs(t, err, context.Canceled)

		t.Log("not found")
		expectedTableNames, err = r.GetTablesForConnection(ctx,
			[]warehouseutils.SourceIDDestinationID{{SourceID: notFound, DestinationID: notFound}})
		require.NoError(t, err)
		require.Empty(t, expectedTableNames, expectedTableNames)

		t.Log("empty")
		_, err = r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{})
		require.EqualError(t, err, errors.New("no source id and destination id pairs provided").Error())

		t.Log("multiple")
		latestNamespace := "latest_namespace"
		schemaLatest := model.WHSchema{
			SourceID:        sourceID,
			Namespace:       latestNamespace,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			Schema:          schemaModel,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		err = r.Insert(ctx, &schemaLatest)
		require.NoError(t, err)
		expectedTableNames, err = r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{connection})
		require.NoError(t, err)
		require.Len(t, expectedTableNames, 1)
		require.Equal(t, sourceID, expectedTableNames[0].SourceID)
		require.Equal(t, destinationID, expectedTableNames[0].DestinationID)
		require.Equal(t, latestNamespace, expectedTableNames[0].Namespace)
		require.ElementsMatch(t, []string{"table_name_1", "table_name_2"}, expectedTableNames[0].Tables)
	})

	t.Run("SetExpiryForDestination", func(t *testing.T) {
		err := r.SetExpiryForDestination(ctx, destinationID, now)
		require.NoError(t, err)

		err = r.Insert(ctx, &schema)
		require.NoError(t, err)

		expiryTime := now.Add(2 * time.Hour)
		err = r.SetExpiryForDestination(ctx, destinationID, expiryTime)
		require.NoError(t, err)

		updatedSchema, err := r.GetForNamespace(ctx, destinationID, namespace)
		require.NoError(t, err)
		require.Equal(t, expiryTime, updatedSchema.ExpiresAt)
	})

	t.Run("Insert schema propagation to all connections with same destination_id and namespace", func(t *testing.T) {
		// Create first connection schema
		firstConnectionSchema := schema
		err := r.Insert(ctx, &firstConnectionSchema)
		require.NoError(t, err)

		// Create second connection schema with same destination_id and namespace
		secondConnectionSchema := firstConnectionSchema
		secondConnectionSchema.SourceID = "other_source_id"
		secondConnectionSchema.ID = 0 // Reset ID for new insert
		err = r.Insert(ctx, &secondConnectionSchema)
		require.NoError(t, err)

		// Verify both connections have the same initial schema
		firstRetrieved, err := r.GetForNamespace(ctx, firstConnectionSchema.DestinationID, firstConnectionSchema.Namespace)
		require.NoError(t, err)
		require.Equal(t, firstConnectionSchema.Schema, firstRetrieved.Schema)

		secondRetrieved, err := r.GetForNamespace(ctx, secondConnectionSchema.DestinationID, secondConnectionSchema.Namespace)
		require.NoError(t, err)
		require.Equal(t, firstConnectionSchema.Schema, secondRetrieved.Schema)

		// Update the first connection with new schema data
		updatedSchema := firstConnectionSchema
		updatedSchema.Schema = model.Schema{
			"new_table": {
				"new_column": "string",
			},
		}
		updatedSchema.ID = 0 // Reset ID for new insert
		err = r.Insert(ctx, &updatedSchema)
		require.NoError(t, err)

		// Verify both connections are updated with the new schema
		firstRetrieved, err = r.GetForNamespace(ctx, firstConnectionSchema.DestinationID, firstConnectionSchema.Namespace)
		require.NoError(t, err)
		require.Equal(t, updatedSchema.Schema, firstRetrieved.Schema)

		secondRetrieved, err = r.GetForNamespace(ctx, secondConnectionSchema.DestinationID, secondConnectionSchema.Namespace)
		require.NoError(t, err)
		require.Equal(t, updatedSchema.Schema, secondRetrieved.Schema)
	})

	t.Run("Insert preserves and updates expiresAt as expected", func(t *testing.T) {
		// Insert initial schema with a non-zero expiresAt
		initialExpiresAt := now.Add(3 * time.Hour)
		initialSchema := schema
		initialSchema.ExpiresAt = initialExpiresAt
		_, err := r.Insert(ctx, &initialSchema)
		require.NoError(t, err)

		retrieved, err := r.GetForNamespace(ctx, destinationID, namespace)
		require.NoError(t, err)
		require.Equal(t, initialExpiresAt, retrieved.ExpiresAt)

		// Update schema with zero time for ExpiresAt (should preserve initialExpiresAt)
		updatedSchema := initialSchema
		updatedSchema.Schema = model.Schema{"table_name_1": {"column_name_1": "int"}} // change schema
		updatedSchema.ExpiresAt = time.Time{}                                         // sentinel value
		_, err = r.Insert(ctx, &updatedSchema)
		require.NoError(t, err)

		// Check that expiresAt is preserved
		retrieved, err = r.GetForNamespace(ctx, destinationID, namespace)
		require.NoError(t, err)
		require.Equal(t, initialExpiresAt, retrieved.ExpiresAt, "expiresAt should be preserved when zero time is passed")
		require.Equal(t, updatedSchema.Schema, retrieved.Schema, "schema should be updated")

		// Update schema with a new non-zero expiresAt (should update expiresAt)
		newExpiresAt := now.Add(5 * time.Hour)
		updatedSchema.ExpiresAt = newExpiresAt
		_, err = r.Insert(ctx, &updatedSchema)
		require.NoError(t, err)

		// Check that expiresAt is updated
		retrieved, err = r.GetForNamespace(ctx, destinationID, namespace)
		require.NoError(t, err)
		require.Equal(t, newExpiresAt, retrieved.ExpiresAt, "expiresAt should be updated when non-zero time is passed")
	})
}

func TestWHSchemasRepoWithTableLevel(t *testing.T) {
	conf := config.New()
	conf.Set("Warehouse.enableTableLevelSchema", true)

	var (
		ctx = context.Background()
		now = time.Now().Truncate(time.Second).UTC()
		db  = setupDB(t)
		r   = repo.NewWHSchemas(db, conf, repo.WithNow(func() time.Time {
			return now
		}))
	)

	const (
		sourceID        = "source_id"
		namespace       = "namespace"
		destinationID   = "destination_id"
		destinationType = "destination_type"
		notFound        = "not_found"
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
		schema = model.WHSchema{
			SourceID:        sourceID,
			Namespace:       namespace,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			Schema:          schemaModel,
			CreatedAt:       now,
			UpdatedAt:       now,
			ExpiresAt:       now.Add(time.Hour),
		}
	)

	t.Run("Insert", func(t *testing.T) {
		t.Log("new")
		err := r.Insert(ctx, &schema)
		require.NoError(t, err)

		t.Log("duplicate")
		err = r.Insert(ctx, &schema)
		require.NoError(t, err)

		t.Log("cancelled context")
		err = r.Insert(cancelledCtx, &schema)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("GetForNamespace (already populated)", func(t *testing.T) {
		expectedSchema, err := r.GetForNamespace(ctx, destinationID, namespace)
		require.NoError(t, err)
		require.Equal(t, sourceID, expectedSchema.SourceID)
		require.Equal(t, namespace, expectedSchema.Namespace)
		require.Equal(t, destinationID, expectedSchema.DestinationID)
		require.Equal(t, destinationType, expectedSchema.DestinationType)
		require.Equal(t, schemaModel, expectedSchema.Schema)
		require.Equal(t, now, expectedSchema.CreatedAt)
		require.Equal(t, now, expectedSchema.UpdatedAt)
		require.Equal(t, now.Add(time.Hour), expectedSchema.ExpiresAt)

		t.Log("cancelled context")
		_, err = r.GetForNamespace(cancelledCtx, destinationID, namespace)
		require.ErrorIs(t, err, context.Canceled)

		t.Log("not found")
		expectedSchema, err = r.GetForNamespace(ctx, notFound, notFound)
		require.NoError(t, err)
		require.Empty(t, expectedSchema)
	})

	t.Run("GetForNamespace (not already populated)", func(t *testing.T) {
		rs1 := repo.NewWHSchemas(db, config.New(), repo.WithNow(func() time.Time {
			return now
		}))
		err := rs1.Insert(ctx, &model.WHSchema{
			SourceID:        "source_id_1",
			Namespace:       "namespace_1",
			DestinationID:   "destination_id_1",
			DestinationType: destinationType,
			Schema:          schemaModel,
			CreatedAt:       now,
			UpdatedAt:       now,
			ExpiresAt:       now.Add(time.Hour),
		})
		require.NoError(t, err)

		rs2 := repo.NewWHSchemas(db, conf, repo.WithNow(func() time.Time {
			return now
		}))
		expectedSchema, err := rs2.GetForNamespace(ctx, "destination_id_1", "namespace_1")
		require.NoError(t, err)
		require.Equal(t, "source_id_1", expectedSchema.SourceID)
		require.Equal(t, "namespace_1", expectedSchema.Namespace)
		require.Equal(t, "destination_id_1", expectedSchema.DestinationID)
		require.Equal(t, destinationType, expectedSchema.DestinationType)
		require.Equal(t, schemaModel, expectedSchema.Schema)
		require.Equal(t, now, expectedSchema.CreatedAt)
		require.Equal(t, now, expectedSchema.UpdatedAt)
		require.Equal(t, now.Add(time.Hour), expectedSchema.ExpiresAt)

		// Verify that the table-level schemas are populated
		tableLevelSchemasQuery, err := db.QueryContext(ctx, "SELECT table_name, schema FROM wh_schemas WHERE destination_id = $1 AND namespace = $2 AND table_name != '';", "destination_id_1", "namespace_1")
		require.NoError(t, err)
		defer func() { _ = tableLevelSchemasQuery.Close() }()

		type TableLevelSchema struct {
			TableName string
			Schema    model.TableSchema
		}

		var tableLevelSchemasResult []TableLevelSchema
		var tableLevelSchema TableLevelSchema
		for tableLevelSchemasQuery.Next() {
			var schemaRaw json.RawMessage
			err = tableLevelSchemasQuery.Scan(&tableLevelSchema.TableName, &schemaRaw)
			require.NoError(t, err)
			err = jsonrs.Unmarshal(schemaRaw, &tableLevelSchema.Schema)
			require.NoError(t, err)
			tableLevelSchemasResult = append(tableLevelSchemasResult, tableLevelSchema)
		}
		require.ElementsMatch(t, []TableLevelSchema{
			{TableName: "table_name_1", Schema: schemaModel["table_name_1"]},
			{TableName: "table_name_2", Schema: schemaModel["table_name_2"]},
		}, tableLevelSchemasResult)
	})
}
