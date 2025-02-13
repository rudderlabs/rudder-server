package repo_test

import (
	"context"
	"errors"
	"testing"
	"time"

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
		r   = repo.NewWHSchemas(db, repo.WithNow(func() time.Time {
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
		id, err := r.Insert(ctx, &schema)
		require.NoError(t, err)

		schema.ID = id

		t.Log("duplicate")
		_, err = r.Insert(ctx, &schema)
		require.NoError(t, err)

		t.Log("cancelled context")
		_, err = r.Insert(cancelledCtx, &schema)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("GetForNamespace", func(t *testing.T) {
		t.Log("existing")
		expectedSchema, err := r.GetForNamespace(ctx, sourceID, destinationID, namespace)
		require.NoError(t, err)
		require.Equal(t, expectedSchema, schema)

		t.Log("cancelled context")
		_, err = r.GetForNamespace(cancelledCtx, sourceID, destinationID, namespace)
		require.ErrorIs(t, err, context.Canceled)

		t.Log("not found")
		expectedSchema, err = r.GetForNamespace(ctx, notFound, notFound, notFound)
		require.NoError(t, err)
		require.Empty(t, expectedSchema)
	})

	t.Run("GetNamespace", func(t *testing.T) {
		t.Log("existing")
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
		t.Log("existing")
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
		_, err = r.Insert(ctx, &schemaLatest)
		require.NoError(t, err)
		expectedTableNames, err = r.GetTablesForConnection(ctx, []warehouseutils.SourceIDDestinationID{connection})
		require.NoError(t, err)
		require.Len(t, expectedTableNames, 1)
		require.Equal(t, sourceID, expectedTableNames[0].SourceID)
		require.Equal(t, destinationID, expectedTableNames[0].DestinationID)
		require.Equal(t, latestNamespace, expectedTableNames[0].Namespace)
		require.ElementsMatch(t, []string{"table_name_1", "table_name_2"}, expectedTableNames[0].Tables)
	})
}
