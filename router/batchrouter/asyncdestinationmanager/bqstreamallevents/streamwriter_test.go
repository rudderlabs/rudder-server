package bqstreamallevents

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	bqhelper "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestStreamWriterIntegration(t *testing.T) {
	if _, exists := os.LookupEnv(bqhelper.TestKey); !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", bqhelper.TestKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), bqhelper.TestKey)
	}

	credentials, err := bqhelper.GetBQTestCredentials()
	require.NoError(t, err)

	ctx := context.Background()
	namespace := "bqstreamv2_swtest_" + whutils.RandHex()
	tableName := "pages"
	tableSchema := whutils.ModelTableSchema{"id": "string", "received_at": "datetime", "val": "int"}

	bqClient, err := bigquery.NewClient(
		ctx,
		credentials.ProjectID,
		option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(credentials.Credentials)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bqClient.Close() })

	dataset := bqClient.Dataset(namespace)
	require.NoError(t, dataset.Create(ctx, &bigquery.DatasetMetadata{Location: credentials.Location}))
	t.Cleanup(func() {
		if err := dataset.DeleteWithContents(ctx); err != nil {
			t.Logf("error deleting dataset: %v", err)
		}
	})
	require.NoError(t, dataset.Table(tableName).Create(ctx, &bigquery.TableMetadata{Schema: toBigQuerySchema(tableSchema)}))

	factory := NewTableStreamWriterFactory(10, 10*1024*1024)

	t.Run("incompatible credentials", func(t *testing.T) {
		_, err := factory.NewTableStreamWriter(ctx, destConfig{
			ProjectID:   credentials.ProjectID,
			Namespace:   namespace,
			Credentials: "{}",
		}, tableName, tableSchema)
		require.Error(t, err)
	})

	t.Run("append rows end-to-end", func(t *testing.T) {
		tableStreamWriter, err := factory.NewTableStreamWriter(ctx, destConfig{
			ProjectID:   credentials.ProjectID,
			Namespace:   namespace,
			Credentials: credentials.Credentials,
		}, tableName, tableSchema)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tableStreamWriter.Close() })

		md, err := descriptorForSchema(tableSchema)
		require.NoError(t, err)

		encoded, err := encodeRows([]Row{
			{"id": "1", "received_at": "2025-01-02T03:04:05.000Z", "val": 10},
			{"id": "2", "received_at": "2025-01-02T03:04:06.000Z", "val": 20},
		}, md, tableSchema)
		require.NoError(t, err)

		err = tableStreamWriter.AppendRows(ctx, encoded)
		require.NoError(t, err)

		records := bqhelper.RetrieveRecordsFromWarehouse(t, bqClient,
			fmt.Sprintf("SELECT id, val FROM `%s`.`%s` ORDER BY id;", namespace, tableName),
		)
		require.Equal(t, [][]string{{"1", "10"}, {"2", "20"}}, records)
	})
}
