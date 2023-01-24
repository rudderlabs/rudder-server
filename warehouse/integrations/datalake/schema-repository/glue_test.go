package schemarepository

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/glue"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestGlueSchemaRepositoryRoundTrip(t *testing.T) {
	type S3Credentials struct {
		AccessKeyID string
		AccessKey   string
		Region      string
		Bucket      string
	}

	var (
		credentialsStr string
		credentials    S3Credentials
		err            error
		credentialsEnv = "S3_DATALAKE_TEST_CREDENTIALS"
		testNamespace  = fmt.Sprintf("test_namespace_%s", warehouseutils.RandHex())
		testTable      = fmt.Sprintf("test_table_%s", warehouseutils.RandHex())
		testFile       = "testdata/load.parquet"
		testColumns    = map[string]string{
			"id":                  "string",
			"received_at":         "datetime",
			"test_array_bool":     "array(boolean)",
			"test_array_datetime": "array(datetime)",
			"test_array_float":    "array(float)",
			"test_array_int":      "array(int)",
			"test_array_string":   "array(string)",
			"test_bool":           "boolean",
			"test_datetime":       "datetime",
			"test_float":          "float",
			"test_int":            "int",
			"test_string":         "string",
		}
	)

	if credentialsStr = os.Getenv(credentialsEnv); credentialsStr == "" {
		t.Skipf("Skipping %s as %s is not set", t.Name(), credentialsEnv)
	}

	err = json.Unmarshal([]byte(credentialsStr), &credentials)
	require.NoError(t, err)

	destination := backendconfig.DestinationT{
		Config: map[string]interface{}{
			"region":      credentials.Region,
			"bucketName":  credentials.Bucket,
			"accessKeyID": credentials.AccessKeyID,
			"accessKey":   credentials.AccessKey,
		},
	}
	warehouse := warehouseutils.Warehouse{
		Destination: destination,
		Namespace:   testNamespace,
	}

	misc.Init()
	warehouseutils.Init()

	g, err := NewGlueSchemaRepository(warehouse)
	require.NoError(t, err)

	t.Logf("Creating schema %s", testNamespace)
	err = g.CreateSchema()
	require.NoError(t, err)

	t.Log("Creating already existing schema should not fail")
	err = g.CreateSchema()
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Log("Cleaning up")
		_, err = g.GlueClient.DeleteDatabase(&glue.DeleteDatabaseInput{
			Name: &testNamespace,
		})
		require.NoError(t, err)
	})

	t.Logf("Creating table %s", testTable)
	err = g.CreateTable(testTable, testColumns)
	require.NoError(t, err)

	t.Log("Creating already existing table should not fail")
	err = g.CreateTable(testTable, testColumns)
	require.NoError(t, err)

	t.Log("Adding columns to table")
	err = g.AddColumns(testTable, []warehouseutils.ColumnInfo{
		{Name: "alter_test_bool", Type: "boolean"},
		{Name: "alter_test_string", Type: "string"},
		{Name: "alter_test_int", Type: "int"},
		{Name: "alter_test_float", Type: "float"},
		{Name: "alter_test_datetime", Type: "datetime"},
	})

	t.Log("Preparing load files metadata")
	f, err := os.Open(testFile)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = f.Close()
	})

	fmFactory := filemanager.FileManagerFactoryT{}
	fm, err := fmFactory.New(&filemanager.SettingsT{
		Provider: warehouseutils.S3,
		Config: map[string]any{
			"bucketName":      credentials.Bucket,
			"accessKeyID":     credentials.AccessKeyID,
			"secretAccessKey": credentials.AccessKey,
			"region":          credentials.Region,
		},
	})
	require.NoError(t, err)

	uploadOutput, err := fm.Upload(context.TODO(), f, fmt.Sprintf("rudder-test-payload/s3-datalake/%s/dt=2006-01-02/", warehouseutils.RandHex()))
	require.NoError(t, err)

	err = g.RefreshPartitions(testTable, []warehouseutils.LoadFileT{
		{
			Location: uploadOutput.Location,
		},
	})
	require.NoError(t, err)
}
