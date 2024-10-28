package schemarepository

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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
		credentialsEnv = "TEST_S3_DATALAKE_CREDENTIALS"
		testFile       = "testdata/load.parquet"
		testColumns    = model.TableSchema{
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

	testCases := []struct {
		name         string
		config       map[string]interface{}
		windowLayout string
	}{
		{
			name: "s3 datalake with glue",
			config: map[string]interface{}{
				"region":      credentials.Region,
				"bucketName":  credentials.Bucket,
				"accessKeyID": credentials.AccessKeyID,
				"accessKey":   credentials.AccessKey,
				"useGlue":     true,
			},
			windowLayout: warehouseutils.DatalakeTimeWindowFormat,
		},
		{
			name: "s3 datalake with glue and layout",
			config: map[string]interface{}{
				"region":           credentials.Region,
				"bucketName":       credentials.Bucket,
				"accessKeyID":      credentials.AccessKeyID,
				"accessKey":        credentials.AccessKey,
				"useGlue":          true,
				"timeWindowLayout": "dt=2006-01-02",
			},
			windowLayout: "dt=2006-01-02",
		},
		{
			name: "invalid window layout",
			config: map[string]interface{}{
				"region":           credentials.Region,
				"bucketName":       credentials.Bucket,
				"accessKeyID":      credentials.AccessKeyID,
				"accessKey":        credentials.AccessKey,
				"useGlue":          true,
				"timeWindowLayout": "dt=2006-01-02",
			},
			windowLayout: warehouseutils.DatalakeTimeWindowFormat,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			var (
				testNamespace = fmt.Sprintf("test_namespace_%s", warehouseutils.RandHex())
				testTable     = fmt.Sprintf("test_table_%s", warehouseutils.RandHex())
			)

			dest := backendconfig.DestinationT{
				Config: tc.config,
			}
			warehouse := model.Warehouse{
				Destination: dest,
				Namespace:   testNamespace,
			}

			misc.Init()
			warehouseutils.Init()

			ctx := context.Background()

			g, err := NewGlueSchemaRepository(config.New(), logger.NOP, warehouse)
			require.NoError(t, err)

			t.Logf("Creating schema %s", testNamespace)
			err = g.CreateSchema(ctx)
			require.NoError(t, err)

			t.Log("Creating already existing schema should not fail")
			err = g.CreateSchema(ctx)
			require.NoError(t, err)

			t.Cleanup(func() {
				t.Log("Cleaning up")
				_, err = g.GlueClient.DeleteDatabase(&glue.DeleteDatabaseInput{
					Name: &testNamespace,
				})
				require.NoError(t, err)
			})

			t.Logf("Creating table %s", testTable)
			err = g.CreateTable(ctx, testTable, testColumns)
			require.NoError(t, err)

			t.Log("Creating already existing table should not fail")
			err = g.CreateTable(ctx, testTable, testColumns)
			require.NoError(t, err)

			t.Log("Adding columns to table")
			err = g.AddColumns(ctx, testTable, []warehouseutils.ColumnInfo{
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

			fm, err := filemanager.New(&filemanager.Settings{
				Provider: warehouseutils.S3,
				Config: map[string]any{
					"bucketName":      credentials.Bucket,
					"accessKeyID":     credentials.AccessKeyID,
					"secretAccessKey": credentials.AccessKey,
					"region":          credentials.Region,
				},
			})
			require.NoError(t, err)

			uploadOutput, err := fm.Upload(ctx, f, fmt.Sprintf("rudder-test-payload/s3-datalake/%s/%s/", warehouseutils.RandHex(), tc.windowLayout))
			require.NoError(t, err)

			err = g.RefreshPartitions(ctx, testTable, []warehouseutils.LoadFile{
				{
					Location: uploadOutput.Location,
				},
			})
			require.NoError(t, err)
		})
	}
}
