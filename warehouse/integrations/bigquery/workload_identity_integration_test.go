package bigquery_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	whbigquery "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	bqhelper "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/testhelper"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

const workloadIdentityTestKey = "BIGQUERY_WIF_INTEGRATION_TEST_CREDENTIALS"

// workloadIdentityTestCredentials describes the CI federation setup: GCP pools whose AWS providers trust
// AWSRoleARN, with access granted only to WorkspaceID. AWS credentials able to assume AWSRoleARN come from
// the environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).
type workloadIdentityTestCredentials struct {
	ProjectID     string `json:"projectID"`
	Location      string `json:"location"`
	BucketName    string `json:"bucketName"`
	ProjectNumber string `json:"projectNumber"`
	WorkspaceID   string `json:"workspaceID"`
	AWSRoleARN    string `json:"awsRoleARN"`
	// ServiceAccount is a pool whose principal may impersonate TargetServiceAccount.
	ServiceAccount struct {
		PoolID               string `json:"poolID"`
		ProviderID           string `json:"providerID"`
		TargetServiceAccount string `json:"targetServiceAccount"`
	} `json:"serviceAccount"`
	// FederatedIdentities is a pool whose principal is granted BigQuery and Cloud Storage roles directly.
	FederatedIdentities struct {
		PoolID     string `json:"poolID"`
		ProviderID string `json:"providerID"`
	} `json:"federatedIdentities"`
}

func TestWorkloadIdentityFederationIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	raw, exists := os.LookupEnv(workloadIdentityTestKey)
	if !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", workloadIdentityTestKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), workloadIdentityTestKey)
	}
	var credentials workloadIdentityTestCredentials
	require.NoError(t, jsonrs.Unmarshal([]byte(raw), &credentials))

	misc.Init()
	validations.Init()
	whutils.Init()

	// the dedicated role path: CI has static AWS keys, which the pod IRSA fallback would reject
	t.Setenv("RUDDER_GCP_FEDERATION_AWS_ROLE_ARN", credentials.AWSRoleARN)

	destType := whutils.BQ
	schema := model.TableSchema{
		"test_bool":     "boolean",
		"test_datetime": "datetime",
		"test_float":    "float",
		"test_int":      "int",
		"test_string":   "string",
		"id":            "string",
		"received_at":   "datetime",
	}

	newWarehouse := func(workspaceID, poolID, providerID, targetServiceAccount, namespace string) model.Warehouse {
		return model.Warehouse{
			Source: backendconfig.SourceT{ID: "test_source_id"},
			Destination: backendconfig.DestinationT{
				ID:                    "test_destination_id",
				DestinationDefinition: backendconfig.DestinationDefinitionT{Name: destType},
				Config: map[string]any{
					"project":                              credentials.ProjectID,
					"location":                             credentials.Location,
					"bucketName":                           credentials.BucketName,
					"namespace":                            namespace,
					"authMethod":                           "workloadIdentityFederation",
					"workloadIdentityProjectNumber":        credentials.ProjectNumber,
					"workloadIdentityPoolId":               poolID,
					"workloadIdentityProviderId":           providerID,
					"workloadIdentityTargetServiceAccount": targetServiceAccount,
				},
			},
			WorkspaceID: workspaceID,
			Namespace:   namespace,
		}
	}

	// loadThroughFederation stages a load file and loads it into a new table, authenticating to both
	// Cloud Storage and BigQuery only through workload identity federation.
	loadThroughFederation := func(t *testing.T, warehouse model.Warehouse) {
		t.Helper()
		ctx := context.Background()
		tableName := "workload_identity_test_table"

		// staging upload through the GCS manager, with the workspace injected like the server does
		fm, err := filemanager.New(&filemanager.Settings{
			Provider: whutils.GCS,
			Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
				Provider:    whutils.GCS,
				Config:      warehouse.Destination.Config,
				WorkspaceID: warehouse.WorkspaceID,
			}),
			Conf: config.Default,
		})
		require.NoError(t, err)
		uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

		loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
		bq := whbigquery.New(config.New(), logger.NOP)
		require.NoError(t, bq.Setup(ctx, warehouse, newMockUploader(t, loadFiles, tableName, schema, schema)))

		db, err := whbigquery.New(config.New(), logger.NOP).Connect(ctx, warehouse)
		require.NoError(t, err)
		t.Cleanup(func() { dropSchema(t, db.BQ, warehouse.Namespace) })

		require.NoError(t, bq.CreateSchema(ctx))
		require.NoError(t, bq.CreateTable(ctx, tableName, schema))

		// LoadTable also fetches job statistics, reusing the federated auth options
		loadTableStat, err := bq.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, int64(14), loadTableStat.RowsInserted)

		records := bqhelper.RetrieveRecordsFromWarehouse(t, db.BQ, fmt.Sprintf(`
			SELECT id, received_at, test_bool, test_datetime, test_float, test_int, test_string
			FROM %s.%s
			WHERE _PARTITIONTIME BETWEEN TIMESTAMP('%s') AND TIMESTAMP('%s')
			ORDER BY id;`,
			warehouse.Namespace, tableName,
			time.Now().Add(-24*time.Hour).Format("2006-01-02"),
			time.Now().Add(+24*time.Hour).Format("2006-01-02"),
		))
		require.Equal(t, whth.SampleTestRecords(), records)
	}

	// deniedForAnotherWorkspace checks that the pool rejects a workspace it was not granted to.
	deniedForAnotherWorkspace := func(t *testing.T, warehouse model.Warehouse) {
		t.Helper()
		ctx := context.Background()
		bq := whbigquery.New(config.New(), logger.NOP)
		// credentials are exchanged lazily, so the rejection surfaces on the first API call
		require.NoError(t, bq.Setup(ctx, warehouse, newMockUploader(t, nil, "", nil, nil)))
		require.Error(t, bq.CreateSchema(ctx))
	}

	t.Run("service account impersonation", func(t *testing.T) {
		pool := credentials.ServiceAccount

		t.Run("loads data", func(t *testing.T) {
			loadThroughFederation(t, newWarehouse(credentials.WorkspaceID, pool.PoolID, pool.ProviderID, pool.TargetServiceAccount, whth.RandSchema(destType)))
		})
		t.Run("another workspace is denied", func(t *testing.T) {
			deniedForAnotherWorkspace(t, newWarehouse("another-workspace", pool.PoolID, pool.ProviderID, pool.TargetServiceAccount, whth.RandSchema(destType)))
		})
	})

	t.Run("federated identities", func(t *testing.T) {
		pool := credentials.FederatedIdentities

		t.Run("loads data", func(t *testing.T) {
			loadThroughFederation(t, newWarehouse(credentials.WorkspaceID, pool.PoolID, pool.ProviderID, "", whth.RandSchema(destType)))
		})
		t.Run("another workspace is denied", func(t *testing.T) {
			deniedForAnotherWorkspace(t, newWarehouse("another-workspace", pool.PoolID, pool.ProviderID, "", whth.RandSchema(destType)))
		})
	})
}
