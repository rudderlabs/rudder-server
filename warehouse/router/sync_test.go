package router

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/schema"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type mockFetchSchemaRepo struct{}

func (m mockFetchSchemaRepo) FetchSchema(ctx context.Context) (model.Schema, error) {
	return model.Schema{}, nil
}

func TestSync_SyncRemoteSchemaIntegration(t *testing.T) {
	destinationType := warehouseutils.POSTGRES
	bucket := "some-bucket"
	sourceID := "test-source-id"
	destinationID := "test-destination-id"
	workspaceID := "test-workspace-id"
	provider := "MINIO"
	sslMode := "disable"
	testNamespace := "test_namespace"
	testTable := "test_table"

	ctx := context.Background()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pgResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	minioResource, err := minio.Setup(pool, t)
	require.NoError(t, err)

	err = minioResource.Client.MakeBucket(ctx, bucket, miniogo.MakeBucketOptions{
		Region: "us-east-1",
	})
	require.NoError(t, err)
	t.Log("db:", pgResource.DBDsn)
	conf := config.New()
	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse")
	require.NoError(t, err)

	db := sqlmiddleware.New(pgResource.DB)

	warehouse := model.Warehouse{
		WorkspaceID: workspaceID,
		Source: backendconfig.SourceT{
			ID: sourceID,
		},
		Destination: backendconfig.DestinationT{
			ID: destinationID,
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: destinationType,
			},
			Config: map[string]interface{}{
				"host":            pgResource.Host,
				"port":            pgResource.Port,
				"database":        pgResource.Database,
				"user":            pgResource.User,
				"password":        pgResource.Password,
				"sslMode":         sslMode,
				"bucketProvider":  provider,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKeyID,
				"secretAccessKey": minioResource.AccessKeySecret,
				"endPoint":        minioResource.Endpoint,
			},
		},
		Namespace:  "test_namespace",
		Identifier: "RS:test-source-id:test-destination-id-create-jobs",
	}
	r := Router{
		logger:       logger.NOP,
		conf:         conf,
		db:           db,
		warehouses:   []model.Warehouse{warehouse},
		destType:     warehouseutils.POSTGRES,
		statsFactory: stats.NOP,
	}
	r.config.syncSchemaFrequency = 1 * time.Hour

	setupCh := make(chan struct{})
	syncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	go func() {
		defer close(setupCh)
		err = r.sync(syncCtx)
		require.NoError(t, err)
	}()

	t.Run("fetching schema from postgres", func(t *testing.T) {
		schemaSql := fmt.Sprintf("CREATE SCHEMA %q;", testNamespace)
		_, err = pgResource.DB.Exec(schemaSql)
		require.NoError(t, err)

		tableSql := fmt.Sprintf(`CREATE TABLE %q.%q (
		job_id BIGSERIAL PRIMARY KEY,
		workspace_id TEXT NOT NULL DEFAULT '',
		uuid UUID NOT NULL,
		user_id TEXT NOT NULL,
		parameters JSONB NOT NULL,
		custom_val VARCHAR(64) NOT NULL,
		event_payload JSONB NOT NULL,
		event_count INTEGER NOT NULL DEFAULT 1,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		expire_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());`, testNamespace, testTable)
		_, err = pgResource.DB.Exec(tableSql)
		require.NoError(t, err)

		<-setupCh
		r.conf.Set("Warehouse.enableSyncSchema", true)
		sh := schema.New(
			ctx,
			r.db,
			warehouse,
			r.conf,
			r.logger.Child("syncer"),
			r.statsFactory,
		)
		require.Eventually(t, func() bool {
			_, err := sh.SyncRemoteSchema(ctx, &mockFetchSchemaRepo{}, 0)
			require.NoError(t, err)
			schema, err := sh.GetLocalSchema(ctx)
			require.NoError(t, err)
			return reflect.DeepEqual(schema, model.Schema{
				"test_table": model.TableSchema{
					"created_at":    "datetime",
					"event_count":   "int",
					"event_payload": "json",
					"expire_at":     "datetime",
					"job_id":        "int",
					"parameters":    "json",
					"user_id":       "string",
					"workspace_id":  "string",
				},
			})
		}, 3*time.Second, 100*time.Millisecond)
		cancel()
	})
}
