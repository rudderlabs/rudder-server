package router

import (
	"context"
	"errors"
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

type mockFetchSchemaRepo struct {
	schemaInWarehouse model.Schema
	err               error
}

func (m *mockFetchSchemaRepo) FetchSchema(_ context.Context) (model.Schema, error) {
	if m.err != nil {
		return model.Schema{}, m.err
	}
	return m.schemaInWarehouse, nil
}

type mockSyncSchemaRepo struct {
	getLocalSchemaerr    error
	updateLocalSchemaErr error
	fetchSchemaErr       error
	hasChanged           bool
	schemaMap            map[string]model.Schema
}

func schemaKey(sourceID, destinationID, namespace string) string {
	return fmt.Sprintf("%s_%s_%s", sourceID, destinationID, namespace)
}

func (m *mockSyncSchemaRepo) GetLocalSchema(_ context.Context) (model.Schema, error) {
	if m.getLocalSchemaerr != nil {
		return model.Schema{}, m.getLocalSchemaerr
	}

	return model.Schema{}, nil
}

func (m *mockSyncSchemaRepo) UpdateLocalSchemaWithWarehouse(_ context.Context, _ model.Schema) error {
	if m.updateLocalSchemaErr != nil && m.hasChanged {
		return m.updateLocalSchemaErr
	}
	return nil
}

func (m *mockSyncSchemaRepo) HasSchemaChanged(_ model.Schema) bool {
	return m.hasChanged
}

func (m *mockSyncSchemaRepo) FetchSchemaFromWarehouse(_ context.Context, _ schema.FetchSchemaRepo) (model.Schema, error) {
	if m.fetchSchemaErr != nil {
		return nil, m.fetchSchemaErr
	}

	return m.schemaMap[schemaKey("test-sourceID", "test-destinationID", "test-namespace")], nil
}

func TestSync_SyncRemoteSchema(t *testing.T) {
	t.Run("fetching schema from local fails", func(t *testing.T) {
		mock := &mockSyncSchemaRepo{
			getLocalSchemaerr: fmt.Errorf("error fetching schema from local"),
		}
		mockFetchSchema := &mockFetchSchemaRepo{}
		r := &Router{
			logger: logger.NOP,
		}
		err := r.syncRemoteSchema(context.Background(), mockFetchSchema, mock)
		require.Error(t, err)
		require.True(t, errors.Is(err, mock.getLocalSchemaerr))
	})
	t.Run("fetching schema from warehouse fails", func(t *testing.T) {
		mock := &mockSyncSchemaRepo{
			fetchSchemaErr: fmt.Errorf("error fetching schema from warehouse"),
		}
		mockFetchSchema := &mockFetchSchemaRepo{}
		r := &Router{
			logger: logger.NOP,
		}
		err := r.syncRemoteSchema(context.Background(), mockFetchSchema, mock)
		require.Error(t, err)
		require.True(t, errors.Is(err, mock.fetchSchemaErr))
	})
	t.Run("schema has changed and updating errors", func(t *testing.T) {
		mock := &mockSyncSchemaRepo{
			hasChanged:           true,
			updateLocalSchemaErr: fmt.Errorf("error updating local schema"),
		}
		mockFetchSchema := &mockFetchSchemaRepo{}
		r := &Router{
			logger: logger.NOP,
		}
		err := r.syncRemoteSchema(context.Background(), mockFetchSchema, mock)
		require.Error(t, err)
	})
	t.Run("schema has changed and updating errors", func(t *testing.T) {
		mock := &mockSyncSchemaRepo{
			hasChanged:           false,
			updateLocalSchemaErr: fmt.Errorf("error updating local schema"),
		}
		mockFetchSchema := &mockFetchSchemaRepo{}
		r := &Router{
			logger: logger.NOP,
		}
		err := r.syncRemoteSchema(context.Background(), mockFetchSchema, mock)
		require.NoError(t, err)
	})
	t.Run("fetching schema succeeds with no error", func(t *testing.T) {
		mock := &mockSyncSchemaRepo{}
		mockFetchSchema := &mockFetchSchemaRepo{}
		r := &Router{
			logger: logger.NOP,
		}
		err := r.syncRemoteSchema(context.Background(), mockFetchSchema, mock)
		require.NoError(t, err)
	})
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

	ctx, cancel := context.WithCancel(context.Background())
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

	setupCh := make(chan struct{})
	go func() {
		defer close(setupCh)
		err = r.sync(ctx)
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

		sh := schema.New(
			r.db,
			warehouse,
			r.conf,
			r.logger.Child("syncer"),
			r.statsFactory,
		)
		require.Eventually(t, func() bool {
			localSchema, err := sh.GetLocalSchema(ctx)
			require.NoError(t, err)
			return reflect.DeepEqual(localSchema, model.Schema{
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
		}, 30*time.Second, 100*time.Millisecond)
		cancel()
	})
	<-setupCh
}
