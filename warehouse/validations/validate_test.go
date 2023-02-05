package validations_test

import (
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/ory/dockertest/v3"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"

	"github.com/rudderlabs/rudder-server/testhelper/destination"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type testResource struct {
	minioResource *destination.MINIOResource
	pgResource    *destination.PostgresResource
}

func setup(t *testing.T, pool *dockertest.Pool) testResource {
	var (
		minioResource *destination.MINIOResource
		pgResource    *destination.PostgresResource
		err           error
	)

	g := errgroup.Group{}
	g.Go(func() error {
		pgResource, err = destination.SetupPostgres(pool, t)
		require.NoError(t, err)

		return nil
	})
	g.Go(func() error {
		minioResource, err = destination.SetupMINIO(pool, t)
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, g.Wait())

	return testResource{
		minioResource: minioResource,
		pgResource:    pgResource,
	}
}

func TestValidator(t *testing.T) {
	misc.Init()
	warehouseutils.Init()
	validations.Init()
	postgres.Init()

	var (
		provider  = "MINIO"
		namespace = "test_namespace"
		table     = "test_table"
		sslmode   = "disable"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Run("Object Storage", func(t *testing.T) {
		t.Parallel()

		tr := setup(t, pool)
		pgResource, minioResource := tr.pgResource, tr.minioResource

		t.Run("Non Datalakes", func(t *testing.T) {
			t.Parallel()

			v, err := validations.NewValidator(model.VerifyingObjectStorage, &backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.POSTGRES,
				},
				Config: map[string]interface{}{
					"host":            pgResource.Host,
					"port":            pgResource.Port,
					"database":        pgResource.Database,
					"user":            pgResource.User,
					"password":        pgResource.Password,
					"bucketProvider":  provider,
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKey,
					"secretAccessKey": minioResource.SecretKey,
					"endPoint":        minioResource.Endpoint,
				},
			})
			require.NoError(t, err)
			require.NoError(t, v.Validate())
		})

		t.Run("Datalakes", func(t *testing.T) {
			t.Parallel()

			tr := setup(t, pool)
			_, minioResource := tr.pgResource, tr.minioResource

			var (
				bucket = "s3-datalake-test"
				region = "us-east-1"
			)

			_ = minioResource.Client.MakeBucket(bucket, "us-east-1")

			v, err := validations.NewValidator(model.VerifyingObjectStorage, &backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.S3_DATALAKE,
				},
				Config: map[string]interface{}{
					"region":           region,
					"bucketName":       bucket,
					"accessKeyID":      minioResource.AccessKey,
					"accessKey":        minioResource.SecretKey,
					"endPoint":         minioResource.Endpoint,
					"enableSSE":        false,
					"s3ForcePathStyle": true,
					"disableSSL":       true,
					"prefix":           "some-prefix",
					"syncFrequency":    "30",
				},
			})
			require.NoError(t, err)
			require.NoError(t, v.Validate())
		})
	})

	t.Run("Connections", func(t *testing.T) {
		t.Parallel()

		tr := setup(t, pool)
		pgResource, minioResource := tr.pgResource, tr.minioResource

		v, err := validations.NewValidator(model.VerifyingConnections, &backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: warehouseutils.POSTGRES,
			},
			Config: map[string]interface{}{
				"host":            pgResource.Host,
				"port":            pgResource.Port,
				"database":        pgResource.Database,
				"user":            pgResource.User,
				"password":        pgResource.Password,
				"sslMode":         sslmode,
				"bucketProvider":  provider,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKey,
				"secretAccessKey": minioResource.SecretKey,
				"endPoint":        minioResource.Endpoint,
			},
		})
		require.NoError(t, err)
		require.NoError(t, v.Validate())
	})

	t.Run("Create Schema", func(t *testing.T) {
		t.Parallel()

		tr := setup(t, pool)
		pgResource, minioResource := tr.pgResource, tr.minioResource

		v, err := validations.NewValidator(model.VerifyingCreateSchema, &backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: warehouseutils.POSTGRES,
			},
			Config: map[string]interface{}{
				"host":            pgResource.Host,
				"port":            pgResource.Port,
				"database":        pgResource.Database,
				"user":            pgResource.User,
				"password":        pgResource.Password,
				"sslMode":         sslmode,
				"namespace":       namespace,
				"bucketProvider":  provider,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKey,
				"secretAccessKey": minioResource.SecretKey,
				"endPoint":        minioResource.Endpoint,
			},
		})
		require.NoError(t, err)
		require.NoError(t, v.Validate())
	})

	t.Run("Create And Alter Table", func(t *testing.T) {
		t.Parallel()

		tr := setup(t, pool)
		pgResource, minioResource := tr.pgResource, tr.minioResource

		v, err := validations.NewValidator(model.VerifyingCreateAndAlterTable, &backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: warehouseutils.POSTGRES,
			},
			Config: map[string]interface{}{
				"host":            pgResource.Host,
				"port":            pgResource.Port,
				"database":        pgResource.Database,
				"user":            pgResource.User,
				"password":        pgResource.Password,
				"sslMode":         sslmode,
				"namespace":       namespace,
				"bucketProvider":  provider,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKey,
				"secretAccessKey": minioResource.SecretKey,
				"endPoint":        minioResource.Endpoint,
			},
		})
		require.NoError(t, err)

		_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", namespace))
		require.NoError(t, err)

		require.NoError(t, v.Validate())
	})

	t.Run("Fetch schema", func(t *testing.T) {
		t.Parallel()

		tr := setup(t, pool)
		pgResource, minioResource := tr.pgResource, tr.minioResource

		v, err := validations.NewValidator(model.VerifyingFetchSchema, &backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: warehouseutils.POSTGRES,
			},
			Config: map[string]interface{}{
				"host":            pgResource.Host,
				"port":            pgResource.Port,
				"database":        pgResource.Database,
				"user":            pgResource.User,
				"password":        pgResource.Password,
				"sslMode":         sslmode,
				"namespace":       namespace,
				"bucketProvider":  provider,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKey,
				"secretAccessKey": minioResource.SecretKey,
				"endPoint":        minioResource.Endpoint,
			},
		})
		require.NoError(t, err)

		_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", namespace))
		require.NoError(t, err)

		_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s(id int, val varchar)", namespace, table))
		require.NoError(t, err)

		require.NoError(t, v.Validate())
	})

	t.Run("Load table", func(t *testing.T) {
		t.Parallel()

		tr := setup(t, pool)
		pgResource, minioResource := tr.pgResource, tr.minioResource

		v, err := validations.NewValidator(model.VerifyingLoadTable, &backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: warehouseutils.POSTGRES,
			},
			Config: map[string]interface{}{
				"host":            pgResource.Host,
				"port":            pgResource.Port,
				"database":        pgResource.Database,
				"user":            pgResource.User,
				"password":        pgResource.Password,
				"sslMode":         sslmode,
				"namespace":       namespace,
				"bucketProvider":  provider,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKey,
				"secretAccessKey": minioResource.SecretKey,
				"endPoint":        minioResource.Endpoint,
			},
		})
		require.NoError(t, err)

		_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", namespace))
		require.NoError(t, err)

		require.NoError(t, v.Validate())
	})
}
