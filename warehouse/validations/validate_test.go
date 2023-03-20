package validations_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	postgreslegacy "github.com/rudderlabs/rudder-server/warehouse/integrations/postgres-legacy"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/ory/dockertest/v3"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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
	pgResource    *resource.PostgresResource
}

func setup(t *testing.T, pool *dockertest.Pool) testResource {
	var (
		minioResource *destination.MINIOResource
		pgResource    *resource.PostgresResource
		err           error
	)

	g := errgroup.Group{}
	g.Go(func() error {
		pgResource, err = resource.SetupPostgres(pool, t)
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
	encoding.Init()
	validations.Init()
	postgres.Init()
	postgreslegacy.Init()

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

			minioResource, err = destination.SetupMINIO(pool, t)
			require.NoError(t, err)

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

		testCases := []struct {
			name      string
			config    map[string]interface{}
			wantError error
		}{
			{
				name: "invalid credentials",
				config: map[string]interface{}{
					"database": "invalid_database",
				},
				wantError: errors.New("pq: database \"invalid_database\" does not exist"),
			},
			{
				name: "valid credentials",
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				conf := map[string]interface{}{
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
				}

				for k, v := range tc.config {
					conf[k] = v
				}

				v, err := validations.NewValidator(model.VerifyingConnections, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				require.NoError(t, err)

				if tc.wantError != nil {
					require.EqualError(t, v.Validate(), tc.wantError.Error())
				} else {
					require.NoError(t, v.Validate())
				}
			})
		}
	})

	t.Run("Create Schema", func(t *testing.T) {
		t.Parallel()

		tr := setup(t, pool)
		pgResource, minioResource := tr.pgResource, tr.minioResource

		var (
			password            = "test_password"
			userWithNoPrivilege = "test_user_with_no_privilege"
		)

		testCases := []struct {
			name      string
			config    map[string]interface{}
			wantError error
		}{
			{
				name: "with no privilege",
				config: map[string]interface{}{
					"user":      userWithNoPrivilege,
					"password":  password,
					"namespace": "test_namespace_with_no_privilege",
				},
				wantError: errors.New("pq: permission denied for database jobsdb"),
			},
			{
				name: "with privilege",
			},
		}

		t.Log("Creating users with no privileges")
		for _, user := range []string{userWithNoPrivilege} {
			_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", user, password))
			require.NoError(t, err)
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				conf := map[string]interface{}{
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
				}

				for k, v := range tc.config {
					conf[k] = v
				}

				v, err := validations.NewValidator(model.VerifyingCreateSchema, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				require.NoError(t, err)

				if tc.wantError != nil {
					require.EqualError(t, v.Validate(), tc.wantError.Error())
				} else {
					require.NoError(t, v.Validate())
				}
			})
		}
	})

	t.Run("Create And Alter Table", func(t *testing.T) {
		t.Parallel()

		tr := setup(t, pool)
		pgResource, minioResource := tr.pgResource, tr.minioResource

		var (
			password                     = "test_password"
			userWithNoPrivilege          = "test_user_with_no_privilege"
			userWithCreateTablePrivilege = "test_user_with_create_table_privilege"
			userWithAlterPrivilege       = "test_user_with_alter_privilege"
		)

		_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", namespace))
		require.NoError(t, err)

		t.Log("Creating users with no privileges")
		for _, user := range []string{userWithNoPrivilege, userWithCreateTablePrivilege, userWithAlterPrivilege} {
			_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", user, password))
			require.NoError(t, err)
		}

		t.Log("Granting create table privilege to users")
		for _, user := range []string{userWithCreateTablePrivilege, userWithAlterPrivilege} {
			_, err = pgResource.DB.Exec(fmt.Sprintf("GRANT CREATE ON SCHEMA %s TO %s;", namespace, user))
			require.NoError(t, err)
		}

		t.Log("Granting insert privilege to users")
		for _, user := range []string{userWithAlterPrivilege} {
			_, err = pgResource.DB.Exec(fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s;", namespace, user))
			require.NoError(t, err)
		}

		testCases := []struct {
			name      string
			config    map[string]interface{}
			wantError error
		}{
			{
				name: "no privilege",
				config: map[string]interface{}{
					"user":     userWithNoPrivilege,
					"password": password,
				},
				wantError: errors.New("create table: pq: permission denied for schema test_namespace"),
			},
			{
				name: "create table privilege",
				config: map[string]interface{}{
					"user":     userWithCreateTablePrivilege,
					"password": password,
				},
				wantError: errors.New("alter table: pq: permission denied for schema test_namespace"),
			},
			{
				name: "alter privilege",
				config: map[string]interface{}{
					"user":     userWithAlterPrivilege,
					"password": password,
				},
			},
			{
				name: "all privileges",
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				conf := map[string]interface{}{
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
				}

				for k, v := range tc.config {
					conf[k] = v
				}

				v, err := validations.NewValidator(model.VerifyingCreateAndAlterTable, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				require.NoError(t, err)

				if tc.wantError != nil {
					require.EqualError(t, v.Validate(), tc.wantError.Error())
				} else {
					require.NoError(t, v.Validate())
				}

				_, err = pgResource.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.setup_test_staging", namespace))
				require.NoError(t, err)
			})
		}
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

		var (
			password                     = "test_password"
			userWithNoPrivilege          = "test_user_with_no_privilege"
			userWithCreateTablePrivilege = "test_user_with_create_table_privilege"
			userWithInsertPrivilege      = "test_user_with_insert_privilege"
		)

		_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", namespace))
		require.NoError(t, err)

		t.Log("Creating users with no privileges")
		for _, user := range []string{userWithNoPrivilege, userWithCreateTablePrivilege, userWithInsertPrivilege} {
			_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", user, password))
			require.NoError(t, err)
		}

		t.Log("Granting create table privilege to users")
		for _, user := range []string{userWithCreateTablePrivilege, userWithInsertPrivilege} {
			_, err = pgResource.DB.Exec(fmt.Sprintf("GRANT CREATE ON SCHEMA %s TO %s;", namespace, user))
			require.NoError(t, err)
		}

		t.Log("Granting insert privilege to users")
		for _, user := range []string{userWithInsertPrivilege} {
			_, err = pgResource.DB.Exec(fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s;", namespace, user))
			require.NoError(t, err)

			_, err = pgResource.DB.Exec(fmt.Sprintf("GRANT INSERT ON ALL TABLES IN SCHEMA %s TO %s;", namespace, user))
			require.NoError(t, err)
		}

		testCases := []struct {
			name      string
			config    map[string]interface{}
			wantError error
		}{
			{
				name: "invalid object storage",
				config: map[string]interface{}{
					"bucketName":      "temp-bucket",
					"accessKeyID":     "temp-access-key",
					"secretAccessKey": "test-secret-key",
				},
				wantError: errors.New("upload file: uploading file: checking bucket: The Access Key Id you provided does not exist in our records."),
			},
			{
				name: "no privilege",
				config: map[string]interface{}{
					"user":     userWithNoPrivilege,
					"password": password,
				},
				wantError: errors.New("create table: pq: permission denied for schema test_namespace"),
			},
			{
				name: "create table privilege",
				config: map[string]interface{}{
					"user":     userWithCreateTablePrivilege,
					"password": password,
				},
				wantError: errors.New("load test table: pq: permission denied for schema test_namespace"),
			},
			{
				name: "insert privilege",
				config: map[string]interface{}{
					"user":     userWithInsertPrivilege,
					"password": password,
				},
			},
			{
				name: "all privileges",
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				conf := map[string]interface{}{
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
				}

				for k, v := range tc.config {
					conf[k] = v
				}

				v, err := validations.NewValidator(model.VerifyingLoadTable, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				require.NoError(t, err)

				if tc.wantError != nil {
					require.EqualError(t, v.Validate(), tc.wantError.Error())
				} else {
					require.NoError(t, v.Validate())
				}

				_, err = pgResource.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.setup_test_staging", namespace))
				require.NoError(t, err)
			})
		}
	})
}
