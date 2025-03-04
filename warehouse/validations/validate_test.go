package validations_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestValidator(t *testing.T) {
	misc.Init()
	warehouseutils.Init()
	validations.Init()

	var (
		provider = "MINIO"
		table    = "test_table"
		sslMode  = "disable"
		bucket   = "some-bucket"
		region   = "us-east-1"
	)

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

	t.Run("Object Storage", func(t *testing.T) {
		t.Run("Non Datalakes", func(t *testing.T) {
			testCases := []struct {
				name                      string
				cleanupObjectStorageFiles bool
			}{
				{
					name:                      "check delete permissions",
					cleanupObjectStorageFiles: true,
				},
				{
					name:                      "skip checking delete permissions",
					cleanupObjectStorageFiles: false,
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					destination := &backendconfig.DestinationT{
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
							"accessKeyID":     minioResource.AccessKeyID,
							"secretAccessKey": minioResource.AccessKeySecret,
							"endPoint":        minioResource.Endpoint,
						},
					}
					destination.Config[model.CleanupObjectStorageFilesSetting.String()] = tc.cleanupObjectStorageFiles
					v, err := validations.NewValidator(ctx, model.VerifyingObjectStorage, destination)
					require.NoError(t, err)
					require.NoError(t, v.Validate(ctx))
				})
			}
		})

		t.Run("Datalakes", func(t *testing.T) {
			v, err := validations.NewValidator(ctx, model.VerifyingObjectStorage, &backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.S3Datalake,
				},
				Config: map[string]interface{}{
					"region":           region,
					"bucketName":       bucket,
					"accessKeyID":      minioResource.AccessKeyID,
					"accessKey":        minioResource.AccessKeySecret,
					"endPoint":         minioResource.Endpoint,
					"enableSSE":        false,
					"s3ForcePathStyle": true,
					"disableSSL":       true,
					"prefix":           "some-prefix",
					"syncFrequency":    "30",
				},
			})
			require.NoError(t, err)
			require.NoError(t, v.Validate(ctx))
		})
	})

	t.Run("Connections", func(t *testing.T) {
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
				wantError: errors.New("pinging: pq: database \"invalid_database\" does not exist"),
			},
			{
				name: "valid credentials",
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
					"sslMode":         sslMode,
					"bucketProvider":  provider,
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKeyID,
					"secretAccessKey": minioResource.AccessKeySecret,
					"endPoint":        minioResource.Endpoint,
				}

				for k, v := range tc.config {
					conf[k] = v
				}

				v, err := validations.NewValidator(ctx, model.VerifyingConnections, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				require.NoError(t, err)

				if tc.wantError != nil {
					require.EqualError(t, v.Validate(ctx), tc.wantError.Error())
				} else {
					require.NoError(t, v.Validate(ctx))
				}
			})
		}
	})

	t.Run("Create Schema", func(t *testing.T) {
		var (
			namespace           = "cs_test_namespace"
			password            = "cs_test_password"
			userWithNoPrivilege = "cs_test_user_with_no_privilege"
		)

		t.Log("Creating users with no privileges")
		for _, user := range []string{userWithNoPrivilege} {
			_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", user, password))
			require.NoError(t, err)
		}

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

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				conf := map[string]interface{}{
					"host":            pgResource.Host,
					"port":            pgResource.Port,
					"database":        pgResource.Database,
					"user":            pgResource.User,
					"password":        pgResource.Password,
					"sslMode":         sslMode,
					"namespace":       namespace,
					"bucketProvider":  provider,
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKeyID,
					"secretAccessKey": minioResource.AccessKeySecret,
					"endPoint":        minioResource.Endpoint,
				}

				for k, v := range tc.config {
					conf[k] = v
				}

				v, err := validations.NewValidator(ctx, model.VerifyingCreateSchema, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				require.NoError(t, err)

				if tc.wantError != nil {
					require.EqualError(t, v.Validate(ctx), tc.wantError.Error())
				} else {
					require.NoError(t, v.Validate(ctx))
				}
			})
		}
	})

	t.Run("Create And Alter Table", func(t *testing.T) {
		var (
			namespace                    = "cat_test_namespace"
			password                     = "cat_test_password"
			userWithNoPrivilege          = "cat_cat_test_user_with_no_privilege"
			userWithCreateTablePrivilege = "cat_test_user_with_create_table_privilege"
			userWithAlterPrivilege       = "cat_test_user_with_alter_privilege"
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
				wantError: errors.New("create table: pq: permission denied for schema cat_test_namespace"),
			},
			{
				name: "create table privilege",
				config: map[string]interface{}{
					"user":     userWithCreateTablePrivilege,
					"password": password,
				},
				wantError: errors.New("alter table: pq: permission denied for schema cat_test_namespace"),
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
					"sslMode":         sslMode,
					"namespace":       namespace,
					"bucketProvider":  provider,
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKeyID,
					"secretAccessKey": minioResource.AccessKeySecret,
					"endPoint":        minioResource.Endpoint,
				}

				for k, v := range tc.config {
					conf[k] = v
				}

				v, err := validations.NewValidator(ctx, model.VerifyingCreateAndAlterTable, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				require.NoError(t, err)

				if tc.wantError != nil {
					require.EqualError(t, v.Validate(ctx), tc.wantError.Error())
				} else {
					require.NoError(t, v.Validate(ctx))
				}

				_, err = pgResource.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.setup_test_staging", namespace))
				require.NoError(t, err)
			})
		}
	})

	t.Run("Fetch schema", func(t *testing.T) {
		namespace := "fs_test_namespace"

		_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", namespace))
		require.NoError(t, err)

		_, err = pgResource.DB.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s(id int, val varchar)", namespace, table))
		require.NoError(t, err)

		v, err := validations.NewValidator(ctx, model.VerifyingFetchSchema, &backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: warehouseutils.POSTGRES,
			},
			Config: map[string]interface{}{
				"host":            pgResource.Host,
				"port":            pgResource.Port,
				"database":        pgResource.Database,
				"user":            pgResource.User,
				"password":        pgResource.Password,
				"sslMode":         sslMode,
				"namespace":       namespace,
				"bucketProvider":  provider,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKeyID,
				"secretAccessKey": minioResource.AccessKeySecret,
				"endPoint":        minioResource.Endpoint,
			},
		})
		require.NoError(t, err)
		require.NoError(t, v.Validate(ctx))
	})

	t.Run("Load table", func(t *testing.T) {
		var (
			namespace                    = "lt_test_namespace"
			password                     = "lt_test_password"
			userWithNoPrivilege          = "lt_test_user_with_no_privilege"
			userWithCreateTablePrivilege = "lt_test_user_with_create_table_privilege"
			userWithInsertPrivilege      = "lt_test_user_with_insert_privilege"
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
				wantError: errors.New("create table: pq: permission denied for schema lt_test_namespace"),
			},
			{
				name: "create table privilege",
				config: map[string]interface{}{
					"user":     userWithCreateTablePrivilege,
					"password": password,
				},
				wantError: errors.New("load test table: pq: permission denied for schema lt_test_namespace"),
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
					"sslMode":         sslMode,
					"namespace":       namespace,
					"bucketProvider":  provider,
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKeyID,
					"secretAccessKey": minioResource.AccessKeySecret,
					"endPoint":        minioResource.Endpoint,
				}

				for k, v := range tc.config {
					conf[k] = v
				}

				v, err := validations.NewValidator(ctx, model.VerifyingLoadTable, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				require.NoError(t, err)

				if tc.wantError != nil {
					require.EqualError(t, v.Validate(ctx), tc.wantError.Error())
				} else {
					require.NoError(t, v.Validate(ctx))
				}

				_, err = pgResource.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.setup_test_staging", namespace))
				require.NoError(t, err)
			})
		}
	})
}
