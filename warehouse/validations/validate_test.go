package validations_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T, paths ...string) *testcompose.TestingCompose {
	c := testcompose.New(t, compose.FilePaths(paths))

	t.Cleanup(func() {
		c.Stop(context.Background())
	})
	c.Start(context.Background())

	return c
}

func TestValidator(t *testing.T) {
	misc.Init()
	warehouseutils.Init()
	encoding.Init()
	validations.Init()

	var (
		provider        = "MINIO"
		namespace       = "test_namespace"
		table           = "test_table"
		sslmode         = "disable"
		host            = "localhost"
		database        = "rudderdb"
		user            = "rudder"
		password        = "rudder-password"
		bucketName      = "testbucket"
		region          = "us-east-1"
		accessKeyID     = "MYACCESSKEY"
		secretAccessKey = "MYSECRETKEY"
	)

	ctx := context.Background()

	t.Run("Object Storage", func(t *testing.T) {
		t.Run("Non Datalakes", func(t *testing.T) {
			c := setup(t, "../testdata/docker-compose.postgres.yml", "../testdata/docker-compose.minio.yml")

			minioPort := c.Port("minio", 9000)
			postgresPort := c.Port("postgres", 5432)

			minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

			v, err := validations.NewValidator(ctx, model.VerifyingObjectStorage, &backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.POSTGRES,
				},
				Config: map[string]interface{}{
					"host":            host,
					"port":            strconv.Itoa(postgresPort),
					"database":        database,
					"user":            user,
					"password":        password,
					"bucketProvider":  provider,
					"bucketName":      bucketName,
					"accessKeyID":     accessKeyID,
					"secretAccessKey": secretAccessKey,
					"endPoint":        minioEndpoint,
				},
			})
			require.NoError(t, err)
			require.NoError(t, v.Validate(ctx))
		})

		t.Run("Datalakes", func(t *testing.T) {
			c := setup(t, "../testdata/docker-compose.minio.yml")

			minioPort := c.Port("minio", 9000)
			minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

			minioClient, err := minio.New(minioEndpoint, accessKeyID, secretAccessKey, false)
			require.NoError(t, err)

			err = minioClient.MakeBucket(bucketName, region)
			require.NoError(t, err)

			v, err := validations.NewValidator(ctx, model.VerifyingObjectStorage, &backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.S3_DATALAKE,
				},
				Config: map[string]interface{}{
					"region":           region,
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"accessKey":        secretAccessKey,
					"endPoint":         minioEndpoint,
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
				c := setup(t, "../testdata/docker-compose.postgres.yml", "../testdata/docker-compose.minio.yml")

				minioPort := c.Port("minio", 9000)
				postgresPort := c.Port("postgres", 5432)

				minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

				conf := map[string]interface{}{
					"host":            host,
					"port":            strconv.Itoa(postgresPort),
					"database":        database,
					"user":            user,
					"password":        password,
					"sslMode":         sslmode,
					"bucketProvider":  provider,
					"bucketName":      bucketName,
					"accessKeyID":     accessKeyID,
					"secretAccessKey": secretAccessKey,
					"endPoint":        minioEndpoint,
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
			testPassword        = "test_password"
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
					"password":  testPassword,
					"namespace": "test_namespace_with_no_privilege",
				},
				wantError: errors.New("pq: permission denied for database rudderdb"),
			},
			{
				name: "with privilege",
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				c := setup(t, "../testdata/docker-compose.postgres.yml", "../testdata/docker-compose.minio.yml")

				minioPort := c.Port("minio", 9000)
				postgresPort := c.Port("postgres", 5432)

				minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

				dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
					user,
					password,
					host,
					strconv.Itoa(postgresPort),
					database,
				)
				db, err := sql.Open("postgres", dsn)
				require.NoError(t, err)
				require.Eventually(t, func() bool { return db.Ping() == nil }, 5*time.Second, 100*time.Millisecond)

				t.Log("Creating users with no privileges")
				for _, user := range []string{userWithNoPrivilege} {
					_, err = db.Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", user, testPassword))
					require.NoError(t, err)
				}

				conf := map[string]interface{}{
					"host":            host,
					"port":            strconv.Itoa(postgresPort),
					"database":        database,
					"user":            user,
					"password":        password,
					"sslMode":         sslmode,
					"namespace":       namespace,
					"bucketProvider":  provider,
					"bucketName":      bucketName,
					"accessKeyID":     accessKeyID,
					"secretAccessKey": secretAccessKey,
					"endPoint":        minioEndpoint,
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
			testPassword                 = "test_password"
			userWithNoPrivilege          = "test_user_with_no_privilege"
			userWithCreateTablePrivilege = "test_user_with_create_table_privilege"
			userWithAlterPrivilege       = "test_user_with_alter_privilege"
		)

		testCases := []struct {
			name      string
			config    map[string]interface{}
			wantError error
		}{
			{
				name: "no privilege",
				config: map[string]interface{}{
					"user":     userWithNoPrivilege,
					"password": testPassword,
				},
				wantError: errors.New("create table: pq: permission denied for schema test_namespace"),
			},
			{
				name: "create table privilege",
				config: map[string]interface{}{
					"user":     userWithCreateTablePrivilege,
					"password": testPassword,
				},
				wantError: errors.New("alter table: pq: permission denied for schema test_namespace"),
			},
			{
				name: "alter privilege",
				config: map[string]interface{}{
					"user":     userWithAlterPrivilege,
					"password": testPassword,
				},
			},
			{
				name: "all privileges",
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				c := setup(t, "../testdata/docker-compose.postgres.yml", "../testdata/docker-compose.minio.yml")

				minioPort := c.Port("minio", 9000)
				postgresPort := c.Port("postgres", 5432)

				minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

				dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
					user,
					password,
					host,
					strconv.Itoa(postgresPort),
					database,
				)
				db, err := sql.Open("postgres", dsn)
				require.NoError(t, err)
				require.Eventually(t, func() bool { return db.Ping() == nil }, 5*time.Second, 100*time.Millisecond)

				_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", namespace))
				require.NoError(t, err)

				t.Log("Creating users with no privileges")
				for _, user := range []string{userWithNoPrivilege, userWithCreateTablePrivilege, userWithAlterPrivilege} {
					_, err = db.Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", user, testPassword))
					require.NoError(t, err)
				}

				t.Log("Granting create table privilege to users")
				for _, user := range []string{userWithCreateTablePrivilege, userWithAlterPrivilege} {
					_, err = db.Exec(fmt.Sprintf("GRANT CREATE ON SCHEMA %s TO %s;", namespace, user))
					require.NoError(t, err)
				}

				t.Log("Granting insert privilege to users")
				for _, user := range []string{userWithAlterPrivilege} {
					_, err = db.Exec(fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s;", namespace, user))
					require.NoError(t, err)
				}

				conf := map[string]interface{}{
					"host":            host,
					"port":            strconv.Itoa(c.Port("postgres", 5432)),
					"database":        database,
					"user":            user,
					"password":        password,
					"sslMode":         sslmode,
					"namespace":       namespace,
					"bucketProvider":  provider,
					"bucketName":      bucketName,
					"accessKeyID":     accessKeyID,
					"secretAccessKey": secretAccessKey,
					"endPoint":        minioEndpoint,
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

				_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.setup_test_staging", namespace))
				require.NoError(t, err)
			})
		}
	})

	t.Run("Fetch schema", func(t *testing.T) {
		c := setup(t, "../testdata/docker-compose.postgres.yml", "../testdata/docker-compose.minio.yml")

		minioPort := c.Port("minio", 9000)
		postgresPort := c.Port("postgres", 5432)

		minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			user,
			password,
			host,
			strconv.Itoa(postgresPort),
			database,
		)
		db, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return db.Ping() == nil }, 5*time.Second, 100*time.Millisecond)

		v, err := validations.NewValidator(ctx, model.VerifyingFetchSchema, &backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: warehouseutils.POSTGRES,
			},
			Config: map[string]interface{}{
				"host":            host,
				"port":            strconv.Itoa(postgresPort),
				"database":        database,
				"user":            user,
				"password":        password,
				"sslMode":         sslmode,
				"namespace":       namespace,
				"bucketProvider":  provider,
				"bucketName":      bucketName,
				"accessKeyID":     accessKeyID,
				"secretAccessKey": secretAccessKey,
				"endPoint":        minioEndpoint,
			},
		})
		require.NoError(t, err)

		_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", namespace))
		require.NoError(t, err)

		_, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s(id int, val varchar)", namespace, table))
		require.NoError(t, err)

		require.NoError(t, v.Validate(ctx))
	})

	t.Run("Load table", func(t *testing.T) {
		var (
			testPassword                 = "test_password"
			userWithNoPrivilege          = "test_user_with_no_privilege"
			userWithCreateTablePrivilege = "test_user_with_create_table_privilege"
			userWithInsertPrivilege      = "test_user_with_insert_privilege"
		)

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
					"password": testPassword,
				},
				wantError: errors.New("create table: pq: permission denied for schema test_namespace"),
			},
			{
				name: "create table privilege",
				config: map[string]interface{}{
					"user":     userWithCreateTablePrivilege,
					"password": testPassword,
				},
				wantError: errors.New("load test table: pq: permission denied for schema test_namespace"),
			},
			{
				name: "insert privilege",
				config: map[string]interface{}{
					"user":     userWithInsertPrivilege,
					"password": testPassword,
				},
			},
			{
				name: "all privileges",
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				c := setup(t, "../testdata/docker-compose.postgres.yml", "../testdata/docker-compose.minio.yml")

				minioPort := c.Port("minio", 9000)
				postgresPort := c.Port("postgres", 5432)

				minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

				dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
					user,
					password,
					host,
					strconv.Itoa(postgresPort),
					database,
				)
				db, err := sql.Open("postgres", dsn)
				require.NoError(t, err)
				require.Eventually(t, func() bool { return db.Ping() == nil }, 5*time.Second, 100*time.Millisecond)

				_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", namespace))
				require.NoError(t, err)

				t.Log("Creating users with no privileges")
				for _, user := range []string{userWithNoPrivilege, userWithCreateTablePrivilege, userWithInsertPrivilege} {
					_, err = db.Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", user, testPassword))
					require.NoError(t, err)
				}

				t.Log("Granting create table privilege to users")
				for _, user := range []string{userWithCreateTablePrivilege, userWithInsertPrivilege} {
					_, err = db.Exec(fmt.Sprintf("GRANT CREATE ON SCHEMA %s TO %s;", namespace, user))
					require.NoError(t, err)
				}

				t.Log("Granting insert privilege to users")
				for _, user := range []string{userWithInsertPrivilege} {
					_, err = db.Exec(fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s;", namespace, user))
					require.NoError(t, err)

					_, err = db.Exec(fmt.Sprintf("GRANT INSERT ON ALL TABLES IN SCHEMA %s TO %s;", namespace, user))
					require.NoError(t, err)
				}

				conf := map[string]interface{}{
					"host":            host,
					"port":            strconv.Itoa(postgresPort),
					"database":        database,
					"user":            user,
					"password":        password,
					"sslMode":         sslmode,
					"namespace":       namespace,
					"bucketProvider":  provider,
					"bucketName":      bucketName,
					"accessKeyID":     accessKeyID,
					"secretAccessKey": secretAccessKey,
					"endPoint":        minioEndpoint,
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

				_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.setup_test_staging", namespace))
				require.NoError(t, err)
			})
		}
	})
}
