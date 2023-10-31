package validations

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/minio/minio-go/v7"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestValidator_Steps(t *testing.T) {
	warehouseutils.Init()

	testCases := []struct {
		name  string
		dest  backendconfig.DestinationT
		steps []string
	}{
		{
			name: "GCS",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.GCSDatalake,
				},
			},
			steps: []string{model.VerifyingObjectStorage},
		},
		{
			name: "Azure",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.AzureDatalake,
				},
			},
			steps: []string{model.VerifyingObjectStorage},
		},
		{
			name: "S3 without Glue",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.S3Datalake,
				},
				Config: map[string]interface{}{},
			},
			steps: []string{model.VerifyingObjectStorage},
		},
		{
			name: "S3 with Glue",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.S3Datalake,
				},
				Config: map[string]interface{}{
					"region":  "us-east-1",
					"useGlue": true,
				},
			},
			steps: []string{
				model.VerifyingObjectStorage,
				model.VerifyingCreateSchema,
				model.VerifyingCreateAndAlterTable,
				model.VerifyingFetchSchema,
			},
		},
		{
			name: "RS",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.RS,
				},
			},
			steps: []string{
				model.VerifyingObjectStorage,
				model.VerifyingConnections,
				model.VerifyingCreateSchema,
				model.VerifyingCreateAndAlterTable,
				model.VerifyingFetchSchema,
				model.VerifyingLoadTable,
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := config.New()
			v := NewValidator(c, logger.NOP, stats.Default, filemanager.New, encoding.NewFactory(c))
			steps := v.Steps(&tc.dest)
			require.Len(t, steps.Steps, len(tc.steps))

			for i, step := range steps.Steps {
				require.Equal(t, step.ID, i+1)
				require.Equal(t, step.Name, tc.steps[i])
			}
		})
	}
}

func TestValidator_ValidateStep(t *testing.T) {
	misc.Init()
	warehouseutils.Init()

	var (
		provider = "MINIO"
		table    = "test_table"
		sslMode  = "disable"
		bucket   = "s3-datalake-test"
		region   = "us-east-1"
	)

	ctx := context.Background()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)
	minioResource, err := resource.SetupMinio(pool, t)
	require.NoError(t, err)

	err = minioResource.Client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{
		Region: "us-east-1",
	})
	require.NoError(t, err)

	defaultHandler := func() handler {
		h := handler{}
		h.conf = config.New()
		h.logger = logger.NOP
		h.statsFactory = stats.Default
		h.fileManagerFactory = filemanager.New
		h.encodingFactory = encoding.NewFactory(h.conf)
		h.config.connectionTestingFolder = misc.RudderTestPayload
		h.config.queryTimeout = 25 * time.Second
		h.config.objectStorageTimeout = 15 * time.Second
		return h
	}

	t.Run("Object Storage", func(t *testing.T) {
		t.Run("Non Datalakes", func(t *testing.T) {
			h := defaultHandler()
			err := h.validateStep(ctx, model.VerifyingObjectStorage, &backendconfig.DestinationT{
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
			})
			require.NoError(t, err)
		})

		t.Run("Datalakes", func(t *testing.T) {
			h := defaultHandler()
			err := h.validateStep(ctx, model.VerifyingObjectStorage, &backendconfig.DestinationT{
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

				h := defaultHandler()
				err := h.validateStep(ctx, model.VerifyingConnections, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
				} else {
					require.NoError(t, err)
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

				h := defaultHandler()
				err := h.validateStep(ctx, model.VerifyingCreateSchema, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
				} else {
					require.NoError(t, err)
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

				h := defaultHandler()
				err := h.validateStep(ctx, model.VerifyingCreateAndAlterTable, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
				} else {
					require.NoError(t, err)
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

		h := defaultHandler()
		err := h.validateStep(ctx, model.VerifyingFetchSchema, &backendconfig.DestinationT{
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

				h := defaultHandler()
				err := h.validateStep(ctx, model.VerifyingLoadTable, &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: conf,
				})
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
				} else {
					require.NoError(t, err)
				}

				_, err = pgResource.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.setup_test_staging", namespace))
				require.NoError(t, err)
			})
		}
	})
}

func TestValidator_Validate(t *testing.T) {
	misc.Init()
	warehouseutils.Init()

	var (
		provider = "MINIO"
		sslMode  = "disable"
	)

	ctx := context.Background()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)
	minioResource, err := resource.SetupMinio(pool, t)
	require.NoError(t, err)

	t.Run("invalid path", func(t *testing.T) {
		c := config.New()
		v := NewValidator(c, logger.NOP, stats.Default, filemanager.New, encoding.NewFactory(c))

		_, err := v.Validate(ctx, &model.ValidationRequest{
			Path: "invalid",
		})
		require.Equal(t, err, errors.New("invalid path: invalid"))
	})

	t.Run("steps", func(t *testing.T) {
		res, err := validations.Validate(ctx, &model.ValidationRequest{
			Path: "steps",
			Destination: &backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.POSTGRES,
				},
			},
		})
		require.NoError(t, err)
		require.Empty(t, res.Error)
		require.JSONEq(t, res.Data, `{"steps":[{"id":1,"name":"Verifying Object Storage","success":false,"error":""},{"id":2,"name":"Verifying Connections","success":false,"error":""},{"id":3,"name":"Verifying Create Schema","success":false,"error":""},{"id":4,"name":"Verifying Create and Alter Table","success":false,"error":""},{"id":5,"name":"Verifying Fetch Schema","success":false,"error":""},{"id":6,"name":"Verifying Load Table","success":false,"error":""}]}`)
	})

	t.Run("validate", func(t *testing.T) {
		t.Run("invalid step", func(t *testing.T) {
			res, err := validations.Validate(ctx, &model.ValidationRequest{
				Path: "validate",
				Step: "invalid",
				Destination: &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
				},
			})
			require.NoError(t, err)
			require.Empty(t, res.Error)
			require.JSONEq(t, res.Data, `{"success":false,"error":"Invalid step: invalid","steps":null}`)
		})

		t.Run("step not found", func(t *testing.T) {
			res, err := validations.Validate(ctx, &model.ValidationRequest{
				Path: "validate",
				Step: "1000",
				Destination: &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
				},
			})
			require.NoError(t, err)
			require.Empty(t, res.Error)
			require.JSONEq(t, res.Data, `{"success":false,"error":"Invalid step: 1000","steps":null}`)
		})

		t.Run("invalid destination", func(t *testing.T) {
			res, err := validations.Validate(ctx, &model.ValidationRequest{
				Path: "validate",
				Step: "2",
				Destination: &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: "invalid",
					},
				},
			})
			require.NoError(t, err)
			require.Empty(t, res.Error)
			require.JSONEq(t, res.Data, `{"success":false,"error":"creating validator: create manager: getting manager: provider of type invalid is not configured for WarehouseManager","steps":[{"id":2,"name":"Verifying Connections","success":false,"error":"creating validator: create manager: getting manager: provider of type invalid is not configured for WarehouseManager"}]}`)
		})

		t.Run("step error", func(t *testing.T) {
			res, err := validations.Validate(ctx, &model.ValidationRequest{
				Path: "validate",
				Destination: &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
				},
			})
			require.NoError(t, err)
			require.Empty(t, res.Error)
			require.JSONEq(t, res.Data, `{"success":false,"error":"upload file: creating file manager: service provider not supported: ","steps":[{"id":1,"name":"Verifying Object Storage","success":false,"error":"upload file: creating file manager: service provider not supported: "},{"id":2,"name":"Verifying Connections","success":false,"error":""},{"id":3,"name":"Verifying Create Schema","success":false,"error":""},{"id":4,"name":"Verifying Create and Alter Table","success":false,"error":""},{"id":5,"name":"Verifying Fetch Schema","success":false,"error":""},{"id":6,"name":"Verifying Load Table","success":false,"error":""}]}`)
		})

		t.Run("invalid destination", func(t *testing.T) {
			res, err := validations.Validate(ctx, &model.ValidationRequest{
				Path: "validate",
				Step: "2",
				Destination: &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: "invalid",
					},
				},
			})
			require.NoError(t, err)
			require.Empty(t, res.Error)
			require.JSONEq(t, res.Data, `{"success":false,"error":"creating validator: create manager: getting manager: provider of type invalid is not configured for WarehouseManager","steps":[{"id":2,"name":"Verifying Connections","success":false,"error":"creating validator: create manager: getting manager: provider of type invalid is not configured for WarehouseManager"}]}`)
		})

		t.Run("empty step", func(t *testing.T) {
			namespace := "es_test_namespace"

			v := NewValidator(config.New(), logger.NOP, stats.Default, filemanager.New, encoding.NewFactory(c))
			res := v.Validate(ctx, &backendconfig.DestinationT{
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
			}, "")
			require.NoError(t, err)
			require.Empty(t, res.Error)
			require.JSONEq(t, res.Steps, `{"success":true,"error":"","steps":[{"id":1,"name":"Verifying Object Storage","success":true,"error":""},{"id":2,"name":"Verifying Connections","success":true,"error":""},{"id":3,"name":"Verifying Create Schema","success":true,"error":""},{"id":4,"name":"Verifying Create and Alter Table","success":true,"error":""},{"id":5,"name":"Verifying Fetch Schema","success":true,"error":""},{"id":6,"name":"Verifying Load Table","success":true,"error":""}]}`)
		})

		t.Run("steps in order", func(t *testing.T) {
			namespace := "sio_test_namespace"

			testCases := []struct {
				name     string
				step     string
				response string
			}{
				{
					name:     "step 1",
					step:     "1",
					response: `{"success":true,"error":"","steps":[{"id":1,"name":"Verifying Object Storage","success":true,"error":""}]}`,
				},
				{
					name:     "step 2",
					step:     "2",
					response: `{"success":true,"error":"","steps":[{"id":2,"name":"Verifying Connections","success":true,"error":""}]}`,
				},
				{
					name:     "step 3",
					step:     "3",
					response: `{"success":true,"error":"","steps":[{"id":3,"name":"Verifying Create Schema","success":true,"error":""}]}`,
				},
				{
					name:     "step 4",
					step:     "4",
					response: `{"success":true,"error":"","steps":[{"id":4,"name":"Verifying Create and Alter Table","success":true,"error":""}]}`,
				},
				{
					name:     "step 5",
					step:     "5",
					response: `{"success":true,"error":"","steps":[{"id":5,"name":"Verifying Fetch Schema","success":true,"error":""}]}`,
				},
				{
					name:     "step 6",
					step:     "6",
					response: `{"success":true,"error":"","steps":[{"id":6,"name":"Verifying Load Table","success":true,"error":""}]}`,
				},
			}

			for _, tc := range testCases {
				tc := tc

				c := config.New()
				v := NewValidator(c, logger.NOP, stats.Default, filemanager.New, encoding.NewFactory(c))

				res := v.Validate(ctx, &backendconfig.DestinationT{
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
				},
					tc.step,
				)
				require.NoError(t, err)
				require.Empty(t, res.Error)
				require.Equal(t, res.Steps, tc.response)
			}
		})
	})
}
