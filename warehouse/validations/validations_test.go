package validations_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	misc.Init()
	warehouseutils.Init()
	encoding.Init()
	validations.Init()

	var (
		provider        = "MINIO"
		namespace       = "test_namespace"
		sslmode         = "disable"
		host            = "localhost"
		database        = "rudderdb"
		user            = "rudder"
		password        = "rudder-password"
		bucketName      = "testbucket"
		accessKeyID     = "MYACCESSKEY"
		secretAccessKey = "MYSECRETKEY"
	)

	ctx := context.Background()

	t.Run("invalid path", func(t *testing.T) {
		_, err := validations.Validate(ctx, &model.ValidationRequest{
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
			c := setup(t, "../testdata/docker-compose.postgres.yml", "../testdata/docker-compose.minio.yml")

			res, err := validations.Validate(ctx, &model.ValidationRequest{
				Path: "validate",
				Step: "",
				Destination: &backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.POSTGRES,
					},
					Config: map[string]interface{}{
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
						"endPoint":        fmt.Sprintf("localhost:%d", c.Port("minio", 9000)),
					},
				},
			})
			require.NoError(t, err)
			require.Empty(t, res.Error)
			require.JSONEq(t, res.Data, `{"success":true,"error":"","steps":[{"id":1,"name":"Verifying Object Storage","success":true,"error":""},{"id":2,"name":"Verifying Connections","success":true,"error":""},{"id":3,"name":"Verifying Create Schema","success":true,"error":""},{"id":4,"name":"Verifying Create and Alter Table","success":true,"error":""},{"id":5,"name":"Verifying Fetch Schema","success":true,"error":""},{"id":6,"name":"Verifying Load Table","success":true,"error":""}]}`)
		})

		t.Run("steps in order", func(t *testing.T) {
			c := setup(t, "../testdata/docker-compose.postgres.yml", "../testdata/docker-compose.minio.yml")

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

				t.Run(tc.name, func(t *testing.T) {
					res, err := validations.Validate(ctx, &model.ValidationRequest{
						Path: "validate",
						Step: tc.step,
						Destination: &backendconfig.DestinationT{
							DestinationDefinition: backendconfig.DestinationDefinitionT{
								Name: warehouseutils.POSTGRES,
							},
							Config: map[string]interface{}{
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
								"endPoint":        fmt.Sprintf("localhost:%d", c.Port("minio", 9000)),
							},
						},
					})
					require.NoError(t, err)
					require.Empty(t, res.Error)
					require.Equal(t, res.Data, tc.response)
				})
			}
		})
	})
}
