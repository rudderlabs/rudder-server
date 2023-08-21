package warehouse

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/multitenant"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendConfig "github.com/rudderlabs/rudder-server/backend-config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestWarehouseGRPC(t *testing.T) {
	var (
		ctx             = context.Background()
		sourceID        = "test-sourceID"
		destinationID   = "test-destinationID"
		workspaceID     = "test-workspaceID"
		destinationType = "POSTGRES"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Run("dedicated workspace", func(t *testing.T) {
		pgResource, resetBackendConfigManager := setupWarehouseGRPCTest(
			ctx, t, pool, sourceID, destinationID, destinationType, workspaceID,
		)

		newRetryWHUploadsRequest := func() *proto.RetryWHUploadsRequest {
			return &proto.RetryWHUploadsRequest{
				WorkspaceId:     workspaceID,
				SourceId:        sourceID,
				DestinationId:   destinationID,
				DestinationType: destinationType,
			}
		}

		t.Run("get health", func(t *testing.T) {
			w := &warehouseGRPC{}
			res, err := w.GetHealth(ctx, &emptypb.Empty{})
			require.NoError(t, err)
			require.True(t, res.Value)
		})

		t.Run("get warehouse upload", func(t *testing.T) {
			t.Run("invalid upload ID", func(t *testing.T) {
				req := &proto.WHUploadRequest{UploadId: -1, WorkspaceId: workspaceID}

				w := &warehouseGRPC{}
				_, err := w.GetWHUpload(ctx, req)
				require.Error(t, err)
			})
			t.Run("unauthorized source", func(t *testing.T) {
				bcManager.sourceIDsByWorkspace = map[string][]string{}
				t.Cleanup(resetBackendConfigManager)

				req := &proto.WHUploadRequest{UploadId: 1, WorkspaceId: workspaceID}

				w := &warehouseGRPC{}
				_, err := w.GetWHUpload(ctx, req)
				require.Error(t, err)
			})
			t.Run("successfully get uploads", func(t *testing.T) {
				req := &proto.WHUploadRequest{UploadId: 1, WorkspaceId: workspaceID}

				w := &warehouseGRPC{}
				res, err := w.GetWHUpload(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.EqualValues(t, 1, res.Id)
				require.Len(t, res.Tables, 1)
				require.EqualValues(t, 1, res.Tables[0].UploadId)
			})
			t.Run("get warehouse uploads", func(t *testing.T) {
				newWHUploadsRequest := func() *proto.WHUploadsRequest {
					return &proto.WHUploadsRequest{
						WorkspaceId:     workspaceID,
						SourceId:        sourceID,
						DestinationId:   destinationID,
						DestinationType: destinationType,
						Limit:           2,
					}
				}

				t.Run("unauthorized source", func(t *testing.T) {
					bcManager.sourceIDsByWorkspace = map[string][]string{}
					t.Cleanup(resetBackendConfigManager)

					w := &warehouseGRPC{}
					res, err := w.GetWHUploads(ctx, newWHUploadsRequest())
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.Uploads, 0)
				})
				t.Run("waiting syncs", func(t *testing.T) {
					req := newWHUploadsRequest()
					req.Status = "waiting"

					w := &warehouseGRPC{}
					res, err := w.GetWHUploads(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.Uploads, 1)
					require.EqualValues(t, 1, res.Uploads[0].Id)
				})
				t.Run("succeeded syncs", func(t *testing.T) {
					req := newWHUploadsRequest()
					req.Status = "success"

					w := &warehouseGRPC{}
					res, err := w.GetWHUploads(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.Uploads, 1)
					require.EqualValues(t, 4, res.Uploads[0].Id)
				})
				t.Run("failed syncs", func(t *testing.T) {
					req := newWHUploadsRequest()
					req.Status = "failed"

					w := &warehouseGRPC{}
					res, err := w.GetWHUploads(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.Uploads, 1)
					require.EqualValues(t, 2, res.Uploads[0].Id)
				})
				t.Run("aborted syncs", func(t *testing.T) {
					req := newWHUploadsRequest()
					req.Status = "aborted"

					w := &warehouseGRPC{}
					res, err := w.GetWHUploads(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.Uploads, 1)
					require.EqualValues(t, 3, res.Uploads[0].Id)
				})
				t.Run("no status", func(t *testing.T) {
					req := newWHUploadsRequest()

					w := &warehouseGRPC{}
					res, err := w.GetWHUploads(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.Uploads, 2)
					require.EqualValues(t, 4, res.Uploads[0].Id)
					require.EqualValues(t, 3, res.Uploads[1].Id)
				})
				t.Run("with offset", func(t *testing.T) {
					req := newWHUploadsRequest()
					req.Offset = 3

					w := &warehouseGRPC{}
					res, err := w.GetWHUploads(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.Uploads, 1)
					require.EqualValues(t, 1, res.Uploads[0].Id)
				})
			})
			t.Run("count warehouse uploads to retry", func(t *testing.T) {
				t.Run("invalid request", func(t *testing.T) {
					w := &warehouseGRPC{}
					res, err := w.CountWHUploadsToRetry(ctx, &proto.RetryWHUploadsRequest{})
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("invalid retry interval and uploadIDs", func(t *testing.T) {
					req := newRetryWHUploadsRequest()
					req.IntervalInHours = -1
					req.UploadIds = []int64{}

					w := &warehouseGRPC{}
					res, err := w.CountWHUploadsToRetry(ctx, req)
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("unauthorized source", func(t *testing.T) {
					bcManager.sourceIDsByWorkspace = map[string][]string{}
					t.Cleanup(resetBackendConfigManager)

					req := newRetryWHUploadsRequest()
					req.IntervalInHours = 12

					w := &warehouseGRPC{}
					res, err := w.CountWHUploadsToRetry(ctx, req)
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("invalid source", func(t *testing.T) {
					req := newRetryWHUploadsRequest()
					req.IntervalInHours = 12
					req.SourceId = "test-sourceID-1"

					w := &warehouseGRPC{}
					res, err := w.CountWHUploadsToRetry(ctx, req)
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("interval in hours", func(t *testing.T) {
					req := newRetryWHUploadsRequest()
					req.IntervalInHours = 24

					w := &warehouseGRPC{}
					res, err := w.CountWHUploadsToRetry(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusOK, res.StatusCode)
					require.EqualValues(t, 1, res.Count)
				})
				t.Run("uploadIDs", func(t *testing.T) {
					req := newRetryWHUploadsRequest()
					req.UploadIds = []int64{3}

					w := &warehouseGRPC{}
					res, err := w.CountWHUploadsToRetry(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusOK, res.StatusCode)
					require.EqualValues(t, 1, res.Count)
				})
			})
			t.Run("retry warehouse uploads", func(t *testing.T) {
				t.Run("invalid request", func(t *testing.T) {
					req := newRetryWHUploadsRequest()

					w := &warehouseGRPC{}
					res, err := w.RetryWHUploads(ctx, req)
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("invalid retry interval and uploadIDs", func(t *testing.T) {
					req := newRetryWHUploadsRequest()
					req.IntervalInHours = -1
					req.UploadIds = []int64{}

					w := &warehouseGRPC{}
					res, err := w.RetryWHUploads(ctx, req)
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("unauthorized source", func(t *testing.T) {
					bcManager.sourceIDsByWorkspace = map[string][]string{}
					t.Cleanup(resetBackendConfigManager)

					req := newRetryWHUploadsRequest()
					req.IntervalInHours = 12

					w := &warehouseGRPC{}
					res, err := w.RetryWHUploads(ctx, req)
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("invalid source", func(t *testing.T) {
					req := newRetryWHUploadsRequest()
					req.IntervalInHours = 12
					req.SourceId = "test-sourceID-1"

					w := &warehouseGRPC{}
					res, err := w.RetryWHUploads(ctx, req)
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("interval in hours", func(t *testing.T) {
					req := newRetryWHUploadsRequest()
					req.IntervalInHours = 12

					w := &warehouseGRPC{}
					res, err := w.RetryWHUploads(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusOK, res.StatusCode)
				})
				t.Run("uploadIDs", func(t *testing.T) {
					req := newRetryWHUploadsRequest()
					req.UploadIds = []int64{3}

					w := &warehouseGRPC{}
					res, err := w.RetryWHUploads(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusOK, res.StatusCode)
				})
			})
			t.Run("trigger warehouse upload", func(t *testing.T) {
				newWHUploadRequest := func() *proto.WHUploadRequest {
					return &proto.WHUploadRequest{
						UploadId:    3,
						WorkspaceId: workspaceID,
					}
				}
				t.Run("unauthorized source", func(t *testing.T) {
					bcManager.sourceIDsByWorkspace = map[string][]string{}
					t.Cleanup(resetBackendConfigManager)

					req := newWHUploadRequest()

					w := &warehouseGRPC{}
					res, err := w.TriggerWHUpload(ctx, req)
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("triggered successfully", func(t *testing.T) {
					req := newWHUploadRequest()

					w := &warehouseGRPC{}
					res, err := w.TriggerWHUpload(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusOK, res.StatusCode)
					require.EqualValues(t, TriggeredSuccessfully, res.Message)
				})
			})
			t.Run("triggering warehouse uploads", func(t *testing.T) {
				newWHUploadsRequest := func() *proto.WHUploadsRequest {
					return &proto.WHUploadsRequest{
						WorkspaceId:   workspaceID,
						SourceId:      sourceID,
						DestinationId: destinationID,
					}
				}

				t.Run("unauthorized source", func(t *testing.T) {
					bcManager.sourceIDsByWorkspace = map[string][]string{}
					t.Cleanup(resetBackendConfigManager)

					req := newWHUploadsRequest()

					w := &warehouseGRPC{}
					res, err := w.TriggerWHUploads(ctx, req)
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("unknown destination", func(t *testing.T) {
					req := newWHUploadsRequest()
					req.DestinationId = ""

					w := &warehouseGRPC{}
					res, err := w.TriggerWHUploads(ctx, req)
					require.Error(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusBadRequest, res.StatusCode)
				})
				t.Run("no pending events", func(t *testing.T) {
					req := newWHUploadsRequest()
					req.DestinationId = "test-destinationID-1"

					w := &warehouseGRPC{}
					res, err := w.TriggerWHUploads(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusOK, res.StatusCode)
					require.EqualValues(t, NoPendingEvents, res.Message)
				})
				t.Run("triggered successfully", func(t *testing.T) {
					req := newWHUploadsRequest()

					w := &warehouseGRPC{}
					res, err := w.TriggerWHUploads(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusOK, res.StatusCode)
					require.EqualValues(t, TriggeredSuccessfully, res.Message)
				})
			})
			t.Run("configuration validations", func(t *testing.T) {
				minioResource, err := destination.SetupMINIO(pool, t)
				require.NoError(t, err)

				newWHValidationRequest := func() *proto.WHValidationRequest {
					return &proto.WHValidationRequest{
						Path: "validate",
						Body: fmt.Sprintf(`{
							"destination": {
								"config": {
									"host":             %q,
									"database":         %q,
									"user":             %q,
									"password":         %q,
									"port":             %q,
									"sslMode":          "disable",
									"namespace":        "",
									"bucketProvider":   "MINIO",
									"bucketName":       %q,
									"accessKeyID":      %q,
									"secretAccessKey":  %q,
									"useSSL":           false,
									"endPoint":         %q,
									"syncFrequency":    "30",
									"useRudderStorage": false
								},
								"destinationDefinition": {
									"id":          "1bJ4YC7INdkvBTzotNh0zta5jDm",
									"name":        "POSTGRES",
									"displayName": "Postgres"
								}
							}
						}`,
							pgResource.Host,
							pgResource.Database,
							pgResource.User,
							pgResource.Password,
							pgResource.Port,
							minioResource.BucketName,
							minioResource.AccessKey,
							minioResource.SecretKey,
							minioResource.Endpoint,
						),
					}
				}

				t.Run("validating with steps", func(t *testing.T) {
					type testCase struct {
						stepID   string
						stepName string
					}
					testCases := []testCase{
						{stepID: "1", stepName: "Verifying Object Storage"},
						{stepID: "2", stepName: "Verifying Connections"},
						{stepID: "3", stepName: "Verifying Create Schema"},
						{stepID: "4", stepName: "Verifying Create and Alter Table"},
						{stepID: "5", stepName: "Verifying Fetch Schema"},
						{stepID: "6", stepName: "Verifying Load Table"},
					}
					for _, tc := range testCases {
						tc := tc
						t.Run(tc.stepName, func(t *testing.T) {
							req := newWHValidationRequest()
							req.Step = tc.stepID

							w := &warehouseGRPC{}
							res, err := w.Validate(ctx, req)
							require.NoError(t, err)
							require.NotNil(t, res)
							require.Empty(t, res.Error)
							require.JSONEq(t, fmt.Sprintf(`{
								"success": true,
								"error": "",
								"steps": [
									{
										"id": %s,
										"name": %q,
										"success": true,
										"error": ""
									}
								]
							}`, tc.stepID, tc.stepName), res.Data)
						})
					}
				})
				t.Run("validating without steps", func(t *testing.T) {
					t.Run("validate", func(t *testing.T) {
						req := newWHValidationRequest()

						w := &warehouseGRPC{}
						res, err := w.Validate(ctx, req)
						require.NoError(t, err)
						require.NotNil(t, res)
						require.Empty(t, res.Error)
						require.JSONEq(t, `{
							"success": true,
							"error": "",
							"steps": [
								{
									"id": 1,
									"name": "Verifying Object Storage",
									"success": true,
									"error": ""
								},
								{
									"id": 2,
									"name": "Verifying Connections",
									"success": true,
									"error": ""
								},
								{
									"id": 3,
									"name": "Verifying Create Schema",
									"success": true,
									"error": ""
								},
								{
									"id": 4,
									"name": "Verifying Create and Alter Table",
									"success": true,
									"error": ""
								},
								{
									"id": 5,
									"name": "Verifying Fetch Schema",
									"success": true,
									"error": ""
								},
								{
									"id": 6,
									"name": "Verifying Load Table",
									"success": true,
									"error": ""
								}
							]
						}`, res.Data)
					})
				})
			})
		})
	})

	t.Run("multi tenant", func(t *testing.T) {
		t.Setenv("DEPLOYMENT_TYPE", "MULTITENANT")
		t.Setenv("HOSTED_SERVICE_SECRET", "test-secret")

		_, _ = setupWarehouseGRPCTest(
			ctx, t, pool, sourceID, destinationID, destinationType, workspaceID,
		)

		newWHUploadsRequest := func() *proto.WHUploadsRequest {
			return &proto.WHUploadsRequest{
				WorkspaceId:     workspaceID,
				SourceId:        sourceID,
				DestinationId:   destinationID,
				DestinationType: destinationType,
				Limit:           2,
			}
		}

		t.Run("getting warehouse uploads", func(t *testing.T) {
			t.Run("waiting syncs", func(t *testing.T) {
				req := newWHUploadsRequest()
				req.Status = "waiting"

				w := &warehouseGRPC{}
				res, err := w.GetWHUploads(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.Len(t, res.Uploads, 1)
				require.EqualValues(t, 1, res.Uploads[0].Id)
			})
			t.Run("succeeded syncs", func(t *testing.T) {
				req := newWHUploadsRequest()
				req.Status = "success"

				w := &warehouseGRPC{}
				res, err := w.GetWHUploads(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.Len(t, res.Uploads, 1)
				require.EqualValues(t, 4, res.Uploads[0].Id)
			})
			t.Run("failed syncs", func(t *testing.T) {
				req := newWHUploadsRequest()
				req.Status = "failed"

				w := &warehouseGRPC{}
				res, err := w.GetWHUploads(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.Len(t, res.Uploads, 1)
				require.EqualValues(t, 2, res.Uploads[0].Id)
			})
			t.Run("aborted syncs", func(t *testing.T) {
				req := newWHUploadsRequest()
				req.Status = "aborted"

				w := &warehouseGRPC{}
				res, err := w.GetWHUploads(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.Len(t, res.Uploads, 1)
				require.EqualValues(t, 3, res.Uploads[0].Id)
			})
			t.Run("no status", func(t *testing.T) {
				req := newWHUploadsRequest()

				w := &warehouseGRPC{}
				res, err := w.GetWHUploads(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.Len(t, res.Uploads, 2)
				require.EqualValues(t, 4, res.Uploads[0].Id)
				require.EqualValues(t, 3, res.Uploads[1].Id)
			})
			t.Run("with offset", func(t *testing.T) {
				req := newWHUploadsRequest()
				req.Offset = 3

				w := &warehouseGRPC{}
				res, err := w.GetWHUploads(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.Len(t, res.Uploads, 1)
				require.EqualValues(t, 1, res.Uploads[0].Id)
			})
		})
	})
}

func setupWarehouseGRPCTest(
	ctx context.Context, t *testing.T, pool *dockertest.Pool,
	sourceID, destinationID, destinationType, workspaceID string,
) (*resource.PostgresResource, func()) {
	pgResource := setupWarehouseJobsDB(pool, t)

	initWarehouse()
	err := setupDB(ctx, getConnectionString())
	require.NoError(t, err)

	sqlStatement, err := os.ReadFile("./testdata/sql/grpc_test.sql")
	require.NoError(t, err)
	_, err = pgResource.DB.Exec(string(sqlStatement))
	require.NoError(t, err)

	pkgLogger = logger.NOP

	tenantManager = &multitenant.Manager{
		BackendConfig: backendConfig.DefaultBackendConfig,
	}

	bcManager = newBackendConfigManager(
		config.Default, wrappedDBHandle, tenantManager, nil,
	)
	resetBackendConfigManager := func() {
		wh := model.Warehouse{
			Source:      backendconfig.SourceT{ID: sourceID},
			Destination: backendconfig.DestinationT{ID: destinationID},
			Identifier:  warehouseutils.GetWarehouseIdentifier(destinationType, sourceID, destinationID),
		}
		bcManager.warehouses = []model.Warehouse{wh}
		bcManager.sourceIDsByWorkspace = map[string][]string{
			workspaceID: {sourceID},
		}
		bcManager.connectionsMap = map[string]map[string]model.Warehouse{
			destinationID: {
				sourceID: wh,
			},
		}
	}
	resetBackendConfigManager()

	require.NoError(t, InitWarehouseAPI(pgResource.DB, bcManager, logger.NOP))

	return pgResource, resetBackendConfigManager
}

// TestWarehouseGRPCValidation validates proto message for object storage destination
func TestWarehouseGRPCValidation(t *testing.T) {
	t.Run("Code_INVALID_ARGUMENT if unsupported/invalid object storage destination type", func(t *testing.T) {
		_, err := validateObjectStorageRequestBody(&proto.ValidateObjectStorageRequest{Type: "ABC"})
		statusError, _ := status.FromError(err)
		require.EqualError(t, err,
			"rpc error: code = InvalidArgument desc = invalid argument err: \ntype: ABC not supported",
		)
		require.Equal(t, statusError.Code(), codes.Code(code.Code_INVALID_ARGUMENT))
	})
	t.Run("Code_INVALID_ARGUMENT if no bucketName(Not applicable for AZURE_BLOB)", func(t *testing.T) {
		configMap := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"containerName": {
					Kind: &structpb.Value_StringValue{
						StringValue: "tempContainerName",
					},
				},
			},
		}
		_, err := validateObjectStorageRequestBody(&proto.ValidateObjectStorageRequest{Type: "S3", Config: configMap})
		statusError, _ := status.FromError(err)
		require.Equal(t, statusError.Code(), codes.Code(code.Code_INVALID_ARGUMENT))
	})
	t.Run("Code_INVALID_ARGUMENT if no containerName(For AZURE_BLOB only)", func(t *testing.T) {
		configMap := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"bucketName": {
					Kind: &structpb.Value_StringValue{
						StringValue: "tempbucket",
					},
				},
			},
		}
		_, err := validateObjectStorageRequestBody(&proto.ValidateObjectStorageRequest{
			Type:   "AZURE_BLOB",
			Config: configMap,
		})
		statusError, _ := status.FromError(err)
		require.EqualError(t, err,
			"rpc error: code = InvalidArgument desc = invalid argument err: \ncontainerName invalid or not present",
		)
		require.Equal(t, statusError.Code(), codes.Code(code.Code_INVALID_ARGUMENT))
	})
}
