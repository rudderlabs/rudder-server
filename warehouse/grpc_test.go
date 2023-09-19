package warehouse

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestGRPC(t *testing.T) {
	Init4()
	validations.Init()
	misc.Init()

	const (
		workspaceID         = "test_workspace_id"
		sourceID            = "test_source_id"
		destinationID       = "test_destination_id"
		destinationType     = "destination_type"
		unusedWorkspaceID   = "unused_test_workspace_id"
		unusedSourceID      = "unused_test_source_id"
		unusedDestinationID = "unused_test_destination_id"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Run("endpoints", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := destination.SetupMINIO(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		ctx, stopTest := context.WithCancel(context.Background())

		db := sqlmw.New(pgResource.DB)

		controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"publicKey": "public_key", "privateKey": "private_key"}`))
		}))
		t.Cleanup(func() {
			controlPlane.Close()
		})

		c := config.New()
		c.Set("ENABLE_TUNNELLING", true)
		c.Set("CONFIG_BACKEND_URL", controlPlane.URL)

		ctrl := gomock.NewController(t)
		mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
		mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
			return nil
		}).AnyTimes()
		mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{
				Data: map[string]backendconfig.ConfigT{
					workspaceID: {
						WorkspaceID: workspaceID,
						Sources: []backendconfig.SourceT{
							{
								ID:      sourceID,
								Enabled: true,
								Destinations: []backendconfig.DestinationT{
									{
										ID:      destinationID,
										Enabled: true,
										DestinationDefinition: backendconfig.DestinationDefinitionT{
											Name: whutils.POSTGRES,
										},
									},
								},
							},
						},
					},
					unusedWorkspaceID: {
						WorkspaceID: unusedWorkspaceID,
						Sources: []backendconfig.SourceT{
							{
								ID:      unusedSourceID,
								Enabled: true,
								Destinations: []backendconfig.DestinationT{
									{
										ID:      unusedDestinationID,
										Enabled: true,
										DestinationDefinition: backendconfig.DestinationDefinitionT{
											Name: whutils.POSTGRES,
										},
									},
								},
							},
						},
					},
				},
				Topic: string(backendconfig.TopicBackendConfig),
			}
			close(ch)
			return ch
		}).AnyTimes()

		tenantManager := multitenant.New(c, mockBackendConfig)
		bcManager := newBackendConfigManager(c, db, tenantManager, logger.NOP)
		grpcServer, err := NewGRPCServer(c, logger.NOP, db, tenantManager, bcManager)
		require.NoError(t, err)

		tcpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		tcpAddress := net.JoinHostPort("", strconv.Itoa(tcpPort))

		listener, err := net.Listen("tcp", tcpAddress)
		require.NoError(t, err)

		server := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
		proto.RegisterWarehouseServer(server, grpcServer)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			tenantManager.Run(gCtx)
			return nil
		})
		g.Go(func() error {
			bcManager.Start(gCtx)
			return nil
		})
		g.Go(func() error {
			return server.Serve(listener)
		})

		grpcClientConn, err := grpc.Dial("localhost:"+strconv.Itoa(tcpPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, grpcClientConn.Close())
		})
		grpcClient := proto.NewWarehouseClient(grpcClientConn)

		t.Run("GetHealth", func(t *testing.T) {
			var healthResponse *wrapperspb.BoolValue
			var err error

			require.Eventually(t, func() bool {
				if healthResponse, err = grpcClient.GetHealth(ctx, &emptypb.Empty{}); err != nil {
					return false
				} else if healthResponse == nil {
					return false
				} else {
					return healthResponse.GetValue()
				}
			},
				time.Second*10,
				time.Millisecond*100,
				"health check failed", err,
			)
		})

		t.Run("Get syncs", func(t *testing.T) {
			tables := []string{"table_name", "table_name_2"}
			now := time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC)
			repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
				return now
			}))
			repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
				return now
			}))
			repoTableUploads := repo.NewTableUploads(db, repo.WithNow(func() time.Time {
				return now
			}))

			totalUploads := 100
			firstEventAt, lastEventAt := now.Add(-2*time.Hour), now.Add(-1*time.Hour)

			for i := 0; i < totalUploads; i++ {
				fid, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
				require.NoError(t, err)
				sid, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
				require.NoError(t, err)

				uploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: destinationType,
					WorkspaceID:     workspaceID,
					Status:          model.Waiting,
					NextRetryTime:   now,
				}, []*model.StagingFile{
					{
						ID:            fid,
						SourceID:      sourceID,
						DestinationID: destinationID,
						WorkspaceID:   workspaceID,
						FirstEventAt:  firstEventAt,
					},
					{
						ID:            sid,
						SourceID:      sourceID,
						DestinationID: destinationID,
						WorkspaceID:   workspaceID,
						LastEventAt:   lastEventAt,
					},
				})
				require.NoError(t, err)

				err = repoTableUploads.Insert(ctx, uploadID, tables)
				require.NoError(t, err)
			}

			t.Run("GetWHUpload", func(t *testing.T) {
				t.Run("invalid id", func(t *testing.T) {
					res, err := grpcClient.GetWHUpload(ctx, &proto.WHUploadRequest{
						UploadId:    -1,
						WorkspaceId: workspaceID,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "upload_id should be greater than 0", statusError.Message())
				})

				t.Run("no sources", func(t *testing.T) {
					res, err := grpcClient.GetWHUpload(ctx, &proto.WHUploadRequest{
						UploadId:    1,
						WorkspaceId: "unknown_workspace_id",
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "no sources found for workspace: unknown_workspace_id", statusError.Message())
				})

				t.Run("unknown id", func(t *testing.T) {
					res, err := grpcClient.GetWHUpload(ctx, &proto.WHUploadRequest{
						UploadId:    1001,
						WorkspaceId: workspaceID,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.NotFound, statusError.Code())
					require.Equal(t, "no sync found for id 1001", statusError.Message())
				})

				t.Run("unauthorized", func(t *testing.T) {
					res, err := grpcClient.GetWHUpload(ctx, &proto.WHUploadRequest{
						UploadId:    1,
						WorkspaceId: unusedWorkspaceID,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "unauthorized request", statusError.Message())
				})

				t.Run("success", func(t *testing.T) {
					res, err := grpcClient.GetWHUpload(ctx, &proto.WHUploadRequest{
						UploadId:    1,
						WorkspaceId: workspaceID,
					})
					require.NoError(t, err)
					require.NotNil(t, res)

					require.EqualValues(t, sourceID, res.GetSourceId())
					require.EqualValues(t, destinationID, res.GetDestinationId())
					require.EqualValues(t, destinationType, res.GetDestinationType())
					require.Equal(t, "{}", res.GetError())
					require.Zero(t, res.GetAttempt())
					require.EqualValues(t, model.Waiting, res.GetStatus())
					require.EqualValues(t, now.UTC(), res.GetCreatedAt().AsTime().UTC())
					require.EqualValues(t, firstEventAt.UTC(), res.GetFirstEventAt().AsTime().UTC())
					require.EqualValues(t, lastEventAt.UTC(), res.GetLastEventAt().AsTime().UTC())
					require.EqualValues(t, now.UTC(), res.GetNextRetryTime().AsTime().UTC())
					require.EqualValues(t, now.Sub(time.Time{})/time.Second, res.GetDuration())
					require.NotEmpty(t, res.GetTables())
					require.False(t, res.GetIsArchivedUpload())
					require.EqualValues(t, tables, lo.Map(res.GetTables(), func(item *proto.WHTable, index int) string {
						return item.GetName()
					}))

					for _, table := range res.GetTables() {
						require.EqualValues(t, 1, table.GetUploadId())
						require.EqualValues(t, model.TableUploadWaiting, table.GetStatus())
						require.EqualValues(t, "{}", table.GetError())
						require.Empty(t, table.GetLastExecAt().AsTime().UTC())
						require.Zero(t, table.GetCount())
						require.EqualValues(t, now.Sub(time.Time{})/time.Second, table.GetDuration())
					}
				})
			})

			t.Run("GetWHUploads", func(t *testing.T) {
				t.Run("no sources", func(t *testing.T) {
					res, err := grpcClient.GetWHUploads(ctx, &proto.WHUploadsRequest{
						Limit:         10,
						Offset:        0,
						WorkspaceId:   "unknown_workspace_id",
						SourceId:      "unknown_source_id",
						DestinationId: "unknown_destination_id",
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "no sources found for workspace: unknown_workspace_id", statusError.Message())
				})
				t.Run("invalid limit and offset", func(t *testing.T) {
					res, err := grpcClient.GetWHUploads(ctx, &proto.WHUploadsRequest{
						Limit:       -1,
						Offset:      -1,
						WorkspaceId: workspaceID,
					})
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.GetUploads(), 10)
					require.NotNil(t, res.GetPagination())
					require.EqualValues(t, int64(100), res.GetPagination().GetTotal())
					require.EqualValues(t, int64(10), res.GetPagination().GetLimit())
					require.EqualValues(t, int64(0), res.GetPagination().GetOffset())
				})
				t.Run("success", func(t *testing.T) {
					res, err := grpcClient.GetWHUploads(ctx, &proto.WHUploadsRequest{
						Limit:           25,
						Offset:          6,
						WorkspaceId:     workspaceID,
						SourceId:        sourceID,
						DestinationId:   destinationID,
						DestinationType: destinationType,
						Status:          model.Waiting,
					})
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.GetUploads(), 25)
					require.NotNil(t, res.GetPagination())
					require.EqualValues(t, int64(100), res.GetPagination().GetTotal())
					require.EqualValues(t, int64(25), res.GetPagination().GetLimit())
					require.EqualValues(t, int64(6), res.GetPagination().GetOffset())

					for _, upload := range res.GetUploads() {
						require.EqualValues(t, sourceID, upload.GetSourceId())
						require.EqualValues(t, destinationID, upload.GetDestinationId())
						require.EqualValues(t, destinationType, upload.GetDestinationType())
						require.Equal(t, "{}", upload.GetError())
						require.Zero(t, upload.GetAttempt())
						require.EqualValues(t, model.Waiting, upload.GetStatus())
						require.EqualValues(t, now.UTC(), upload.GetCreatedAt().AsTime().UTC())
						require.EqualValues(t, firstEventAt.UTC(), upload.GetFirstEventAt().AsTime().UTC())
						require.EqualValues(t, lastEventAt.UTC(), upload.GetLastEventAt().AsTime().UTC())
						require.EqualValues(t, now.UTC(), upload.GetNextRetryTime().AsTime().UTC())
						require.EqualValues(t, now.Sub(time.Time{})/time.Second, upload.GetDuration())
						require.Empty(t, upload.GetTables())
						require.False(t, upload.GetIsArchivedUpload())
					}
				})
			})
		})

		t.Run("Trigger", func(t *testing.T) {
			now := time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC)
			repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
				return now
			}))
			repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
				return now
			}))

			totalUploads := 100
			firstEventAt, lastEventAt := now.Add(-2*time.Hour), now.Add(-1*time.Hour)

			for i := 0; i < totalUploads; i++ {
				fid, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
				require.NoError(t, err)
				sid, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
				require.NoError(t, err)

				_, err = repoUpload.CreateWithStagingFiles(ctx, model.Upload{
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: destinationType,
					WorkspaceID:     workspaceID,
					Status:          model.Waiting,
					NextRetryTime:   now,
				}, []*model.StagingFile{
					{
						ID:            fid,
						SourceID:      sourceID,
						DestinationID: destinationID,
						WorkspaceID:   workspaceID,
						FirstEventAt:  firstEventAt,
					},
					{
						ID:            sid,
						SourceID:      sourceID,
						DestinationID: destinationID,
						WorkspaceID:   workspaceID,
						LastEventAt:   lastEventAt,
					},
				})
				require.NoError(t, err)
			}

			t.Run("TriggerWHUpload", func(t *testing.T) {
				t.Run("no sources", func(t *testing.T) {
					res, err := grpcClient.TriggerWHUpload(ctx, &proto.WHUploadRequest{
						UploadId:    1,
						WorkspaceId: "unknown_workspace_id",
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "no sources found for workspace: unknown_workspace_id", statusError.Message())
				})

				t.Run("unknown id", func(t *testing.T) {
					res, err := grpcClient.TriggerWHUpload(ctx, &proto.WHUploadRequest{
						UploadId:    -1,
						WorkspaceId: workspaceID,
					})
					require.NoError(t, err)
					require.NotEmpty(t, res)
					require.EqualValues(t, noSuchSync, res.GetMessage())
					require.EqualValues(t, http.StatusOK, res.GetStatusCode())
				})

				t.Run("unauthorized", func(t *testing.T) {
					res, err := grpcClient.TriggerWHUpload(ctx, &proto.WHUploadRequest{
						UploadId:    1,
						WorkspaceId: unusedWorkspaceID,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "unauthorized request", statusError.Message())
				})

				t.Run("success", func(t *testing.T) {
					res, err := grpcClient.TriggerWHUpload(ctx, &proto.WHUploadRequest{
						UploadId:    1,
						WorkspaceId: workspaceID,
					})
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, triggeredSuccessfully, res.GetMessage())
					require.EqualValues(t, http.StatusOK, res.GetStatusCode())

					upload, err := repoUpload.Get(ctx, 1)
					require.NoError(t, err)
					require.Equal(t, upload.Status, model.Waiting)
					require.True(t, upload.Retried)
					require.Equal(t, upload.Priority, 50)
				})
			})

			t.Run("TriggerWHUploads", func(t *testing.T) {
				t.Run("no sources", func(t *testing.T) {
					res, err := grpcClient.TriggerWHUploads(ctx, &proto.WHUploadsRequest{
						WorkspaceId:   "unknown_workspace_id",
						SourceId:      "unknown_source_id",
						DestinationId: "unknown_destination_id",
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "no sources found for workspace: unknown_workspace_id", statusError.Message())
				})
				t.Run("empty destination id", func(t *testing.T) {
					res, err := grpcClient.TriggerWHUploads(ctx, &proto.WHUploadsRequest{
						WorkspaceId: workspaceID,
						SourceId:    sourceID,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "destination id is required", statusError.Message())
				})

				t.Run("empty source id", func(t *testing.T) {
					res, err := grpcClient.TriggerWHUploads(ctx, &proto.WHUploadsRequest{
						WorkspaceId:   workspaceID,
						DestinationId: destinationID,
					})
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusOK, res.GetStatusCode())
					require.EqualValues(t, triggeredSuccessfully, res.GetMessage())
				})

				t.Run("success", func(t *testing.T) {
					t.Cleanup(func() {
						clearTriggeredUpload(model.Warehouse{
							Identifier: "POSTGRES:test_source_id:test_destination_id",
						})
					})

					res, err := grpcClient.TriggerWHUploads(ctx, &proto.WHUploadsRequest{
						WorkspaceId:   workspaceID,
						SourceId:      sourceID,
						DestinationId: destinationID,
					})
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusOK, res.GetStatusCode())
					require.EqualValues(t, triggeredSuccessfully, res.GetMessage())
					require.True(t, isUploadTriggered(model.Warehouse{
						Identifier: "POSTGRES:test_source_id:test_destination_id",
					}))
				})
				t.Run("no warehouses", func(t *testing.T) {})
				t.Run("no pending count", func(t *testing.T) {
					res, err := grpcClient.TriggerWHUploads(ctx, &proto.WHUploadsRequest{
						WorkspaceId:   unusedWorkspaceID,
						SourceId:      unusedSourceID,
						DestinationId: unusedDestinationID,
					})
					require.NoError(t, err)
					require.NotNil(t, res)
					require.EqualValues(t, http.StatusOK, res.GetStatusCode())
					require.EqualValues(t, noPendingEvents, res.GetMessage())
				})
			})
		})

		t.Run("Retry", func(t *testing.T) {
			now := time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC)
			repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
				return now
			}))
			repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
				return now
			}))

			totalUploads := 100
			firstEventAt, lastEventAt := now.Add(-2*time.Hour), now.Add(-1*time.Hour)

			for i := 0; i < totalUploads; i++ {
				fid, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
				require.NoError(t, err)
				sid, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
				require.NoError(t, err)

				_, err = repoUpload.CreateWithStagingFiles(ctx, model.Upload{
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: destinationType,
					WorkspaceID:     workspaceID,
					Status:          model.Waiting,
					NextRetryTime:   now,
				}, []*model.StagingFile{
					{
						ID:            fid,
						SourceID:      sourceID,
						DestinationID: destinationID,
						WorkspaceID:   workspaceID,
						FirstEventAt:  firstEventAt,
					},
					{
						ID:            sid,
						SourceID:      sourceID,
						DestinationID: destinationID,
						WorkspaceID:   workspaceID,
						LastEventAt:   lastEventAt,
					},
				})
				require.NoError(t, err)
			}

			t.Run("RetryWHUploads", func(t *testing.T) {
				t.Run("no source + destination + workspace", func(t *testing.T) {
					res, err := grpcClient.RetryWHUploads(ctx, &proto.RetryWHUploadsRequest{})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "please provide valid request parameters while retrying jobs with workspaceId or sourceId or destinationId", statusError.Message())
				})
				t.Run("no uploadsIDs + intervalInHours", func(t *testing.T) {
					res, err := grpcClient.RetryWHUploads(ctx, &proto.RetryWHUploadsRequest{
						WorkspaceId:   workspaceID,
						SourceId:      sourceID,
						DestinationId: destinationID,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "please provide valid request parameters while retrying jobs with UploadIds or IntervalInHours", statusError.Message())
				})
				t.Run("no sources", func(t *testing.T) {
					res, err := grpcClient.RetryWHUploads(ctx, &proto.RetryWHUploadsRequest{
						SourceId:      sourceID,
						DestinationId: destinationID,
						WorkspaceId:   "unknown_workspace_id",
						UploadIds:     []int64{1},
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "no sources found for workspace: unknown_workspace_id", statusError.Message())
				})
				t.Run("unauthorized", func(t *testing.T) {
					res, err := grpcClient.RetryWHUploads(ctx, &proto.RetryWHUploadsRequest{
						SourceId:      sourceID,
						DestinationId: destinationID,
						WorkspaceId:   unusedWorkspaceID,
						UploadIds:     []int64{1},
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "unauthorized request", statusError.Message())
				})
				t.Run("success", func(t *testing.T) {
					res, err := grpcClient.RetryWHUploads(ctx, &proto.RetryWHUploadsRequest{
						SourceId:      sourceID,
						DestinationId: destinationID,
						WorkspaceId:   workspaceID,
						UploadIds:     []int64{1},
						ForceRetry:    true,
					})
					require.NoError(t, err)
					require.NotEmpty(t, res)

					require.EqualValues(t, 1, res.GetCount())
					require.EqualValues(t, http.StatusOK, res.GetStatusCode())

					upload, err := repoUpload.Get(ctx, 1)
					require.NoError(t, err)
					require.Equal(t, upload.Status, model.Waiting)
					require.True(t, upload.Retried)
					require.Equal(t, upload.Priority, 50)
				})
			})
			t.Run("CountWHUploadsToRetry", func(t *testing.T) {
				t.Run("no source + destination + workspace", func(t *testing.T) {
					res, err := grpcClient.CountWHUploadsToRetry(ctx, &proto.RetryWHUploadsRequest{})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "please provide valid request parameters while retrying jobs with workspaceId or sourceId or destinationId", statusError.Message())
				})
				t.Run("no uploadsIDs + intervalInHours", func(t *testing.T) {
					res, err := grpcClient.CountWHUploadsToRetry(ctx, &proto.RetryWHUploadsRequest{
						WorkspaceId:   workspaceID,
						SourceId:      sourceID,
						DestinationId: destinationID,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "please provide valid request parameters while retrying jobs with UploadIds or IntervalInHours", statusError.Message())
				})
				t.Run("no sources", func(t *testing.T) {
					res, err := grpcClient.CountWHUploadsToRetry(ctx, &proto.RetryWHUploadsRequest{
						SourceId:      sourceID,
						DestinationId: destinationID,
						WorkspaceId:   "unknown_workspace_id",
						UploadIds:     []int64{1},
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "no sources found for workspace: unknown_workspace_id", statusError.Message())
				})
				t.Run("unauthorized", func(t *testing.T) {
					res, err := grpcClient.CountWHUploadsToRetry(ctx, &proto.RetryWHUploadsRequest{
						SourceId:      sourceID,
						DestinationId: destinationID,
						WorkspaceId:   unusedWorkspaceID,
						UploadIds:     []int64{1},
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "unauthorized request", statusError.Message())
				})
				t.Run("success", func(t *testing.T) {
					res, err := grpcClient.CountWHUploadsToRetry(ctx, &proto.RetryWHUploadsRequest{
						SourceId:      sourceID,
						DestinationId: destinationID,
						WorkspaceId:   workspaceID,
						UploadIds:     []int64{1},
						ForceRetry:    true,
					})
					require.NoError(t, err)
					require.NotEmpty(t, res)

					require.EqualValues(t, 1, res.GetCount())
					require.EqualValues(t, http.StatusOK, res.GetStatusCode())
				})
			})
		})

		t.Run("Validate", func(t *testing.T) {
			t.Run("warehouse destination", func(t *testing.T) {
				t.Run("invalid payload", func(t *testing.T) {
					res, err := grpcClient.Validate(ctx, &proto.WHValidationRequest{
						Body: "invalid",
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "invalid JSON in request body", statusError.Message())
				})
				t.Run("empty destination config", func(t *testing.T) {
					res, err := grpcClient.Validate(ctx, &proto.WHValidationRequest{
						Body: `{}`,
					})
					require.Error(t, err)
					require.Empty(t, res)
				})
				t.Run("success", func(t *testing.T) {
					destConfig, err := json.Marshal(map[string]interface{}{
						"destination": map[string]interface{}{
							"DestinationDefinition": map[string]interface{}{
								"Name": whutils.POSTGRES,
							},
							"Config": map[string]interface{}{
								"host":            pgResource.Host,
								"port":            pgResource.Port,
								"database":        pgResource.Database,
								"user":            pgResource.User,
								"password":        pgResource.Password,
								"sslMode":         "disable",
								"namespace":       "test_namespace",
								"bucketProvider":  "MINIO",
								"bucketName":      minioResource.BucketName,
								"accessKeyID":     minioResource.AccessKey,
								"secretAccessKey": minioResource.SecretKey,
								"endPoint":        minioResource.Endpoint,
							},
						},
					})
					require.NoError(t, err)

					t.Run("steps", func(t *testing.T) {
						res, err := grpcClient.Validate(ctx, &proto.WHValidationRequest{
							Body: string(destConfig),
							Path: "steps",
						})
						require.NoError(t, err)
						require.NotEmpty(t, res)
						require.Empty(t, res.GetError())
						require.Equal(t, res.GetData(), `{"steps":[{"id":1,"name":"Verifying Object Storage","success":false,"error":""},{"id":2,"name":"Verifying Connections","success":false,"error":""},{"id":3,"name":"Verifying Create Schema","success":false,"error":""},{"id":4,"name":"Verifying Create and Alter Table","success":false,"error":""},{"id":5,"name":"Verifying Fetch Schema","success":false,"error":""},{"id":6,"name":"Verifying Load Table","success":false,"error":""}]}`)
					})
					t.Run("validate", func(t *testing.T) {
						res, err := grpcClient.Validate(ctx, &proto.WHValidationRequest{
							Body: string(destConfig),
							Path: "validate",
						})
						require.NoError(t, err)
						require.NotEmpty(t, res)
						require.Empty(t, res.GetError())
						require.Equal(t, res.GetData(), `{"success":true,"error":"","steps":[{"id":1,"name":"Verifying Object Storage","success":true,"error":""},{"id":2,"name":"Verifying Connections","success":true,"error":""},{"id":3,"name":"Verifying Create Schema","success":true,"error":""},{"id":4,"name":"Verifying Create and Alter Table","success":true,"error":""},{"id":5,"name":"Verifying Fetch Schema","success":true,"error":""},{"id":6,"name":"Verifying Load Table","success":true,"error":""}]}`)
					})
				})
				t.Run("tunneling", func(t *testing.T) {
					testCases := []struct {
						name           string
						inputConfig    map[string]interface{}
						wantError      error
						expectedConfig map[string]interface{}
					}{
						{
							name: "no tunneling",
							inputConfig: map[string]interface{}{
								"host":   "host",
								"useSSH": false,
							},
							expectedConfig: map[string]interface{}{
								"host":   "host",
								"useSSH": false,
							},
						},
						{
							name: "no SSH key id",
							inputConfig: map[string]interface{}{
								"host":   "host",
								"useSSH": true,
							},
							wantError: errors.New("missing sshKeyId in validation payload"),
						},
						{
							name: "with SSH key id",
							inputConfig: map[string]interface{}{
								"host":     "host",
								"useSSH":   true,
								"sshKeyId": "sshKeyId",
							},
							expectedConfig: map[string]interface{}{
								"host":          "host",
								"useSSH":        true,
								"sshKeyId":      "sshKeyId",
								"sshPrivateKey": "private_key",
							},
						},
					}
					for _, tc := range testCases {
						t.Run(tc.name, func(t *testing.T) {
							err := grpcServer.manageTunnellingSecrets(ctx, tc.inputConfig)
							if tc.wantError != nil {
								require.Equal(t, tc.wantError, err)
								return
							}
							require.NoError(t, err)
							require.Equal(t, tc.expectedConfig, tc.inputConfig)
						})
					}
				})
			})
			t.Run("object storage", func(t *testing.T) {
				t.Run("unsupported type", func(t *testing.T) {
					res, err := grpcClient.ValidateObjectStorageDestination(ctx, &proto.ValidateObjectStorageRequest{
						Type: "unknown",
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "invalid argument err: \ntype: unknown not supported", statusError.Message())
				})
				t.Run("containerName should be present for azure blob", func(t *testing.T) {
					res, err := grpcClient.ValidateObjectStorageDestination(ctx, &proto.ValidateObjectStorageRequest{
						Type: whutils.AzureBlob,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "invalid argument err: \ncontainerName invalid or not present", statusError.Message())
				})
				t.Run("bucketName should be present for s3", func(t *testing.T) {
					res, err := grpcClient.ValidateObjectStorageDestination(ctx, &proto.ValidateObjectStorageRequest{
						Type: whutils.S3,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "invalid argument err: \nbucketName invalid or not present", statusError.Message())
				})
				t.Run("success", func(t *testing.T) {
					res, err := grpcClient.ValidateObjectStorageDestination(ctx, &proto.ValidateObjectStorageRequest{
						Type: whutils.MINIO,
						Config: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"region": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.SiteRegion,
									},
								},
								"bucketName": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.BucketName,
									},
								},
								"secretAccessKey": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.SecretKey,
									},
								},
								"accessKeyID": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.AccessKey,
									},
								},
								"endPoint": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.Endpoint,
									},
								},
							},
						},
					})
					require.NoError(t, err)
					require.NotEmpty(t, res)
					require.Empty(t, res.GetError())
					require.True(t, res.GetIsValid())
				})
				t.Run("invalid config", func(t *testing.T) {
					res, err := grpcClient.ValidateObjectStorageDestination(ctx, &proto.ValidateObjectStorageRequest{
						Type: whutils.MINIO,
						Config: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"bucketName": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.BucketName,
									},
								},
							},
						},
					})
					require.NoError(t, err)
					require.NotEmpty(t, res)
					require.Equal(t, "Invalid destination creds, failed for operation: list with err: \nEndpoint:  does not follow ip address or domain name standards.", res.GetError())
					require.False(t, res.GetIsValid())
				})
				t.Run("list permissions missing", func(t *testing.T) {
					res, err := grpcClient.ValidateObjectStorageDestination(ctx, &proto.ValidateObjectStorageRequest{
						Type: whutils.MINIO,
						Config: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"region": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.SiteRegion,
									},
								},
								"bucketName": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.BucketName,
									},
								},
								"secretAccessKey": {
									Kind: &structpb.Value_StringValue{
										StringValue: "wrongSecretKey",
									},
								},
								"accessKeyID": {
									Kind: &structpb.Value_StringValue{
										StringValue: "wrongAccessKey",
									},
								},
								"endPoint": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.Endpoint,
									},
								},
							},
						},
					})
					require.NoError(t, err)
					require.NotEmpty(t, res)
					require.Equal(t, "Invalid destination creds, failed for operation: list with err: \nThe Access Key Id you provided does not exist in our records.", res.GetError())
					require.False(t, res.GetIsValid())
				})
				t.Run("checkMapForValidKey", func(t *testing.T) {
					testCases := []struct {
						name     string
						inputMap map[string]interface{}
						key      string
						want     bool
					}{
						{
							name:     "empty map",
							inputMap: map[string]interface{}{},
							key:      "key",
							want:     false,
						},
						{
							name:     "key not present",
							inputMap: map[string]interface{}{"key1": "value1"},
							key:      "key",
							want:     false,
						},
						{
							name:     "key present",
							inputMap: map[string]interface{}{"key": "value"},
							key:      "key",
							want:     true,
						},
						{
							name:     "key present with empty value",
							inputMap: map[string]interface{}{"key": ""},
							key:      "key",
							want:     false,
						},
						{
							name:     "key present with nil value",
							inputMap: map[string]interface{}{"key": nil},
							key:      "key",
							want:     false,
						},
					}
					for _, tc := range testCases {
						t.Run(tc.name, func(t *testing.T) {
							require.Equal(t, tc.want, checkMapForValidKey(tc.inputMap, tc.key))
						})
					}
				})
				t.Run("overrideWithEnv", func(t *testing.T) {
					t.Run("should fallback to backup credentials when fields missing(as of now backup only supported for s3", func(t *testing.T) {
						fm := &filemanager.Settings{
							Provider: "AZURE_BLOB",
							Config:   map[string]interface{}{"containerName": "containerName1", "prefix": "prefix1", "accountKey": "accountKey1"},
						}
						overrideWithEnv(ctx, fm)
						require.Nil(t, fm.Config["accountName"])

						fm.Provider = "S3"
						fm.Config = map[string]interface{}{"bucketName": "bucket1", "prefix": "prefix1", "accessKeyID": "KeyID1"}
						overrideWithEnv(ctx, fm)
						require.NotNil(t, fm.Config["accessKey"])
					})
					t.Run("Should set value for key when key not present", func(t *testing.T) {
						jsonMap := make(map[string]interface{})
						jsonMap["config"] = "{}"
						typeValue := "GCS"
						configValue := "{\"bucketName\":\"temp\"}"
						ifNotExistThenSet("type", typeValue, jsonMap)
						ifNotExistThenSet("config", configValue, jsonMap)
						require.Equal(t, jsonMap["type"], typeValue)
						require.Equal(t, jsonMap["config"], "{}")
					})
				})
			})
		})

		server.GracefulStop()

		setupCh := make(chan struct{})
		go func() {
			require.NoError(t, g.Wait())

			close(setupCh)
		}()

		stopTest()
		<-setupCh
	})
}
