package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestGRPC(t *testing.T) {
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
		pgResource, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		minioResource, err := minio.Setup(pool, t)
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
										Config: map[string]interface{}{
											"syncFrequency": "30",
										},
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
										Config: map[string]interface{}{
											"syncFrequency": "30",
										},
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

		triggerStore := &sync.Map{}
		tenantManager := multitenant.New(c, mockBackendConfig)
		bcManager := bcm.New(c, db, tenantManager, logger.NOP, stats.NOP)
		grpcServer, err := NewGRPCServer(c, logger.NOP, stats.NOP, db, tenantManager, bcManager, triggerStore)
		require.NoError(t, err)

		tcpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		tcpAddress := net.JoinHostPort("", strconv.Itoa(tcpPort))

		listener, err := net.Listen("tcp", tcpAddress)
		require.NoError(t, err)

		server := grpc.NewServer(grpc.Creds(insecure.NewCredentials()), grpc.UnaryInterceptor(statsInterceptor(stats.NOP)))
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

		grpcClientConn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", tcpPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, grpcClientConn.Close())
		})
		grpcClient := proto.NewWarehouseClient(grpcClientConn)

		cleanUpTables := func() {
			tables := []string{
				whutils.WarehouseUploadsTable,
				whutils.WarehouseTableUploadsTable,
				whutils.WarehouseStagingFilesTable,
				whutils.WarehouseLoadFilesTable,
				whutils.WarehouseSchemasTable,
				whutils.WarehouseAsyncJobTable,
			}
			for _, table := range tables {
				_, err := db.ExecContext(ctx, "TRUNCATE TABLE "+table+" CASCADE;")
				require.NoError(t, err)
			}
		}

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
			lastExecAt := now.Add(-1 * time.Minute)

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

				if (i+1)%2 == 0 {
					err = repoUpload.Update(ctx, uploadID, []repo.UpdateKeyValue{
						repo.UploadFieldStatus(model.ExportedData),
						repo.UploadFieldLastExecAt(lastExecAt),
					})
					require.NoError(t, err)

					for _, table := range tables {
						err = repoTableUploads.Set(ctx, uploadID, table, repo.TableUploadSetOptions{
							Status:       lo.ToPtr(model.TableUploadExported),
							LastExecTime: lo.ToPtr(lastExecAt),
							TotalEvents:  lo.ToPtr(int64(10)),
						})
						require.NoError(t, err)
					}
				}
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

				t.Run("unknown workspace", func(t *testing.T) {
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

				t.Run("success (waiting)", func(t *testing.T) {
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
					require.Zero(t, res.GetDuration())
					require.Zero(t, res.GetLastExecAt())
					require.NotEmpty(t, res.GetTables())
					require.False(t, res.GetIsArchivedUpload())
					require.EqualValues(t, tables, lo.Map(res.GetTables(), func(item *proto.WHTable, index int) string {
						return item.GetName()
					}))

					for _, table := range res.GetTables() {
						require.EqualValues(t, 1, table.GetUploadId())
						require.EqualValues(t, model.TableUploadWaiting, table.GetStatus())
						require.EqualValues(t, "{}", table.GetError())
						require.Zero(t, table.GetLastExecAt())
						require.Zero(t, table.GetCount())
						require.Zero(t, table.GetDuration())
					}
				})

				t.Run("success (exported)", func(t *testing.T) {
					res, err := grpcClient.GetWHUpload(ctx, &proto.WHUploadRequest{
						UploadId:    2,
						WorkspaceId: workspaceID,
					})
					require.NoError(t, err)
					require.NotNil(t, res)

					require.EqualValues(t, sourceID, res.GetSourceId())
					require.EqualValues(t, destinationID, res.GetDestinationId())
					require.EqualValues(t, destinationType, res.GetDestinationType())
					require.Equal(t, "{}", res.GetError())
					require.Zero(t, res.GetAttempt())
					require.EqualValues(t, model.ExportedData, res.GetStatus())
					require.EqualValues(t, now.UTC(), res.GetCreatedAt().AsTime().UTC())
					require.EqualValues(t, firstEventAt.UTC(), res.GetFirstEventAt().AsTime().UTC())
					require.EqualValues(t, lastEventAt.UTC(), res.GetLastEventAt().AsTime().UTC())
					require.Zero(t, res.GetNextRetryTime())
					require.EqualValues(t, now.Sub(lastExecAt)/time.Second, res.GetDuration())
					require.EqualValues(t, lastExecAt, res.GetLastExecAt().AsTime().UTC())
					require.NotEmpty(t, res.GetTables())
					require.False(t, res.GetIsArchivedUpload())
					require.EqualValues(t, tables, lo.Map(res.GetTables(), func(item *proto.WHTable, index int) string {
						return item.GetName()
					}))

					for _, table := range res.GetTables() {
						require.EqualValues(t, 2, table.GetUploadId())
						require.EqualValues(t, model.TableUploadExported, table.GetStatus())
						require.EqualValues(t, "{}", table.GetError())
						require.EqualValues(t, lastExecAt, table.GetLastExecAt().AsTime().UTC())
						require.EqualValues(t, 10, table.GetCount())
						require.EqualValues(t, now.Sub(lastExecAt)/time.Second, table.GetDuration())
					}
				})
			})

			t.Run("GetWHUploads", func(t *testing.T) {
				t.Run("unknown workspace", func(t *testing.T) {
					res, err := grpcClient.GetWHUploads(ctx, &proto.WHUploadsRequest{
						Limit:       10,
						Offset:      0,
						WorkspaceId: "unknown_workspace_id",
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "no sources found for workspace: unknown_workspace_id", statusError.Message())
				})
				t.Run("unknown source", func(t *testing.T) {
					res, err := grpcClient.GetWHUploads(ctx, &proto.WHUploadsRequest{
						Limit:         10,
						Offset:        0,
						WorkspaceId:   workspaceID,
						SourceId:      "unknown_source_id",
						DestinationId: destinationID,
					})
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.GetUploads(), 0)
					require.NotNil(t, res.GetPagination())
					require.EqualValues(t, int64(0), res.GetPagination().GetTotal())
					require.EqualValues(t, int64(10), res.GetPagination().GetLimit())
					require.EqualValues(t, int64(0), res.GetPagination().GetOffset())
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
				t.Run("success (waiting)", func(t *testing.T) {
					res, err := grpcClient.GetWHUploads(ctx, &proto.WHUploadsRequest{
						Limit:           25,
						Offset:          6,
						WorkspaceId:     workspaceID,
						SourceId:        sourceID,
						DestinationId:   destinationID,
						DestinationType: destinationType,
						Status:          "waiting",
					})
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.GetUploads(), 25)
					require.NotNil(t, res.GetPagination())
					require.EqualValues(t, int64(50), res.GetPagination().GetTotal())
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
						require.Zero(t, upload.GetLastExecAt())
						require.Zero(t, upload.GetDuration())
						require.Empty(t, upload.GetTables())
						require.False(t, upload.GetIsArchivedUpload())
					}
				})
				t.Run("success (exported)", func(t *testing.T) {
					res, err := grpcClient.GetWHUploads(ctx, &proto.WHUploadsRequest{
						Limit:           25,
						Offset:          6,
						WorkspaceId:     workspaceID,
						SourceId:        sourceID,
						DestinationId:   destinationID,
						DestinationType: destinationType,
						Status:          "success",
					})
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.GetUploads(), 25)
					require.NotNil(t, res.GetPagination())
					require.EqualValues(t, int64(50), res.GetPagination().GetTotal())
					require.EqualValues(t, int64(25), res.GetPagination().GetLimit())
					require.EqualValues(t, int64(6), res.GetPagination().GetOffset())

					for _, upload := range res.GetUploads() {
						require.EqualValues(t, sourceID, upload.GetSourceId())
						require.EqualValues(t, destinationID, upload.GetDestinationId())
						require.EqualValues(t, destinationType, upload.GetDestinationType())
						require.Equal(t, "{}", upload.GetError())
						require.Zero(t, upload.GetAttempt())
						require.EqualValues(t, model.ExportedData, upload.GetStatus())
						require.EqualValues(t, now.UTC(), upload.GetCreatedAt().AsTime().UTC())
						require.EqualValues(t, firstEventAt.UTC(), upload.GetFirstEventAt().AsTime().UTC())
						require.EqualValues(t, lastEventAt.UTC(), upload.GetLastEventAt().AsTime().UTC())
						require.Zero(t, upload.GetNextRetryTime())
						require.EqualValues(t, now.Sub(lastExecAt)/time.Second, upload.GetDuration())
						require.EqualValues(t, lastExecAt, upload.GetLastExecAt().AsTime().UTC())
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
						triggerStore.Delete("POSTGRES:test_source_id:test_destination_id")
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

					_, triggered := triggerStore.Load("POSTGRES:test_source_id:test_destination_id")
					require.True(t, triggered)
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
								"accessKeyID":     minioResource.AccessKeyID,
								"secretAccessKey": minioResource.AccessKeySecret,
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
										StringValue: minioResource.Region,
									},
								},
								"bucketName": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.BucketName,
									},
								},
								"secretAccessKey": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.AccessKeySecret,
									},
								},
								"accessKeyID": {
									Kind: &structpb.Value_StringValue{
										StringValue: minioResource.AccessKeyID,
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
										StringValue: minioResource.Region,
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

		t.Run("Failed Batch Operations", func(t *testing.T) {
			now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

			start := now.Add(-24 * time.Hour).Format(time.RFC3339)
			end := now.Add(24 * time.Hour).Format(time.RFC3339)

			prepareData := func(
				db *sqlmw.DB,
				status string,
				error json.RawMessage,
				errorCategory string,
				generateTableUploads bool,
				timings model.Timings,
			) {
				repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
					return now
				}))
				repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
					return now
				}))
				repoTableUpload := repo.NewTableUploads(db, repo.WithNow(func() time.Time {
					return now
				}))

				var stagingFiles []*model.StagingFile
				for i := 0; i < 10; i++ {
					stagingFile := &model.StagingFile{
						WorkspaceID:   workspaceID,
						Location:      "s3://bucket/path/to/file",
						SourceID:      sourceID,
						DestinationID: destinationID,
						TotalEvents:   60,
						FirstEventAt:  now,
						LastEventAt:   now,
					}
					stagingFileWithSchema := stagingFile.WithSchema(json.RawMessage(`{"type": "object"}`))

					stagingID, err := repoStaging.Insert(ctx, &stagingFileWithSchema)
					require.NoError(t, err)

					stagingFile.ID = stagingID
					stagingFiles = append(stagingFiles, stagingFile)
				}

				uploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: whutils.POSTGRES,
					Status:          status,
					UploadSchema:    model.Schema{},
					WorkspaceID:     workspaceID,
				}, stagingFiles)
				require.NoError(t, err)

				if len(error) > 0 {
					errorJson, err := json.Marshal(error)
					require.NoError(t, err)

					_, err = db.ExecContext(ctx, `UPDATE wh_uploads SET error = $1, error_category = $2 WHERE id = $3`,
						errorJson,
						errorCategory,
						uploadID,
					)
					require.NoError(t, err)
				}
				if len(timings) > 0 {
					timingsJson, err := json.Marshal(timings)
					require.NoError(t, err)

					_, err = db.ExecContext(ctx, `UPDATE wh_uploads SET timings = $1 WHERE id = $2`,
						timingsJson,
						uploadID,
					)
					require.NoError(t, err)
				}

				if generateTableUploads {
					tables := []string{
						"table_1",
						"table_2",
						"table_3",
						"table_4",
						"table_5",
						whutils.DiscardsTable,
						whutils.ToProviderCase(whutils.SNOWFLAKE, whutils.DiscardsTable),
					}
					err = repoTableUpload.Insert(ctx, uploadID, tables)
					require.NoError(t, err)

					totalEvents := int64(100)
					for _, table := range tables {
						err = repoTableUpload.Set(ctx, uploadID, table, repo.TableUploadSetOptions{
							TotalEvents: &totalEvents,
							Status:      &status,
						})
						require.NoError(t, err)
					}
				}
			}

			entries := []struct {
				status              string
				error               json.RawMessage
				errorCategory       string
				prepareTableUploads bool
				timings             model.Timings
			}{
				{
					status:              "internal_processing_failed",
					error:               json.RawMessage(`{"internal_processing_failed":{"errors":["some error 1","some error 2"],"attempt":2}}`),
					errorCategory:       model.UncategorizedError,
					prepareTableUploads: false,
					timings: model.Timings{
						{
							"internal_processing_failed": now,
						},
					},
				},
				{
					status:              "generating_load_files_failed",
					error:               json.RawMessage(`{"generating_load_files_failed":{"errors":["some error 3","some error 4"],"attempt":2}}`),
					errorCategory:       model.UncategorizedError,
					prepareTableUploads: false,
					timings: model.Timings{
						{
							"generating_load_files_failed": now,
						},
					},
				},
				{
					status:              "exporting_data_failed",
					error:               json.RawMessage(`{"exporting_data_failed":{"errors":["some error 5","some error 6"],"attempt":2}}`),
					errorCategory:       model.PermissionError,
					prepareTableUploads: true,
					timings: model.Timings{
						{
							"exporting_data_failed": now,
						},
					},
				},
				{
					status:              "aborted",
					error:               json.RawMessage(`{"exporting_data_failed":{"errors":["some error 7","some error 8"],"attempt":2}}`),
					prepareTableUploads: true,
					errorCategory:       model.ResourceNotFoundError,
					timings: model.Timings{
						{
							"exporting_data_failed": now,
						},
					},
				},
				{
					status:              "exported_data",
					prepareTableUploads: true,
				},
			}

			prepare := func() {
				cleanUpTables()

				for _, entry := range entries {
					prepareData(
						db, entry.status, entry.error, entry.errorCategory,
						entry.prepareTableUploads, entry.timings,
					)
				}
			}

			t.Run("RetrieveFailedBatches", func(t *testing.T) {
				prepare()

				t.Run("no destination + workspace", func(t *testing.T) {
					res, err := grpcClient.RetrieveFailedBatches(ctx, &proto.RetrieveFailedBatchesRequest{})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "workspaceId and destinationId cannot be empty", statusError.Message())
				})
				t.Run("invalid start/end time", func(t *testing.T) {
					res, err := grpcClient.RetrieveFailedBatches(ctx, &proto.RetrieveFailedBatchesRequest{
						WorkspaceID:   workspaceID,
						DestinationID: destinationID,
						Start:         now.Add(-24 * time.Hour).Format(time.RFC850),
						End:           end,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "start time should be in correct 2006-01-02T15:04:05Z07:00 format", statusError.Message())

					res, err = grpcClient.RetrieveFailedBatches(ctx, &proto.RetrieveFailedBatchesRequest{
						WorkspaceID:   workspaceID,
						DestinationID: destinationID,
						Start:         start,
						End:           now.Add(24 * time.Hour).Format(time.RFC850),
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok = status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "end time should be in correct 2006-01-02T15:04:05Z07:00 format", statusError.Message())
				})
				t.Run("optional end time", func(t *testing.T) {
					res, err := grpcClient.RetrieveFailedBatches(ctx, &proto.RetrieveFailedBatchesRequest{
						WorkspaceID:   workspaceID,
						DestinationID: destinationID,
						Start:         start,
					})
					require.NoError(t, err)
					require.NotEmpty(t, res)

					failedBatches := lo.Map(res.GetFailedBatches(), func(detail *proto.FailedBatchInfo, index int) model.RetrieveFailedBatchesResponse {
						return model.RetrieveFailedBatchesResponse{
							Error:           detail.GetError(),
							ErrorCategory:   detail.GetErrorCategory(),
							SourceID:        detail.GetSourceID(),
							TotalEvents:     detail.GetFailedEventsCount(),
							TotalSyncs:      detail.GetFailedSyncsCount(),
							FirstHappenedAt: detail.GetFirstHappened().AsTime(),
							LastHappenedAt:  detail.GetLastHappened().AsTime(),
							Status:          detail.GetStatus(),
						}
					})
					require.EqualValues(t, failedBatches, []model.RetrieveFailedBatchesResponse{
						{
							Error:           "some error 6",
							ErrorCategory:   model.PermissionError,
							SourceID:        sourceID,
							TotalEvents:     500,
							TotalSyncs:      1,
							LastHappenedAt:  now.UTC(),
							FirstHappenedAt: now.UTC(),
							Status:          model.Failed,
						},
						{
							Error:           "some error 8",
							ErrorCategory:   model.ResourceNotFoundError,
							SourceID:        sourceID,
							TotalEvents:     500,
							TotalSyncs:      1,
							LastHappenedAt:  now.UTC(),
							FirstHappenedAt: now.UTC(),
							Status:          model.Aborted,
						},
						{
							Error:           "some error 2",
							ErrorCategory:   model.UncategorizedError,
							SourceID:        sourceID,
							TotalEvents:     1200,
							TotalSyncs:      2,
							LastHappenedAt:  now.UTC(),
							FirstHappenedAt: now.UTC(),
							Status:          model.Failed,
						},
					})
				})
				t.Run("no sources", func(t *testing.T) {
					res, err := grpcClient.RetrieveFailedBatches(ctx, &proto.RetrieveFailedBatchesRequest{
						WorkspaceID:   "unknown_workspace_id",
						DestinationID: destinationID,
						Start:         start,
						End:           end,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "no sources found for workspace: unknown_workspace_id", statusError.Message())
				})
				t.Run("success", func(t *testing.T) {
					res, err := grpcClient.RetrieveFailedBatches(ctx, &proto.RetrieveFailedBatchesRequest{
						WorkspaceID:   workspaceID,
						DestinationID: destinationID,
						Start:         start,
						End:           end,
					})
					require.NoError(t, err)
					require.NotEmpty(t, res)

					failedBatches := lo.Map(res.GetFailedBatches(), func(detail *proto.FailedBatchInfo, index int) model.RetrieveFailedBatchesResponse {
						return model.RetrieveFailedBatchesResponse{
							Error:           detail.GetError(),
							ErrorCategory:   detail.GetErrorCategory(),
							SourceID:        detail.GetSourceID(),
							TotalEvents:     detail.GetFailedEventsCount(),
							TotalSyncs:      detail.GetFailedSyncsCount(),
							FirstHappenedAt: detail.GetFirstHappened().AsTime(),
							LastHappenedAt:  detail.GetLastHappened().AsTime(),
							Status:          detail.GetStatus(),
						}
					})
					require.EqualValues(t, failedBatches, []model.RetrieveFailedBatchesResponse{
						{
							Error:           "some error 6",
							ErrorCategory:   model.PermissionError,
							SourceID:        sourceID,
							TotalEvents:     500,
							TotalSyncs:      1,
							LastHappenedAt:  now.UTC(),
							FirstHappenedAt: now.UTC(),
							Status:          model.Failed,
						},
						{
							Error:           "some error 8",
							ErrorCategory:   model.ResourceNotFoundError,
							SourceID:        sourceID,
							TotalEvents:     500,
							TotalSyncs:      1,
							LastHappenedAt:  now.UTC(),
							FirstHappenedAt: now.UTC(),
							Status:          model.Aborted,
						},
						{
							Error:           "some error 2",
							ErrorCategory:   model.UncategorizedError,
							SourceID:        sourceID,
							TotalEvents:     1200,
							TotalSyncs:      2,
							LastHappenedAt:  now.UTC(),
							FirstHappenedAt: now.UTC(),
							Status:          model.Failed,
						},
					})
				})
			})

			t.Run("RetryFailedBatches", func(t *testing.T) {
				prepare()

				t.Run("no destination + workspace", func(t *testing.T) {
					res, err := grpcClient.RetryFailedBatches(ctx, &proto.RetryFailedBatchesRequest{})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "workspaceId and destinationId cannot be empty", statusError.Message())
				})
				t.Run("invalid start/end time", func(t *testing.T) {
					res, err := grpcClient.RetryFailedBatches(ctx, &proto.RetryFailedBatchesRequest{
						WorkspaceID:   workspaceID,
						DestinationID: destinationID,
						Start:         now.Add(-24 * time.Hour).Format(time.RFC850),
						End:           end,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "start time should be in correct 2006-01-02T15:04:05Z07:00 format", statusError.Message())

					res, err = grpcClient.RetryFailedBatches(ctx, &proto.RetryFailedBatchesRequest{
						WorkspaceID:   workspaceID,
						DestinationID: destinationID,
						Start:         start,
						End:           now.Add(-24 * time.Hour).Format(time.RFC850),
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok = status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.InvalidArgument, statusError.Code())
					require.Equal(t, "end time should be in correct 2006-01-02T15:04:05Z07:00 format", statusError.Message())
				})
				t.Run("optional end time", func(t *testing.T) {
					res, err := grpcClient.RetryFailedBatches(ctx, &proto.RetryFailedBatchesRequest{
						WorkspaceID:   workspaceID,
						DestinationID: destinationID,
						Start:         start,
						End:           end,
						ErrorCategory: model.PermissionError,
						SourceID:      sourceID,
						Status:        model.Failed,
					})
					require.NoError(t, err)
					require.NotEmpty(t, res)
					require.EqualValues(t, 1, res.GetRetriedSyncsCount())
				})
				t.Run("no sources", func(t *testing.T) {
					res, err := grpcClient.RetryFailedBatches(ctx, &proto.RetryFailedBatchesRequest{
						WorkspaceID:   "unknown_workspace_id",
						DestinationID: destinationID,
						Start:         start,
						End:           end,
						ErrorCategory: model.PermissionError,
						SourceID:      sourceID,
						Status:        model.Failed,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "no sources found for workspace: unknown_workspace_id", statusError.Message())
				})
				t.Run("unauthorized", func(t *testing.T) {
					res, err := grpcClient.RetryFailedBatches(ctx, &proto.RetryFailedBatchesRequest{
						WorkspaceID:   unusedWorkspaceID,
						DestinationID: destinationID,
						Start:         start,
						End:           end,
						ErrorCategory: model.PermissionError,
						SourceID:      sourceID,
						Status:        model.Failed,
					})
					require.Error(t, err)
					require.Empty(t, res)

					statusError, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Unauthenticated, statusError.Code())
					require.Equal(t, "unauthorized request", statusError.Message())
				})
				t.Run("success", func(t *testing.T) {
					t.Run("with filters", func(t *testing.T) {
						res, err := grpcClient.RetryFailedBatches(ctx, &proto.RetryFailedBatchesRequest{
							WorkspaceID:   workspaceID,
							DestinationID: destinationID,
							Start:         start,
							End:           end,
							ErrorCategory: model.ResourceNotFoundError,
							SourceID:      sourceID,
							Status:        model.Aborted,
						})
						require.NoError(t, err)
						require.NotEmpty(t, res)
						require.EqualValues(t, 1, res.GetRetriedSyncsCount())
					})
					t.Run("without filters", func(t *testing.T) {
						res, err := grpcClient.RetryFailedBatches(ctx, &proto.RetryFailedBatchesRequest{
							WorkspaceID:   workspaceID,
							DestinationID: destinationID,
							Start:         start,
							End:           end,
						})
						require.NoError(t, err)
						require.NotEmpty(t, res)
						require.EqualValues(t, 4, res.GetRetriedSyncsCount())
					})
				})
			})
		})

		t.Run("GetFirstAbortedUploadsInContinuousAborts", func(t *testing.T) {
			now := time.Now().UTC()
			repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
				return time.Now().UTC()
			}))
			repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
				return time.Now().UTC()
			}))

			uploads := []model.Upload{
				{
					WorkspaceID:     workspaceID,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: destinationType,
					Status:          model.Aborted,
					Namespace:       "namespace",
					SourceTaskRunID: "task_run_id",
					LastEventAt:     now.Add(-7 * time.Hour),
					FirstEventAt:    now.Add(-8 * time.Hour),
				},
				{
					WorkspaceID:     workspaceID,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: destinationType,
					Status:          model.Aborted,
					Namespace:       "namespace",
					SourceTaskRunID: "task_run_id",
					LastEventAt:     now.Add(-3 * time.Hour),
					FirstEventAt:    now.Add(-4 * time.Hour),
				},
			}

			for i := range uploads {
				stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
				require.NoError(t, err)

				id, err := repoUpload.CreateWithStagingFiles(ctx, uploads[i], []*model.StagingFile{{
					ID:              stagingID,
					SourceID:        uploads[i].SourceID,
					DestinationID:   uploads[i].DestinationID,
					SourceTaskRunID: uploads[i].SourceTaskRunID,
					FirstEventAt:    uploads[i].FirstEventAt,
					LastEventAt:     uploads[i].LastEventAt,
					Status:          uploads[i].Status,
					WorkspaceID:     uploads[i].WorkspaceID,
				}})
				require.NoError(t, err)

				uploads[i].ID = id
				uploads[i].Error = []byte("{}")
				uploads[i].UploadSchema = model.Schema{}
				uploads[i].LoadFileType = "csv"
				uploads[i].StagingFileStartID = int64(i + 1)
				uploads[i].StagingFileEndID = int64(i + 1)
			}

			t.Run("success", func(t *testing.T) {
				res, err := grpcClient.GetFirstAbortedUploadInContinuousAbortsByDestination(ctx, &proto.FirstAbortedUploadInContinuousAbortsByDestinationRequest{
					WorkspaceId: workspaceID,
				})
				require.NoError(t, err)
				require.NotNil(t, res)
				require.Len(t, res.GetUploads(), 1)
				require.Equal(t, res.GetUploads()[0].SourceId, sourceID)
				require.Equal(t, res.GetUploads()[0].DestinationId, destinationID)
				require.Equal(t, res.GetUploads()[0].LastEventAt.AsTime().Unix(), now.Add(-7*time.Hour).Unix())
				require.Equal(t, res.GetUploads()[0].FirstEventAt.AsTime().Unix(), now.Add(-8*time.Hour).Unix())
			})
		})

		t.Run("GetSyncLatency", func(t *testing.T) {
			cleanUpTables()

			now := time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC)

			uploads := []model.Upload{
				{
					WorkspaceID:     workspaceID,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: destinationType,
					Status:          model.ExportedData,
					Namespace:       "namespace",
					SourceTaskRunID: "task_run_id",
					LastEventAt:     now,
					FirstEventAt:    now,
				},
			}

			for i := range uploads {
				repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
					return now
				}))
				repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
					return now.Add(time.Duration(i) * time.Hour)
				}))

				stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
				require.NoError(t, err)

				id, err := repoUpload.CreateWithStagingFiles(ctx, uploads[i], []*model.StagingFile{{
					ID:              stagingID,
					SourceID:        uploads[i].SourceID,
					DestinationID:   uploads[i].DestinationID,
					SourceTaskRunID: uploads[i].SourceTaskRunID,
					FirstEventAt:    uploads[i].FirstEventAt,
					LastEventAt:     uploads[i].LastEventAt,
					Status:          uploads[i].Status,
					WorkspaceID:     uploads[i].WorkspaceID,
				}})
				require.NoError(t, err)

				require.NoError(t, repoUpload.Update(ctx, id, []repo.UpdateKeyValue{
					repo.UploadFieldUpdatedAt(now.Add(time.Duration(i+1) * time.Minute)),
				}))
			}

			t.Run("no destination + workspace", func(t *testing.T) {
				res, err := grpcClient.GetSyncLatency(ctx, &proto.SyncLatencyRequest{})
				require.Error(t, err)
				require.Empty(t, res)

				statusError, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, codes.InvalidArgument, statusError.Code())
				require.Equal(t, "workspaceID and destinationID cannot be empty", statusError.Message())
			})
			t.Run("no start time", func(t *testing.T) {
				res, err := grpcClient.GetSyncLatency(ctx, &proto.SyncLatencyRequest{
					WorkspaceId:   workspaceID,
					DestinationId: destinationID,
				})
				require.Error(t, err)
				require.Empty(t, res)

				statusError, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, codes.InvalidArgument, statusError.Code())
				require.Equal(t, "start time cannot be empty", statusError.Message())
			})
			t.Run("no aggregation minutes", func(t *testing.T) {
				res, err := grpcClient.GetSyncLatency(ctx, &proto.SyncLatencyRequest{
					WorkspaceId:   workspaceID,
					DestinationId: destinationID,
					StartTime:     now.Format(time.RFC3339),
				})
				require.Error(t, err)
				require.Empty(t, res)

				statusError, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, codes.InvalidArgument, statusError.Code())
				require.Equal(t, "aggregation minutes cannot be empty", statusError.Message())
			})
			t.Run("invalid start time", func(t *testing.T) {
				res, err := grpcClient.GetSyncLatency(ctx, &proto.SyncLatencyRequest{
					WorkspaceId:        workspaceID,
					DestinationId:      destinationID,
					StartTime:          "invalid",
					AggregationMinutes: "1440",
				})
				require.Error(t, err)
				require.Empty(t, res)

				statusError, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, codes.InvalidArgument, statusError.Code())
				require.Equal(t, "start time invalid should be in correct 2006-01-02T15:04:05Z07:00 format", statusError.Message())
			})
			t.Run("start time older than 90 days", func(t *testing.T) {
				res, err := grpcClient.GetSyncLatency(ctx, &proto.SyncLatencyRequest{
					WorkspaceId:        workspaceID,
					DestinationId:      destinationID,
					StartTime:          now.AddDate(0, -120, 0).Format(time.RFC3339),
					AggregationMinutes: "1440",
				})
				require.Error(t, err)
				require.Empty(t, res)

				statusError, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, codes.InvalidArgument, statusError.Code())
				require.Equal(t, "start time cannot be older than 90 days", statusError.Message())
			})
			t.Run("invalid interval", func(t *testing.T) {
				res, err := grpcClient.GetSyncLatency(ctx, &proto.SyncLatencyRequest{
					WorkspaceId:        workspaceID,
					DestinationId:      destinationID,
					StartTime:          now.Format(time.RFC3339),
					AggregationMinutes: "invalid",
				})
				require.Error(t, err)
				require.Empty(t, res)

				statusError, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, codes.InvalidArgument, statusError.Code())
				require.Equal(t, "aggregation minutes invalid should be an integer", statusError.Message())
			})
			t.Run("no sources", func(t *testing.T) {
				res, err := grpcClient.GetSyncLatency(ctx, &proto.SyncLatencyRequest{
					WorkspaceId:        workspaceID,
					DestinationId:      "unknown_destination_id",
					StartTime:          now.Format(time.RFC3339),
					AggregationMinutes: "1440",
					SourceId:           sourceID,
				})
				require.Error(t, err)
				require.Empty(t, res)

				statusError, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, codes.Unauthenticated, statusError.Code())
				require.Equal(t, "unauthorized request", statusError.Message())
			})
			t.Run("success", func(t *testing.T) {
				res, err := grpcClient.GetSyncLatency(ctx, &proto.SyncLatencyRequest{
					WorkspaceId:        workspaceID,
					DestinationId:      destinationID,
					StartTime:          now.Format(time.RFC3339),
					AggregationMinutes: "60",
					SourceId:           sourceID,
				})
				require.NoError(t, err)
				require.NotEmpty(t, res)
				require.Equal(t, 1, len(res.GetTimeSeriesDataPoints()))
				require.Equal(t, float64(1609462800000), res.GetTimeSeriesDataPoints()[0].GetTimestampMillis().GetValue())
				require.Equal(t, float64(60), res.GetTimeSeriesDataPoints()[0].GetLatencySeconds().GetValue())
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

	t.Run("getLatencyAggregationType", func(t *testing.T) {
		testCases := []struct {
			name            string
			srcMap          map[string]model.Warehouse
			sourceID        string
			configOverride  map[string]any
			aggregationType model.LatencyAggregationType
			wantErr         bool
		}{
			{
				name: "empty sourceID",
				srcMap: map[string]model.Warehouse{
					"sid-1": {
						WorkspaceID: "wid1",
						Source: backendconfig.SourceT{
							ID: "sid-1",
						},
						Destination: backendconfig.DestinationT{
							ID: "did-1",
							Config: map[string]interface{}{
								"syncFrequency": "60",
							},
						},
					},
				},
				aggregationType: model.MaxLatency,
			},
			{
				name: "unable to parse sync frequency",
				srcMap: map[string]model.Warehouse{
					"sid-1": {
						WorkspaceID: "wid1",
						Source: backendconfig.SourceT{
							ID: "sid-1",
						},
						Destination: backendconfig.DestinationT{
							ID: "did-1",
							Config: map[string]interface{}{
								"syncFrequency": "abc",
							},
						},
					},
				},
				wantErr: true,
			},
			{
				name: "min threshold",
				srcMap: map[string]model.Warehouse{
					"sid-1": {
						WorkspaceID: "wid1",
						Source: backendconfig.SourceT{
							ID: "sid-1",
						},
						Destination: backendconfig.DestinationT{
							ID: "did-1",
							Config: map[string]interface{}{
								"syncFrequency": "5",
							},
						},
					},
				},
				configOverride: map[string]any{
					"Warehouse.grpc.defaultLatencyAggregationType": "p90",
				},
				aggregationType: model.P90Latency,
			},
			{
				name:   "empty srcMap",
				srcMap: map[string]model.Warehouse{},
				configOverride: map[string]any{
					"Warehouse.grpc.defaultLatencyAggregationType": "p90",
				},
				aggregationType: model.MaxLatency,
			},
			{
				name: "some sourceID and syncFrequency override",
				srcMap: map[string]model.Warehouse{
					"sid-1": {
						WorkspaceID: "wid1",
						Source: backendconfig.SourceT{
							ID: "sid-1",
						},
						Destination: backendconfig.DestinationT{
							ID: "did-1",
							Config: map[string]interface{}{
								"syncFrequency": "60",
							},
						},
					},
					"sid-2": {
						WorkspaceID: "wid1",
						Source: backendconfig.SourceT{
							ID: "sid-2",
						},
						Destination: backendconfig.DestinationT{
							ID: "did-1",
							Config: map[string]interface{}{
								"syncFrequency": "60",
							},
						},
					},
				},
				sourceID: "sid-2",
				configOverride: map[string]any{
					"Warehouse.grpc.defaultLatencyAggregationType":  "p90",
					"Warehouse.pipelines.sid-2.did-1.syncFrequency": "10",
				},
				aggregationType: model.MaxLatency,
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				c := config.New()
				for k, v := range tc.configOverride {
					c.Set(k, v)
				}

				g, err := NewGRPCServer(c, logger.NOP, stats.NOP, nil, nil, nil, nil)
				require.NoError(t, err)

				aggType, err := g.getLatencyAggregationType(tc.srcMap, tc.sourceID)
				if tc.wantErr {
					require.Zero(t, aggType)
					require.Error(t, err)
				} else {
					require.Equal(t, tc.aggregationType, aggType)
					require.NoError(t, err)
				}
			})
		}
	})
}
