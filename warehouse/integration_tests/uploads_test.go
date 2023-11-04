package integration_tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/jobsdb"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/tunnelling"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"

	"go.uber.org/atomic"

	"github.com/rudderlabs/rudder-server/services/controlplane/identity"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/testhelper/health"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/admin"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	"github.com/rudderlabs/rudder-server/warehouse"
)

// TODO: check for the data as well
// TODO: remove event if it is not mandatory
func TestUploads(t *testing.T) {
	admin.Init()
	validations.Init()

	t.Run("tracks", func(t *testing.T) {
		const (
			workspaceID           = "test_workspace_id"
			sourceID              = "test_source_id"
			destinationID         = "test_destination_id"
			destinationRevisionID = "test_destination_id"
			namespace             = "test_namespace"
		)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := resource.SetupMinio(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		backendConfigCh := make(chan pubsub.DataEvent)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			defer close(backendConfigCh)

			for {
				select {
				case <-gCtx.Done():
					return nil
				case <-time.After(time.Second):
					backendConfigCh <- pubsub.DataEvent{
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
													Name: warehouseutils.POSTGRES,
												},
												Config: map[string]interface{}{
													"host":             pgResource.Host,
													"database":         pgResource.Database,
													"user":             pgResource.User,
													"password":         pgResource.Password,
													"port":             pgResource.Port,
													"sslMode":          "disable",
													"namespace":        "test_namespace",
													"bucketProvider":   "MINIO",
													"bucketName":       minioResource.BucketName,
													"accessKeyID":      minioResource.AccessKeyID,
													"secretAccessKey":  minioResource.AccessKeySecret,
													"useSSL":           false,
													"endPoint":         minioResource.Endpoint,
													"syncFrequency":    "0",
													"useRudderStorage": false,
												},
												RevisionID: destinationRevisionID,
											},
										},
									},
								},
							},
						},
						Topic: string(backendconfig.TopicBackendConfig),
					}
				}
			}
		})
		g.Go(func() error {
			return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh)
		})
		g.Go(func() error {
			defer cancel()

			serverURL := fmt.Sprintf("http://localhost:%d", webPort)
			db := sqlmiddleware.New(pgResource.DB)
			events := 100
			jobs := 1

			payload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

			whClient := warehouseclient.NewWarehouse(serverURL)
			err := whClient.Process(ctx, warehouseclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]interface{}{
					"tracks": {
						"id":          "string",
						"event":       "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			})
			require.NoError(t, err)

			requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
			requireLoadFilesCount(t, db, sourceID, destinationID, events)
			requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
			requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
			requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
			return nil
		})
		require.NoError(t, g.Wait())
	})
	t.Run("destination revision", func(t *testing.T) {
		const (
			workspaceID           = "test_workspace_id"
			sourceID              = "test_source_id"
			destinationID         = "test_destination_id"
			destinationRevisionID = "test_updated_destination_id"
			namespace             = "test_namespace"
		)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := resource.SetupMinio(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		backendConfigCh := make(chan pubsub.DataEvent)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			defer close(backendConfigCh)

			for {
				select {
				case <-gCtx.Done():
					return nil
				case <-time.After(time.Second):
					backendConfigCh <- pubsub.DataEvent{
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
													Name: warehouseutils.POSTGRES,
												},
												Config: map[string]interface{}{
													"host":             pgResource.Host,
													"database":         pgResource.Database,
													"user":             pgResource.User,
													"password":         pgResource.Password,
													"port":             pgResource.Port,
													"sslMode":          "disable",
													"namespace":        "test_namespace",
													"bucketProvider":   "MINIO",
													"bucketName":       minioResource.BucketName,
													"accessKeyID":      minioResource.AccessKeyID,
													"secretAccessKey":  minioResource.AccessKeySecret,
													"useSSL":           false,
													"endPoint":         minioResource.Endpoint,
													"syncFrequency":    "0",
													"useRudderStorage": false,
												},
												RevisionID: destinationID,
											},
										},
									},
								},
							},
						},
						Topic: string(backendconfig.TopicBackendConfig),
					}
				}
			}
		})
		g.Go(func() error {
			calls := atomic.NewBool(false)
			cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/workspaces/destinationHistory/test_updated_destination_id":
					require.Equal(t, http.MethodGet, r.Method)
					body, err := json.Marshal(backendconfig.DestinationT{
						ID:      destinationID,
						Enabled: true,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							Name: warehouseutils.POSTGRES,
						},
						Config: map[string]interface{}{
							"host":             pgResource.Host,
							"database":         pgResource.Database,
							"user":             pgResource.User,
							"password":         pgResource.Password,
							"port":             pgResource.Port,
							"sslMode":          "disable",
							"namespace":        "test_namespace",
							"bucketProvider":   "MINIO",
							"bucketName":       minioResource.BucketName,
							"accessKeyID":      minioResource.AccessKeyID,
							"secretAccessKey":  minioResource.AccessKeySecret,
							"useSSL":           false,
							"endPoint":         minioResource.Endpoint,
							"syncFrequency":    "0",
							"useRudderStorage": false,
						},
						RevisionID: destinationID,
					})
					require.NoError(t, err)

					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(body)

					defer func() {
						calls.Store(true)
					}()
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer func() {
				cp.Close()
				require.True(t, calls.Load())
			}()

			overrideConfigs := []lo.Tuple2[string, interface{}]{
				{
					A: "CONFIG_BACKEND_URL",
					B: cp.URL,
				},
			}

			return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh, overrideConfigs...)
		})
		g.Go(func() error {
			defer cancel()

			serverURL := fmt.Sprintf("http://localhost:%d", webPort)
			db := sqlmiddleware.New(pgResource.DB)
			events := 100
			jobs := 1

			payload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

			whClient := warehouseclient.NewWarehouse(serverURL)
			err := whClient.Process(ctx, warehouseclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationRevisionID,
				Schema: map[string]map[string]interface{}{
					"tracks": {
						"id":          "string",
						"event":       "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			})
			require.NoError(t, err)

			requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
			requireLoadFilesCount(t, db, sourceID, destinationID, events)
			requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
			requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
			requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
			return nil
		})
		require.NoError(t, g.Wait())
	})
	t.Run("tunnelling", func(t *testing.T) {
		const (
			workspaceID           = "test_workspace_id"
			sourceID              = "test_source_id"
			destinationID         = "test_destination_id"
			destinationRevisionID = "test_destination_id"
			namespace             = "test_namespace"
		)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.ssh-server.yml"}))
		c.Start(context.Background())

		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := resource.SetupMinio(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		tunnelledHost := "db-private-postgres"
		tunnelledDatabase := "postgres"
		tunnelledPassword := "postgres"
		tunnelledUser := "postgres"
		tunnelledPort := "5432"
		tunnelledSSHUser := "rudderstack"
		tunnelledSSHHost := "localhost"
		tunnelledPrivateKey := "-----BEGIN OPENSSH PRIVATE KEY-----\\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABlwAAAAdzc2gtcn\\nNhAAAAAwEAAQAAAYEA0f/mqkkZ3c9qw8MTz5FoEO3PGecO/dtUFfJ4g1UBu9E7hi/pyVYY\\nfLfdsd5bqA2pXdU0ROymyVe683I1VzJcihUtwB1eQxP1mUhmoo0ixK0IUUGm4PRieCGv+r\\n0/gMvaYbVGUPCi5tAUVh02vZB7p2cTIaz872lvCnRhYbhGUHSbhNSSQOjnCtZfjuZZnE0l\\nPKjWV/wbJ7Pvoc/FZMlWOqL1AjAKuwFH5zs1RMrPDDv5PCZksq4a7DDxziEdq39jvA3sOm\\npQXvzBBBLBOzu7rM3/MPJb6dvAGJcYxkptfL4YXTscIMINr0g24cn+Thvt9yqA93rkb9RB\\nkw6RIEwMlQKqserA+pfsaoW0SkvnlDKzS1DLwXioL4Uc1Jpr/9jTMEfR+W7v7gJPB1JDnV\\ngen5FBfiMqbsG1amUS+mjgNfC8I00tR+CUHxpqUWANtcWTinhSnLJ2skj/2QnciPHkHurR\\nEKyEwCVecgn+xVKyRgVDCGsJ+QnAdn51+i/kO3nvAAAFqENNbN9DTWzfAAAAB3NzaC1yc2\\nEAAAGBANH/5qpJGd3PasPDE8+RaBDtzxnnDv3bVBXyeINVAbvRO4Yv6clWGHy33bHeW6gN\\nqV3VNETspslXuvNyNVcyXIoVLcAdXkMT9ZlIZqKNIsStCFFBpuD0Ynghr/q9P4DL2mG1Rl\\nDwoubQFFYdNr2Qe6dnEyGs/O9pbwp0YWG4RlB0m4TUkkDo5wrWX47mWZxNJTyo1lf8Gyez\\n76HPxWTJVjqi9QIwCrsBR+c7NUTKzww7+TwmZLKuGuww8c4hHat/Y7wN7DpqUF78wQQSwT\\ns7u6zN/zDyW+nbwBiXGMZKbXy+GF07HCDCDa9INuHJ/k4b7fcqgPd65G/UQZMOkSBMDJUC\\nqrHqwPqX7GqFtEpL55Qys0tQy8F4qC+FHNSaa//Y0zBH0flu7+4CTwdSQ51YHp+RQX4jKm\\n7BtWplEvpo4DXwvCNNLUfglB8aalFgDbXFk4p4UpyydrJI/9kJ3Ijx5B7q0RCshMAlXnIJ\\n/sVSskYFQwhrCfkJwHZ+dfov5Dt57wAAAAMBAAEAAAGAd9pxr+ag2LO0353LBMCcgGz5sn\\nLpX4F6cDw/A9XUc3lrW56k88AroaLe6NFbxoJlk6RHfL8EQg3MKX2Za/bWUgjcX7VjQy11\\nEtL7oPKkUVPgV1/8+o8AVEgFxDmWsM+oB/QJ+dAdaVaBBNUPlQmNSXHOvX2ZrpqiQXlCyx\\n79IpYq3JjmEB3dH5ZSW6CkrExrYD+MdhLw/Kv5rISEyI0Qpc6zv1fkB+8nNpXYRTbrDLR9\\n/xJ6jnBH9V3J5DeKU4MUQ39nrAp6iviyWydB973+MOygpy41fXO6hHyVZ2aSCysn1t6J/K\\nQdeEjqAOI/5CbdtiFGp06et799EFyzPItW0FKetW1UTOL2YHqdb+Q9sNjiNlUSzgxMbJWJ\\nRGO6g9B1mJsHl5mJZUiHQPsG/wgBER8VOP4bLOEB6gzVO2GE9HTJTOh5C+eEfrl52wPfXj\\nTqjtWAnhssxtgmWjkS0ibi+u1KMVXKHfaiqJ7nH0jMx+eu1RpMvuR8JqkU8qdMMGChAAAA\\nwHkQMfpCnjNAo6sllEB5FwjEdTBBOt7gu6nLQ2O3uGv0KNEEZ/BWJLQ5fKOfBtDHO+kl+5\\nQoxc0cE7cg64CyBF3+VjzrEzuX5Tuh4NwrsjT4vTTHhCIbIynxEPmKzvIyCMuglqd/nhu9\\n6CXhghuTg8NrC7lY+cImiBfhxE32zqNITlpHW7exr95Gz1sML2TRJqxDN93oUFfrEuInx8\\nHpXXnvMQxPRhcp9nDMU9/ahUamMabQqVVMwKDi8n3sPPzTiAAAAMEA+/hm3X/yNotAtMAH\\ny11parKQwPgEF4HYkSE0bEe+2MPJmEk4M4PGmmt/MQC5N5dXdUGxiQeVMR+Sw0kN9qZjM6\\nSIz0YHQFMsxVmUMKFpAh4UI0GlsW49jSpVXs34Fg95AfhZOYZmOcGcYosp0huCeRlpLeIH\\n7Vv2bkfQaic3uNaVPg7+cXg7zdY6tZlzwa/4Fj0udfTjGQJOPSzIihdMLHnV81rZ2cUOZq\\nMSk6b02aMpVB4TV0l1w4j2mlF2eGD9AAAAwQDVW6p2VXKuPR7SgGGQgHXpAQCFZPGLYd8K\\nduRaCbxKJXzUnZBn53OX5fuLlFhmRmAMXE6ztHPN1/5JjwILn+O49qel1uUvzU8TaWioq7\\nAre3SJR2ZucR4AKUvzUHGP3GWW96xPN8lq+rgb0th1eOSU2aVkaIdeTJhV1iPfaUUf+15S\\nYcJlSHLGgeqkok+VfuudZ73f3RFFhjoe1oAjlPB4leeMsBD9UBLx2U3xAevnfkecF4Lm83\\n4sVswWATSFAFsAAAAsYWJoaW1hbnl1YmFiYmFyQEFiaGltYW55dXMtTWFjQm9vay1Qcm8u\\nbG9jYWwBAgMEBQYH\\n-----END OPENSSH PRIVATE KEY-----"
		sshPort := c.Port("ssh-server", 2222)

		backendConfigCh := make(chan pubsub.DataEvent)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			defer close(backendConfigCh)

			for {
				select {
				case <-gCtx.Done():
					return nil
				case <-time.After(time.Second):
					backendConfigCh <- pubsub.DataEvent{
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
													Name: warehouseutils.POSTGRES,
												},
												Config: map[string]interface{}{
													"host":             tunnelledHost,
													"database":         tunnelledDatabase,
													"user":             tunnelledUser,
													"password":         tunnelledPassword,
													"port":             tunnelledPort,
													"sslMode":          "disable",
													"namespace":        "test_namespace",
													"bucketProvider":   "MINIO",
													"bucketName":       minioResource.BucketName,
													"accessKeyID":      minioResource.AccessKeyID,
													"secretAccessKey":  minioResource.AccessKeySecret,
													"useSSL":           false,
													"endPoint":         minioResource.Endpoint,
													"syncFrequency":    "0",
													"useRudderStorage": false,
													"useSSH":           true,
													"sshUser":          tunnelledSSHUser,
													"sshPort":          strconv.Itoa(sshPort),
													"sshHost":          tunnelledSSHHost,
													"sshPrivateKey":    strings.ReplaceAll(tunnelledPrivateKey, "\\n", "\n"),
												},
												RevisionID: destinationRevisionID,
											},
										},
									},
								},
							},
						},
						Topic: string(backendconfig.TopicBackendConfig),
					}
				}
			}
		})
		g.Go(func() error {
			return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh)
		})
		g.Go(func() error {
			defer cancel()

			serverURL := fmt.Sprintf("http://localhost:%d", webPort)
			db := sqlmiddleware.New(pgResource.DB)
			events := 100
			jobs := 1

			payload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

			whClient := warehouseclient.NewWarehouse(serverURL)
			err := whClient.Process(ctx, warehouseclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]interface{}{
					"tracks": {
						"id":          "string",
						"event":       "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			})
			require.NoError(t, err)

			requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
			requireLoadFilesCount(t, db, sourceID, destinationID, events)
			requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
			requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)

			dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
				tunnelledUser,
				tunnelledPassword,
				tunnelledHost,
				tunnelledPort,
				tunnelledDatabase,
			)
			tunnelInfo := &tunnelling.TunnelInfo{
				Config: map[string]interface{}{
					"sshUser":       tunnelledSSHUser,
					"sshPort":       strconv.Itoa(sshPort),
					"sshHost":       tunnelledSSHHost,
					"sshPrivateKey": strings.ReplaceAll(tunnelledPrivateKey, "\\n", "\n"),
				},
			}

			tunnelDB, err := tunnelling.Connect(dsn, tunnelInfo.Config)
			require.NoError(t, err)
			require.NoError(t, tunnelDB.Ping())

			requireWarehouseEventsCount(t, sqlmiddleware.New(tunnelDB), fmt.Sprintf("%s.%s", namespace, "tracks"), events)
			return nil
		})
		require.NoError(t, g.Wait())
	})
	t.Run("reports", func(t *testing.T) {
		const (
			workspaceID           = "test_workspace_id"
			sourceID              = "test_source_id"
			destinationID         = "test_destination_id"
			destinationRevisionID = "test_destination_id"
			namespace             = "test_namespace"
		)

		t.Run("succeeded", func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := resource.SetupMinio(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			backendConfigCh := make(chan pubsub.DataEvent)

			g, gCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				defer close(backendConfigCh)

				for {
					select {
					case <-gCtx.Done():
						return nil
					case <-time.After(time.Second):
						backendConfigCh <- pubsub.DataEvent{
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
														Name: warehouseutils.POSTGRES,
													},
													Config: map[string]interface{}{
														"host":             pgResource.Host,
														"database":         pgResource.Database,
														"user":             pgResource.User,
														"password":         pgResource.Password,
														"port":             pgResource.Port,
														"sslMode":          "disable",
														"namespace":        "test_namespace",
														"bucketProvider":   "MINIO",
														"bucketName":       minioResource.BucketName,
														"accessKeyID":      minioResource.AccessKeyID,
														"secretAccessKey":  minioResource.AccessKeySecret,
														"useSSL":           false,
														"endPoint":         minioResource.Endpoint,
														"syncFrequency":    "0",
														"useRudderStorage": false,
													},
													RevisionID: destinationRevisionID,
												},
											},
										},
									},
								},
							},
							Topic: string(backendconfig.TopicBackendConfig),
						}
					}
				}
			})
			g.Go(func() error {
				return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh)
			})
			g.Go(func() error {
				defer cancel()

				serverURL := fmt.Sprintf("http://localhost:%d", webPort)
				db := sqlmiddleware.New(pgResource.DB)
				events := 100
				jobs := 1

				payload := strings.Join(lo.RepeatBy(events, func(int) string {
					return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
						uuid.New().String(),
						uuid.New().String(),
					)
				}), "\n")

				health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

				whClient := warehouseclient.NewWarehouse(serverURL)
				err := whClient.Process(ctx, warehouseclient.StagingFile{
					WorkspaceID:           workspaceID,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
					TotalEvents:           events,
					FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
					LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
					UseRudderStorage:      false,
					DestinationRevisionID: destinationID,
					Schema: map[string]map[string]interface{}{
						"tracks": {
							"id":          "string",
							"event":       "string",
							"user_id":     "string",
							"received_at": "datetime",
						},
					},
				})
				require.NoError(t, err)

				requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
				requireLoadFilesCount(t, db, sourceID, destinationID, events)
				requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
				requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
				requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				requireReportsCount(t, db, sourceID, destinationID, jobsdb.Succeeded.State, 200, events)
				return nil
			})
			require.NoError(t, g.Wait())
		})
		t.Run("aborted", func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := resource.SetupMinio(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			backendConfigCh := make(chan pubsub.DataEvent)

			g, gCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				defer close(backendConfigCh)

				for {
					select {
					case <-gCtx.Done():
						return nil
					case <-time.After(time.Second):
						backendConfigCh <- pubsub.DataEvent{
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
														Name: warehouseutils.POSTGRES,
													},
													Config: map[string]interface{}{
														"host":             pgResource.Host,
														"database":         pgResource.Database,
														"user":             pgResource.User,
														"password":         pgResource.Password,
														"port":             "5432",
														"sslMode":          "disable",
														"namespace":        "test_namespace",
														"bucketProvider":   "MINIO",
														"bucketName":       minioResource.BucketName,
														"accessKeyID":      minioResource.AccessKeyID,
														"secretAccessKey":  minioResource.AccessKeySecret,
														"useSSL":           false,
														"endPoint":         minioResource.Endpoint,
														"syncFrequency":    "0",
														"useRudderStorage": false,
													},
													RevisionID: destinationRevisionID,
												},
											},
										},
									},
								},
							},
							Topic: string(backendconfig.TopicBackendConfig),
						}
					}
				}
			})
			g.Go(func() error {
				configOverrides := []lo.Tuple2[string, interface{}]{
					{A: "Warehouse.minRetryAttempts", B: 2},
					{A: "Warehouse.retryTimeWindow", B: "0s"},
					{A: "Warehouse.minUploadBackoff", B: "0s"},
					{A: "Warehouse.maxUploadBackoff", B: "0s"},
				}
				return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh, configOverrides...)
			})
			g.Go(func() error {
				defer cancel()

				serverURL := fmt.Sprintf("http://localhost:%d", webPort)
				db := sqlmiddleware.New(pgResource.DB)
				events := 100
				jobs := 1

				payload := strings.Join(lo.RepeatBy(events, func(int) string {
					return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
						uuid.New().String(),
						uuid.New().String(),
					)
				}), "\n")

				health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

				whClient := warehouseclient.NewWarehouse(serverURL)
				err := whClient.Process(ctx, warehouseclient.StagingFile{
					WorkspaceID:           workspaceID,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
					TotalEvents:           events,
					FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
					LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
					UseRudderStorage:      false,
					DestinationRevisionID: destinationID,
					Schema: map[string]map[string]interface{}{
						"tracks": {
							"id":          "string",
							"event":       "string",
							"user_id":     "string",
							"received_at": "datetime",
						},
					},
				})
				require.NoError(t, err)

				requireUploadsCount(t, db, sourceID, destinationID, namespace, model.Aborted, jobs)
				requireReportsCount(t, db, sourceID, destinationID, jobsdb.Failed.State, 400, events*2)
				requireReportsCount(t, db, sourceID, destinationID, jobsdb.Aborted.State, 400, events)
				return nil
			})
			require.NoError(t, g.Wait())
		})
	})
	t.Run("retries then aborts", func(t *testing.T) {
		const (
			workspaceID           = "test_workspace_id"
			sourceID              = "test_source_id"
			destinationID         = "test_destination_id"
			destinationRevisionID = "test_destination_id"
			namespace             = "test_namespace"
		)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := resource.SetupMinio(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		backendConfigCh := make(chan pubsub.DataEvent)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			defer close(backendConfigCh)

			for {
				select {
				case <-gCtx.Done():
					return nil
				case <-time.After(time.Second):
					backendConfigCh <- pubsub.DataEvent{
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
													Name: warehouseutils.POSTGRES,
												},
												Config: map[string]interface{}{
													"host":             pgResource.Host,
													"database":         pgResource.Database,
													"user":             pgResource.User,
													"password":         pgResource.Password,
													"port":             "5432",
													"sslMode":          "disable",
													"namespace":        "test_namespace",
													"bucketProvider":   "MINIO",
													"bucketName":       minioResource.BucketName,
													"accessKeyID":      minioResource.AccessKeyID,
													"secretAccessKey":  minioResource.AccessKeySecret,
													"useSSL":           false,
													"endPoint":         minioResource.Endpoint,
													"syncFrequency":    "0",
													"useRudderStorage": true,
												},
												RevisionID: destinationRevisionID,
											},
										},
									},
								},
							},
						},
						Topic: string(backendconfig.TopicBackendConfig),
					}
				}
			}
		})
		g.Go(func() error {
			configOverrides := []lo.Tuple2[string, interface{}]{
				{A: "Warehouse.minRetryAttempts", B: 2},
				{A: "Warehouse.retryTimeWindow", B: "0s"},
				{A: "Warehouse.minUploadBackoff", B: "0s"},
				{A: "Warehouse.maxUploadBackoff", B: "0s"},
			}
			return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh, configOverrides...)
		})
		g.Go(func() error {
			defer cancel()

			serverURL := fmt.Sprintf("http://localhost:%d", webPort)
			db := sqlmiddleware.New(pgResource.DB)
			events := 100
			jobs := 1

			payload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

			whClient := warehouseclient.NewWarehouse(serverURL)
			err := whClient.Process(ctx, warehouseclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]interface{}{
					"tracks": {
						"id":          "string",
						"event":       "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			})
			require.NoError(t, err)

			requireUploadsCount(t, db, sourceID, destinationID, namespace, model.Aborted, jobs)
			requireRetriedUploadsCount(t, db, sourceID, destinationID, namespace, model.Aborted, 3)
			return nil
		})
		require.NoError(t, g.Wait())
	})
	t.Run("user and identifies", func(t *testing.T) {
		const (
			workspaceID           = "test_workspace_id"
			sourceID              = "test_source_id"
			destinationID         = "test_destination_id"
			destinationRevisionID = "test_destination_id"
			namespace             = "test_namespace"
		)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := resource.SetupMinio(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		backendConfigCh := make(chan pubsub.DataEvent)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			defer close(backendConfigCh)

			for {
				select {
				case <-gCtx.Done():
					return nil
				case <-time.After(time.Second):
					backendConfigCh <- pubsub.DataEvent{
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
													Name: warehouseutils.POSTGRES,
												},
												Config: map[string]interface{}{
													"host":             pgResource.Host,
													"database":         pgResource.Database,
													"user":             pgResource.User,
													"password":         pgResource.Password,
													"port":             pgResource.Port,
													"sslMode":          "disable",
													"namespace":        "test_namespace",
													"bucketProvider":   "MINIO",
													"bucketName":       minioResource.BucketName,
													"accessKeyID":      minioResource.AccessKeyID,
													"secretAccessKey":  minioResource.AccessKeySecret,
													"useSSL":           false,
													"endPoint":         minioResource.Endpoint,
													"syncFrequency":    "0",
													"useRudderStorage": false,
												},
												RevisionID: destinationRevisionID,
											},
										},
									},
								},
							},
						},
						Topic: string(backendconfig.TopicBackendConfig),
					}
				}
			}
		})
		g.Go(func() error {
			return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh)
		})
		g.Go(func() error {
			defer cancel()

			serverURL := fmt.Sprintf("http://localhost:%d", webPort)
			db := sqlmiddleware.New(pgResource.DB)
			events := 100
			jobs := 1

			users := lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "identifies"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			})
			identifies := lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"user_id":"string","received_at":"datetime"}, "table": "users"}}`,
					uuid.New().String(),
				)
			})
			payloads := make([]string, 0, events*2)
			for i := 0; i < events; i++ {
				payloads = append(payloads, users[i], identifies[i])
			}

			payload := strings.Join(payloads, "\n")

			health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

			whClient := warehouseclient.NewWarehouse(serverURL)
			err := whClient.Process(ctx, warehouseclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]interface{}{
					"users": {
						"user_id":     "string",
						"received_at": "datetime",
					},
					"identifies": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			})
			require.NoError(t, err)

			requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
			requireLoadFilesCount(t, db, sourceID, destinationID, events*2)
			requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events*2)
			requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
			requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "users"), events)
			requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "identifies"), events)
			return nil
		})
		require.NoError(t, g.Wait())
	})
	t.Run("discards", func(t *testing.T) {
		const (
			workspaceID           = "test_workspace_id"
			sourceID              = "test_source_id"
			destinationID         = "test_destination_id"
			destinationRevisionID = "test_destination_id"
			namespace             = "test_namespace"
		)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := resource.SetupMinio(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		backendConfigCh := make(chan pubsub.DataEvent)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			defer close(backendConfigCh)

			for {
				select {
				case <-gCtx.Done():
					return nil
				case <-time.After(time.Second):
					backendConfigCh <- pubsub.DataEvent{
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
													Name: warehouseutils.POSTGRES,
												},
												Config: map[string]interface{}{
													"host":             pgResource.Host,
													"database":         pgResource.Database,
													"user":             pgResource.User,
													"password":         pgResource.Password,
													"port":             pgResource.Port,
													"sslMode":          "disable",
													"namespace":        "test_namespace",
													"bucketProvider":   "MINIO",
													"bucketName":       minioResource.BucketName,
													"accessKeyID":      minioResource.AccessKeyID,
													"secretAccessKey":  minioResource.AccessKeySecret,
													"useSSL":           false,
													"endPoint":         minioResource.Endpoint,
													"syncFrequency":    "0",
													"useRudderStorage": false,
												},
												RevisionID: destinationRevisionID,
											},
										},
									},
								},
							},
						},
						Topic: string(backendconfig.TopicBackendConfig),
					}
				}
			}
		})
		g.Go(func() error {
			return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh)
		})
		g.Go(func() error {
			defer cancel()

			serverURL := fmt.Sprintf("http://localhost:%d", webPort)
			db := sqlmiddleware.New(pgResource.DB)
			events := 100
			jobs := 1

			goodPayloads := lo.RepeatBy(events/2, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			})
			badPayloads := lo.RepeatBy(events/2, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%d,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"int","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					rand.Intn(1000),
				)
			})
			payloads := make([]string, 0, events*2)
			for i := 0; i < events/2; i++ {
				payloads = append(payloads, goodPayloads[i], badPayloads[i])
			}

			payload := strings.Join(payloads, "\n")

			health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

			whClient := warehouseclient.NewWarehouse(serverURL)
			err := whClient.Process(ctx, warehouseclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]interface{}{
					"tracks": {
						"id":          "string",
						"event":       "string",
						"user_id":     "int",
						"received_at": "datetime",
					},
				},
			})
			require.NoError(t, err)

			requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
			requireLoadFilesCount(t, db, sourceID, destinationID, events+(events/2))
			requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events+(events/2))
			requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
			requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
			requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "rudder_discards"), events/2)
			return nil
		})
		require.NoError(t, g.Wait())
	})
	t.Run("archiver", func(t *testing.T) {
		const (
			workspaceID           = "test_workspace_id"
			sourceID              = "test_source_id"
			destinationID         = "test_destination_id"
			destinationRevisionID = "test_destination_id"
			namespace             = "test_namespace"
		)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := resource.SetupMinio(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		backendConfigCh := make(chan pubsub.DataEvent)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			defer close(backendConfigCh)

			for {
				select {
				case <-gCtx.Done():
					return nil
				case <-time.After(time.Second):
					backendConfigCh <- pubsub.DataEvent{
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
													Name: warehouseutils.POSTGRES,
												},
												Config: map[string]interface{}{
													"host":             pgResource.Host,
													"database":         pgResource.Database,
													"user":             pgResource.User,
													"password":         pgResource.Password,
													"port":             pgResource.Port,
													"sslMode":          "disable",
													"namespace":        "test_namespace",
													"bucketProvider":   "MINIO",
													"bucketName":       minioResource.BucketName,
													"accessKeyID":      minioResource.AccessKeyID,
													"secretAccessKey":  minioResource.AccessKeySecret,
													"useSSL":           false,
													"endPoint":         minioResource.Endpoint,
													"syncFrequency":    "0",
													"useRudderStorage": false,
												},
												RevisionID: destinationRevisionID,
											},
										},
									},
								},
							},
						},
						Topic: string(backendconfig.TopicBackendConfig),
					}
				}
			}
		})
		g.Go(func() error {
			overrides := []lo.Tuple2[string, interface{}]{
				{A: "Warehouse.archiveUploadRelatedRecords", B: true},
				{A: "Warehouse.uploadsArchivalTimeInDays", B: 0},
				{A: "Warehouse.archiverTickerTime", B: "5s"},
				{A: "JOBS_BACKUP_STORAGE_PROVIDER", B: "MINIO"},
				{A: "JOBS_BACKUP_BUCKET", B: minioResource.BucketName},
				{A: "MINIO_ENDPOINT", B: minioResource.Endpoint},
				{A: "MINIO_ACCESS_KEY_ID", B: minioResource.AccessKeyID},
				{A: "MINIO_SECRET_ACCESS_KEY", B: minioResource.AccessKeySecret},
				{A: "MINIO_SSL", B: "false"},
			}
			return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh, overrides...)
		})
		g.Go(func() error {
			defer cancel()

			serverURL := fmt.Sprintf("http://localhost:%d", webPort)
			db := sqlmiddleware.New(pgResource.DB)
			events := 100
			jobs := 1

			payload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

			whClient := warehouseclient.NewWarehouse(serverURL)
			err := whClient.Process(ctx, warehouseclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]interface{}{
					"tracks": {
						"id":          "string",
						"event":       "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			})
			require.NoError(t, err)

			requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
			requireLoadFilesCount(t, db, sourceID, destinationID, events)
			requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
			requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
			requireArchivedUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
			return nil
		})
		require.NoError(t, g.Wait())
	})
	t.Run("sync behaviour", func(t *testing.T) {
		const (
			workspaceID           = "test_workspace_id"
			sourceID              = "test_source_id"
			destinationID         = "test_destination_id"
			destinationRevisionID = "test_destination_id"
			namespace             = "test_namespace"
		)

		t.Run("default behaviour", func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := resource.SetupMinio(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			backendConfigCh := make(chan pubsub.DataEvent)

			g, gCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				defer close(backendConfigCh)

				for {
					select {
					case <-gCtx.Done():
						return nil
					case <-time.After(time.Second):
						backendConfigCh <- pubsub.DataEvent{
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
														Name: warehouseutils.POSTGRES,
													},
													Config: map[string]interface{}{
														"host":             pgResource.Host,
														"database":         pgResource.Database,
														"user":             pgResource.User,
														"password":         pgResource.Password,
														"port":             pgResource.Port,
														"sslMode":          "disable",
														"namespace":        "test_namespace",
														"bucketProvider":   "MINIO",
														"bucketName":       minioResource.BucketName,
														"accessKeyID":      minioResource.AccessKeyID,
														"secretAccessKey":  minioResource.AccessKeySecret,
														"useSSL":           false,
														"endPoint":         minioResource.Endpoint,
														"syncFrequency":    "0",
														"useRudderStorage": false,
													},
													RevisionID: destinationRevisionID,
												},
											},
										},
									},
								},
							},
							Topic: string(backendconfig.TopicBackendConfig),
						}
					}
				}
			})
			g.Go(func() error {
				return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh)
			})
			g.Go(func() error {
				defer cancel()

				serverURL := fmt.Sprintf("http://localhost:%d", webPort)
				db := sqlmiddleware.New(pgResource.DB)
				events := 100
				jobs := 1

				payload := strings.Join(lo.RepeatBy(events, func(int) string {
					return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
						uuid.New().String(),
						uuid.New().String(),
					)
				}), "\n")

				health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

				whClient := warehouseclient.NewWarehouse(serverURL)
				stagingFile := warehouseclient.StagingFile{
					WorkspaceID:           workspaceID,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
					TotalEvents:           events,
					FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
					LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
					UseRudderStorage:      false,
					DestinationRevisionID: destinationID,
					Schema: map[string]map[string]interface{}{
						"tracks": {
							"id":          "string",
							"event":       "string",
							"user_id":     "string",
							"received_at": "datetime",
						},
					},
				}

				func() {
					t.Logf("first sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
					requireLoadFilesCount(t, db, sourceID, destinationID, events)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				func() {
					t.Logf("second sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events*2)
					requireLoadFilesCount(t, db, sourceID, destinationID, events*2)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events*2)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs*2)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				return nil
			})
			require.NoError(t, g.Wait())
		})
		t.Run("allowMerge=false", func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := resource.SetupMinio(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			backendConfigCh := make(chan pubsub.DataEvent)

			g, gCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				defer close(backendConfigCh)

				for {
					select {
					case <-gCtx.Done():
						return nil
					case <-time.After(time.Second):
						backendConfigCh <- pubsub.DataEvent{
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
														Name: warehouseutils.POSTGRES,
													},
													Config: map[string]interface{}{
														"host":             pgResource.Host,
														"database":         pgResource.Database,
														"user":             pgResource.User,
														"password":         pgResource.Password,
														"port":             pgResource.Port,
														"sslMode":          "disable",
														"namespace":        "test_namespace",
														"bucketProvider":   "MINIO",
														"bucketName":       minioResource.BucketName,
														"accessKeyID":      minioResource.AccessKeyID,
														"secretAccessKey":  minioResource.AccessKeySecret,
														"useSSL":           false,
														"endPoint":         minioResource.Endpoint,
														"syncFrequency":    "0",
														"useRudderStorage": false,
													},
													RevisionID: destinationRevisionID,
												},
											},
										},
									},
								},
							},
							Topic: string(backendconfig.TopicBackendConfig),
						}
					}
				}
			})
			g.Go(func() error {
				overrides := []lo.Tuple2[string, interface{}]{
					{A: "Warehouse.postgres.allowMerge", B: false},
				}
				return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh, overrides...)
			})
			g.Go(func() error {
				defer cancel()

				serverURL := fmt.Sprintf("http://localhost:%d", webPort)
				db := sqlmiddleware.New(pgResource.DB)
				events := 100
				jobs := 1

				payload := strings.Join(lo.RepeatBy(events, func(int) string {
					return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
						uuid.New().String(),
						uuid.New().String(),
					)
				}), "\n")

				health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

				whClient := warehouseclient.NewWarehouse(serverURL)
				stagingFile := warehouseclient.StagingFile{
					WorkspaceID:           workspaceID,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
					TotalEvents:           events,
					FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
					LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
					UseRudderStorage:      false,
					DestinationRevisionID: destinationID,
					Schema: map[string]map[string]interface{}{
						"tracks": {
							"id":          "string",
							"event":       "string",
							"user_id":     "string",
							"received_at": "datetime",
						},
					},
				}

				func() {
					t.Logf("first sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
					requireLoadFilesCount(t, db, sourceID, destinationID, events)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				func() {
					t.Logf("second sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events*2)
					requireLoadFilesCount(t, db, sourceID, destinationID, events*2)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events*2)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs*2)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events*2)
				}()
				return nil
			})
			require.NoError(t, g.Wait())
		})
		t.Run("allowMerge=true, enableMerge=false", func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := resource.SetupMinio(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			backendConfigCh := make(chan pubsub.DataEvent)

			g, gCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				defer close(backendConfigCh)

				for {
					select {
					case <-gCtx.Done():
						return nil
					case <-time.After(time.Second):
						backendConfigCh <- pubsub.DataEvent{
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
														Name: warehouseutils.POSTGRES,
													},
													Config: map[string]interface{}{
														"host":             pgResource.Host,
														"database":         pgResource.Database,
														"user":             pgResource.User,
														"password":         pgResource.Password,
														"port":             pgResource.Port,
														"sslMode":          "disable",
														"namespace":        "test_namespace",
														"bucketProvider":   "MINIO",
														"bucketName":       minioResource.BucketName,
														"accessKeyID":      minioResource.AccessKeyID,
														"secretAccessKey":  minioResource.AccessKeySecret,
														"useSSL":           false,
														"endPoint":         minioResource.Endpoint,
														"syncFrequency":    "0",
														"useRudderStorage": false,
														"enableMerge":      false,
													},
													RevisionID: destinationRevisionID,
												},
											},
										},
									},
								},
							},
							Topic: string(backendconfig.TopicBackendConfig),
						}
					}
				}
			})
			g.Go(func() error {
				overrides := []lo.Tuple2[string, interface{}]{
					{A: "Warehouse.postgres.allowMerge", B: true},
				}
				return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh, overrides...)
			})
			g.Go(func() error {
				defer cancel()

				serverURL := fmt.Sprintf("http://localhost:%d", webPort)
				db := sqlmiddleware.New(pgResource.DB)
				events := 100
				jobs := 1

				payload := strings.Join(lo.RepeatBy(events, func(int) string {
					return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
						uuid.New().String(),
						uuid.New().String(),
					)
				}), "\n")

				health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

				whClient := warehouseclient.NewWarehouse(serverURL)
				stagingFile := warehouseclient.StagingFile{
					WorkspaceID:           workspaceID,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
					TotalEvents:           events,
					FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
					LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
					UseRudderStorage:      false,
					DestinationRevisionID: destinationID,
					Schema: map[string]map[string]interface{}{
						"tracks": {
							"id":          "string",
							"event":       "string",
							"user_id":     "string",
							"received_at": "datetime",
						},
					},
				}

				func() {
					t.Logf("first sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
					requireLoadFilesCount(t, db, sourceID, destinationID, events)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				func() {
					t.Logf("second sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events*2)
					requireLoadFilesCount(t, db, sourceID, destinationID, events*2)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events*2)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs*2)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events*2)
				}()
				return nil
			})
			require.NoError(t, g.Wait())
		})
		t.Run("allowMerge=false, enableMerge=false", func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := resource.SetupMinio(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			backendConfigCh := make(chan pubsub.DataEvent)

			g, gCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				defer close(backendConfigCh)

				for {
					select {
					case <-gCtx.Done():
						return nil
					case <-time.After(time.Second):
						backendConfigCh <- pubsub.DataEvent{
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
														Name: warehouseutils.POSTGRES,
													},
													Config: map[string]interface{}{
														"host":             pgResource.Host,
														"database":         pgResource.Database,
														"user":             pgResource.User,
														"password":         pgResource.Password,
														"port":             pgResource.Port,
														"sslMode":          "disable",
														"namespace":        "test_namespace",
														"bucketProvider":   "MINIO",
														"bucketName":       minioResource.BucketName,
														"accessKeyID":      minioResource.AccessKeyID,
														"secretAccessKey":  minioResource.AccessKeySecret,
														"useSSL":           false,
														"endPoint":         minioResource.Endpoint,
														"syncFrequency":    "0",
														"useRudderStorage": false,
														"enableMerge":      false,
													},
													RevisionID: destinationRevisionID,
												},
											},
										},
									},
								},
							},
							Topic: string(backendconfig.TopicBackendConfig),
						}
					}
				}
			})
			g.Go(func() error {
				overrides := []lo.Tuple2[string, interface{}]{
					{A: "Warehouse.postgres.allowMerge", B: false},
				}
				return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh, overrides...)
			})
			g.Go(func() error {
				defer cancel()

				serverURL := fmt.Sprintf("http://localhost:%d", webPort)
				db := sqlmiddleware.New(pgResource.DB)
				events := 100
				jobs := 1

				payload := strings.Join(lo.RepeatBy(events, func(int) string {
					return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
						uuid.New().String(),
						uuid.New().String(),
					)
				}), "\n")

				health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

				whClient := warehouseclient.NewWarehouse(serverURL)
				stagingFile := warehouseclient.StagingFile{
					WorkspaceID:           workspaceID,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
					TotalEvents:           events,
					FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
					LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
					UseRudderStorage:      false,
					DestinationRevisionID: destinationID,
					Schema: map[string]map[string]interface{}{
						"tracks": {
							"id":          "string",
							"event":       "string",
							"user_id":     "string",
							"received_at": "datetime",
						},
					},
				}

				func() {
					t.Logf("first sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
					requireLoadFilesCount(t, db, sourceID, destinationID, events)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				func() {
					t.Logf("second sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events*2)
					requireLoadFilesCount(t, db, sourceID, destinationID, events*2)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events*2)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs*2)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events*2)
				}()
				return nil
			})
			require.NoError(t, g.Wait())
		})
		t.Run("allowMerge=false, enableMerge=false, isSourceETL=true", func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := resource.SetupMinio(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			backendConfigCh := make(chan pubsub.DataEvent)

			g, gCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				defer close(backendConfigCh)

				for {
					select {
					case <-gCtx.Done():
						return nil
					case <-time.After(time.Second):
						backendConfigCh <- pubsub.DataEvent{
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
														Name: warehouseutils.POSTGRES,
													},
													Config: map[string]interface{}{
														"host":             pgResource.Host,
														"database":         pgResource.Database,
														"user":             pgResource.User,
														"password":         pgResource.Password,
														"port":             pgResource.Port,
														"sslMode":          "disable",
														"namespace":        "test_namespace",
														"bucketProvider":   "MINIO",
														"bucketName":       minioResource.BucketName,
														"accessKeyID":      minioResource.AccessKeyID,
														"secretAccessKey":  minioResource.AccessKeySecret,
														"useSSL":           false,
														"endPoint":         minioResource.Endpoint,
														"syncFrequency":    "0",
														"useRudderStorage": false,
														"enableMerge":      false,
													},
													RevisionID: destinationRevisionID,
												},
											},
										},
									},
								},
							},
							Topic: string(backendconfig.TopicBackendConfig),
						}
					}
				}
			})
			g.Go(func() error {
				overrides := []lo.Tuple2[string, interface{}]{
					{A: "Warehouse.postgres.allowMerge", B: false},
				}
				return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh, overrides...)
			})
			g.Go(func() error {
				defer cancel()

				serverURL := fmt.Sprintf("http://localhost:%d", webPort)
				db := sqlmiddleware.New(pgResource.DB)
				events := 100
				jobs := 1

				payload := strings.Join(lo.RepeatBy(events, func(int) string {
					return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
						uuid.New().String(),
						uuid.New().String(),
					)
				}), "\n")

				health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

				whClient := warehouseclient.NewWarehouse(serverURL)
				stagingFile := warehouseclient.StagingFile{
					WorkspaceID:           workspaceID,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
					TotalEvents:           events,
					FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
					LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
					UseRudderStorage:      false,
					DestinationRevisionID: destinationID,
					SourceJobID:           uuid.NewString(),
					SourceJobRunID:        uuid.NewString(),
					SourceTaskRunID:       uuid.NewString(),
					Schema: map[string]map[string]interface{}{
						"tracks": {
							"id":          "string",
							"event":       "string",
							"user_id":     "string",
							"received_at": "datetime",
						},
					},
				}

				func() {
					t.Logf("first sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
					requireLoadFilesCount(t, db, sourceID, destinationID, events)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				func() {
					t.Logf("second sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events*2)
					requireLoadFilesCount(t, db, sourceID, destinationID, events*2)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events*2)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs*2)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				return nil
			})
			require.NoError(t, g.Wait())
		})
		t.Run("allowMerge=false, enableMerge=false, IsReplaySource=true", func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := resource.SetupMinio(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			backendConfigCh := make(chan pubsub.DataEvent)

			g, gCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				defer close(backendConfigCh)

				for {
					select {
					case <-gCtx.Done():
						return nil
					case <-time.After(time.Second):
						backendConfigCh <- pubsub.DataEvent{
							Data: map[string]backendconfig.ConfigT{
								workspaceID: {
									WorkspaceID: workspaceID,
									Sources: []backendconfig.SourceT{
										{
											ID:         sourceID,
											OriginalID: sourceID,
											Enabled:    true,
											Destinations: []backendconfig.DestinationT{
												{
													ID:      destinationID,
													Enabled: true,
													DestinationDefinition: backendconfig.DestinationDefinitionT{
														Name: warehouseutils.POSTGRES,
													},
													Config: map[string]interface{}{
														"host":             pgResource.Host,
														"database":         pgResource.Database,
														"user":             pgResource.User,
														"password":         pgResource.Password,
														"port":             pgResource.Port,
														"sslMode":          "disable",
														"namespace":        "test_namespace",
														"bucketProvider":   "MINIO",
														"bucketName":       minioResource.BucketName,
														"accessKeyID":      minioResource.AccessKeyID,
														"secretAccessKey":  minioResource.AccessKeySecret,
														"useSSL":           false,
														"endPoint":         minioResource.Endpoint,
														"syncFrequency":    "0",
														"useRudderStorage": false,
														"enableMerge":      false,
													},
													RevisionID: destinationRevisionID,
												},
											},
										},
									},
								},
							},
							Topic: string(backendconfig.TopicBackendConfig),
						}
					}
				}
			})
			g.Go(func() error {
				overrides := []lo.Tuple2[string, interface{}]{
					{A: "Warehouse.postgres.allowMerge", B: false},
				}
				return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh, overrides...)
			})
			g.Go(func() error {
				defer cancel()

				serverURL := fmt.Sprintf("http://localhost:%d", webPort)
				db := sqlmiddleware.New(pgResource.DB)
				events := 100
				jobs := 1

				payload := strings.Join(lo.RepeatBy(events, func(int) string {
					return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
						uuid.New().String(),
						uuid.New().String(),
					)
				}), "\n")

				health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

				whClient := warehouseclient.NewWarehouse(serverURL)
				stagingFile := warehouseclient.StagingFile{
					WorkspaceID:           workspaceID,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
					TotalEvents:           events,
					FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
					LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
					UseRudderStorage:      false,
					DestinationRevisionID: destinationID,
					Schema: map[string]map[string]interface{}{
						"tracks": {
							"id":          "string",
							"event":       "string",
							"user_id":     "string",
							"received_at": "datetime",
						},
					},
				}

				func() {
					t.Logf("first sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
					requireLoadFilesCount(t, db, sourceID, destinationID, events)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				func() {
					t.Logf("second sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events*2)
					requireLoadFilesCount(t, db, sourceID, destinationID, events*2)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events*2)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs*2)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				return nil
			})
			require.NoError(t, g.Wait())
		})
		t.Run("allowMerge=false, enableMerge=false, sourceCategory=cloud", func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := resource.SetupMinio(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			backendConfigCh := make(chan pubsub.DataEvent)

			g, gCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				defer close(backendConfigCh)

				for {
					select {
					case <-gCtx.Done():
						return nil
					case <-time.After(time.Second):
						backendConfigCh <- pubsub.DataEvent{
							Data: map[string]backendconfig.ConfigT{
								workspaceID: {
									WorkspaceID: workspaceID,
									Sources: []backendconfig.SourceT{
										{
											ID:      sourceID,
											Enabled: true,
											SourceDefinition: backendconfig.SourceDefinitionT{
												Category: "cloud",
											},
											Destinations: []backendconfig.DestinationT{
												{
													ID:      destinationID,
													Enabled: true,
													DestinationDefinition: backendconfig.DestinationDefinitionT{
														Name: warehouseutils.POSTGRES,
													},
													Config: map[string]interface{}{
														"host":             pgResource.Host,
														"database":         pgResource.Database,
														"user":             pgResource.User,
														"password":         pgResource.Password,
														"port":             pgResource.Port,
														"sslMode":          "disable",
														"namespace":        "test_namespace",
														"bucketProvider":   "MINIO",
														"bucketName":       minioResource.BucketName,
														"accessKeyID":      minioResource.AccessKeyID,
														"secretAccessKey":  minioResource.AccessKeySecret,
														"useSSL":           false,
														"endPoint":         minioResource.Endpoint,
														"syncFrequency":    "0",
														"useRudderStorage": false,
														"enableMerge":      false,
													},
													RevisionID: destinationRevisionID,
												},
											},
										},
									},
								},
							},
							Topic: string(backendconfig.TopicBackendConfig),
						}
					}
				}
			})
			g.Go(func() error {
				overrides := []lo.Tuple2[string, interface{}]{
					{A: "Warehouse.postgres.allowMerge", B: false},
				}
				return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh, overrides...)
			})
			g.Go(func() error {
				defer cancel()

				serverURL := fmt.Sprintf("http://localhost:%d", webPort)
				db := sqlmiddleware.New(pgResource.DB)
				events := 100
				jobs := 1

				payload := strings.Join(lo.RepeatBy(events, func(int) string {
					return fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
						uuid.New().String(),
						uuid.New().String(),
					)
				}), "\n")

				health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

				whClient := warehouseclient.NewWarehouse(serverURL)
				stagingFile := warehouseclient.StagingFile{
					WorkspaceID:           workspaceID,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
					TotalEvents:           events,
					FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
					LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
					UseRudderStorage:      false,
					DestinationRevisionID: destinationID,
					Schema: map[string]map[string]interface{}{
						"tracks": {
							"id":          "string",
							"event":       "string",
							"user_id":     "string",
							"received_at": "datetime",
						},
					},
				}

				func() {
					t.Logf("first sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events)
					requireLoadFilesCount(t, db, sourceID, destinationID, events)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				func() {
					t.Logf("second sync")

					err := whClient.Process(ctx, stagingFile)
					require.NoError(t, err)

					requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events*2)
					requireLoadFilesCount(t, db, sourceID, destinationID, events*2)
					requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, events*2)
					requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs*2)
					requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
				}()
				return nil
			})
			require.NoError(t, g.Wait())
		})
	})
	t.Run("id resolution", func(t *testing.T) {
		const (
			workspaceID           = "test_workspace_id"
			sourceID              = "test_source_id"
			destinationID         = "test_destination_id"
			destinationRevisionID = "test_destination_id"
			namespace             = "test_namespace"
		)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := resource.SetupMinio(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		backendConfigCh := make(chan pubsub.DataEvent)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			defer close(backendConfigCh)

			for {
				select {
				case <-gCtx.Done():
					return nil
				case <-time.After(time.Second):
					backendConfigCh <- pubsub.DataEvent{
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
													Name: warehouseutils.POSTGRES,
												},
												Config: map[string]interface{}{
													"host":             pgResource.Host,
													"database":         pgResource.Database,
													"user":             pgResource.User,
													"password":         pgResource.Password,
													"port":             pgResource.Port,
													"sslMode":          "disable",
													"namespace":        "test_namespace",
													"bucketProvider":   "MINIO",
													"bucketName":       minioResource.BucketName,
													"accessKeyID":      minioResource.AccessKeyID,
													"secretAccessKey":  minioResource.AccessKeySecret,
													"useSSL":           false,
													"endPoint":         minioResource.Endpoint,
													"syncFrequency":    "0",
													"useRudderStorage": false,
												},
												RevisionID: destinationRevisionID,
											},
										},
									},
								},
							},
						},
						Topic: string(backendconfig.TopicBackendConfig),
					}
				}
			}
		})
		g.Go(func() error {
			return runWarehouseServer(t, gCtx, webPort, pgResource, backendConfigCh)
		})
		g.Go(func() error {
			defer cancel()

			serverURL := fmt.Sprintf("http://localhost:%d", webPort)
			db := sqlmiddleware.New(pgResource.DB)
			events := 100
			jobs := 1

			mergeRules := lo.RepeatBy(events, func(index int) string {
				return fmt.Sprintf(`{"data":{"merge_property_1_type":"email","merge_property_2_type":"phone","merge_property_1_value":%q,"merge_property_2_value":%q},"metadata":{"columns":{"merge_property_1_type":"string","merge_property_2_type":"string","merge_property_1_value":"string","merge_property_2_value":"string"},"table":"rudder_identity_merge_rules"}}`,
					fmt.Sprintf("demo-%d@rudderstack.com", index+1),
					fmt.Sprintf("rudderstack-%d", index+1),
				)
			})
			emailsMappings := lo.RepeatBy(events, func(index int) string {
				return fmt.Sprintf(`{"data":{"rudder_id":%q,"updated_at":"2023-05-12T04:36:50.199Z","merge_property_type":"email","merge_property_value":%q},"metadata":{"columns":{"rudder_id":"string","updated_at":"datetime","merge_property_type":"string","merge_property_value":"string"},"table":"rudder_identity_mappings"}}`,
					uuid.New().String(),
					fmt.Sprintf("demo-%d@rudderstack.com", index+1),
				)
			})
			phoneMappings := lo.RepeatBy(events, func(index int) string {
				return fmt.Sprintf(`{"data":{"rudder_id":%q,"updated_at":"2023-05-12T04:36:50.199Z","merge_property_type":"phone","merge_property_value":%q},"metadata":{"columns":{"rudder_id":"string","updated_at":"datetime","merge_property_type":"string","merge_property_value":"string"},"table":"rudder_identity_mappings"}}`,
					uuid.New().String(),
					fmt.Sprintf("rudderstack-%d", index+1),
				)
			})

			payloads := make([]string, 0, 3*events)
			for i := 0; i < events; i++ {
				payloads = append(payloads, mergeRules[i], emailsMappings[i], phoneMappings[i])
			}
			payload := strings.Join(payloads, "\n")

			health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

			whClient := warehouseclient.NewWarehouse(serverURL)
			err := whClient.Process(ctx, warehouseclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, gCtx, minioResource, payload).ObjectName,
				TotalEvents:           events * 3,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]interface{}{
					"rudder_identity_mappings": {
						"rudder_id":            "string",
						"updated_at":           "datetime",
						"merge_property_type":  "string",
						"merge_property_value": "string",
					},
					"rudder_identity_merge_rules": {
						"merge_property_1_type":  "string",
						"merge_property_2_type":  "string",
						"merge_property_1_value": "string",
						"merge_property_2_value": "string",
					},
				},
			})
			require.NoError(t, err)

			requireStagingFilesCount(t, db, sourceID, destinationID, warehouseutils.StagingFileSucceededState, events*3)
			requireLoadFilesCount(t, db, sourceID, destinationID, events*3)
			requireTableUploadsCount(t, db, sourceID, destinationID, namespace, model.Waiting, events*3) // not supported for postgres yet
			requireUploadsCount(t, db, sourceID, destinationID, namespace, model.ExportedData, jobs)
			return nil
		})
		require.NoError(t, g.Wait())
	})
	t.Run("source job", func(t *testing.T) {})
}

func runWarehouseServer(
	t testing.TB,
	ctx context.Context,
	webPort int,
	pgResource *resource.PostgresResource,
	bcConfigCh chan pubsub.DataEvent,
	overrideConfigs ...lo.Tuple2[string, interface{}],
) error {
	t.Helper()

	mockCtrl := gomock.NewController(t)

	mockApp := mocksApp.NewMockApp(mockCtrl)
	mockApp.EXPECT().Features().Return(&app.Features{Reporting: &reporting.Factory{}}).AnyTimes()

	ap := app.New(&app.Options{
		EnterpriseToken: "some-token",
	})
	ap.Setup()

	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).DoAndReturn(func(ctx context.Context) error { return nil }).AnyTimes()
	mockBackendConfig.EXPECT().Identity().Return(&identity.NOOP{})
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
		return bcConfigCh
	}).AnyTimes()

	conf := config.New()
	conf.Set("WAREHOUSE_JOBS_DB_HOST", pgResource.Host)
	conf.Set("WAREHOUSE_JOBS_DB_PORT", pgResource.Port)
	conf.Set("WAREHOUSE_JOBS_DB_USER", pgResource.User)
	conf.Set("WAREHOUSE_JOBS_DB_PASSWORD", pgResource.Password)
	conf.Set("WAREHOUSE_JOBS_DB_DB_NAME", pgResource.Database)
	conf.Set("Warehouse.mode", config.MasterSlaveMode)
	conf.Set("Warehouse.runningMode", "")
	conf.Set("Warehouse.webPort", webPort)
	conf.Set("Warehouse.jobs.maxAttemptsPerJob", 3)
	conf.Set("Warehouse.archiveUploadRelatedRecords", false)
	conf.Set("Warehouse.waitForWorkerSleep", "1s")
	conf.Set("Warehouse.uploadAllocatorSleep", "1s")
	conf.Set("Warehouse.uploadStatusTrackFrequency", "1s")
	conf.Set("Warehouse.mainLoopSleep", "1s")
	conf.Set("Warehouse.jobs.processingSleepInterval", "1s")
	conf.Set("PgNotifier.maxPollSleep", "1s")
	conf.Set("PgNotifier.trackBatchIntervalInS", "1s")
	conf.Set("PgNotifier.maxAttempt", 1)
	for _, overrideConfig := range overrideConfigs {
		conf.Set(overrideConfig.A, overrideConfig.B)
	}

	a := warehouse.New(ap, conf, logger.NOP, memstats.New(), mockBackendConfig, filemanager.New)
	if err := a.Setup(ctx); err != nil {
		return fmt.Errorf("setting up warehouse: %w", err)
	}
	return a.Run(ctx)
}

func prepareStagingFile(
	t testing.TB,
	ctx context.Context,
	minioResource *resource.MinioResource,
	payload string,
) filemanager.UploadedFile {
	t.Helper()

	filePath := path.Join(t.TempDir(), "staging.json")

	gz, err := misc.CreateGZ(filePath)
	require.NoError(t, err)

	require.NoError(t, gz.WriteGZ(payload))
	require.NoError(t, gz.Close())

	f, err := os.Open(filePath)
	require.NoError(t, err)

	minioConfig := map[string]any{
		"bucketName":      minioResource.BucketName,
		"accessKeyID":     minioResource.AccessKeyID,
		"secretAccessKey": minioResource.AccessKeySecret,
		"endPoint":        minioResource.Endpoint,
	}

	fm, err := filemanager.NewMinioManager(minioConfig, logger.NewLogger(), func() time.Duration {
		return time.Minute
	})
	require.NoError(t, err)

	uploadFile, err := fm.Upload(ctx, f)
	require.NoError(t, err)

	return uploadFile
}

// nolint:unparam
func requireStagingFilesCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	sourceID, destinationID, status string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow(`
			SELECT
			  COALESCE(
				sum(total_events),
				0
			  )
			FROM
			  wh_staging_files
			WHERE
			  source_id = $1
			  AND destination_id = $2
			  AND status = $3;
`,
			sourceID,
			destinationID,
			status,
		).Scan(&eventsCount))
		t.Logf("Staging files count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		120*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s staging files count to be %d", status, expectedCount),
	)
}

// nolint:unparam
func requireLoadFilesCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	sourceID, destinationID string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow(`
			SELECT
			  COALESCE(
				sum(total_events),
				0
			  )
			FROM
			  wh_load_files
			WHERE
			  source_id = $1
			  AND destination_id = $2;
`,
			sourceID,
			destinationID,
		).Scan(&eventsCount))
		t.Logf("Load files count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		120*time.Second,
		1*time.Second,
		fmt.Sprintf("expected load files count to be %d", expectedCount),
	)
}

// nolint:unparam
func requireTableUploadsCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	sourceID, destinationID, namespace, status string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow(`
			SELECT
			  COALESCE(
				sum(total_events),
				0
			  )
			FROM
			  wh_table_uploads
			WHERE
			  status = $1
			  AND wh_upload_id IN (
				SELECT
				  id
				FROM
				  wh_uploads
				WHERE
				  wh_uploads.source_id = $2
				  AND wh_uploads.destination_id = $3
				  AND wh_uploads.namespace = $4
			  );
`,
			status,
			sourceID,
			destinationID,
			namespace,
		).Scan(&eventsCount))
		t.Logf("Table uploads count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		120*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s table uploads count to be %d", status, expectedCount),
	)
}

// nolint:unparam
func requireUploadsCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	sourceID, destinationID, namespace, status string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRow(`
			SELECT
			  count(*)
			FROM
			  wh_uploads
			WHERE
			  source_id = $1
			  AND destination_id = $2
			  AND namespace = $3
			  AND status = $4;
`,
			sourceID,
			destinationID,
			namespace,
			status,
		).Scan(&jobsCount))
		t.Logf("uploads count: %d", jobsCount)
		return jobsCount == expectedCount
	},
		120*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s uploads count to be %d", status, expectedCount),
	)
}

func requireArchivedUploadsCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	sourceID, destinationID, namespace, status string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRow(`
			SELECT
			  count(*)
			FROM
			  wh_uploads
			WHERE
			  source_id = $1
			  AND destination_id = $2
			  AND namespace = $3
			  AND status = $4
			  AND metadata ->> 'archivedStagingAndLoadFiles' = 'true';
`,
			sourceID,
			destinationID,
			namespace,
			status,
		).Scan(&jobsCount))
		t.Logf("uploads count: %d", jobsCount)
		return jobsCount == expectedCount
	},
		120*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s uploads count to be %d", status, expectedCount),
	)
}

func requireRetriedUploadsCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	sourceID, destinationID, namespace, status string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var jobsCount sql.NullInt64
		require.NoError(t, db.QueryRow(`
			SELECT
			  SUM(
				CAST(value ->> 'attempt' AS INT)
			  ) AS total_attempts
			FROM
			  wh_uploads,
			  jsonb_each(error)
			WHERE
			  source_id = $1
			  AND destination_id = $2
			  AND namespace = $3
			  AND status = $4;
`,
			sourceID,
			destinationID,
			namespace,
			status,
		).Scan(&jobsCount))
		t.Logf("uploads count: %d", jobsCount.Int64)
		return jobsCount.Int64 == int64(expectedCount)
	},
		120*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s uploads count to be %d", status, expectedCount),
	)
}

func requireWarehouseEventsCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	tableName string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s;`, tableName)).Scan(&eventsCount))
		t.Logf("warehouse events count for table %s: %d", tableName, eventsCount)
		return eventsCount == expectedCount
	},
		10*time.Second,
		1*time.Second,
		fmt.Sprintf("expected warehouse events count for table %s to be %d", tableName, expectedCount),
	)
}

func requireReportsCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	sourceID, destinationID string,
	status string, statusCode int,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var reportsCount sql.NullInt64
		require.NoError(t, db.DB.QueryRow(`
			SELECT
			  sum(count)
			FROM
			  reports
			WHERE
			  source_id = $1
			  and destination_id = $2
			  AND status = $3
			  AND status_code = $4
			  AND in_pu = 'batch_router'
			  AND pu = 'warehouse'
			  AND initial_state = FALSE
			  AND terminal_state = TRUE;
`,
			sourceID,
			destinationID,
			status,
			statusCode,
		).Scan(&reportsCount))
		t.Logf("reports count: %d", reportsCount.Int64)
		return reportsCount.Int64 == int64(expectedCount)
	}, 10*time.Second, 1*time.Second, fmt.Sprintf("expected %s reports count to be %d", status, expectedCount))
}
