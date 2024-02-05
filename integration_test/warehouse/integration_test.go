package warehouse_test

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
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/warehouse"
	whclient "github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/tunnelling"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

const (
	succeeded    = "succeeded"
	waiting      = "waiting"
	aborted      = "aborted"
	exportedData = "exported_data"

	workspaceID           = "test_workspace_id"
	sourceID              = "test_source_id"
	destinationID         = "test_destination_id"
	destinationRevisionID = "test_destination_revision_id"
	namespace             = "test_namespace"
)

func TestMain(m *testing.M) {
	admin.Init()
	validations.Init()
	os.Exit(m.Run())
}

func TestUploads(t *testing.T) {
	t.Run("tracks loading", func(t *testing.T) {
		db, minioResource, whClient := setupServer(t, false, nil, nil)

		ctx := context.Background()
		events := 100
		jobs := 1

		eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
			return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
				uuid.New().String(),
				uuid.New().String(),
			)
		}), "\n")

		require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
			WorkspaceID:           workspaceID,
			SourceID:              sourceID,
			DestinationID:         destinationID,
			Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
			TotalEvents:           events,
			FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
			LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
			UseRudderStorage:      false,
			DestinationRevisionID: destinationID,
			Schema: map[string]map[string]any{
				"tracks": {
					"id":          "string",
					"user_id":     "string",
					"received_at": "datetime",
				},
			},
		}))
		requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "status", B: succeeded},
		}...)
		requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
		}...)
		requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "status", B: exportedData},
			{A: "wh_uploads.source_id", B: sourceID},
			{A: "wh_uploads.destination_id", B: destinationID},
			{A: "wh_uploads.namespace", B: namespace},
		}...)
		requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "namespace", B: namespace},
			{A: "status", B: exportedData},
		}...)
		requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
	})
	t.Run("user and identifies loading", func(t *testing.T) {
		db, minioResource, whClient := setupServer(t, false, nil, nil)

		ctx := context.Background()
		events := 100
		jobs := 1

		userEvents := lo.RepeatBy(events, func(int) string {
			return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "identifies"}}`,
				uuid.New().String(),
				uuid.New().String(),
			)
		})
		identifyEvents := lo.RepeatBy(events, func(int) string {
			return fmt.Sprintf(`{"data":{"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"user_id":"string","received_at":"datetime"}, "table": "users"}}`,
				uuid.New().String(),
			)
		})
		eventsPayload := strings.Join(append(append([]string{}, userEvents...), identifyEvents...), "\n")

		require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
			WorkspaceID:           workspaceID,
			SourceID:              sourceID,
			DestinationID:         destinationID,
			Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
			TotalEvents:           events,
			FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
			LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
			UseRudderStorage:      false,
			DestinationRevisionID: destinationID,
			Schema: map[string]map[string]any{
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
		}))
		requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "status", B: succeeded},
		}...)
		requireLoadFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
		}...)
		requireTableUploadEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
			{A: "status", B: exportedData},
			{A: "wh_uploads.source_id", B: sourceID},
			{A: "wh_uploads.destination_id", B: destinationID},
			{A: "wh_uploads.namespace", B: namespace},
		}...)
		requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "namespace", B: namespace},
			{A: "status", B: exportedData},
		}...)
		requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "users"), events)
		requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "identifies"), events)
	})
	t.Run("schema change", func(t *testing.T) {
		t.Run("add columns", func(t *testing.T) {
			db, minioResource, whClient := setupServer(t, false, nil, nil)

			ctx := context.Background()
			events := 100
			jobs := 1

			t.Log("first sync")
			eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")
			require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]any{
					"tracks": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			}))
			requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)

			t.Log("second sync with new properties")
			eventsPayload = strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z","new_property_string":%q,"new_property_int":%d},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime","new_property_string":"string","new_property_int":"int"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
					uuid.New().String(),
					rand.Intn(1000),
				)
			}), "\n")
			require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]any{
					"tracks": {
						"id":                  "string",
						"user_id":             "string",
						"received_at":         "datetime",
						"new_property_string": "string",
						"new_property_int":    "int",
					},
				},
			}))
			requireStagingFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events*2)
		})
	})
	t.Run("destination revision", func(t *testing.T) {
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		pgResource, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		minioResource, err := minio.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		bcConfig := defaultBackendConfig(pgResource, minioResource, false)

		hasRevisionEndpointBeenCalled := atomic.NewBool(false)
		cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/workspaces/destinationHistory/test_destination_revision_id":
				defer func() {
					hasRevisionEndpointBeenCalled.Store(true)
				}()

				require.Equal(t, http.MethodGet, r.Method)
				body, err := json.Marshal(backendconfig.DestinationT{
					ID:      destinationID,
					Enabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: whutils.POSTGRES,
					},
					Config: map[string]any{
						"host":             pgResource.Host,
						"database":         pgResource.Database,
						"user":             pgResource.User,
						"password":         pgResource.Password,
						"port":             pgResource.Port,
						"sslMode":          "disable",
						"namespace":        namespace,
						"bucketProvider":   whutils.MINIO,
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
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer cp.Close()

		done := make(chan struct{})
		go func() {
			defer close(done)
			require.NoError(t, runWarehouseServer(t, ctx, webPort, pgResource, bcConfig, []lo.Tuple2[string, any]{
				{A: "CONFIG_BACKEND_URL", B: cp.URL},
			}...))
		}()
		t.Cleanup(func() {
			cancel()
			<-done
		})

		serverURL := fmt.Sprintf("http://localhost:%d", webPort)
		db := sqlmw.New(pgResource.DB)
		events := 100
		jobs := 1

		eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
			return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
				uuid.New().String(),
				uuid.New().String(),
			)
		}), "\n")

		health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

		require.NoError(t, whclient.NewWarehouse(serverURL).Process(ctx, whclient.StagingFile{
			WorkspaceID:           workspaceID,
			SourceID:              sourceID,
			DestinationID:         destinationID,
			Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
			TotalEvents:           events,
			FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
			LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
			UseRudderStorage:      false,
			DestinationRevisionID: destinationRevisionID,
			Schema: map[string]map[string]any{
				"tracks": {
					"id":          "string",
					"user_id":     "string",
					"received_at": "datetime",
				},
			},
		}))
		requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "status", B: succeeded},
		}...)
		requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
		}...)
		requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "status", B: exportedData},
			{A: "wh_uploads.source_id", B: sourceID},
			{A: "wh_uploads.destination_id", B: destinationID},
			{A: "wh_uploads.namespace", B: namespace},
		}...)
		requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "namespace", B: namespace},
			{A: "status", B: exportedData},
		}...)
		requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
		require.True(t, hasRevisionEndpointBeenCalled.Load())
	})
	t.Run("tunnelling", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.ssh-server.yml"}))
		c.Start(context.Background())

		tunnelledHost := "db-private-postgres"
		tunnelledDatabase := "postgres"
		tunnelledPassword := "postgres"
		tunnelledUser := "postgres"
		tunnelledPort := "5432"
		tunnelledSSHUser := "rudderstack"
		tunnelledSSHHost := "localhost"
		tunnelledPrivateKey := "-----BEGIN OPENSSH PRIVATE KEY-----\\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABlwAAAAdzc2gtcn\\nNhAAAAAwEAAQAAAYEA0f/mqkkZ3c9qw8MTz5FoEO3PGecO/dtUFfJ4g1UBu9E7hi/pyVYY\\nfLfdsd5bqA2pXdU0ROymyVe683I1VzJcihUtwB1eQxP1mUhmoo0ixK0IUUGm4PRieCGv+r\\n0/gMvaYbVGUPCi5tAUVh02vZB7p2cTIaz872lvCnRhYbhGUHSbhNSSQOjnCtZfjuZZnE0l\\nPKjWV/wbJ7Pvoc/FZMlWOqL1AjAKuwFH5zs1RMrPDDv5PCZksq4a7DDxziEdq39jvA3sOm\\npQXvzBBBLBOzu7rM3/MPJb6dvAGJcYxkptfL4YXTscIMINr0g24cn+Thvt9yqA93rkb9RB\\nkw6RIEwMlQKqserA+pfsaoW0SkvnlDKzS1DLwXioL4Uc1Jpr/9jTMEfR+W7v7gJPB1JDnV\\ngen5FBfiMqbsG1amUS+mjgNfC8I00tR+CUHxpqUWANtcWTinhSnLJ2skj/2QnciPHkHurR\\nEKyEwCVecgn+xVKyRgVDCGsJ+QnAdn51+i/kO3nvAAAFqENNbN9DTWzfAAAAB3NzaC1yc2\\nEAAAGBANH/5qpJGd3PasPDE8+RaBDtzxnnDv3bVBXyeINVAbvRO4Yv6clWGHy33bHeW6gN\\nqV3VNETspslXuvNyNVcyXIoVLcAdXkMT9ZlIZqKNIsStCFFBpuD0Ynghr/q9P4DL2mG1Rl\\nDwoubQFFYdNr2Qe6dnEyGs/O9pbwp0YWG4RlB0m4TUkkDo5wrWX47mWZxNJTyo1lf8Gyez\\n76HPxWTJVjqi9QIwCrsBR+c7NUTKzww7+TwmZLKuGuww8c4hHat/Y7wN7DpqUF78wQQSwT\\ns7u6zN/zDyW+nbwBiXGMZKbXy+GF07HCDCDa9INuHJ/k4b7fcqgPd65G/UQZMOkSBMDJUC\\nqrHqwPqX7GqFtEpL55Qys0tQy8F4qC+FHNSaa//Y0zBH0flu7+4CTwdSQ51YHp+RQX4jKm\\n7BtWplEvpo4DXwvCNNLUfglB8aalFgDbXFk4p4UpyydrJI/9kJ3Ijx5B7q0RCshMAlXnIJ\\n/sVSskYFQwhrCfkJwHZ+dfov5Dt57wAAAAMBAAEAAAGAd9pxr+ag2LO0353LBMCcgGz5sn\\nLpX4F6cDw/A9XUc3lrW56k88AroaLe6NFbxoJlk6RHfL8EQg3MKX2Za/bWUgjcX7VjQy11\\nEtL7oPKkUVPgV1/8+o8AVEgFxDmWsM+oB/QJ+dAdaVaBBNUPlQmNSXHOvX2ZrpqiQXlCyx\\n79IpYq3JjmEB3dH5ZSW6CkrExrYD+MdhLw/Kv5rISEyI0Qpc6zv1fkB+8nNpXYRTbrDLR9\\n/xJ6jnBH9V3J5DeKU4MUQ39nrAp6iviyWydB973+MOygpy41fXO6hHyVZ2aSCysn1t6J/K\\nQdeEjqAOI/5CbdtiFGp06et799EFyzPItW0FKetW1UTOL2YHqdb+Q9sNjiNlUSzgxMbJWJ\\nRGO6g9B1mJsHl5mJZUiHQPsG/wgBER8VOP4bLOEB6gzVO2GE9HTJTOh5C+eEfrl52wPfXj\\nTqjtWAnhssxtgmWjkS0ibi+u1KMVXKHfaiqJ7nH0jMx+eu1RpMvuR8JqkU8qdMMGChAAAA\\nwHkQMfpCnjNAo6sllEB5FwjEdTBBOt7gu6nLQ2O3uGv0KNEEZ/BWJLQ5fKOfBtDHO+kl+5\\nQoxc0cE7cg64CyBF3+VjzrEzuX5Tuh4NwrsjT4vTTHhCIbIynxEPmKzvIyCMuglqd/nhu9\\n6CXhghuTg8NrC7lY+cImiBfhxE32zqNITlpHW7exr95Gz1sML2TRJqxDN93oUFfrEuInx8\\nHpXXnvMQxPRhcp9nDMU9/ahUamMabQqVVMwKDi8n3sPPzTiAAAAMEA+/hm3X/yNotAtMAH\\ny11parKQwPgEF4HYkSE0bEe+2MPJmEk4M4PGmmt/MQC5N5dXdUGxiQeVMR+Sw0kN9qZjM6\\nSIz0YHQFMsxVmUMKFpAh4UI0GlsW49jSpVXs34Fg95AfhZOYZmOcGcYosp0huCeRlpLeIH\\n7Vv2bkfQaic3uNaVPg7+cXg7zdY6tZlzwa/4Fj0udfTjGQJOPSzIihdMLHnV81rZ2cUOZq\\nMSk6b02aMpVB4TV0l1w4j2mlF2eGD9AAAAwQDVW6p2VXKuPR7SgGGQgHXpAQCFZPGLYd8K\\nduRaCbxKJXzUnZBn53OX5fuLlFhmRmAMXE6ztHPN1/5JjwILn+O49qel1uUvzU8TaWioq7\\nAre3SJR2ZucR4AKUvzUHGP3GWW96xPN8lq+rgb0th1eOSU2aVkaIdeTJhV1iPfaUUf+15S\\nYcJlSHLGgeqkok+VfuudZ73f3RFFhjoe1oAjlPB4leeMsBD9UBLx2U3xAevnfkecF4Lm83\\n4sVswWATSFAFsAAAAsYWJoaW1hbnl1YmFiYmFyQEFiaGltYW55dXMtTWFjQm9vay1Qcm8u\\nbG9jYWwBAgMEBQYH\\n-----END OPENSSH PRIVATE KEY-----"
		sshPort := c.Port("ssh-server", 2222)

		db, minioResource, whClient := setupServer(t, false,
			func(m map[string]backendconfig.ConfigT, minioResource *minio.Resource) {
				m[workspaceID] = backendconfig.ConfigT{
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
									Config: map[string]any{
										"host":             tunnelledHost,
										"database":         tunnelledDatabase,
										"user":             tunnelledUser,
										"password":         tunnelledPassword,
										"port":             tunnelledPort,
										"sslMode":          "disable",
										"namespace":        namespace,
										"bucketProvider":   whutils.MINIO,
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
									RevisionID: destinationID,
								},
							},
						},
					},
				}
			},
			nil,
		)

		ctx := context.Background()
		events := 100
		jobs := 1

		eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
			return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
				uuid.New().String(),
				uuid.New().String(),
			)
		}), "\n")

		require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
			WorkspaceID:           workspaceID,
			SourceID:              sourceID,
			DestinationID:         destinationID,
			Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
			TotalEvents:           events,
			FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
			LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
			UseRudderStorage:      false,
			DestinationRevisionID: destinationID,
			Schema: map[string]map[string]any{
				"tracks": {
					"id":          "string",
					"user_id":     "string",
					"received_at": "datetime",
				},
			},
		}))
		requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "status", B: succeeded},
		}...)
		requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
		}...)
		requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "status", B: exportedData},
			{A: "wh_uploads.source_id", B: sourceID},
			{A: "wh_uploads.destination_id", B: destinationID},
			{A: "wh_uploads.namespace", B: namespace},
		}...)
		requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "namespace", B: namespace},
			{A: "status", B: exportedData},
		}...)

		tunnelDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			tunnelledUser,
			tunnelledPassword,
			tunnelledHost,
			tunnelledPort,
			tunnelledDatabase,
		)
		tunnelInfo := &tunnelling.TunnelInfo{
			Config: map[string]any{
				"sshUser":       tunnelledSSHUser,
				"sshPort":       strconv.Itoa(sshPort),
				"sshHost":       tunnelledSSHHost,
				"sshPrivateKey": strings.ReplaceAll(tunnelledPrivateKey, "\\n", "\n"),
			},
		}

		tunnelDB, err := tunnelling.Connect(tunnelDSN, tunnelInfo.Config)
		require.NoError(t, err)
		require.NoError(t, tunnelDB.Ping())
		requireDownstreamEventsCount(t, ctx, sqlmw.New(tunnelDB), fmt.Sprintf("%s.%s", namespace, "tracks"), events)
	})
	t.Run("reports", func(t *testing.T) {
		t.Run("succeeded", func(t *testing.T) {
			db, minioResource, whClient := setupServer(t, false, nil, nil)

			ctx := context.Background()
			events := 100
			jobs := 1

			eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]any{
					"tracks": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			}))
			requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
			requireReportsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: jobsdb.Succeeded.State},
				{A: "status_code", B: 200},
				{A: "in_pu", B: "batch_router"},
				{A: "pu", B: "warehouse"},
				{A: "initial_state", B: false},
				{A: "terminal_state", B: true},
			}...)
		})
		t.Run("aborted", func(t *testing.T) {
			db, minioResource, whClient := setupServer(t, false,
				func(m map[string]backendconfig.ConfigT, _ *minio.Resource) {
					m[workspaceID].Sources[0].Destinations[0].Config["port"] = "5432"
				},
				func(_ *minio.Resource) []lo.Tuple2[string, any] {
					return []lo.Tuple2[string, any]{
						{A: "Warehouse.minRetryAttempts", B: 2},
						{A: "Warehouse.retryTimeWindow", B: "0s"},
						{A: "Warehouse.minUploadBackoff", B: "0s"},
						{A: "Warehouse.maxUploadBackoff", B: "0s"},
					}
				},
			)

			ctx := context.Background()
			events := 100
			jobs := 1

			eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]any{
					"tracks": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			}))
			requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: aborted},
			}...)
			requireReportsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: jobsdb.Failed.State},
				{A: "status_code", B: 400},
				{A: "in_pu", B: "batch_router"},
				{A: "pu", B: "warehouse"},
				{A: "initial_state", B: false},
				{A: "terminal_state", B: true},
			}...)
			requireReportsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: jobsdb.Aborted.State},
				{A: "status_code", B: 400},
				{A: "in_pu", B: "batch_router"},
				{A: "pu", B: "warehouse"},
				{A: "initial_state", B: false},
				{A: "terminal_state", B: true},
			}...)
		})
	})
	t.Run("retries then aborts", func(t *testing.T) {
		db, minioResource, whClient := setupServer(t, false,
			func(m map[string]backendconfig.ConfigT, _ *minio.Resource) {
				m[workspaceID].Sources[0].Destinations[0].Config["port"] = "5432"
			},
			func(_ *minio.Resource) []lo.Tuple2[string, any] {
				return []lo.Tuple2[string, any]{
					{A: "Warehouse.minRetryAttempts", B: 2},
					{A: "Warehouse.retryTimeWindow", B: "0s"},
					{A: "Warehouse.minUploadBackoff", B: "0s"},
					{A: "Warehouse.maxUploadBackoff", B: "0s"},
				}
			},
		)

		ctx := context.Background()
		events := 100
		jobs := 1

		eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
			return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
				uuid.New().String(),
				uuid.New().String(),
			)
		}), "\n")

		require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
			WorkspaceID:           workspaceID,
			SourceID:              sourceID,
			DestinationID:         destinationID,
			Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
			TotalEvents:           events,
			FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
			LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
			UseRudderStorage:      false,
			DestinationRevisionID: destinationID,
			Schema: map[string]map[string]any{
				"tracks": {
					"id":          "string",
					"user_id":     "string",
					"received_at": "datetime",
				},
			},
		}))
		requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "namespace", B: namespace},
			{A: "status", B: aborted},
		}...)
		requireRetriedUploadJobsCount(t, ctx, db, 3, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "namespace", B: namespace},
			{A: "status", B: aborted},
		}...)
	})
	t.Run("discards", func(t *testing.T) {
		db, minioResource, whClient := setupServer(t, false, nil, nil)

		ctx := context.Background()
		events := 100
		jobs := 1

		goodEvents := lo.RepeatBy(events/2, func(int) string {
			return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
				uuid.New().String(),
				uuid.New().String(),
			)
		})
		badEvents := lo.RepeatBy(events/2, func(int) string {
			return fmt.Sprintf(`{"data":{"id":%q,"user_id":%d,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"int","received_at":"datetime"}, "table": "tracks"}}`,
				uuid.New().String(),
				rand.Intn(1000),
			)
		})
		eventsPayload := strings.Join(append(append([]string{}, goodEvents...), badEvents...), "\n")

		require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
			WorkspaceID:           workspaceID,
			SourceID:              sourceID,
			DestinationID:         destinationID,
			Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
			TotalEvents:           events,
			FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
			LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
			UseRudderStorage:      false,
			DestinationRevisionID: destinationID,
			Schema: map[string]map[string]any{
				"tracks": {
					"id":          "string",
					"user_id":     "int",
					"received_at": "datetime",
				},
			},
		}))
		requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "status", B: succeeded},
		}...)
		requireLoadFileEventsCount(t, ctx, db, events+(events/2), []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
		}...)
		requireTableUploadEventsCount(t, ctx, db, events+(events/2), []lo.Tuple2[string, any]{
			{A: "status", B: exportedData},
			{A: "wh_uploads.source_id", B: sourceID},
			{A: "wh_uploads.destination_id", B: destinationID},
			{A: "wh_uploads.namespace", B: namespace},
		}...)
		requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "namespace", B: namespace},
			{A: "status", B: exportedData},
		}...)
		requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
		requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "rudder_discards"), events/2)
	})
	t.Run("archiver", func(t *testing.T) {
		db, minioResource, whClient := setupServer(t, false, nil,
			func(minioResource *minio.Resource) []lo.Tuple2[string, any] {
				return []lo.Tuple2[string, any]{
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
			},
		)

		ctx := context.Background()
		events := 100
		jobs := 1

		eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
			return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
				uuid.New().String(),
				uuid.New().String(),
			)
		}), "\n")

		require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
			WorkspaceID:           workspaceID,
			SourceID:              sourceID,
			DestinationID:         destinationID,
			Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
			TotalEvents:           events,
			FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
			LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
			UseRudderStorage:      false,
			DestinationRevisionID: destinationID,
			Schema: map[string]map[string]any{
				"tracks": {
					"id":          "string",
					"user_id":     "string",
					"received_at": "datetime",
				},
			},
		}))
		requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
			{A: "status", B: exportedData},
			{A: "wh_uploads.source_id", B: sourceID},
			{A: "wh_uploads.destination_id", B: destinationID},
			{A: "wh_uploads.namespace", B: namespace},
		}...)
		requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "namespace", B: namespace},
			{A: "status", B: exportedData},
		}...)
		requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "namespace", B: namespace},
			{A: "status", B: exportedData},
			{A: "metadata->>'archivedStagingAndLoadFiles'", B: "true"},
		}...)
	})
	t.Run("sync behaviour", func(t *testing.T) {
		t.Run("default behaviour", func(t *testing.T) {
			db, minioResource, whClient := setupServer(t, false, nil, nil)

			ctx := context.Background()
			events := 100
			jobs := 1

			eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			stagingFile := whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]any{
					"tracks": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			}

			t.Logf("first sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)

			t.Logf("second sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)
		})
		t.Run("allowMerge=false,preferAppend=false", func(t *testing.T) {
			db, minioResource, whClient := setupServer(t, false, nil,
				func(_ *minio.Resource) []lo.Tuple2[string, any] {
					return []lo.Tuple2[string, any]{
						{A: "Warehouse.postgres.allowMerge", B: false},
					}
				},
			)

			ctx := context.Background()
			events := 100
			jobs := 1

			payload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			stagingFile := whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, payload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]any{
					"tracks": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			}

			t.Logf("first sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)

			t.Logf("second sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events*2)
		})
		t.Run("allowMerge=true,preferAppend=true", func(t *testing.T) {
			db, minioResource, whClient := setupServer(t, true, nil,
				func(_ *minio.Resource) []lo.Tuple2[string, any] {
					return []lo.Tuple2[string, any]{
						{A: "Warehouse.postgres.allowMerge", B: true},
					}
				},
			)

			ctx := context.Background()
			events := 100
			jobs := 1

			payload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			stagingFile := whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, payload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]any{
					"tracks": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			}

			t.Logf("first sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)

			t.Logf("second sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events*2)
		})
		t.Run("allowMerge=false,preferAppend=true", func(t *testing.T) {
			db, minioResource, whClient := setupServer(t, true, nil,
				func(_ *minio.Resource) []lo.Tuple2[string, any] {
					return []lo.Tuple2[string, any]{
						{A: "Warehouse.postgres.allowMerge", B: false},
					}
				},
			)

			ctx := context.Background()
			events := 100
			jobs := 1

			eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			stagingFile := whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]any{
					"tracks": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			}

			t.Logf("first sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)

			t.Logf("second sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events*2)
		})
		t.Run("allowMerge=false,preferAppend=true,isSourceETL=true", func(t *testing.T) {
			db, minioResource, whClient := setupServer(t, true, nil,
				func(_ *minio.Resource) []lo.Tuple2[string, any] {
					return []lo.Tuple2[string, any]{
						{A: "Warehouse.postgres.allowMerge", B: false},
					}
				},
			)

			ctx := context.Background()
			events := 100
			jobs := 1

			eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			stagingFile := whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				SourceJobID:           uuid.NewString(),
				SourceJobRunID:        uuid.NewString(),
				SourceTaskRunID:       uuid.NewString(),
				Schema: map[string]map[string]any{
					"tracks": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			}

			t.Logf("first sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)

			t.Logf("second sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events*2)
		})
		t.Run("allowMerge=false,preferAppend=true,IsReplaySource=true", func(t *testing.T) {
			db, minioResource, whClient := setupServer(t, true,
				func(m map[string]backendconfig.ConfigT, _ *minio.Resource) {
					m[workspaceID].Sources[0].OriginalID = sourceID
				},
				func(_ *minio.Resource) []lo.Tuple2[string, any] {
					return []lo.Tuple2[string, any]{
						{A: "Warehouse.postgres.allowMerge", B: false},
					}
				},
			)

			ctx := context.Background()
			events := 100
			jobs := 1

			eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			stagingFile := whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]any{
					"tracks": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			}

			t.Logf("first sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)

			t.Logf("second sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events*2)
		})
		t.Run("allowMerge=false,preferAppend=true,sourceCategory=cloud", func(t *testing.T) {
			db, minioResource, whClient := setupServer(t, true,
				func(m map[string]backendconfig.ConfigT, _ *minio.Resource) {
					m[workspaceID].Sources[0].SourceDefinition.Category = "cloud"
				},
				func(_ *minio.Resource) []lo.Tuple2[string, any] {
					return []lo.Tuple2[string, any]{
						{A: "Warehouse.postgres.allowMerge", B: false},
					}
				},
			)

			ctx := context.Background()
			events := 100
			jobs := 1

			eventsPayload := strings.Join(lo.RepeatBy(events, func(int) string {
				return fmt.Sprintf(`{"data":{"id":%q,"user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
					uuid.New().String(),
					uuid.New().String(),
				)
			}), "\n")

			stagingFile := whclient.StagingFile{
				WorkspaceID:           workspaceID,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
				TotalEvents:           events,
				FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
				LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
				UseRudderStorage:      false,
				DestinationRevisionID: destinationID,
				Schema: map[string]map[string]any{
					"tracks": {
						"id":          "string",
						"user_id":     "string",
						"received_at": "datetime",
					},
				},
			}

			t.Logf("first sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events)

			t.Logf("second sync")
			require.NoError(t, whClient.Process(ctx, stagingFile))
			requireStagingFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "status", B: succeeded},
			}...)
			requireLoadFileEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
			}...)
			requireTableUploadEventsCount(t, ctx, db, events*2, []lo.Tuple2[string, any]{
				{A: "status", B: exportedData},
				{A: "wh_uploads.source_id", B: sourceID},
				{A: "wh_uploads.destination_id", B: destinationID},
				{A: "wh_uploads.namespace", B: namespace},
			}...)
			requireUploadJobsCount(t, ctx, db, jobs*2, []lo.Tuple2[string, any]{
				{A: "source_id", B: sourceID},
				{A: "destination_id", B: destinationID},
				{A: "namespace", B: namespace},
				{A: "status", B: exportedData},
			}...)
			requireDownstreamEventsCount(t, ctx, db, fmt.Sprintf("%s.%s", namespace, "tracks"), events*2)
		})
	})
	t.Run("id resolution", func(t *testing.T) {
		db, minioResource, whClient := setupServer(t, false, nil, nil)

		ctx := context.Background()
		events := 100
		jobs := 1

		mergeRuleEvents := lo.RepeatBy(events, func(index int) string {
			return fmt.Sprintf(`{"data":{"merge_property_1_type":"email","merge_property_2_type":"phone","merge_property_1_value":%q,"merge_property_2_value":%q},"metadata":{"columns":{"merge_property_1_type":"string","merge_property_2_type":"string","merge_property_1_value":"string","merge_property_2_value":"string"},"table":"rudder_identity_merge_rules"}}`,
				fmt.Sprintf("demo-%d@rudderstack.com", index+1),
				fmt.Sprintf("rudderstack-%d", index+1),
			)
		})
		emailMappingEvents := lo.RepeatBy(events, func(index int) string {
			return fmt.Sprintf(`{"data":{"rudder_id":%q,"updated_at":"2023-05-12T04:36:50.199Z","merge_property_type":"email","merge_property_value":%q},"metadata":{"columns":{"rudder_id":"string","updated_at":"datetime","merge_property_type":"string","merge_property_value":"string"},"table":"rudder_identity_mappings"}}`,
				uuid.New().String(),
				fmt.Sprintf("demo-%d@rudderstack.com", index+1),
			)
		})
		phoneMappingEvents := lo.RepeatBy(events, func(index int) string {
			return fmt.Sprintf(`{"data":{"rudder_id":%q,"updated_at":"2023-05-12T04:36:50.199Z","merge_property_type":"phone","merge_property_value":%q},"metadata":{"columns":{"rudder_id":"string","updated_at":"datetime","merge_property_type":"string","merge_property_value":"string"},"table":"rudder_identity_mappings"}}`,
				uuid.New().String(),
				fmt.Sprintf("rudderstack-%d", index+1),
			)
		})

		eventsPayload := strings.Join(append(append(append([]string{}, mergeRuleEvents...), emailMappingEvents...), phoneMappingEvents...), "\n")

		require.NoError(t, whClient.Process(ctx, whclient.StagingFile{
			WorkspaceID:           workspaceID,
			SourceID:              sourceID,
			DestinationID:         destinationID,
			Location:              prepareStagingFile(t, ctx, minioResource, eventsPayload).ObjectName,
			TotalEvents:           events * 3,
			FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
			LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
			UseRudderStorage:      false,
			DestinationRevisionID: destinationID,
			Schema: map[string]map[string]any{
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
		}))
		requireStagingFileEventsCount(t, ctx, db, events*3, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "status", B: succeeded},
		}...)
		requireLoadFileEventsCount(t, ctx, db, events*3, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
		}...)
		requireTableUploadEventsCount(t, ctx, db, events*3, []lo.Tuple2[string, any]{
			{A: "status", B: waiting},
			{A: "wh_uploads.source_id", B: sourceID},
			{A: "wh_uploads.destination_id", B: destinationID},
			{A: "wh_uploads.namespace", B: namespace},
		}...) // not supported for postgres yet
		requireUploadJobsCount(t, ctx, db, jobs, []lo.Tuple2[string, any]{
			{A: "source_id", B: sourceID},
			{A: "destination_id", B: destinationID},
			{A: "namespace", B: namespace},
			{A: "status", B: exportedData},
		}...)
	})
}

func runWarehouseServer(
	t testing.TB,
	ctx context.Context,
	webPort int,
	pgResource *postgres.Resource,
	bcConfig map[string]backendconfig.ConfigT,
	configOverrides ...lo.Tuple2[string, any],
) error {
	mockCtrl := gomock.NewController(t)

	mockApp := mocksApp.NewMockApp(mockCtrl)
	mockApp.EXPECT().Features().Return(&app.Features{Reporting: &reporting.Factory{}}).AnyTimes()

	mainApp := app.New(&app.Options{
		EnterpriseToken: "some-token",
	})
	mainApp.Setup()

	bcConfigWg := sync.WaitGroup{}

	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).DoAndReturn(func(ctx context.Context) error { return nil }).AnyTimes()
	mockBackendConfig.EXPECT().Identity().Return(&identity.NOOP{})
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
		ch := make(chan pubsub.DataEvent, 1)
		ch <- pubsub.DataEvent{
			Data:  bcConfig,
			Topic: string(backendconfig.TopicBackendConfig),
		}

		bcConfigWg.Add(1)
		go func() {
			defer bcConfigWg.Done()

			<-ctx.Done()
			close(ch)
		}()

		return ch
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
	conf.Set("Warehouse.mainLoopSleep", "5s")
	conf.Set("Warehouse.jobs.processingSleepInterval", "1s")
	conf.Set("PgNotifier.maxPollSleep", "1s")
	conf.Set("PgNotifier.trackBatchIntervalInS", "1s")
	conf.Set("PgNotifier.maxAttempt", 1)
	for _, override := range configOverrides {
		conf.Set(override.A, override.B)
	}

	warehouseApp := warehouse.New(mainApp, conf, logger.NewLogger(), stats.NOP, mockBackendConfig, filemanager.New)
	if err := warehouseApp.Setup(ctx); err != nil {
		return fmt.Errorf("setting up warehouse: %w", err)
	}
	if err := warehouseApp.Run(ctx); err != nil {
		return fmt.Errorf("running warehouse: %w", err)
	}

	bcConfigWg.Wait()
	return nil
}

func defaultBackendConfig(
	pgResource *postgres.Resource,
	minioResource *minio.Resource,
	preferAppend bool,
) map[string]backendconfig.ConfigT {
	return map[string]backendconfig.ConfigT{
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
							Config: map[string]any{
								"host":             pgResource.Host,
								"database":         pgResource.Database,
								"user":             pgResource.User,
								"password":         pgResource.Password,
								"port":             pgResource.Port,
								"sslMode":          "disable",
								"namespace":        namespace,
								"bucketProvider":   whutils.MINIO,
								"bucketName":       minioResource.BucketName,
								"accessKeyID":      minioResource.AccessKeyID,
								"secretAccessKey":  minioResource.AccessKeySecret,
								"useSSL":           false,
								"endPoint":         minioResource.Endpoint,
								"syncFrequency":    "0",
								"useRudderStorage": false,
								"preferAppend":     preferAppend,
							},
							RevisionID: destinationID,
						},
					},
				},
			},
		},
	}
}

func prepareStagingFile(
	t testing.TB,
	ctx context.Context,
	minioResource *minio.Resource,
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
func requireStagingFileEventsCount(
	t testing.TB,
	ctx context.Context,
	db *sqlmw.DB,
	expectedCount int,
	filters ...lo.Tuple2[string, any],
) {
	t.Helper()

	query := "SELECT COALESCE(sum(total_events), 0) FROM wh_staging_files WHERE 1 = 1"
	query += strings.Join(lo.Map(filters, func(t lo.Tuple2[string, any], index int) string {
		return fmt.Sprintf(" AND %s = $%d", t.A, index+1)
	}), "")
	queryArgs := lo.Map(filters, func(t lo.Tuple2[string, any], _ int) any {
		return t.B
	})

	require.Eventuallyf(t,
		func() bool {
			var eventsCount int
			err := db.QueryRowContext(ctx, query, queryArgs...).Scan(&eventsCount)
			if err != nil {
				t.Logf("error getting staging file events count: %v", err)
				return false
			}
			t.Logf("Staging file events count: %d", eventsCount)
			return eventsCount == expectedCount
		},
		10*time.Second,
		250*time.Millisecond,
		"expected staging file events count to be %d", expectedCount,
	)
}

// nolint:unparam
func requireLoadFileEventsCount(
	t testing.TB,
	ctx context.Context,
	db *sqlmw.DB,
	expectedCount int,
	filters ...lo.Tuple2[string, any],
) {
	t.Helper()

	query := "SELECT COALESCE(sum(total_events), 0) FROM wh_load_files WHERE 1 = 1"
	query += strings.Join(lo.Map(filters, func(t lo.Tuple2[string, any], index int) string {
		return fmt.Sprintf(" AND %s = $%d", t.A, index+1)
	}), "")
	queryArgs := lo.Map(filters, func(t lo.Tuple2[string, any], _ int) any {
		return t.B
	})

	require.Eventuallyf(t,
		func() bool {
			var eventsCount int
			err := db.QueryRowContext(ctx, query, queryArgs...).Scan(&eventsCount)
			if err != nil {
				t.Logf("error getting load file events count: %v", err)
				return false
			}
			t.Logf("Load file events count: %d", eventsCount)
			return eventsCount == expectedCount
		},
		10*time.Second,
		250*time.Millisecond,
		"expected load file events count to be %d", expectedCount,
	)
}

// nolint:unparam
func requireTableUploadEventsCount(
	t testing.TB,
	ctx context.Context,
	db *sqlmw.DB,
	expectedCount int,
	filters ...lo.Tuple2[string, any],
) {
	t.Helper()

	tableUploadsFilters := lo.Filter(filters, func(t lo.Tuple2[string, any], index int) bool {
		return !strings.HasPrefix(t.A, "wh_uploads")
	})
	uploadFilters := lo.Filter(filters, func(t lo.Tuple2[string, any], index int) bool {
		return strings.HasPrefix(t.A, "wh_uploads")
	})

	query := "SELECT COALESCE(sum(total_events), 0) FROM wh_table_uploads WHERE 1 = 1"
	query += strings.Join(lo.Map(tableUploadsFilters, func(t lo.Tuple2[string, any], index int) string {
		return fmt.Sprintf(" AND %s = $%d", t.A, index+1)
	}), "")
	queryArgs := lo.Map(tableUploadsFilters, func(t lo.Tuple2[string, any], _ int) any {
		return t.B
	})
	if len(uploadFilters) > 0 {
		query += " AND wh_upload_id IN (SELECT id FROM wh_uploads WHERE 1 = 1"
		query += strings.Join(lo.Map(uploadFilters, func(t lo.Tuple2[string, any], index int) string {
			return fmt.Sprintf(" AND %s = $%d", t.A, len(tableUploadsFilters)+index+1)
		}), "")
		query += ")"
		for _, t := range uploadFilters {
			queryArgs = append(queryArgs, t.B)
		}
	}

	require.Eventuallyf(t,
		func() bool {
			var eventsCount int
			err := db.QueryRowContext(ctx, query, queryArgs...).Scan(&eventsCount)
			if err != nil {
				t.Logf("error getting table upload events count: %v", err)
				return false
			}
			t.Logf("Table upload events count: %d", eventsCount)
			return eventsCount == expectedCount
		},
		10*time.Second,
		250*time.Millisecond,
		"expected table upload events count to be %d", expectedCount,
	)
}

// nolint:unparam
func requireUploadJobsCount(
	t testing.TB,
	ctx context.Context,
	db *sqlmw.DB,
	expectedCount int,
	filters ...lo.Tuple2[string, any],
) {
	t.Helper()

	query := "SELECT count(*) FROM wh_uploads WHERE 1 = 1"
	query += strings.Join(lo.Map(filters, func(t lo.Tuple2[string, any], index int) string {
		return fmt.Sprintf(" AND %s = $%d", t.A, index+1)
	}), "")
	queryArgs := lo.Map(filters, func(t lo.Tuple2[string, any], _ int) any {
		return t.B
	})

	require.Eventuallyf(t,
		func() bool {
			var jobsCount int
			err := db.QueryRowContext(ctx, query, queryArgs...).Scan(&jobsCount)
			if err != nil {
				t.Logf("error getting upload jobs count: %v", err)
				return false
			}
			t.Logf("Upload jobs count: %d", jobsCount)
			return jobsCount == expectedCount
		},
		10*time.Second,
		250*time.Millisecond,
		"expected upload jobs count to be %d", expectedCount,
	)
}

func requireRetriedUploadJobsCount(
	t testing.TB,
	ctx context.Context,
	db *sqlmw.DB,
	expectedCount int,
	filters ...lo.Tuple2[string, any],
) {
	t.Helper()

	query := "SELECT SUM(CAST(value ->> 'attempt' AS INT)) AS total_attempts FROM wh_uploads, jsonb_each(error) WHERE 1 = 1"
	query += strings.Join(lo.Map(filters, func(t lo.Tuple2[string, any], index int) string {
		return fmt.Sprintf(" AND %s = $%d", t.A, index+1)
	}), "")
	queryArgs := lo.Map(filters, func(t lo.Tuple2[string, any], _ int) any {
		return t.B
	})

	require.Eventuallyf(t,
		func() bool {
			var jobsCount sql.NullInt64
			err := db.QueryRowContext(ctx, query, queryArgs...).Scan(&jobsCount)
			if err != nil {
				t.Logf("error getting retried upload jobs count: %v", err)
				return false
			}
			t.Logf("Retried upload jobs count: %d", jobsCount.Int64)
			return jobsCount.Int64 == int64(expectedCount)
		},
		120*time.Second,
		250*time.Millisecond,
		"expected retried upload jobs count to be %d", expectedCount,
	)
}

func requireDownstreamEventsCount(
	t testing.TB,
	ctx context.Context,
	db *sqlmw.DB,
	tableName string,
	expectedCount int,
) {
	t.Helper()

	require.Eventuallyf(t,
		func() bool {
			var count int
			err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s;`, tableName)).Scan(&count)
			if err != nil {
				t.Logf("error getting downstream events count: %v", err)
				return false
			}
			t.Logf("Downstream events count for %q: %d", tableName, count)
			return count == expectedCount
		},
		10*time.Second,
		250*time.Millisecond,
		"expected downstream events count for table %s to be %d", tableName, expectedCount,
	)
}

func requireReportsCount(
	t testing.TB,
	ctx context.Context,
	db *sqlmw.DB,
	expectedCount int,
	filters ...lo.Tuple2[string, any],
) {
	t.Helper()

	query := "SELECT sum(count) FROM reports WHERE 1 = 1"
	query += strings.Join(lo.Map(filters, func(t lo.Tuple2[string, any], index int) string {
		return fmt.Sprintf(" AND %s = $%d", t.A, index+1)
	}), "")
	queryArgs := lo.Map(filters, func(t lo.Tuple2[string, any], _ int) any {
		return t.B
	})

	require.Eventuallyf(t,
		func() bool {
			var reportsCount sql.NullInt64
			err := db.QueryRowContext(ctx, query, queryArgs...).Scan(&reportsCount)
			if err != nil {
				t.Logf("error getting reports count: %v", err)
				return false
			}
			t.Logf("Reports count: %d", reportsCount.Int64)
			return reportsCount.Int64 == int64(expectedCount)
		},
		10*time.Second,
		250*time.Millisecond,
		"expected reports count to be %d", expectedCount,
	)
}

func setupServer(
	t *testing.T, preferAppend bool,
	bcConfigFunc func(map[string]backendconfig.ConfigT, *minio.Resource),
	configOverridesFunc func(*minio.Resource) []lo.Tuple2[string, any],
) (
	*sqlmw.DB,
	*minio.Resource,
	*whclient.Warehouse,
) {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	minioResource, err := minio.Setup(pool, t)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	webPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	bcConfig := defaultBackendConfig(pgResource, minioResource, preferAppend)
	if bcConfigFunc != nil {
		bcConfigFunc(bcConfig, minioResource)
	}

	var configOverrides []lo.Tuple2[string, any]
	if configOverridesFunc != nil {
		configOverrides = configOverridesFunc(minioResource)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		require.NoError(t, runWarehouseServer(t, ctx, webPort, pgResource, bcConfig, configOverrides...))
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	serverURL := fmt.Sprintf("http://localhost:%d", webPort)
	health.WaitUntilReady(ctx, t, serverURL+"/health", time.Second*30, 100*time.Millisecond, t.Name())

	return sqlmw.New(pgResource.DB), minioResource, whclient.NewWarehouse(serverURL)
}
