package transformer_contract

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

func TestTransformerContract(t *testing.T) {
	t.Run("User Transformer", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		workspaceConfig := backendconfig.ConfigT{
			WorkspaceID: "workspace-1",
			Sources: []backendconfig.SourceT{
				{
					ID:   "source-1",
					Name: "source-name-1",
					SourceDefinition: backendconfig.SourceDefinitionT{
						ID:       "source-def-1",
						Name:     "source-def-name-1",
						Category: "source-def-category-1",
						Type:     "source-def-type-1",
					},
					WriteKey:    "writekey-1",
					WorkspaceID: "workspace-1",
					Enabled:     true,
					Destinations: []backendconfig.DestinationT{
						{
							ID:   "destination-1",
							Name: "destination-name-1",
							DestinationDefinition: backendconfig.DestinationDefinitionT{
								ID:          "destination-def-1",
								Name:        "destination-def-name-1",
								DisplayName: "destination-def-display-name-1",
							},
							Enabled:     true,
							WorkspaceID: "workspace-1",
							Transformations: []backendconfig.TransformationT{
								{
									ID:        "transformation-1",
									VersionID: "version-1",
								},
							},
							IsProcessorEnabled: true,
							RevisionID:         "revision-1",
						},
					},
					DgSourceTrackingPlanConfig: backendconfig.DgSourceTrackingPlanConfigT{
						SourceId:            "source-1",
						SourceConfigVersion: 1,
						Deleted:             false,
						TrackingPlan: backendconfig.TrackingPlanT{
							Id:      "tracking-plan-1",
							Version: 1,
						},
					},
				},
			},
			Credentials: map[string]backendconfig.Credential{
				"credential-1": {
					Key:      "key-1",
					Value:    "value-1",
					IsSecret: false,
				},
			},
		}

		bcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/workspaceConfig":
				response, _ := json.Marshal(workspaceConfig)
				_, _ = w.Write(response)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))

		trServer := transformertest.NewBuilder().
			WithUserTransformHandler(
				func(request []transformer.TransformerEvent) (response []transformer.TransformerResponse) {
					for i := range request {
						req := request[i]

						require.Equal(t, req.Metadata.SourceID, "source-1")
						require.Equal(t, req.Metadata.SourceName, "source-name-1")
						require.Equal(t, req.Metadata.SourceType, "source-def-name-1")
						require.Equal(t, req.Metadata.SourceCategory, "source-def-category-1")
						require.Equal(t, req.Metadata.SourceDefinitionID, "source-def-1")
						require.Equal(t, req.Metadata.WorkspaceID, "workspace-1")
						require.Equal(t, req.Metadata.DestinationID, "destination-1")
						require.Equal(t, req.Metadata.DestinationType, "destination-def-name-1")
						require.Equal(t, req.Metadata.DestinationName, "destination-name-1")
						require.Equal(t, req.Metadata.TransformationID, "transformation-1")
						require.Equal(t, req.Metadata.TransformationVersionID, "version-1")
						require.Equal(t, req.Metadata.EventType, "identify")
						require.Equal(t, req.Credentials, []transformer.Credential{
							{
								ID:       "credential-1",
								Key:      "key-1",
								Value:    "value-1",
								IsSecret: false,
							},
						})
						response = append(response, transformer.TransformerResponse{
							Metadata:   req.Metadata,
							Output:     req.Message,
							StatusCode: http.StatusOK,
						})
					}
					return
				},
			).
			Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(t, ctx, gwPort, postgresContainer, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12

		err = sendEvents(eventsCount, "identify", "writekey-1", url)
		require.NoError(t, err)

		requireJobsCount(t, ctx, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, ctx, postgresContainer.DB, "rt", jobsdb.Succeeded.State, eventsCount)

		cancel()
		require.NoError(t, wg.Wait())
	})
	// TODO: Add tests for dest transformer and tracking plan validation
	t.Run("Dest Transformer", func(t *testing.T) {})
	t.Run("Tracking Plan Validation", func(t *testing.T) {})
}

func runRudderServer(
	t testing.TB,
	ctx context.Context,
	port int,
	postgresContainer *postgres.Resource,
	cbURL, transformerURL,
	tmpDir string,
) (err error) {
	t.Setenv("CONFIG_BACKEND_URL", cbURL)
	t.Setenv("WORKSPACE_TOKEN", "token")
	t.Setenv("DEST_TRANSFORM_URL", transformerURL)

	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.host"), postgresContainer.Host)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.port"), postgresContainer.Port)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.user"), postgresContainer.User)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.name"), postgresContainer.Database)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.password"), postgresContainer.Password)

	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Warehouse.mode"), "off")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DestinationDebugger.disableEventDeliveryStatusUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "SourceDebugger.disableEventUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "TransformationDebugger.disableTransformationStatusUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.backup.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.migrateDSLoopSleepDuration"), "60m")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "archival.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.syncer.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BatchRouter.mainLoopFreq"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BatchRouter.uploadFreq"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.webPort"), strconv.Itoa(port))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RUDDER_TMPDIR"), os.TempDir())
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.storagePath"), path.Join(tmpDir, "/recovery_data.json"))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Profiler.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.enableSuppressUserFeature"), "false")

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, []string{"transformer-contract"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return
}

// nolint: unparam, bodyclose
func sendEvents(
	num int,
	eventType, writeKey,
	url string,
) error {
	for i := 0; i < num; i++ {
		payload := []byte(fmt.Sprintf(`
			{
			  "batch": [
				{
				  "userId": %[1]q,
				  "type": %[2]q,
				  "context": {
					"traits": {
					  "trait1": "new-val"
					},
					"ip": "14.5.67.21",
					"library": {
					  "name": "http"
					}
				  },
				  "timestamp": "2020-02-02T00:23:09.544Z"
				}
			  ]
			}`,
			rand.String(10),
			eventType,
		))
		req, err := http.NewRequest(http.MethodPost, url+"/v1/batch", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.SetBasicAuth(writeKey, "password")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to send event to rudder server, status code: %d: %s", resp.StatusCode, string(b))
		}
		kithttputil.CloseResponse(resp)
	}
	return nil
}

// nolint: unparam
func requireJobsCount(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	queue, state string,
	expectedCount int,
) {
	t.Helper()

	query := fmt.Sprintf(`
		SELECT
		  count(*)
		FROM
		  unionjobsdbmetadata('%s', 1)
		WHERE
		  job_state = '%s';
	`,
		queue,
		state,
	)
	require.Eventuallyf(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&jobsCount))
		t.Logf("%s %sJobCount: %d", queue, state, jobsCount)
		return jobsCount == expectedCount
	},
		30*time.Second,
		1*time.Second,
		"%d %s events should be in %s state", expectedCount, queue, state,
	)
}
