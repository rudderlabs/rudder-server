package transformer_contract

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

// TestHTTPV2AccountDelivery exercises workspace-config enrichment and the processor's
// destination-transformer request, not the transformer's HTTP authentication logic.
func TestHTTPV2AccountDelivery(t *testing.T) {
	config.Reset()
	defer config.Reset()

	workspaceConfig := backendconfig.ConfigT{
		WorkspaceID: "workspace-1",
		Sources: []backendconfig.SourceT{{
			ID: "source-1", WriteKey: "writekey-1", WorkspaceID: "workspace-1", Enabled: true,
			SourceDefinition: backendconfig.SourceDefinitionT{Name: "webhook"},
			Destinations: []backendconfig.DestinationT{
				{ID: "basic", Enabled: true, Version: 2, WorkspaceID: "workspace-1", IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "HTTP"},
					Config:                map[string]any{"authenticationEnabled": true, "rudderAccountId": "basic-account"}},
				{ID: "bearer", Enabled: true, Version: 2, WorkspaceID: "workspace-1", IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "HTTP"},
					Config:                map[string]any{"authenticationEnabled": true, "rudderAccountId": "bearer-account"}},
				{ID: "api-key", Enabled: true, Version: 2, WorkspaceID: "workspace-1", IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "HTTP"},
					Config:                map[string]any{"authenticationEnabled": true, "rudderAccountId": "api-key-account"}},
				{ID: "v1", Enabled: true, Version: 1, WorkspaceID: "workspace-1", IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "HTTP"},
					Config:                map[string]any{"auth": "basicAuth", "username": "legacy-user", "password": "legacy-password"}},
			},
		}},
		Accounts: map[string]backendconfig.Account{
			"basic-account": {AccountDefinitionName: "DESTINATION_HTTP_BASIC_AUTH",
				Options: map[string]any{"username": "account-user"}, Secret: map[string]any{"password": ""}},
			"bearer-account": {AccountDefinitionName: "DESTINATION_HTTP_BEARER_TOKEN",
				Secret: map[string]any{"bearerToken": "fixture-token"}},
			"api-key-account": {AccountDefinitionName: "DESTINATION_HTTP_API_KEY",
				Options: map[string]any{"apiKeyName": "X-API-Key"}, Secret: map[string]any{"apiKeyValue": "fixture-key"}},
		},
		AccountDefinitions: map[string]backendconfig.AccountDefinition{
			"DESTINATION_HTTP_BASIC_AUTH":   {Name: "DESTINATION_HTTP_BASIC_AUTH", AuthenticationType: "basic_auth"},
			"DESTINATION_HTTP_BEARER_TOKEN": {Name: "DESTINATION_HTTP_BEARER_TOKEN", AuthenticationType: "bearer_token"},
			"DESTINATION_HTTP_API_KEY":      {Name: "DESTINATION_HTTP_API_KEY", AuthenticationType: "api_key"},
		},
	}
	body, err := jsonrs.Marshal(workspaceConfig)
	require.NoError(t, err)
	bcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/workspaceConfig" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(body)
	}))
	defer bcServer.Close()

	received := make(chan backendconfig.DestinationT, 4)
	trServer := transformertest.NewBuilder().
		WithDestTransformHandler("HTTP", func(request []types.TransformerEvent) []types.TransformerResponse {
			for _, event := range request {
				received <- event.Destination
			}
			return transformertest.MirroringTransformerHandler(request)
		}).
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
		return runRudderServer(t, ctx, cancel, gwPort, postgresContainer, bcServer.URL, trServer.URL, t.TempDir(), true)
	})

	url := fmt.Sprintf("http://localhost:%d", gwPort)
	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	require.NoError(t, sendEvents(1, "identify", "writekey-1", url))
	requireJobsCount(t, ctx, postgresContainer.DB, "gw", jobsdb.Succeeded.State, 1)
	require.Eventually(t, func() bool { return len(received) == 4 }, 30*time.Second, 100*time.Millisecond)

	destinations := make(map[string]backendconfig.DestinationT, 4)
	for range 4 {
		destination := <-received
		destinations[destination.ID] = destination
	}
	require.Len(t, destinations, 4)
	for _, tc := range []struct {
		id, accountID, definitionName string
		options, secret               map[string]any
	}{
		{"basic", "basic-account", "DESTINATION_HTTP_BASIC_AUTH", map[string]any{"username": "account-user"}, map[string]any{"password": ""}},
		{"bearer", "bearer-account", "DESTINATION_HTTP_BEARER_TOKEN", nil, map[string]any{"bearerToken": "fixture-token"}},
		{"api-key", "api-key-account", "DESTINATION_HTTP_API_KEY", map[string]any{"apiKeyName": "X-API-Key"}, map[string]any{"apiKeyValue": "fixture-key"}},
	} {
		destination := destinations[tc.id]
		require.Equal(t, 2, destination.Version)
		require.Equal(t, map[string]any{"authenticationEnabled": true, "rudderAccountId": tc.accountID}, destination.Config)
		require.NotNil(t, destination.DeliveryAccount)
		require.Equal(t, tc.accountID, destination.DeliveryAccount.ID)
		require.Equal(t, tc.definitionName, destination.DeliveryAccount.AccountDefinitionName)
		require.Equal(t, tc.options, destination.DeliveryAccount.Options)
		require.Equal(t, tc.secret, destination.DeliveryAccount.Secret)
		require.NotNil(t, destination.DeliveryAccount.AccountDefinition)
		require.Equal(t, tc.definitionName, destination.DeliveryAccount.AccountDefinition.Name)
	}
	v1 := destinations["v1"]
	require.Equal(t, 1, v1.Version)
	require.Equal(t, map[string]any{"auth": "basicAuth", "username": "legacy-user", "password": "legacy-password"}, v1.Config)
	require.Nil(t, v1.DeliveryAccount)

	cancel()
	require.NoError(t, wg.Wait())
}

func TestTransformerContract(t *testing.T) {
	transformationV2 := []bool{true, false}
	for _, v := range transformationV2 {
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
					response, _ := jsonrs.Marshal(workspaceConfig)
					_, _ = w.Write(response)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))

			trServer := transformertest.NewBuilder().
				WithUserTransformHandler(
					func(request []types.TransformerEvent) (response []types.TransformerResponse) {
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
							require.Equal(t, req.Credentials, []types.Credential{
								{
									ID:       "credential-1",
									Key:      "key-1",
									Value:    "value-1",
									IsSecret: false,
								},
							})
							response = append(response, types.TransformerResponse{
								Metadata:   req.Metadata,
								Output:     req.Message,
								StatusCode: http.StatusOK,
							})
						}
						return response
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
				err := runRudderServer(t, ctx, cancel, gwPort, postgresContainer, bcServer.URL, trServer.URL, t.TempDir(), v)
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
}

func runRudderServer(
	t testing.TB,
	ctx context.Context,
	cancel context.CancelFunc,
	port int,
	postgresContainer *postgres.Resource,
	cbURL, transformerURL,
	tmpDir string,
	transformationV2 bool,
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
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.gw.pendingEvents.enabled"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DestinationDebugger.disableEventDeliveryStatusUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "SourceDebugger.disableEventUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "TransformationDebugger.disableTransformationStatusUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.backup.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.compactionLoopSleepDuration"), "60m")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "archival.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.syncer.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BatchRouter.pingFrequency"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BatchRouter.uploadFreq"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.webPort"), strconv.Itoa(port))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RUDDER_TMPDIR"), os.TempDir())
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.storagePath"), path.Join(tmpDir, "/recovery_data.json"))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Profiler.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.enableSuppressUserFeature"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.enableTransformationV2"), strconv.FormatBool(transformationV2))
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, cancel, []string{"transformer-contract"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return err
}

// nolint: unparam, bodyclose
func sendEvents(
	num int,
	eventType, writeKey,
	url string,
) error {
	for range num {
		payload := fmt.Appendf(nil, `
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
		)
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
