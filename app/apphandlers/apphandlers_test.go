package apphandlers

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/services/diagnostics"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/httptest"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
	"golang.org/x/sync/errgroup"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/app"
)

func TestAppHandlerStartSequence(t *testing.T) {
	options := app.LoadOptions([]string{"app"})
	application := app.New(options)
	application.Setup()
	versionHandler := func(w http.ResponseWriter, _ *http.Request) {}

	suite := func(t *testing.T, appHandler AppHandler) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		t.Run("it shouldn't be able to start without setup being called first", func(t *testing.T) {
			require.Error(t, appHandler.StartRudderCore(ctx, options))
		})

		t.Run("it shouldn't be able to setup if database is down", func(t *testing.T) {
			require.Error(t, appHandler.Setup())
		})

		t.Run("it should be able to setup if database is up", func(t *testing.T) {
			startJobsDBPostgresql(t)
			require.NoError(t, appHandler.Setup())
		})
	}

	gwSpecificSuite := func(t *testing.T, appHandler AppHandler) {
		t.Run("server should not accept request if features are not fetched", func(t *testing.T) {
			config.Reset()
			startJobsDBPostgresql(t)

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

			featuresChan := make(chan []struct{})
			mockTransformerResp := `{
				"routerTransform": {
				  "a": true,
				  "b": true
				},
				"regulations": ["AM"],
				"supportSourceTransformV1": true,
				"upgradedToSourceTransformV2": true,
				"supportTransformerProxyV1": true
			  }`

			trServer := transformertest.NewBuilder().
				WithFeaturesHandler(func(w http.ResponseWriter, r *http.Request) {
					select {
					case <-featuresChan:
						_, _ = w.Write([]byte(mockTransformerResp))
					}
				}).
				Build()
			defer trServer.Close()

			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			t.Setenv("CONFIG_BACKEND_URL", bcServer.URL)
			t.Setenv("WORKSPACE_TOKEN", "token")
			t.Setenv("DEST_TRANSFORM_URL", trServer.URL)
			t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.webPort"), strconv.Itoa(gwPort))
			setDefaultEnv(t)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			diagnostics.Init()
			backendconfig.Init()
			stats.Default = stats.NOP
			require.NoError(t, backendconfig.Setup(nil))
			defer backendconfig.DefaultBackendConfig.Stop()
			backendconfig.DefaultBackendConfig.StartWithIDs(ctx, "")

			require.NoError(t, appHandler.Setup())

			wg, ctx := errgroup.WithContext(ctx)
			wg.Go(func() error {
				err := appHandler.StartRudderCore(ctx, options)
				if err != nil {
					t.Fatalf("rudder-server exited with error: %v", err)
				}
				return err
			})

			url := fmt.Sprintf("http://localhost:%d", gwPort)
			require.Never(t, func() bool {
				return health.IsReady(t, url+"/health")
			}, 5*time.Second, 1*time.Second, "server should not be ready until features are received")

			close(featuresChan)

			require.Eventually(t, func() bool {
				return health.IsReady(t, url+"/health")
			}, 5*time.Second, 1*time.Second, "server should be ready after features are received")

			cancel()
			require.NoError(t, wg.Wait())
		})
	}

	t.Run("embedded", func(t *testing.T) {
		h, err := GetAppHandler(application, app.EMBEDDED, versionHandler)
		require.NoError(t, err)
		suite(t, h)
		gwSpecificSuite(t, h)
	})

	t.Run("gateway", func(t *testing.T) {
		h, err := GetAppHandler(application, app.GATEWAY, versionHandler)
		require.NoError(t, err)
		suite(t, h)
		gwSpecificSuite(t, h)
	})

	t.Run("processor", func(t *testing.T) {
		h, err := GetAppHandler(application, app.PROCESSOR, versionHandler)
		require.NoError(t, err)
		suite(t, h)
	})
}

func startJobsDBPostgresql(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	r, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	config.Set("DB.host", r.Host)
	config.Set("DB.port", r.Port)
	config.Set("DB.user", r.User)
	config.Set("DB.name", r.Database)
	config.Set("DB.password", r.Password)
}

func setDefaultEnv(t *testing.T) {
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
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RUDDER_TMPDIR"), os.TempDir())
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Profiler.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.enableSuppressUserFeature"), "false")
}
