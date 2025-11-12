package srchydration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"

	"github.com/rudderlabs/rudder-go-kit/jsonparser"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	webhookutil "github.com/rudderlabs/rudder-server/testhelper/webhook"

	"github.com/samber/lo"

	proctypes "github.com/rudderlabs/rudder-server/processor/types"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

const (
	writeKey    = "writekey-1"
	sourceID    = "source-1"
	workspaceID = "workspace-1"
	srcDefName  = "Source-Def-Name-1"
)

func TestSrcHydration(t *testing.T) {
	t.Run("SrcHydrationWithTransformer", func(t *testing.T) {
		pageAccessToken := os.Getenv("FB_TEST_PAGE_ACCESS_TOKEN")
		if pageAccessToken == "" {
			t.Skip("FB_TEST_PAGE_ACCESS_TOKEN is not set, skipping test")
		}

		webhook := webhookutil.NewRecorder()
		t.Cleanup(webhook.Close)
		webhookURL := webhook.Server.URL

		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(backendconfigtest.NewConfigBuilder().
				WithWorkspaceID(workspaceID).
				WithSource(
					backendconfigtest.NewSourceBuilder().
						WithWorkspaceID(workspaceID).
						WithID(sourceID).
						WithWriteKey(writeKey).
						WithSourceCategory("webhook").
						WithInternalSecrets(json.RawMessage(fmt.Sprintf(`{"pageAccessToken": "%s"}`, pageAccessToken))).
						WithSourceType("FACEBOOK_LEAD_ADS_NATIVE").
						WithSourceDefOptions(backendconfig.SourceDefinitionOptions{
							Hydration: struct {
								Enabled bool
							}{Enabled: true},
						}).
						WithConnection(
							backendconfigtest.NewDestinationBuilder("WEBHOOK").
								WithID("destination-1").
								WithConfigOption("webhookMethod", "POST").
								WithConfigOption("webhookUrl", webhookURL).
								Build()).
						Build()).
				Build()).
			Build()
		t.Cleanup(bcServer.Close)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		tr, err := transformer.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(t, ctx, gwPort, postgresContainer, bcServer.URL, tr.TransformerURL, t.TempDir())
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

		require.Eventually(t, func() bool {
			return webhook.RequestsCount() == eventsCount
		}, 2*time.Minute, 10*time.Second, "unexpected number of events received, count of events: %d", webhook.RequestsCount())
	})

	t.Run("SrcHydration", func(t *testing.T) {
		webhook := webhookutil.NewRecorder()
		t.Cleanup(webhook.Close)
		webhookURL := webhook.Server.URL

		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(backendconfigtest.NewConfigBuilder().
				WithWorkspaceID(workspaceID).
				WithSource(
					backendconfigtest.NewSourceBuilder().
						WithWorkspaceID(workspaceID).
						WithID(sourceID).
						WithWriteKey(writeKey).
						WithSourceCategory("webhook").
						WithSourceType(srcDefName).
						WithSourceDefOptions(backendconfig.SourceDefinitionOptions{
							Hydration: struct {
								Enabled bool
							}{Enabled: true},
						}).
						WithConnection(
							backendconfigtest.NewDestinationBuilder("WEBHOOK").
								WithID("destination-1").
								WithConfigOption("webhookMethod", "POST").
								WithConfigOption("webhookUrl", webhookURL).
								Build()).
						Build()).
				Build()).
			Build()
		t.Cleanup(bcServer.Close)

		trServer := transformertest.NewBuilder().
			WithSrcHydrationHandler(strings.ToLower(srcDefName), func(request proctypes.SrcHydrationRequest) (proctypes.SrcHydrationResponse, error) {
				if request.Source.SourceDefinition.Name != srcDefName {
					return proctypes.SrcHydrationResponse{}, fmt.Errorf("unexpected source name: %s", request.Source.SourceDefinition.Name)
				}
				lo.ForEach(request.Batch, func(event proctypes.SrcHydrationEvent, index int) {
					event.Event["source_hydration_test"] = "success"
				})
				return proctypes.SrcHydrationResponse{
					Batch: request.Batch,
				}, nil
			}).
			WithDestTransformHandler(
				"WEBHOOK",
				transformertest.RESTJSONDestTransformerHandler(http.MethodPost, webhookURL),
			).
			Build()
		t.Cleanup(trServer.Close)

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

		require.Eventuallyf(t, func() bool {
			return webhook.RequestsCount() == eventsCount
		},
			1*time.Minute, 5*time.Second, "unexpected number of events received, count of events: %d", webhook.RequestsCount())

		lo.ForEach(webhook.Requests(), func(req *http.Request, _ int) {
			reqBody, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, jsonparser.GetStringOrEmpty(reqBody, "source_hydration_test"), "success")
		})
		cancel()
		require.NoError(t, wg.Wait())
	})
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
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BatchRouter.pingFrequency"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BatchRouter.uploadFreq"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.webPort"), strconv.Itoa(port))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RUDDER_TMPDIR"), os.TempDir())
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.storagePath"), path.Join(tmpDir, "/recovery_data.json"))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Profiler.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.enableSuppressUserFeature"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.archiveInPreProcess"), "true")
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, []string{"src-hydration"})
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
	for i := 0; i < num; i++ {
		payload := []byte(fmt.Sprintf(`
			{
			  "batch": [
				{
				  "anonymousId": "1201391398519677",
				  "type": %[1]q,
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
