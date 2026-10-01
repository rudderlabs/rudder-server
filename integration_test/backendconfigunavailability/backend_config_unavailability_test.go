package backendconfigunavailability

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	statshelper "github.com/rudderlabs/rudder-go-kit/stats/testhelper"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	"github.com/rudderlabs/rudder-go-kit/testhelper/httptest"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	webhookutil "github.com/rudderlabs/rudder-server/testhelper/webhook"
)

const (
	transformationVerID = "regular-transformation-id"
	writeKey            = "writekey-1"
)

type testConfig struct {
	transformerResource       *transformer.Resource
	postgresResource          *postgres.Resource
	gwPort                    int
	prometheusPort            int
	transformerConfigBEServer *httptest.Server
	webhook                   *webhookutil.Recorder
	configBEServer            *httptest.Server
}

func TestBackendConfigUnavailabilityForTransformer(t *testing.T) {
	tc := setup(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		err := runRudderServer(t, ctx, cancel, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.configBEServer.URL, tc.transformerResource.TransformerURL, t.TempDir())
		if err != nil {
			t.Logf("rudder-server exited with error: %v", err)
		}
		return err
	})

	url := fmt.Sprintf("http://localhost:%d", tc.gwPort)
	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

	eventsCount := 12

	err := sendEvents(eventsCount, "identify", writeKey, url)
	require.NoError(t, err)

	// events should not be delivered now
	require.Never(t, func() bool {
		return tc.webhook.RequestsCount() != 0
	}, time.Minute, time.Second)
	// and they are pending in the gw jobsdb
	require.Eventually(t, func() bool {
		v, ok := gwPendingEvents(t, tc.prometheusPort, "source-1")
		return ok && v == float64(eventsCount)
	}, time.Minute, time.Second, "gw pending events should equal the events sent")

	// starting backend config
	tc.transformerConfigBEServer.Start()

	require.Eventuallyf(t, func() bool {
		return tc.webhook.RequestsCount() == eventsCount
	}, time.Minute, time.Second, "unexpected number of events received, count of events: %d", tc.webhook.RequestsCount())

	lo.ForEach(tc.webhook.Requests(), func(req *http.Request, _ int) {
		body, _ := io.ReadAll(req.Body)
		require.True(t, gjson.GetBytes(body, "transformed").Bool())
	})
	require.Eventually(t, func() bool {
		v, ok := gwPendingEvents(t, tc.prometheusPort, "source-1")
		return ok && v == 0
	}, time.Minute, time.Second, "no gw events should be pending once all are delivered")

	cancel()
	require.NoError(t, wg.Wait())
}

func setup(t testing.TB) testConfig {
	t.Helper()

	config.Reset()
	t.Cleanup(config.Reset)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgresResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	prometheusPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	webhook := webhookutil.NewRecorder()
	t.Cleanup(webhook.Close)
	webhookURL := webhook.Server.URL

	bcServer := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(
			backendconfigtest.NewConfigBuilder().
				WithSource(
					backendconfigtest.NewSourceBuilder().
						WithID("source-1").
						WithWriteKey(writeKey).
						WithConnection(
							backendconfigtest.NewDestinationBuilder("WEBHOOK").
								WithID("destination-1").
								WithUserTransformation("transform-id", transformationVerID).
								WithConfigOption("webhookMethod", "POST").
								WithConfigOption("webhookUrl", webhookURL).
								Build()).
						Build()).
				Build()).
		Build()
	t.Cleanup(bcServer.Close)

	transformerBEConfigHandler := transformer.NewTransformerBackendConfigHandler(map[string]string{
		transformationVerID: `export function transformEvent(event, metadata) {
													event.transformed=true
													return event;
												}`,
	})
	// initialise un-started config BE server, to be started later
	unStartedTransformerConfigBEServer := httptest.NewUnstartedServer(transformerBEConfigHandler)
	_, transformerConfigBEPort, err := net.SplitHostPort(unStartedTransformerConfigBEServer.Listener.Addr().String())
	require.NoError(t, err)
	transformerConfigBEServerUrl := fmt.Sprintf("http://%s:%s", "localhost", transformerConfigBEPort)

	transformerResource, err := transformer.Setup(pool, t,
		transformer.WithConnectionToHostEnabled(),
		transformer.WithConfigBackendURL(transformerConfigBEServerUrl))
	require.NoError(t, err)

	return testConfig{
		postgresResource:          postgresResource,
		gwPort:                    gwPort,
		prometheusPort:            prometheusPort,
		transformerResource:       transformerResource,
		transformerConfigBEServer: unStartedTransformerConfigBEServer,
		webhook:                   webhook,
		configBEServer:            bcServer,
	}
}

func runRudderServer(
	t testing.TB,
	ctx context.Context,
	cancel context.CancelFunc,
	port, prometheusPort int,
	postgresContainer *postgres.Resource,
	cbURL, transformerURL, tmpDir string,
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
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.gw.pendingEvents.interval"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "enableStats"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RuntimeStats.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.enabled"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.metrics.prometheus.enabled"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.metrics.prometheus.port"), strconv.Itoa(prometheusPort))
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

	t.Setenv("Processor.maxRetry", strconv.Itoa(1))

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, cancel, []string{"rudder-backend-config-unavailability"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return err
}

// nolint: bodyclose
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

// gwPendingEvents scrapes the server's prometheus endpoint and returns the gw pending events gauge of a source.
func gwPendingEvents(t testing.TB, prometheusPort int, sourceID string) (float64, bool) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", prometheusPort))
	if err != nil {
		return 0, false
	}
	defer func() { kithttputil.CloseResponse(resp) }()
	families, err := statshelper.ParsePrometheusMetrics(resp.Body)
	if err != nil {
		return 0, false
	}
	family, ok := families["jobsdb_gw_pending_events_count"]
	if !ok {
		return 0, false
	}
	for _, m := range family.GetMetric() {
		for _, label := range m.GetLabel() {
			if label.GetName() == "sourceId" && label.GetValue() == sourceID {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}
