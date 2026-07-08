package activationrecordsreporting

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/segmentio/go-hll"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/httptest"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
	webhookutil "github.com/rudderlabs/rudder-server/testhelper/webhook"
)

const (
	sourceID      = "source-1"
	destinationID = "destination-1"
	workspaceID   = "workspace-1"
	instanceID    = "test-instance-1"
	origin        = "test-origin"

	// activationRecordsRoute is the path the reporting flusher POSTs MAR payloads to
	// (enterprise/reporting/client.RouteActivationRecords).
	activationRecordsRoute = "/activationRecords"

	activationRecordsTable = "activation_records_reports"
)

// distinctFingerprints is the exact, small set of unique activation fingerprints
// sent through the pipeline. Because the HLL params default to 16/5, cardinality
// is exact at this scale, so the expected unioned cardinality is len(distinct).
var distinctFingerprints = []string{
	"fingerprint-aaa-111",
	"fingerprint-bbb-222",
	"fingerprint-ccc-333",
}

// sentFingerprints is what we actually send: every distinct fingerprint once,
// plus a duplicate of the first. The duplicate exercises HLL dedup (it must NOT
// change cardinality) while still being delivered as its own event.
var sentFingerprints = append(append([]string{}, distinctFingerprints...), distinctFingerprints[0])

type testConfig struct {
	postgresResource *postgres.Resource
	gwPort           int
	webhook          *webhookutil.Recorder
	configBEServer   *httptest.Server
	transformerURL   string
	reportingServer  *mockReportingServer
}

// activationGrain accumulates the unioned fingerprint HLL for a single
// workspace-source-destination-instance grain, as the reporting service would.
type activationGrain struct {
	workspaceID   string
	sourceID      string
	destinationID string
	instanceID    string
	origin        string
	hll           *hll.Hll
}

// grainSnapshot is an immutable view of a grain, safe to read outside the lock.
type grainSnapshot struct {
	workspaceID   string
	sourceID      string
	destinationID string
	instanceID    string
	origin        string
	cardinality   int
}

// activationRecordEntry mirrors the MAR payload element posted by the flusher
// (enterprise/reporting/flusher/aggregator.ActivationRecordsReport).
type activationRecordEntry struct {
	ReportedAt        time.Time `json:"reportedAt"`
	WorkspaceID       string    `json:"workspaceId"`
	SourceID          string    `json:"sourceId"`
	DestinationID     string    `json:"destinationId"`
	Origin            string    `json:"origin"`
	InstanceID        string    `json:"instanceId"`
	FingerprintHLLHex string    `json:"fingerprintHLL"`
}

// mockReportingServer stands in for the reporting service: it decodes the MAR
// /activationRecords payload and unions the per-grain fingerprint HLLs so the
// test can assert the reported cardinality.
type mockReportingServer struct {
	Server *httptest.Server
	mu     sync.RWMutex
	grains map[string]*activationGrain
}

func newMockReportingServer() *mockReportingServer {
	m := &mockReportingServer{grains: make(map[string]*activationGrain)}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only the activation records route carries MAR payloads; any other
		// reporting traffic (e.g. /metrics) is acknowledged but ignored so a
		// stray POST cannot fail the MAR decode.
		if r.URL.Path != activationRecordsRoute {
			w.WriteHeader(http.StatusOK)
			return
		}
		if err := m.record(r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	return m
}

// record decodes one MAR payload and unions each entry's HLL into its grain.
func (m *mockReportingServer) record(body io.Reader) error {
	var entries []activationRecordEntry
	if err := jsonrs.NewDecoder(body).Decode(&entries); err != nil {
		return fmt.Errorf("decoding activation records payload: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range entries {
		raw, err := hex.DecodeString(e.FingerprintHLLHex)
		if err != nil {
			return fmt.Errorf("hex-decoding fingerprint hll: %w", err)
		}
		h, err := hll.FromBytes(raw)
		if err != nil {
			return fmt.Errorf("parsing fingerprint hll: %w", err)
		}
		key := grainKey(e.WorkspaceID, e.SourceID, e.DestinationID, e.InstanceID)
		if g, ok := m.grains[key]; ok {
			g.hll.Union(h)
			continue
		}
		m.grains[key] = &activationGrain{
			workspaceID:   e.WorkspaceID,
			sourceID:      e.SourceID,
			destinationID: e.DestinationID,
			instanceID:    e.InstanceID,
			origin:        e.Origin,
			hll:           &h,
		}
	}
	return nil
}

func (m *mockReportingServer) snapshot(key string) (grainSnapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g, ok := m.grains[key]
	if !ok {
		return grainSnapshot{}, false
	}
	return grainSnapshot{
		workspaceID:   g.workspaceID,
		sourceID:      g.sourceID,
		destinationID: g.destinationID,
		instanceID:    g.instanceID,
		origin:        g.origin,
		cardinality:   int(g.hll.Cardinality()),
	}, true
}

func (m *mockReportingServer) Close() { m.Server.Close() }

func grainKey(workspaceID, sourceID, destinationID, instanceID string) string {
	return fmt.Sprintf("%s-%s-%s-%s", workspaceID, sourceID, destinationID, instanceID)
}

func TestActivationRecordsReporting(t *testing.T) {
	tc := setup(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		err := runRudderServer(t, ctx, cancel, tc)
		if err != nil {
			t.Logf("rudder-server exited with error: %v", err)
		}
		return err
	})

	gwURL := fmt.Sprintf("http://localhost:%d", tc.gwPort)
	health.WaitUntilReady(ctx, t, gwURL+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

	sendActivationRecords(t, gwURL, sentFingerprints)

	// (a) LEAK FIX + DELIVERY: every event is delivered to the destination (an exact,
	// nonzero count — a miswired fail-closed gate would deliver nothing), and the
	// delivered payload no longer carries context.activation or any raw fingerprint.
	require.Eventuallyf(t, func() bool {
		return tc.webhook.RequestsCount() == len(sentFingerprints)
	}, time.Minute, 100*time.Millisecond, "all %d events should be delivered to the destination", len(sentFingerprints))
	for _, req := range tc.webhook.Requests() {
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.Falsef(t, gjson.GetBytes(body, "context.activation").Exists(),
			"context.activation must be stripped from the delivered payload: %s", body)
		for _, fp := range distinctFingerprints {
			require.NotContainsf(t, string(body), fp,
				"fingerprint %q leaked into the delivered payload: %s", fp, body)
		}
	}

	// (b) DB METERING: the grain is metered into activation_records_reports, then the
	// flusher drains the table back to zero. The metered-rows>0 probe is a transient
	// check, but safe: the flusher excludes rows newer than recentExclusionWindow (1s)
	// and only flushes a full flushWindow (1s), so a metered row lives >=2s — far
	// longer than the 100ms poll interval.
	require.Eventually(t, func() bool {
		n, err := countActivationRecords(tc.postgresResource)
		return err == nil && n > 0
	}, time.Minute, 100*time.Millisecond, "activation_records_reports should be metered for the grain")
	require.Eventually(t, func() bool {
		n, err := countActivationRecords(tc.postgresResource)
		return err == nil && n == 0
	}, time.Minute, 100*time.Millisecond, "activation_records_reports should drain to zero after flush")

	// (c) REPORTING POST: the mock reporting service received the grain, with fields
	// matching the connection and an exact unioned HLL cardinality.
	key := grainKey(workspaceID, sourceID, destinationID, instanceID)
	require.Eventually(t, func() bool {
		_, ok := tc.reportingServer.snapshot(key)
		return ok
	}, time.Minute, 100*time.Millisecond, "reporting service should receive the activation records grain")
	snap, ok := tc.reportingServer.snapshot(key)
	require.True(t, ok)
	require.Equal(t, workspaceID, snap.workspaceID)
	require.Equal(t, sourceID, snap.sourceID)
	require.Equal(t, destinationID, snap.destinationID)
	require.Equal(t, instanceID, snap.instanceID)
	require.Equal(t, origin, snap.origin)
	// Fail-closed guard: assert the SPECIFIC nonzero cardinality so a vacuously
	// passing (under-metered) pipeline is caught. The duplicate fingerprint must
	// not inflate it.
	require.Equal(t, len(distinctFingerprints), snap.cardinality,
		"unioned HLL cardinality must equal the number of distinct fingerprints sent")

	cancel()
	require.NoError(t, wg.Wait())
}

// countActivationRecords returns the number of rows metered for the test grain.
// It tolerates the table not yet existing (migration runs at app boot) by
// returning the error to the caller's eventually loop instead of failing.
func countActivationRecords(db *postgres.Resource) (int, error) {
	var count int
	err := db.DB.QueryRow(
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE workspace_id = $1 AND source_id = $2 AND destination_id = $3 AND origin = $4", activationRecordsTable),
		workspaceID, sourceID, destinationID, origin,
	).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
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

	webhook := webhookutil.NewRecorder()
	t.Cleanup(webhook.Close)
	webhookURL := webhook.Server.URL

	reportingServer := newMockReportingServer()
	t.Cleanup(reportingServer.Close)

	trServer := transformertest.NewBuilder().
		WithDestTransformHandler(
			"WEBHOOK",
			transformertest.RESTJSONDestTransformerHandler(http.MethodPost, webhookURL),
		).
		Build()
	t.Cleanup(trServer.Close)

	// rETL source -> WEBHOOK destination. SourceBuilder.WithConnection puts the
	// destination into source.Destinations so the gateway's authDestIDForSource can
	// resolve the X-Rudder-Destination-Id header into the job's destination_id
	// parameter (which MAR metering requires). The source carries the "warehouse"
	// category (reverse-ETL): MAR meters warehouse sources only, so the gateway stamps
	// source_category=warehouse into the job params for these records to be metered.
	// ConfigBuilder.WithConnection additionally registers the top-level connection the
	// router requires before it will deliver warehouse-source jobs (router/worker.go).
	bcServer := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(backendconfigtest.NewConfigBuilder().
			WithWorkspaceID(workspaceID).
			WithSource(
				backendconfigtest.NewSourceBuilder().
					WithWorkspaceID(workspaceID).
					WithID(sourceID).
					WithSourceCategory("warehouse").
					WithConnection(
						backendconfigtest.NewDestinationBuilder("WEBHOOK").
							WithID(destinationID).
							WithConfigOption("webhookMethod", "POST").
							WithConfigOption("webhookUrl", webhookURL).
							Build()).
					Build()).
			WithConnection(sourceID+":"+destinationID, backendconfig.Connection{
				SourceID:         sourceID,
				DestinationID:    destinationID,
				Enabled:          true,
				ProcessorEnabled: true,
			}).
			Build()).
		Build()
	t.Cleanup(bcServer.Close)

	return testConfig{
		postgresResource: postgresResource,
		gwPort:           gwPort,
		webhook:          webhook,
		configBEServer:   bcServer,
		transformerURL:   trServer.URL,
		reportingServer:  reportingServer,
	}
}

func runRudderServer(t testing.TB, ctx context.Context, cancel context.CancelFunc, tc testConfig) (err error) {
	t.Setenv("CONFIG_BACKEND_URL", tc.configBEServer.URL)
	t.Setenv("WORKSPACE_TOKEN", "token")
	t.Setenv("DEST_TRANSFORM_URL", tc.transformerURL)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.host"), tc.postgresResource.Host)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.port"), tc.postgresResource.Port)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.user"), tc.postgresResource.User)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.name"), tc.postgresResource.Database)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.password"), tc.postgresResource.Password)

	// Disable noise so the pipeline is deterministic and fast.
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Warehouse.mode"), "off")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DestinationDebugger.disableEventDeliveryStatusUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "SourceDebugger.disableEventUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "TransformationDebugger.disableTransformationStatusUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.backup.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.migrateDSLoopSleepDuration"), "60m")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "archival.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.syncer.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.webPort"), strconv.Itoa(tc.gwPort))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RUDDER_TMPDIR"), os.TempDir())
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.storagePath"), path.Join(t.TempDir(), "/recovery_data.json"))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Profiler.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.enableSuppressUserFeature"), "false")

	// Fast processor/router loops keep the e2e well under a couple of minutes.
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.maxLoopProcessEvents"), "10")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.readLoopSleep"), "50ms")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.maxLoopSleep"), "100ms")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Router.readSleep"), "50ms")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Router.maxStatusUpdateWait"), "50ms")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Router.updateStatusBatchSize"), "5")

	// Enable MAR metering + reporting, and the fixed instance id that lands in the
	// instance_id column / aggregation grain. INSTANCE_ID is already an env-style
	// key, so config.GetStringVar reads it verbatim (no RSERVER_ prefix).
	t.Setenv("INSTANCE_ID", instanceID)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "ActivationRecords.enabled"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.enabled"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "REPORTING_URL"), tc.reportingServer.Server.URL)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.flusher.flushWindow"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.flusher.recentExclusionWindowInSeconds"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.flusher.sleepInterval"), "2s")

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, cancel, []string{"activation-records-reporting"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return err
}

// retl payload envelope: a SingularEventBatch whose elements carry
// context.activation.{fingerprint,origin}, sent to the rETL record endpoint.
type retlBatch struct {
	Batch []retlEvent `json:"batch"`
}

type retlEvent struct {
	Type      string       `json:"type"`
	MessageID string       `json:"messageId"`
	UserID    string       `json:"userId"`
	Context   retlEventCtx `json:"context"`
}

type retlEventCtx struct {
	Activation activationCtx `json:"activation"`
}

type activationCtx struct {
	Fingerprint string `json:"fingerprint"`
	Origin      string `json:"origin"`
}

// sendActivationRecords posts one rETL batch carrying the given fingerprints.
// Each event has a unique messageId (so none are deduped) and a shared origin
// (so all land on the same grain). Mirrors retl_test/sut.go SendRETL.
func sendActivationRecords(t *testing.T, gwURL string, fingerprints []string) {
	t.Helper()

	events := make([]retlEvent, len(fingerprints))
	for i, fp := range fingerprints {
		events[i] = retlEvent{
			Type:      "identify",
			MessageID: fmt.Sprintf("message-%d", i),
			UserID:    fmt.Sprintf("user-%d", i),
			Context:   retlEventCtx{Activation: activationCtx{Fingerprint: fp, Origin: origin}},
		}
	}

	body, err := jsonrs.Marshal(retlBatch{Batch: events})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, gwURL+"/internal/v1/retl", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Rudder-Source-Id", sourceID)
	req.Header.Set("X-Rudder-Destination-Id", destinationID)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { kithttputil.CloseResponse(resp) }()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "retl ingestion failed: %s", respBody)
}
