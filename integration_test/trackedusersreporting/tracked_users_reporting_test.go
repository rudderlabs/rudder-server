package trackedusersreporting

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	nethttptest "net/http/httptest"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/testhelper/transformertest"

	"github.com/segmentio/go-hll"

	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	webhookutil "github.com/rudderlabs/rudder-server/testhelper/webhook"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	writeKey    = "writekey-1"
	sourceID    = "source-1"
	workspaceID = "workspace-1"
)

type testConfig struct {
	postgresResource *postgres.Resource
	gwPort           int
	webhook          *webhookutil.Recorder
	configBEServer   *nethttptest.Server
	transformerUrl   string
}

type userIdentifier struct {
	userID      string
	anonymousID string
}

func TestTrackedUsersReporting(t *testing.T) {
	tc := setup(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		err := runRudderServer(t, ctx, tc.gwPort, tc.postgresResource, tc.configBEServer.URL, t.TempDir(), tc.transformerUrl)
		if err != nil {
			t.Logf("rudder-server exited with error: %v", err)
		}
		return err
	})

	url := fmt.Sprintf("http://localhost:%d", tc.gwPort)
	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

	eventsCount, err := sendEvents([]userIdentifier{
		{userID: "user-1", anonymousID: "anon-1"},
		{userID: "user-2", anonymousID: "anon-2"},
		{userID: "user-1"},
		{anonymousID: "anon-1"},
		{userID: "user-2"},
		{anonymousID: "anon-2"},
	}, "identify", writeKey, url)
	require.NoError(t, err)

	require.Eventuallyf(t, func() bool {
		return tc.webhook.RequestsCount() == eventsCount
	}, 1*time.Minute, 5*time.Second, "unexpected number of events received, count of events: %d", tc.webhook.RequestsCount())

	cardinalityMap := getCardinalityFromDB(t, tc.postgresResource)
	require.Equal(t, 2, cardinalityMap[workspaceID][sourceID])
	cancel()
	require.NoError(t, wg.Wait())
}

func getCardinalityFromDB(t *testing.T, postgresResource *postgres.Resource) map[string]map[string]int {
	type trackedUsersEntry struct {
		WorkspaceID string
		SourceID    string
		InstanceID  string
		userIDHll   string
		annIDHll    string
		combHll     string
	}
	rows, err := postgresResource.DB.Query("SELECT workspace_id, source_id, instance_id, userid_hll, anonymousid_hll, identified_anonymousid_hll FROM tracked_users_reports")
	require.NoError(t, err)
	require.NoError(t, rows.Err())
	defer func() { _ = rows.Close() }()
	var entry trackedUsersEntry
	entries := make([]trackedUsersEntry, 0)
	for rows.Next() {
		err = rows.Scan(&entry.WorkspaceID, &entry.SourceID, &entry.InstanceID, &entry.userIDHll, &entry.annIDHll, &entry.combHll)
		require.NoError(t, err)
		entries = append(entries, entry)
	}
	result := make(map[string]map[string]int)
	for _, e := range entries {
		if result[e.WorkspaceID] == nil {
			result[e.WorkspaceID] = make(map[string]int)
		}
		userHllBytes, err := hex.DecodeString(e.userIDHll[2:])
		require.NoError(t, err)
		userHll, err := hll.FromBytes(userHllBytes)
		require.NoError(t, err)
		result[e.WorkspaceID][e.SourceID] += int(userHll.Cardinality())
		annIDHllBytes, err := hex.DecodeString(e.annIDHll[2:])
		require.NoError(t, err)
		annHll, err := hll.FromBytes(annIDHllBytes)
		require.NoError(t, err)
		result[e.WorkspaceID][e.SourceID] += int(annHll.Cardinality())
		combineHllBytes, err := hex.DecodeString(e.combHll[2:])
		require.NoError(t, err)
		combHll, err := hll.FromBytes(combineHllBytes)
		require.NoError(t, err)
		result[e.WorkspaceID][e.SourceID] -= int(combHll.Cardinality())
	}
	return result
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

	trServer := transformertest.NewBuilder().
		WithDestTransformHandler(
			"WEBHOOK",
			transformertest.RESTJSONDestTransformerHandler(http.MethodPost, webhookURL),
		).
		Build()
	t.Cleanup(trServer.Close)

	bcServer := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(backendconfigtest.NewConfigBuilder().
			WithWorkspaceID(workspaceID).
			WithSource(
				backendconfigtest.NewSourceBuilder().
					WithWorkspaceID(workspaceID).
					WithID(sourceID).
					WithWriteKey(writeKey).
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

	return testConfig{
		postgresResource: postgresResource,
		gwPort:           gwPort,
		webhook:          webhook,
		configBEServer:   bcServer,
		transformerUrl:   trServer.URL,
	}
}

func runRudderServer(
	t testing.TB,
	ctx context.Context,
	port int,
	postgresContainer *postgres.Resource,
	cbURL, tmpDir, transformerURL string,
) (err error) {
	t.Setenv("CONFIG_BACKEND_URL", cbURL)
	t.Setenv("WORKSPACE_TOKEN", "token")
	t.Setenv("DEST_TRANSFORM_URL", transformerURL)
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
	// enable tracked users feature
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "TrackedUsers.enabled"), "true")

	t.Setenv("Processor.maxRetry", strconv.Itoa(1))

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, []string{"tracked-users-reporting"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return
}

// nolint: bodyclose
func sendEvents(
	identifiers []userIdentifier,
	eventType, writeKey,
	url string,
) (int, error) {
	count := 0
	for _, identifier := range identifiers {
		num := rand.Intn(10)
		for i := 0; i < num; i++ {
			count++
			payload := []byte(fmt.Sprintf(`
			{
			  "batch": [
				{
				  "userId": %[1]q,
				  "type": %[2]q,
				  "anonymousId": %[3]q,
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
				identifier.userID,
				eventType,
				identifier.anonymousID,
			))
			req, err := http.NewRequest(http.MethodPost, url+"/v1/batch", bytes.NewReader(payload))
			if err != nil {
				return 0, err
			}
			req.SetBasicAuth(writeKey, "password")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return 0, err
			}

			if resp.StatusCode != http.StatusOK {
				b, _ := io.ReadAll(resp.Body)
				return 0, fmt.Errorf("failed to send event to rudder server, status code: %d: %s", resp.StatusCode, string(b))
			}
			kithttputil.CloseResponse(resp)
		}
	}
	return count, nil
}
