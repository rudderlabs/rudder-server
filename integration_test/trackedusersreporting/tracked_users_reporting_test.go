package trackedusersreporting

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
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

	"github.com/samber/lo"

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
	reportingServer  *mockReportingServer
}

type userIdentifier struct {
	userID      string
	anonymousID string
}

type mockReportingServer struct {
	*webhookutil.Recorder
}

func TestTrackedUsersReporting(t *testing.T) {
	tc := setup(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		err := runRudderServer(t, ctx, tc)
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

	require.Eventuallyf(t, func() bool {
		cardinalityMap := tc.reportingServer.getCardinalityFromReportingServer(t)
		return cardinalityMap[workspaceID][sourceID].userIDCount == 2 &&
			cardinalityMap[workspaceID][sourceID].anonIDCount == 2 &&
			cardinalityMap[workspaceID][sourceID].identifiedUsersCount == 2
	}, 1*time.Minute, 5*time.Second, "data not reported to reporting service, count of reqs: %d", tc.reportingServer.RequestsCount())

	cancel()
	require.NoError(t, wg.Wait())
}

func (m *mockReportingServer) getCardinalityFromReportingServer(t *testing.T) map[string]map[string]struct {
	userIDCount          int
	anonIDCount          int
	identifiedUsersCount int
} {
	type trackedUsersEntry struct {
		ReportedAt                  time.Time `json:"reportedAt"`
		WorkspaceID                 string    `json:"workspaceId"`
		SourceID                    string    `json:"sourceId"`
		InstanceID                  string    `json:"instanceId"`
		UserIDHLLHex                string    `json:"userIdHLL"`
		AnonymousIDHLLHex           string    `json:"anonymousIdHLL"`
		IdentifiedAnonymousIDHLLHex string    `json:"identifiedAnonymousIdHLL"`
	}
	entries := make([]trackedUsersEntry, 0)
	for _, req := range m.Requests() {
		unmarshalledReqs := make([]trackedUsersEntry, 0)
		err := json.NewDecoder(req.Body).Decode(&unmarshalledReqs)
		require.NoError(t, err)
		entries = append(entries, unmarshalledReqs...)
	}
	result := make(map[string]map[string]struct {
		userIDHll          *hll.Hll
		anonIDHll          *hll.Hll
		identifiedUsersHll *hll.Hll
	})
	for _, e := range entries {
		if result[e.WorkspaceID] == nil {
			result[e.WorkspaceID] = make(map[string]struct {
				userIDHll          *hll.Hll
				anonIDHll          *hll.Hll
				identifiedUsersHll *hll.Hll
			})
		}
		cardinalityMap := result[e.WorkspaceID][e.SourceID]
		userHllBytes, err := hex.DecodeString(e.UserIDHLLHex)
		require.NoError(t, err)
		userHll, err := hll.FromBytes(userHllBytes)
		require.NoError(t, err)
		if cardinalityMap.userIDHll == nil {
			cardinalityMap.userIDHll = &userHll
		} else {
			cardinalityMap.userIDHll.Union(userHll)
		}
		annIDHllBytes, err := hex.DecodeString(e.AnonymousIDHLLHex)
		require.NoError(t, err)
		annHll, err := hll.FromBytes(annIDHllBytes)
		require.NoError(t, err)
		if cardinalityMap.anonIDHll == nil {
			cardinalityMap.anonIDHll = &annHll
		} else {
			cardinalityMap.anonIDHll.Union(annHll)
		}
		combineHllBytes, err := hex.DecodeString(e.IdentifiedAnonymousIDHLLHex)
		require.NoError(t, err)
		combHll, err := hll.FromBytes(combineHllBytes)
		require.NoError(t, err)
		if cardinalityMap.identifiedUsersHll == nil {
			cardinalityMap.identifiedUsersHll = &combHll
		} else {
			cardinalityMap.identifiedUsersHll.Union(combHll)
		}
		result[e.WorkspaceID][e.SourceID] = cardinalityMap
	}
	return lo.MapEntries(result, func(workspaceID string, mp map[string]struct {
		userIDHll          *hll.Hll
		anonIDHll          *hll.Hll
		identifiedUsersHll *hll.Hll
	}) (string, map[string]struct {
		userIDCount          int
		anonIDCount          int
		identifiedUsersCount int
	},
	) {
		return workspaceID, lo.MapEntries(mp, func(sourceID string, value struct {
			userIDHll          *hll.Hll
			anonIDHll          *hll.Hll
			identifiedUsersHll *hll.Hll
		}) (string, struct {
			userIDCount          int
			anonIDCount          int
			identifiedUsersCount int
		},
		) {
			return sourceID, struct {
				userIDCount          int
				anonIDCount          int
				identifiedUsersCount int
			}{
				userIDCount:          int(value.userIDHll.Cardinality()),
				anonIDCount:          int(value.anonIDHll.Cardinality()),
				identifiedUsersCount: int(value.identifiedUsersHll.Cardinality()),
			}
		})
	})
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

	reportingServer := &mockReportingServer{
		Recorder: webhookutil.NewRecorder(),
	}
	t.Cleanup(reportingServer.Close)

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
					WithSourceCategory("webhook").
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
		reportingServer:  reportingServer,
	}
}

func runRudderServer(
	t testing.TB,
	ctx context.Context,
	tc testConfig,
) (err error) {
	t.Setenv("CONFIG_BACKEND_URL", tc.configBEServer.URL)
	t.Setenv("WORKSPACE_TOKEN", "token")
	t.Setenv("DEST_TRANSFORM_URL", tc.transformerUrl)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.port"), tc.postgresResource.Port)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.user"), tc.postgresResource.User)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.name"), tc.postgresResource.Database)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.password"), tc.postgresResource.Password)

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
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.webPort"), strconv.Itoa(tc.gwPort))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RUDDER_TMPDIR"), os.TempDir())
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.storagePath"), path.Join(t.TempDir(), "/recovery_data.json"))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Profiler.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.enableSuppressUserFeature"), "false")
	// enable tracked users feature
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "TrackedUsers.enabled"), "true")
	// setup reporting server
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "REPORTING_URL"), tc.reportingServer.Server.URL)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.flusher.flushWindow"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.flusher.recentExclusionWindowInSeconds"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.flusher.sleepInterval"), "2s")
	// so that multiple processor batches are processed
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.maxLoopProcessEvents"), "10")
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
		// generate 1 or more events
		num := 1 + rand.Intn(100)
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
