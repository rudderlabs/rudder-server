package processor_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

func TestProcessorBotEnrichment(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		new(botScenario).
			WithBotEnrichmentEnabled(false).
			WithBotInfo(true, "flag", "test-bot", "https://test-bot.com", false).
			Run(t, func(t *testing.T, event string) {
				// missing fields checks
				require.False(t, gjson.Get(event, "context.isBot").Exists(), "no bot information should be present when feature is disabled")
				require.False(t, gjson.Get(event, "context.bot").Exists(), "bot key should not be present when feature is disabled")
			})
	})

	t.Run("feature enabled with non-bot event", func(t *testing.T) {
		new(botScenario).
			WithBotEnrichmentEnabled(true).
			WithBotInfo(false, "flag", "", "", false).
			Run(t, func(t *testing.T, event string) {
				// missing fields checks
				require.False(t, gjson.Get(event, "context.isBot").Exists(), "no bot information should be present for non-bot event")
				require.False(t, gjson.Get(event, "context.bot").Exists(), "bot key should not be present for non-bot event")
			})
	})

	t.Run("feature enabled with bot event", func(t *testing.T) {
		new(botScenario).
			WithBotEnrichmentEnabled(true).
			WithBotInfo(true, "flag", "test-bot", "https://test-bot.com", false).
			Run(t, func(t *testing.T, event string) {
				require.True(t, gjson.Get(event, "context.isBot").Bool(), "isBot should be true")
				require.Equal(t, "test-bot", gjson.Get(event, "context.bot.name").String(), "bot name should be set")
				require.Equal(t, "https://test-bot.com", gjson.Get(event, "context.bot.url").String(), "bot URL should be set")
				// missing fields checks
				require.False(t, gjson.Get(event, "context.bot.isInvalidBrowser").Exists(), "isInvalidBrowser should not be present when false")
			})
	})

	t.Run("feature enabled with bot details and empty bot URL", func(t *testing.T) {
		new(botScenario).
			WithBotEnrichmentEnabled(true).
			WithBotInfo(true, "flag", "test-bot", "", false).
			Run(t, func(t *testing.T, event string) {
				require.True(t, gjson.Get(event, "context.isBot").Bool(), "isBot should be true")
				require.Equal(t, "test-bot", gjson.Get(event, "context.bot.name").String(), "bot name should be set")
				// missing fields checks
				require.False(t, gjson.Get(event, "context.bot.url").Exists(), "bot URL should not be present when empty")
				require.False(t, gjson.Get(event, "context.bot.isInvalidBrowser").Exists(), "isInvalidBrowser should not be present when false")
			})
	})

	t.Run("feature enabled with invalid browser", func(t *testing.T) {
		new(botScenario).
			WithBotEnrichmentEnabled(true).
			WithBotInfo(true, "flag", "", "", true).
			Run(t, func(t *testing.T, event string) {
				require.True(t, gjson.Get(event, "context.isBot").Bool(), "isBot should be true")
				require.True(t, gjson.Get(event, "context.bot.isInvalidBrowser").Bool(), "isInvalidBrowser should be true")
				// missing fields checks
				require.False(t, gjson.Get(event, "context.bot.name").Exists(), "bot name should not be present when invalid browser")
				require.False(t, gjson.Get(event, "context.bot.url").Exists(), "bot URL should not be present when invalid browser")
			})
	})

	t.Run("feature enabled with bot event and flag BotAction", func(t *testing.T) {
		new(botScenario).
			WithBotEnrichmentEnabled(true).
			WithBotInfo(true, "flag", "test-bot", "https://test-bot.com", false).
			Run(t, func(t *testing.T, event string) {
				require.True(t, gjson.Get(event, "context.isBot").Bool(), "isBot should be true")
				require.Equal(t, "test-bot", gjson.Get(event, "context.bot.name").String(), "bot name should be set")
				require.Equal(t, "https://test-bot.com", gjson.Get(event, "context.bot.url").String(), "bot URL should be set")
				// missing fields checks
				require.False(t, gjson.Get(event, "context.bot.isInvalidBrowser").Exists(), "isInvalidBrowser should not be present when false")
			})
	})

	t.Run("feature enabled with bot event and disable BotAction", func(t *testing.T) {
		new(botScenario).
			WithBotEnrichmentEnabled(true).
			WithBotInfo(true, "disable", "test-bot", "https://test-bot.com", false).
			Run(t, func(t *testing.T, event string) {
				// missing fields checks
				require.False(t, gjson.Get(event, "context.isBot").Exists(), "no bot information should be present when BotAction is disable")
				require.False(t, gjson.Get(event, "context.bot").Exists(), "bot key should not be present when BotAction is disable")
			})
	})
}

type botScenario struct {
	botEnrichmentEnabled bool
	isBot                bool
	botAction            string
	botName              string
	botURL               string
	isInvalidBrowser     bool
}

func (s *botScenario) WithBotEnrichmentEnabled(enabled bool) *botScenario {
	s.botEnrichmentEnabled = enabled
	return s
}

func (s *botScenario) WithBotInfo(isBot bool, botAction, name, url string, isInvalidBrowser bool) *botScenario {
	s.isBot = isBot
	s.botAction = botAction
	s.botName = name
	s.botURL = url
	s.isInvalidBrowser = isInvalidBrowser
	return s
}

func (s *botScenario) Run(t *testing.T, verification func(t *testing.T, event string)) {
	config.Reset()
	defer config.Reset()
	writeKey := "writekey-1"
	workspaceID := "workspace-1"
	sourceID := "source-1"

	gatewayUrl, db, cancel, wg := s.startAll(t, writeKey, workspaceID, sourceID)
	defer func() {
		cancel()
		_ = wg.Wait()
	}()
	require.NoError(t, s.sendEvent(gatewayUrl, writeKey, workspaceID, sourceID))

	s.requireJobsCount(t, db, "gw", "succeeded", 1)
	s.requireJobsCount(t, db, "rt", "aborted", 1)
	var payload string
	require.NoError(t, db.QueryRow("SELECT event_payload FROM rt_jobs_1").Scan(&payload))
	verification(t, payload)
}

func (s *botScenario) startAll(t *testing.T, writeKey, workspaceID, sourceID string) (gatewayUrl string, db *sql.DB, cancel context.CancelFunc, wg *errgroup.Group) {
	ctx, cancel := context.WithCancel(context.Background())
	wg, ctx = errgroup.WithContext(ctx)

	bcserver := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(
			backendconfigtest.NewConfigBuilder().
				WithWorkspaceID(workspaceID).
				WithSource(
					backendconfigtest.NewSourceBuilder().
						WithID(sourceID).
						WithWriteKey(writeKey).
						WithConnection(
							backendconfigtest.NewDestinationBuilder("WEBHOOK").
								WithID("destination-1").
								Build()).
						Build()).
				Build()).
		Build()

	trServer := transformertest.NewBuilder().Build()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	wg.Go(func() error {
		err := s.runRudderServer(ctx, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir())
		if err != nil {
			t.Logf("rudder-server exited with error: %v", err)
		} else {
			t.Log("rudder-server stopped")
		}
		bcserver.Close()
		trServer.Close()
		return err
	})
	url := fmt.Sprintf("http://localhost:%d", gwPort)
	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	return url, postgresContainer.DB, cancel, wg
}

func (s *botScenario) runRudderServer(ctx context.Context, port int, postgresContainer *postgres.Resource, cbURL, transformerURL, tmpDir string) (err error) {
	config.Set("enableStats", false)
	config.Set("CONFIG_BACKEND_URL", cbURL)
	config.Set("WORKSPACE_TOKEN", "token")
	config.Set("DB.host", postgresContainer.Host)
	config.Set("DB.port", postgresContainer.Port)
	config.Set("DB.user", postgresContainer.User)
	config.Set("DB.name", postgresContainer.Database)
	config.Set("DB.password", postgresContainer.Password)
	config.Set("DEST_TRANSFORM_URL", transformerURL)

	config.Set("Warehouse.mode", "off")
	config.Set("DestinationDebugger.disableEventDeliveryStatusUploads", true)
	config.Set("SourceDebugger.disableEventUploads", true)
	config.Set("TransformationDebugger.disableTransformationStatusUploads", true)
	config.Set("JobsDB.backup.enabled", false)
	config.Set("JobsDB.migrateDSLoopSleepDuration", "60m")
	config.Set("archival.Enabled", false)
	config.Set("Reporting.syncer.enabled", false)
	config.Set("BatchRouter.pingFrequency", "1s")
	config.Set("BatchRouter.uploadFreq", "1s")
	config.Set("Gateway.webPort", strconv.Itoa(port))
	config.Set("RUDDER_TMPDIR", tmpDir)
	config.Set("recovery.storagePath", path.Join(tmpDir, "/recovery_data.json"))
	config.Set("recovery.enabled", false)
	config.Set("Profiler.Enabled", false)
	config.Set("Router.toAbortDestinationIDs", "destination-1")
	config.Set("Gateway.enableSuppressUserFeature", false)
	config.Set("Router.readSleep", "10ms")
	config.Set("Processor.pingerSleep", "10ms")
	config.Set("Processor.readLoopSleep", "10ms")
	config.Set("Processor.maxLoopSleep", "10ms")
	config.Set("AdminServer.enabled", false)
	config.Set("BotEnrichment.enabled", s.botEnrichmentEnabled)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "TOKEN"})
	c := r.Run(ctx, []string{"proc-bot-test-rudder-server"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return err
}

func (s *botScenario) sendEvent(url, writeKey, workspaceID, sourceID string) error {
	payload := []byte(fmt.Sprintf(`[
		{
			"properties": {
				"isBot": %v,
				"botAction": %q,
				"botName": %q,
				"botURL": %q,
				"botIsInvalidBrowser": %v,
				"routingKey": "a1",
				"requestType": "identify",
				"workspaceID": "%s",
				"userID": "identified_user_id_1",
				"sourceID": "%s",
				"requestIP": "1.2.3.4",
				"receivedAt": "2024-01-01T01:01:01.000000001Z"
			},
			"payload": {
				"userId": "identified_user_id_1",
				"anonymousId": "anonymousId_1",
				"type": "identify"
			}
		}
	]`, s.isBot, s.botAction, s.botName, s.botURL, s.isInvalidBrowser, workspaceID, sourceID))
	req, err := http.NewRequest("POST", url+"/internal/v1/batch", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.SetBasicAuth(writeKey, "password")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to send event to rudder server, status code: %d: %s", resp.StatusCode, string(b))
	}
	func() { kithttputil.CloseResponse(resp) }()

	return nil
}

func (s *botScenario) requireJobsCount(t *testing.T, db *sql.DB, queue, state string, expectedCount int) {
	require.Eventually(t, func() bool {
		var jobsCount int
		err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM unionjobsdbmetadata('%s',10) WHERE job_state = '%s'", queue, state)).Scan(&jobsCount)
		t.Logf("%s %s Count: %d", queue, state, jobsCount)
		return err == nil && jobsCount == expectedCount
	}, 20*time.Second, 1*time.Second, fmt.Sprintf("%d %s events should be in %s state", expectedCount, queue, state))
}
