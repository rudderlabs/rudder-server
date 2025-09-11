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
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
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

func TestProcessorEventDropping(t *testing.T) {
	t.Run("bot events with drop action should be dropped", func(t *testing.T) {
		new(eventDropScenario).
			withEventConfigs([]eventConfig{
				newBotEventConfig("track", "TestEvent", "drop"),
				newBotEventConfig("identify", "", "drop"),
				newBotEventConfig("screen", "", "drop"),
			}).
			run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 3)
				scenario.requireJobsCount(t, "rt", "succeeded", 0)
				scenario.requireJobsCount(t, "rt", "aborted", 0)
				scenario.requireJobsCount(t, "rt", "failed", 0)

				scenario.requireTotalJobsCount(t, "rt", 0)

				// reporting metrics
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "track", eventName: "TestEvent", botDroppedCount: 1})
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "identify", eventName: "", botDroppedCount: 1})
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "screen", eventName: "", botDroppedCount: 1})

				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "track", eventName: "TestEvent", ingestedCount: 0})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "identify", eventName: "", ingestedCount: 0})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "screen", eventName: "", ingestedCount: 0})
			})
	})

	t.Run("bot events with flag action should not be dropped", func(t *testing.T) {
		new(eventDropScenario).
			withEventConfigs([]eventConfig{
				newBotEventConfig("identify", "", "flag"),
				newBotEventConfig("track", "TestEvent", "flag"),
				newBotEventConfig("screen", "", "flag"),
			}).
			run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 3)
				scenario.requireJobsCount(t, "rt", "aborted", 3)

				// reporting metrics
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "identify", eventName: "", botDetectedCount: 1})
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "track", eventName: "TestEvent", botDetectedCount: 1})
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "screen", eventName: "", botDetectedCount: 1})

				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "identify", eventName: "", ingestedCount: 1, botFlaggedCount: 1})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "track", eventName: "TestEvent", ingestedCount: 1, botFlaggedCount: 1})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "screen", eventName: "", ingestedCount: 1, botFlaggedCount: 1})
			})
	})

	t.Run("blocked events should be dropped", func(t *testing.T) {
		new(eventDropScenario).
			withBlockedEventsConfig(map[string][]string{
				"track": {"user-login", "user-logout", "add-to-cart"},
			}).
			withEventConfigs([]eventConfig{
				newTrackEventConfig("user-login"),
				newTrackEventConfig("user-logout"),
				newTrackEventConfig("add-to-cart"),
			}).
			run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 3)
				scenario.requireJobsCount(t, "rt", "succeeded", 0)
				scenario.requireJobsCount(t, "rt", "aborted", 0)
				scenario.requireJobsCount(t, "rt", "failed", 0)

				scenario.requireTotalJobsCount(t, "rt", 0)

				// reporting metrics
				scenario.requireReportsFromEventBlocking(t, "track", "user-login", 1)
				scenario.requireReportsFromEventBlocking(t, "track", "user-logout", 1)
				scenario.requireReportsFromEventBlocking(t, "track", "add-to-cart", 1)

				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "track", eventName: "user-login", ingestedCount: 0})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "track", eventName: "user-logout", ingestedCount: 0})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "track", eventName: "add-to-cart", ingestedCount: 0})
			})
	})

	t.Run("non blocked events should not be dropped", func(t *testing.T) {
		new(eventDropScenario).
			withBlockedEventsConfig(map[string][]string{
				"track": {"user-login", "user-logout", "add-to-cart"},
			}).
			withEventConfigs([]eventConfig{
				newIdentifyEventConfig(),
				newTrackEventConfig("test-event"),
			}).
			run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 2)
				scenario.requireJobsCount(t, "rt", "aborted", 2)

				// reporting metrics
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "identify", eventName: "", ingestedCount: 1})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "track", eventName: "test-event", ingestedCount: 1})

				scenario.requireReportsFromEventBlocking(t, "identify", "", 0)
				scenario.requireReportsFromEventBlocking(t, "track", "test-event", 0)
			})
	})

	t.Run("non-track events should not be blocked even if event name is blocked", func(t *testing.T) {
		new(eventDropScenario).
			withBlockedEventsConfig(map[string][]string{
				"track": {"BlockedEvent"},
			}).
			withEventConfigs([]eventConfig{
				{eventType: "identify", eventName: "BlockedEvent"},
				{eventType: "screen", eventName: "BlockedEvent"},
			}).
			run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 2)
				scenario.requireJobsCount(t, "rt", "aborted", 2)

				// reporting metrics
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "identify", eventName: "", ingestedCount: 1})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "screen", eventName: "", ingestedCount: 1})

				scenario.requireReportsFromEventBlocking(t, "identify", "BlockedEvent", 0)
				scenario.requireReportsFromEventBlocking(t, "screen", "BlockedEvent", 0)
			})
	})

	t.Run("mixed event types with different dropping conditions", func(t *testing.T) {
		new(eventDropScenario).
			withBlockedEventsConfig(map[string][]string{
				"track": {"BlockedEvent"},
			}).
			withEventConfigs([]eventConfig{
				newTrackEventConfig("NormalEvent"),                 // Normal track event - should reach router
				newBotEventConfig("track", "TestEvent", "drop"),    // Bot track event - should be dropped
				newTrackEventConfig("BlockedEvent"),                // Blocked track event - should be dropped
				newBotEventConfig("identify", "", "drop"),          // Bot identify event - should be dropped
				newIdentifyEventConfig(),                           // Normal identify event - should reach router
				newBotEventConfig("screen", "", "flag"),            // Bot screen event with flag action - should reach router
				newBotEventConfig("track", "BlockedEvent", "drop"), // Bot track event with drop action and blocked event - should be dropped
			}).
			run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 7)
				scenario.requireJobsCount(t, "rt", "aborted", 3)
				scenario.requireJobsCount(t, "rt", "succeeded", 0)
				scenario.requireJobsCount(t, "rt", "failed", 0)

				// reporting metrics
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "track", eventName: "TestEvent", botDetectedCount: 0, botDroppedCount: 1})
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "track", eventName: "NormalEvent", botDetectedCount: 0, botDroppedCount: 0})
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "track", eventName: "BlockedEvent", botDetectedCount: 0, botDroppedCount: 1})
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "identify", eventName: "", botDetectedCount: 0, botDroppedCount: 1})
				scenario.requireReportsFromBotManagement(t, botManagementReportExpectations{eventType: "screen", eventName: "", botDetectedCount: 1, botDroppedCount: 0})

				scenario.requireReportsFromEventBlocking(t, "track", "NormalEvent", 0)
				scenario.requireReportsFromEventBlocking(t, "track", "TestEvent", 0)    // this event was dropped by bot management
				scenario.requireReportsFromEventBlocking(t, "track", "BlockedEvent", 1) // only one event was blocked other event was dropped by bot management
				scenario.requireReportsFromEventBlocking(t, "identify", "", 0)
				scenario.requireReportsFromEventBlocking(t, "screen", "", 0)

				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "track", eventName: "NormalEvent", ingestedCount: 1})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "track", eventName: "TestEvent", ingestedCount: 0})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "track", eventName: "BlockedEvent", ingestedCount: 0})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "identify", eventName: "", ingestedCount: 1})
				scenario.requireReportsFromGateway(t, gatewayReportExpectations{eventType: "screen", eventName: "", ingestedCount: 1, botFlaggedCount: 1})
			})
	})
}

type eventConfig struct {
	eventType string
	eventName string
	isBot     bool
	botAction string
}

type eventDropScenario struct {
	db                  *sql.DB
	blockedEventsConfig map[string][]string
	eventConfigs        []eventConfig
}

type botManagementReportExpectations struct {
	eventType        string
	eventName        string
	botDetectedCount int
	botDroppedCount  int
}

type gatewayReportExpectations struct {
	eventType        string
	eventName        string
	ingestedCount    int
	botFlaggedCount  int
	botDetectedCount int
}

func (s *eventDropScenario) withBlockedEventsConfig(config map[string][]string) *eventDropScenario {
	s.blockedEventsConfig = config
	return s
}

func (s *eventDropScenario) withEventConfigs(configs []eventConfig) *eventDropScenario {
	s.eventConfigs = configs
	return s
}

func (s *eventDropScenario) run(t *testing.T, verification func(t *testing.T, scenario *eventDropScenario)) {
	writeKey := "writekey-1"
	workspaceID := "workspace-1"
	sourceID := "source-1"

	gatewayUrl, db, cancel, wg := s.startAll(t, writeKey, workspaceID, sourceID)
	s.db = db
	defer func() {
		cancel()
		_ = wg.Wait()
	}()

	require.NoError(t, s.sendEvents(gatewayUrl, writeKey, workspaceID, sourceID))

	verification(t, s)
}

func (s *eventDropScenario) startAll(t *testing.T, writeKey, workspaceID, sourceID string) (gatewayUrl string, db *sql.DB, cancel context.CancelFunc, wg *errgroup.Group) {
	ctx, cancel := context.WithCancel(context.Background())
	wg, ctx = errgroup.WithContext(ctx)

	workspaceConfig := backendconfigtest.NewConfigBuilder().
		WithWorkspaceID(workspaceID).
		WithSource(
			backendconfigtest.NewSourceBuilder().
				WithID(sourceID).
				WithSourceCategory("webhook").
				WithWriteKey(writeKey).
				WithConnection(
					backendconfigtest.NewDestinationBuilder("WEBHOOK").
						WithID("destination-1").
						Build()).
				Build()).
		Build()

	// Set up event blocking configuration if specified
	if s.blockedEventsConfig != nil {
		workspaceConfig.Settings.EventBlocking.Events = s.blockedEventsConfig
	}

	bcserver := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(workspaceConfig).
		Build()

	trServer := transformertest.NewBuilder().Build()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	wg.Go(func() error {
		err := s.runRudderServer(ctx, t, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir())
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

func (s *eventDropScenario) runRudderServer(ctx context.Context, t *testing.T, port int, postgresContainer *postgres.Resource, cbURL, transformerURL, tmpDir string) (err error) {
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "enableStats"), "false")
	t.Setenv("CONFIG_BACKEND_URL", cbURL)
	t.Setenv("WORKSPACE_TOKEN", "token")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.host"), postgresContainer.Host)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.port"), postgresContainer.Port)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.user"), postgresContainer.User)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.name"), postgresContainer.Database)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.password"), postgresContainer.Password)
	t.Setenv("DEST_TRANSFORM_URL", transformerURL)

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
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RUDDER_TMPDIR"), tmpDir)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.storagePath"), path.Join(tmpDir, "/recovery_data.json"))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Profiler.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Router.toAbortDestinationIDs"), "destination-1")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.enableSuppressUserFeature"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Router.readSleep"), "10ms")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.pingerSleep"), "10ms")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.readLoopSleep"), "10ms")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.maxLoopSleep"), "10ms")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "AdminServer.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BotEnrichment.enabled"), "true")

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "TOKEN"})
	c := r.Run(ctx, []string{"proc-events-dropping-test-rudder-server"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return err
}

func (s *eventDropScenario) sendEvents(url, writeKey, workspaceID, sourceID string) error {
	if len(s.eventConfigs) == 0 {
		return fmt.Errorf("eventConfigs must be provided")
	}
	var wrappedEvents []string
	for i, config := range s.eventConfigs {
		var eventPayload string
		if config.eventType == "track" {
			eventPayload = fmt.Sprintf(`{
				"userId": "user_%d",
				"anonymousId": "anonymousId_%d",
				"type": "track",
				"event": "%s",
				"properties": {
					"testProperty": "testValue"
				}
			}`, i+1, i+1, config.eventName)
		} else {
			eventPayload = fmt.Sprintf(`{
				"userId": "user_%d",
				"anonymousId": "anonymousId_%d",
				"type": "identify",
				"traits": {
					"testTrait": "testValue"
				}
			}`, i+1, i+1)
		}

		// Wrap each event with its properties
		wrappedEvent := fmt.Sprintf(`{
			"properties": {
				"isBot": %v,
				"botAction": "%s",
				"routingKey": "a1",
				"requestType": "%s",
				"workspaceID": "%s",
				"userID": "user_%d",
				"sourceID": "%s",
				"requestIP": "1.2.3.4",
				"receivedAt": "2024-01-01T01:01:01.000000001Z"
			},
			"payload": %s
		}`, config.isBot, config.botAction, config.eventType, workspaceID, i+1, sourceID, eventPayload)

		wrappedEvents = append(wrappedEvents, wrappedEvent)
	}

	// Build the final payload as an array of wrapped events
	payload := []byte(fmt.Sprintf("[%s]", strings.Join(wrappedEvents, ",\n")))

	return s.sendHTTPRequest(url, writeKey, payload)
}

func (s *eventDropScenario) sendHTTPRequest(url, writeKey string, payload []byte) error {
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

func (s *eventDropScenario) requireJobsCount(t *testing.T, queue, state string, expectedCount int) {
	require.Eventually(t, func() bool {
		var jobsCount int
		err := s.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM unionjobsdbmetadata('%s',10) WHERE job_state = '%s'", queue, state)).Scan(&jobsCount)
		t.Logf("%s %s Count: %d", queue, state, jobsCount)
		return err == nil && jobsCount == expectedCount
	}, 20*time.Second, 1*time.Second, fmt.Sprintf("%d %s events should be in %s state", expectedCount, queue, state))
}

func (s *eventDropScenario) requireTotalJobsCount(t *testing.T, queue string, expectedCount int) {
	require.Eventually(t, func() bool {
		var jobsCount int
		err := s.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM unionjobsdbmetadata('%s',10)", queue)).Scan(&jobsCount)
		t.Logf("%s Total Count: %d", queue, jobsCount)
		return err == nil && jobsCount == expectedCount
	}, 20*time.Second, 1*time.Second, fmt.Sprintf("%d total events should be in %s queue", expectedCount, queue))
}

func (s *eventDropScenario) requireReportsFromBotManagement(t *testing.T, expectations botManagementReportExpectations) {
	require.Eventually(t, func() bool {
		var detectedCount int
		var droppedCount int
		commonLabel := `workspace_id = 'workspace-1' AND source_id = 'source-1' AND destination_id = '' AND in_pu = '' AND pu = 'bot_management' AND error_type = '' AND initial_state IS FALSE AND terminal_state IS FALSE AND event_type = $1 AND event_name = $2`

		err := s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status_code = 200 AND status = 'bot_detected'", commonLabel), expectations.eventType, expectations.eventName).Scan(&detectedCount)
		t.Logf("Bot events detected reports count: %d", detectedCount)

		if err != nil || detectedCount != expectations.botDetectedCount {
			t.Logf("Bot management reports not matching expectations for %s event '%s': expected detected=%d, got=%d", expectations.eventType, expectations.eventName, expectations.botDetectedCount, detectedCount)
			return false
		}

		err = s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status_code = 298 AND status = 'filtered'", commonLabel), expectations.eventType, expectations.eventName).Scan(&droppedCount)
		t.Logf("Bot events dropped reports count: %d", droppedCount)

		if err != nil || droppedCount != expectations.botDroppedCount {
			t.Logf("Bot management reports not matching expectations for %s event '%s': expected dropped=%d, got=%d", expectations.eventType, expectations.eventName, expectations.botDroppedCount, droppedCount)
			return false
		}

		return true
	}, 20*time.Second, 1*time.Second, fmt.Sprintf("Bot management reports not matching expectations for %s event '%s': expected detected=%d, dropped=%d",
		expectations.eventType, expectations.eventName, expectations.botDetectedCount, expectations.botDroppedCount))
}

func (s *eventDropScenario) requireReportsFromEventBlocking(t *testing.T, eventType, eventName string, expectedBlockedCount int) {
	require.Eventually(t, func() bool {
		var blockedCount int
		commonLabel := `workspace_id = 'workspace-1' AND source_id = 'source-1' AND destination_id = '' AND in_pu = '' AND pu = 'event_blocking' AND error_type = '' AND initial_state IS FALSE AND terminal_state IS FALSE AND event_type = $1 AND event_name = $2`

		err := s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status_code = 298 AND status = 'filtered'", commonLabel), eventType, eventName).Scan(&blockedCount)
		t.Logf("Event blocking reports count: %d", blockedCount)

		if err != nil || blockedCount != expectedBlockedCount {
			t.Logf("Event blocking reports not matching expectations for %s event '%s': expected blocked=%d, got=%d", eventType, eventName, expectedBlockedCount, blockedCount)
			return false
		}

		return true
	}, 20*time.Second, 1*time.Second, fmt.Sprintf("Event blocking reports not matching expectations for %s event '%s': expected blocked=%d",
		eventType, eventName, expectedBlockedCount))
}

func (s *eventDropScenario) requireReportsFromGateway(t *testing.T, expectations gatewayReportExpectations) {
	require.Eventually(t, func() bool {
		var ingestedCount int
		var botFlaggedCount int
		var botDetectedCount int
		commonLabel := `workspace_id = 'workspace-1' AND source_id = 'source-1' AND destination_id = '' AND in_pu = '' AND pu = 'gateway' AND status_code = 0 AND error_type = '' AND initial_state IS TRUE AND terminal_state IS FALSE AND event_type = $1 AND event_name = $2`

		err := s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status = 'bot_detected'", commonLabel), expectations.eventType, expectations.eventName).Scan(&botDetectedCount)
		t.Logf("Bot events detected reports count: %d", botDetectedCount)

		if err != nil || botDetectedCount != expectations.botDetectedCount {
			t.Logf("Gateway reports not matching expectations for %s event '%s': expected bot_detected=%d, got=%d", expectations.eventType, expectations.eventName, expectations.botDetectedCount, botDetectedCount)
			return false
		}

		err = s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status = 'bot_flagged'", commonLabel), expectations.eventType, expectations.eventName).Scan(&botFlaggedCount)
		t.Logf("Bot events flagged reports count: %d", botFlaggedCount)

		if err != nil || botFlaggedCount != expectations.botFlaggedCount {
			t.Logf("Gateway reports not matching expectations for %s event '%s': expected bot_flagged=%d, got=%d", expectations.eventType, expectations.eventName, expectations.botFlaggedCount, botFlaggedCount)
			return false
		}

		err = s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status = 'succeeded'", commonLabel), expectations.eventType, expectations.eventName).Scan(&ingestedCount)
		t.Logf("Bot events ingested reports count: %d", ingestedCount)

		if err != nil || ingestedCount != expectations.ingestedCount {
			t.Logf("Gateway reports not matching expectations for %s event '%s': expected ingested=%d, got=%d", expectations.eventType, expectations.eventName, expectations.ingestedCount, ingestedCount)
			return false
		}

		return true
	}, 20*time.Second, 1*time.Second, fmt.Sprintf("Gateway reports not matching expectations for %s event '%s': expected ingested=%d, bot_flagged=%d, bot_detected=%d",
		expectations.eventType, expectations.eventName, expectations.ingestedCount, expectations.botFlaggedCount, expectations.botDetectedCount))
}

// Helper functions to create eventConfig objects
func newTrackEventConfig(eventName string) eventConfig {
	return eventConfig{
		eventType: "track",
		eventName: eventName,
		isBot:     false,
		botAction: "",
	}
}

func newIdentifyEventConfig() eventConfig {
	return eventConfig{
		eventType: "identify",
		eventName: "",
		isBot:     false,
		botAction: "",
	}
}

func newBotEventConfig(eventType, eventName, botAction string) eventConfig {
	return eventConfig{
		eventType: eventType,
		eventName: eventName,
		isBot:     true,
		botAction: botAction,
	}
}
