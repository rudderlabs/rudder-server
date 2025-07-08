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
			WithEventConfigs([]eventConfig{
				NewBotEventConfig("track", "TestEvent", "drop"),
				NewBotEventConfig("identify", "", "drop"),
				NewBotEventConfig("screen", "", "drop"),
			}).
			Run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 3)
				scenario.requireJobsCount(t, "rt", "succeeded", 0)
				scenario.requireJobsCount(t, "rt", "aborted", 0)
				scenario.requireJobsCount(t, "rt", "failed", 0)

				scenario.requireTotalJobsCount(t, "rt", 0)

				// reporting metrics
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "track", EventName: "TestEvent", ExpectedDroppedCount: 1})
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "identify", EventName: "", ExpectedDroppedCount: 1})
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "screen", EventName: "", ExpectedDroppedCount: 1})

				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "track", EventName: "TestEvent", ExpectedIngestedCount: 0})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "identify", EventName: "", ExpectedIngestedCount: 0})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "screen", EventName: "", ExpectedIngestedCount: 0})
			})
	})

	t.Run("bot events with flag action should not be dropped", func(t *testing.T) {
		new(eventDropScenario).
			WithEventConfigs([]eventConfig{
				NewBotEventConfig("identify", "", "flag"),
				NewBotEventConfig("track", "TestEvent", "flag"),
				NewBotEventConfig("screen", "", "flag"),
			}).
			Run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 3)
				scenario.requireJobsCount(t, "rt", "aborted", 3)

				// reporting metrics
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "identify", EventName: "", ExpectedDetectedCount: 1})
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "track", EventName: "TestEvent", ExpectedDetectedCount: 1})
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "screen", EventName: "", ExpectedDetectedCount: 1})

				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "identify", EventName: "", ExpectedIngestedCount: 1, ExpectedBotFlaggedCount: 1})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "track", EventName: "TestEvent", ExpectedIngestedCount: 1, ExpectedBotFlaggedCount: 1})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "screen", EventName: "", ExpectedIngestedCount: 1, ExpectedBotFlaggedCount: 1})
			})
	})

	t.Run("blocked events should be dropped", func(t *testing.T) {
		new(eventDropScenario).
			WithBlockedEventsConfig(map[string][]string{
				"track": {"user-login", "user-logout", "add-to-cart"},
			}).
			WithEventConfigs([]eventConfig{
				NewTrackEventConfig("user-login"),
				NewTrackEventConfig("user-logout"),
				NewTrackEventConfig("add-to-cart"),
			}).
			Run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 3)
				scenario.requireJobsCount(t, "rt", "succeeded", 0)
				scenario.requireJobsCount(t, "rt", "aborted", 0)
				scenario.requireJobsCount(t, "rt", "failed", 0)

				scenario.requireTotalJobsCount(t, "rt", 0)

				// reporting metrics
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "track", EventName: "user-login", ExpectedIngestedCount: 0})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "track", EventName: "user-logout", ExpectedIngestedCount: 0})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "track", EventName: "add-to-cart", ExpectedIngestedCount: 0})
			})
	})

	t.Run("non blocked events should not be dropped", func(t *testing.T) {
		new(eventDropScenario).
			WithBlockedEventsConfig(map[string][]string{
				"track": {"user-login", "user-logout", "add-to-cart"},
			}).
			WithEventConfigs([]eventConfig{
				NewIdentifyEventConfig(),
				NewTrackEventConfig("test-event"),
			}).
			Run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 2)
				scenario.requireJobsCount(t, "rt", "aborted", 2)

				// reporting metrics
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "identify", EventName: "", ExpectedIngestedCount: 1})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "track", EventName: "test-event", ExpectedIngestedCount: 1})
			})
	})

	t.Run("non-track events should not be blocked even if event name is blocked", func(t *testing.T) {
		new(eventDropScenario).
			WithBlockedEventsConfig(map[string][]string{
				"track": {"BlockedEvent"},
			}).
			WithEventConfigs([]eventConfig{
				{eventType: "identify", eventName: "BlockedEvent"},
				{eventType: "screen", eventName: "BlockedEvent"},
			}).
			Run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 2)
				scenario.requireJobsCount(t, "rt", "aborted", 2)

				// reporting metrics
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "identify", EventName: "", ExpectedIngestedCount: 1})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "screen", EventName: "", ExpectedIngestedCount: 1})
			})
	})

	t.Run("mixed event types with different dropping conditions", func(t *testing.T) {
		new(eventDropScenario).
			WithBlockedEventsConfig(map[string][]string{
				"track": {"BlockedEvent"},
			}).
			WithEventConfigs([]eventConfig{
				NewTrackEventConfig("NormalEvent"),                 // Normal track event - should reach router
				NewBotEventConfig("track", "TestEvent", "drop"),    // Bot track event - should be dropped
				NewTrackEventConfig("BlockedEvent"),                // Blocked track event - should be dropped
				NewBotEventConfig("identify", "", "drop"),          // Bot identify event - should be dropped
				NewIdentifyEventConfig(),                           // Normal identify event - should reach router
				NewBotEventConfig("screen", "", "flag"),            // Bot screen event with flag action - should reach router
				NewBotEventConfig("track", "BlockedEvent", "drop"), // Bot track event with drop action and blocked event - should be dropped
			}).
			Run(t, func(t *testing.T, scenario *eventDropScenario) {
				scenario.requireJobsCount(t, "gw", "succeeded", 7)
				scenario.requireJobsCount(t, "rt", "aborted", 3)
				scenario.requireJobsCount(t, "rt", "succeeded", 0)
				scenario.requireJobsCount(t, "rt", "failed", 0)

				// reporting metrics
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "track", EventName: "TestEvent", ExpectedDetectedCount: 0, ExpectedDroppedCount: 1})
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "track", EventName: "NormalEvent", ExpectedDetectedCount: 0, ExpectedDroppedCount: 0})
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "track", EventName: "BlockedEvent", ExpectedDetectedCount: 0, ExpectedDroppedCount: 1})
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "identify", EventName: "", ExpectedDetectedCount: 0, ExpectedDroppedCount: 1})
				scenario.requireReportsFromBotManagement(t, BotManagementReportExpectations{EventType: "screen", EventName: "", ExpectedDetectedCount: 1, ExpectedDroppedCount: 0})

				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "track", EventName: "NormalEvent", ExpectedIngestedCount: 1})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "track", EventName: "TestEvent", ExpectedIngestedCount: 0})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "track", EventName: "BlockedEvent", ExpectedIngestedCount: 0})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "identify", EventName: "", ExpectedIngestedCount: 1})
				scenario.requireReportsFromGateway(t, GatewayReportExpectations{EventType: "screen", EventName: "", ExpectedIngestedCount: 1, ExpectedBotFlaggedCount: 1})
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

type BotManagementReportExpectations struct {
	EventType             string
	EventName             string
	ExpectedDetectedCount int
	ExpectedDroppedCount  int
}

type GatewayReportExpectations struct {
	EventType                string
	EventName                string
	ExpectedIngestedCount    int
	ExpectedBotFlaggedCount  int
	ExpectedBotDetectedCount int
}

func (s *eventDropScenario) WithBlockedEventsConfig(config map[string][]string) *eventDropScenario {
	s.blockedEventsConfig = config
	return s
}

func (s *eventDropScenario) WithEventConfigs(configs []eventConfig) *eventDropScenario {
	s.eventConfigs = configs
	return s
}

func (s *eventDropScenario) Run(t *testing.T, verification func(t *testing.T, scenario *eventDropScenario)) {
	config.Reset()
	defer config.Reset()
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

func (s *eventDropScenario) runRudderServer(ctx context.Context, port int, postgresContainer *postgres.Resource, cbURL, transformerURL, tmpDir string) (err error) {
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
	config.Set("BotEnrichment.enabled", true)
	config.Set("enableEventBlocking", true)

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
	return
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

func (s *eventDropScenario) requireReportsFromBotManagement(t *testing.T, expectations BotManagementReportExpectations) {
	require.Eventually(t, func() bool {
		var detectedCount int
		var droppedCount int
		commonLabel := `workspace_id = 'workspace-1' AND source_id = 'source-1' AND destination_id = '' AND in_pu = '' AND pu = 'bot_management' AND error_type = '' AND initial_state IS FALSE AND terminal_state IS FALSE AND event_type = $1 AND event_name = $2`

		err := s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status_code = 200 AND status = 'bot_detected'", commonLabel), expectations.EventType, expectations.EventName).Scan(&detectedCount)
		t.Logf("Bot events detected reports count: %d", detectedCount)

		if err != nil || detectedCount != expectations.ExpectedDetectedCount {
			t.Logf("Bot management reports not matching expectations for %s event '%s': expected detected=%d, got=%d", expectations.EventType, expectations.EventName, expectations.ExpectedDetectedCount, detectedCount)
			return false
		}

		err = s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status_code = 298 AND status = 'filtered'", commonLabel), expectations.EventType, expectations.EventName).Scan(&droppedCount)
		t.Logf("Bot events dropped reports count: %d", droppedCount)

		if err != nil || droppedCount != expectations.ExpectedDroppedCount {
			t.Logf("Bot management reports not matching expectations for %s event '%s': expected dropped=%d, got=%d", expectations.EventType, expectations.EventName, expectations.ExpectedDroppedCount, droppedCount)
			return false
		}

		return true
	}, 20*time.Second, 1*time.Second, fmt.Sprintf("Bot management reports not matching expectations for %s event '%s': expected detected=%d, dropped=%d",
		expectations.EventType, expectations.EventName, expectations.ExpectedDetectedCount, expectations.ExpectedDroppedCount))
}

func (s *eventDropScenario) requireReportsFromGateway(t *testing.T, expectations GatewayReportExpectations) {
	require.Eventually(t, func() bool {
		var ingestedCount int
		var botFlaggedCount int
		var botDetectedCount int
		commonLabel := `workspace_id = 'workspace-1' AND source_id = 'source-1' AND destination_id = '' AND in_pu = '' AND pu = 'gateway' AND status_code = 0 AND error_type = '' AND initial_state IS TRUE AND terminal_state IS FALSE AND event_type = $1 AND event_name = $2`

		err := s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status = 'bot_detected'", commonLabel), expectations.EventType, expectations.EventName).Scan(&botDetectedCount)
		t.Logf("Bot events detected reports count: %d", botDetectedCount)

		if err != nil || botDetectedCount != expectations.ExpectedBotDetectedCount {
			t.Logf("Gateway reports not matching expectations for %s event '%s': expected bot_detected=%d, got=%d", expectations.EventType, expectations.EventName, expectations.ExpectedBotDetectedCount, botDetectedCount)
			return false
		}

		err = s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status = 'bot_flagged'", commonLabel), expectations.EventType, expectations.EventName).Scan(&botFlaggedCount)
		t.Logf("Bot events flagged reports count: %d", botFlaggedCount)

		if err != nil || botFlaggedCount != expectations.ExpectedBotFlaggedCount {
			t.Logf("Gateway reports not matching expectations for %s event '%s': expected bot_flagged=%d, got=%d", expectations.EventType, expectations.EventName, expectations.ExpectedBotFlaggedCount, botFlaggedCount)
			return false
		}

		err = s.db.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM reports WHERE %s AND status = 'succeeded'", commonLabel), expectations.EventType, expectations.EventName).Scan(&ingestedCount)
		t.Logf("Bot events ingested reports count: %d", ingestedCount)

		if err != nil || ingestedCount != expectations.ExpectedIngestedCount {
			t.Logf("Gateway reports not matching expectations for %s event '%s': expected ingested=%d, got=%d", expectations.EventType, expectations.EventName, expectations.ExpectedIngestedCount, ingestedCount)
			return false
		}

		return true
	}, 20*time.Second, 1*time.Second, fmt.Sprintf("Gateway reports not matching expectations for %s event '%s': expected ingested=%d, bot_flagged=%d, bot_detected=%d",
		expectations.EventType, expectations.EventName, expectations.ExpectedIngestedCount, expectations.ExpectedBotFlaggedCount, expectations.ExpectedBotDetectedCount))
}

// Helper functions to create eventConfig objects
func NewTrackEventConfig(eventName string) eventConfig {
	return eventConfig{
		eventType: "track",
		eventName: eventName,
		isBot:     false,
		botAction: "",
	}
}

func NewIdentifyEventConfig() eventConfig {
	return eventConfig{
		eventType: "identify",
		eventName: "",
		isBot:     false,
		botAction: "",
	}
}

func NewBotEventConfig(eventType, eventName, botAction string) eventConfig {
	return eventConfig{
		eventType: eventType,
		eventName: eventName,
		isBot:     true,
		botAction: botAction,
	}
}
