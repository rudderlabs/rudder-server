package gateway

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-schemas/go/stream"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocks_gateway "github.com/rudderlabs/rudder-server/mocks/gateway"
)

// createTestGateway creates a minimal Handle instance for testing event blocking
func createTestGateway(t *testing.T, eventBlockingSettings backendconfig.EventBlocking) *Handle {
	statsStore, err := memstats.New()
	require.NoError(t, err)

	configData := make(map[string]backendconfig.ConfigT)

	configData["workspace1"] = backendconfig.ConfigT{
		Settings: backendconfig.Settings{
			EventBlocking: eventBlockingSettings,
		},
		Sources: []backendconfig.SourceT{
			{
				ID:       "source-id-1",
				WriteKey: "write-key-1",
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name:     "JavaScript",
					Category: "", // event stream source
				},
				Name:    "JS Source",
				Enabled: true,
			},
			{
				ID:       "source-id-2",
				WriteKey: "write-key-2",
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name:     "Webhook",
					Category: "webhook", // event stream source
				},
				Name:    "Webhook Source",
				Enabled: true,
			},
			{
				ID:       "warehouse-source-id-1",
				WriteKey: "warehouse-write-key",
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name:     "Warehouse",
					Category: "warehouse", // non-event stream source
				},
				Name:    "Warehouse Source",
				Enabled: true,
			},
		},
	}

	// Create a mock controller and webhook handler
	mockCtrl := gomock.NewController(t)
	t.Cleanup(mockCtrl.Finish)
	mockWebhook := mocks_gateway.NewMockWebhookRequestHandler(mockCtrl)
	mockWebhook.EXPECT().Register(gomock.Any()).AnyTimes() // Allow any number of Register calls

	gw := &Handle{
		stats:   statsStore,
		logger:  logger.NOP,
		webhook: mockWebhook,
		conf: struct {
			webPort, maxUserWebRequestWorkerProcess, maxDBWriterProcess                       int
			maxUserWebRequestBatchSize, maxDBBatchSize, maxHeaderBytes, maxConcurrentRequests int
			userWebRequestBatchTimeout, dbBatchWriteTimeout                                   config.ValueLoader[time.Duration]
			maxReqSize                                                                        config.ValueLoader[int]
			enableRateLimit                                                                   config.ValueLoader[bool]
			enableSuppressUserFeature                                                         bool
			diagnosisTickerTime                                                               time.Duration
			ReadTimeout                                                                       time.Duration
			ReadHeaderTimeout                                                                 time.Duration
			WriteTimeout                                                                      time.Duration
			IdleTimeout                                                                       time.Duration
			allowReqsWithoutUserIDAndAnonymousID                                              config.ValueLoader[bool]
			gwAllowPartialWriteWithErrors                                                     config.ValueLoader[bool]
			webhookV2HandlerEnabled                                                           bool
		}{
			webhookV2HandlerEnabled: false,
		},
		configSubscriberLock: sync.RWMutex{},
		requestSizeStat:      statsStore.NewStat("gateway.request_size", stats.HistogramType),
	}

	// Use the same logic as backendConfigSubscriber to process the config data
	gw.processBackendConfig(configData)

	return gw
}

func TestIsEventBlocked(t *testing.T) {
	tests := []struct {
		name                  string
		workspaceID           string
		sourceID              string
		eventType             string
		eventName             string
		eventBlockingSettings backendconfig.EventBlocking
		expected              bool
		description           string
	}{
		{
			name:        "empty event name",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
			expected:    false,
			description: "Empty event names should not be blocked",
		},
		{
			name:        "non-track event type",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "identify",
			eventName:   "Purchase",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
			expected:    false,
			description: "Non-track events should not be blocked",
		},
		{
			name:        "blocked event",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "Purchase",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
			expected:    true,
			description: "Event should be blocked when it matches the blocked events list",
		},
		{
			name:        "non-blocked event",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "PageView",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
			expected:    false,
			description: "Event should not be blocked when it's not in the blocked events list",
		},
		{
			name:        "case sensitive event matching",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "purchase", // lowercase
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"}, // uppercase
				},
			},
			expected:    false,
			description: "Event matching should be case sensitive",
		},
		{
			name:        "events map is nil",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "Purchase",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: nil,
			},
			expected:    false,
			description: "When Events map is nil, no events should be blocked",
		},
		{
			name:        "workspace not found",
			workspaceID: "nonexistent",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "Purchase",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
			expected:    false,
			description: "When workspace is not found, events should not be blocked",
		},
		{
			name:        "non-event stream source",
			workspaceID: "workspace1",
			sourceID:    "warehouse-source-id-1",
			eventType:   "track",
			eventName:   "Purchase",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
			expected:    false,
			description: "Events from non-event stream sources should not be blocked",
		},
		{
			name:        "event stream source - blocked event",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "Purchase",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
			expected:    true,
			description: "Events from event stream sources should be blocked when they match the blocked events list",
		},
		{
			name:        "empty track event list",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "Purchase",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {},
				},
			},
			expected:    false,
			description: "track event list is empty",
		},
		{
			name:        "empty events map",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "Purchase",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{},
			},
			expected:    false,
			description: "events map is empty",
		},
		{
			name:        "track event with large blocked events list",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "AddToCart",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase", "AddToCart", "RemoveFromCart", "Checkout", "PaymentInfo", "OrderComplete"},
				},
			},
			expected:    true,
			description: "Event should be blocked when present in a large list of blocked track events",
		},
		{
			name:        "track event not in large blocked events list",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "ProductView",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase", "AddToCart", "RemoveFromCart", "Checkout", "PaymentInfo", "OrderComplete"},
				},
			},
			expected:    false,
			description: "Event should not be blocked when not present in a large list of blocked track events",
		},
		{
			name:        "track event with special characters - blocked",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "Product Purchased - $100",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Product Purchased - $100", "Cart Abandoned!", "Sign-Up Complete"},
				},
			},
			expected:    true,
			description: "Track event names with special characters should be blocked when they match exactly",
		},
		{
			name:        "track event with special characters - not blocked",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "Product Purchased - $200",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Product Purchased - $100", "Cart Abandoned!", "Sign-Up Complete"},
				},
			},
			expected:    false,
			description: "Track event names with special characters should not be blocked when they don't match exactly",
		},
		{
			name:        "track event with unicode characters - blocked",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "购买商品",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"购买商品", "添加到购物车", "查看产品"},
				},
			},
			expected:    true,
			description: "Track event names with unicode characters should be blocked when they match exactly",
		},
		{
			name:        "track event with whitespace - blocked",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "  Purchase  ",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"  Purchase  ", "AddToCart", " Checkout "},
				},
			},
			expected:    true,
			description: "Track event names with whitespace should be blocked when they match exactly including whitespace",
		},
		{
			name:        "track event with whitespace - not blocked due to trim difference",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "Purchase",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"  Purchase  ", "AddToCart", " Checkout "},
				},
			},
			expected:    false,
			description: "Track event names should not be blocked when whitespace doesn't match exactly",
		},
		{
			name:        "track event - empty string in blocked events list",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"", "Purchase", "AddToCart"},
				},
			},
			expected:    false,
			description: "Empty track event names should not be blocked even if empty string is in the blocked list",
		},
		{
			name:        "track event with very long name - blocked",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "User_Completed_Very_Detailed_Product_Configuration_With_Multiple_Options_And_Customizations_Before_Adding_To_Cart",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"User_Completed_Very_Detailed_Product_Configuration_With_Multiple_Options_And_Customizations_Before_Adding_To_Cart", "Purchase"},
				},
			},
			expected:    true,
			description: "Very long track event names should be blocked when they match exactly",
		},
		{
			name:        "track event with numeric name - blocked",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "12345",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"12345", "67890", "Purchase"},
				},
			},
			expected:    true,
			description: "Track event names that are purely numeric should be blocked when they match exactly",
		},
		{
			name:        "track event with mixed case in blocked list - exact match",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "PuRcHaSe",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"PuRcHaSe", "AddToCart", "checkout"},
				},
			},
			expected:    true,
			description: "Track event names with mixed case should be blocked when they match exactly",
		},
		{
			name:        "track event with single character name - blocked",
			workspaceID: "workspace1",
			sourceID:    "source-id-1",
			eventType:   "track",
			eventName:   "A",
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"A", "B", "Purchase"},
				},
			},
			expected:    true,
			description: "Single character track event names should be blocked when they match exactly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := createTestGateway(t, tt.eventBlockingSettings)

			result := gw.isEventBlocked(tt.workspaceID, tt.sourceID, tt.eventType, tt.eventName)
			require.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestExtractJobsFromInternalBatchPayload_EventBlocking(t *testing.T) {
	type expectedJob struct {
		eventName              string
		isEventBlocked         bool
		skipLiveEventRecording bool
		shouldBeDropped        bool
	}

	gw := createTestGateway(t, backendconfig.EventBlocking{
		Events: map[string][]string{
			"track": {"Purchase", "batch-request-type-with-type-track", "track-request-type-with-no-type", "batch-request-type-with-no-type"},
		},
	})

	tests := []struct {
		name         string
		messages     []stream.Message
		expectedJobs []expectedJob
		description  string
	}{
		{
			name: "mixed batch - blocked, non-blocked, and non-event stream events",
			messages: []stream.Message{
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-1",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1", // Event stream source
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
					},
					Payload: json.RawMessage(`{"type":"track","event":"Purchase","messageId":"msg-1","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-2",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-2", // Event stream source
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
					},
					Payload: json.RawMessage(`{"type":"track","event":"PageView","messageId":"msg-2","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-3",
						WorkspaceID: "workspace1",
						SourceID:    "warehouse-source-id-1", // Non-event stream source
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
					},
					Payload: json.RawMessage(`{"type":"track","event":"Purchase","messageId":"msg-3","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "batch",
						RoutingKey:  "routing-key-3",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1", // Event stream source
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
					},
					Payload: json.RawMessage(`{"type":"track","event":"batch-request-type-with-type-track","messageId":"msg-3","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "batch",
						RoutingKey:  "routing-key-3",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1", // Event stream source
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
					},
					Payload: json.RawMessage(`{"event":"batch-request-type-with-no-type","messageId":"msg-3","userId":"user1"}`), // type is not present
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-3",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1", // Event stream source
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
					},
					Payload: json.RawMessage(`{"event":"track-request-type-with-no-type","messageId":"msg-3","userId":"user1"}`), // type is not present
				},
			},
			expectedJobs: []expectedJob{
				{
					eventName:              "Purchase",
					isEventBlocked:         true,
					skipLiveEventRecording: true,
					shouldBeDropped:        true,
				},
				{
					eventName:              "PageView",
					isEventBlocked:         false,
					skipLiveEventRecording: false,
					shouldBeDropped:        false,
				},
				{
					eventName:              "Purchase",
					isEventBlocked:         false,
					skipLiveEventRecording: false,
					shouldBeDropped:        false,
				},
				{
					eventName:              "batch-request-type-with-type-track",
					isEventBlocked:         true,
					skipLiveEventRecording: true,
					shouldBeDropped:        true,
				},
				{
					eventName:              "batch-request-type-with-no-type",
					isEventBlocked:         false,
					skipLiveEventRecording: false,
					shouldBeDropped:        false,
				},
				{
					eventName:              "track-request-type-with-no-type",
					isEventBlocked:         false,
					skipLiveEventRecording: false,
					shouldBeDropped:        false,
				},
			},
			description: "Mixed batch should handle blocked, non-blocked, and non-event stream events correctly",
		},
		{
			name: "live event recording disabled for blocked events",
			messages: []stream.Message{
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-1",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
					},
					Payload: json.RawMessage(`{"type":"track","event":"Purchase","messageId":"msg-1","userId":"user1"}`),
				},
			},
			expectedJobs: []expectedJob{
				{
					eventName:              "Purchase",
					isEventBlocked:         true,
					skipLiveEventRecording: true,
					shouldBeDropped:        true,
				},
			},
			description: "Blocked events should have skipLiveEventRecording set to true",
		},
		{
			name: "processor integration - blocked events should be dropped",
			messages: []stream.Message{
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-1",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
					},
					Payload: json.RawMessage(`{"type":"track","event":"Purchase","messageId":"msg-1","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-2",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
					},
					Payload: json.RawMessage(`{"type":"track","event":"PageView","messageId":"msg-2","userId":"user1"}`),
				},
			},
			expectedJobs: []expectedJob{
				{
					eventName:              "Purchase",
					isEventBlocked:         true,
					skipLiveEventRecording: true,
					shouldBeDropped:        true,
				},
				{
					eventName:              "PageView",
					isEventBlocked:         false,
					skipLiveEventRecording: false,
					shouldBeDropped:        false,
				},
			},
			description: "Processor should drop blocked events while processing non-blocked events normally",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create JSON payload directly as array of stream.Message
			payloadBytes, err := jsonrs.Marshal(tt.messages)
			require.NoError(t, err)

			// Extract jobs
			jobs, err := gw.extractJobsFromInternalBatchPayload("batch", payloadBytes)
			require.NoError(t, err, "extractJobsFromInternalBatchPayload should not return error")

			// Verify we got the expected number of jobs
			require.Len(t, jobs, len(tt.expectedJobs), "Number of jobs should match expected")

			// Verify each job's properties
			for i, expectedJob := range tt.expectedJobs {
				job := jobs[i]

				// Parse the job parameters to check event blocking properties
				var eventParams map[string]interface{}
				err := jsonrs.Unmarshal(job.job.Parameters, &eventParams)
				require.NoError(t, err, "Should be able to parse job parameters")

				// Check IsEventBlocked parameter
				isEventBlocked, exists := eventParams["is_event_blocked"]
				if expectedJob.isEventBlocked {
					require.True(t, exists, "Job %d: is_event_blocked parameter should exist for blocked events", i)
					require.True(t, isEventBlocked.(bool), "Job %d: is_event_blocked should be true", i)
				} else {
					// For non-blocked events, is_event_blocked should either not exist or be false
					if exists {
						require.False(t, isEventBlocked.(bool), "Job %d: is_event_blocked should be false", i)
					}
				}

				// Check skipLiveEventRecording field
				require.Equal(t, expectedJob.skipLiveEventRecording, job.skipLiveEventRecording,
					"Job %d: skipLiveEventRecording should match expected", i)

				// Verify processor behavior (events marked as blocked should be dropped)
				if expectedJob.shouldBeDropped {
					// Verify the job has the is_event_blocked parameter set to true
					// This simulates what the processor would check before dropping
					require.True(t, eventParams["is_event_blocked"].(bool),
						"Job %d: Events that should be dropped must have is_event_blocked=true", i)
				}
			}
		})
	}
}

func TestExtractJobsFromInternalBatchPayload_LiveEventRecording(t *testing.T) {
	type testCase struct {
		name                      string
		messages                  []stream.Message
		eventBlockingSettings     backendconfig.EventBlocking
		expectedSkipLiveEventRecs []bool
		description               string
	}

	tests := []testCase{
		{
			name: "normal events should not skip live event recording",
			messages: []stream.Message{
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-1",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       false,
					},
					Payload: json.RawMessage(`{"type":"track","event":"PageView","messageId":"msg-1","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "identify",
						RoutingKey:  "routing-key-2",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       false,
					},
					Payload: json.RawMessage(`{"type":"identify","messageId":"msg-2","userId":"user1","traits":{"name":"John"}}`),
				},
			},
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{},
			},
			expectedSkipLiveEventRecs: []bool{false, false},
			description:               "Normal events should allow live event recording",
		},
		{
			name: "blocked events should skip live event recording",
			messages: []stream.Message{
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-1",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1", // Event stream source
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       false,
					},
					Payload: json.RawMessage(`{"type":"track","event":"Purchase","messageId":"msg-1","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-2",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       false,
					},
					Payload: json.RawMessage(`{"type":"track","event":"PageView","messageId":"msg-2","userId":"user1"}`),
				},
			},
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
			expectedSkipLiveEventRecs: []bool{true, false},
			description:               "Blocked events should skip live event recording while non-blocked events should not",
		},
		{
			name: "bot events with drop action should skip live event recording",
			messages: []stream.Message{
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-1",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       true,
						BotName:     "test-bot",
						BotURL:      "https://test-bot.com",
						BotAction:   "drop",
					},
					Payload: json.RawMessage(`{"type":"track","event":"PageView","messageId":"msg-1","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-2",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       false,
					},
					Payload: json.RawMessage(`{"type":"track","event":"AddToCart","messageId":"msg-2","userId":"user1"}`),
				},
			},
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{},
			},
			expectedSkipLiveEventRecs: []bool{true, false},
			description:               "Bot events with 'drop' action should skip live event recording",
		},
		{
			name: "bot events with flag action should not skip live event recording",
			messages: []stream.Message{
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-1",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       true,
						BotName:     "test-bot",
						BotURL:      "https://test-bot.com",
						BotAction:   "flag",
					},
					Payload: json.RawMessage(`{"type":"track","event":"PageView","messageId":"msg-1","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-2",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       true,
						BotName:     "another-bot",
						BotAction:   "disable",
					},
					Payload: json.RawMessage(`{"type":"track","event":"Purchase","messageId":"msg-2","userId":"user1"}`),
				},
			},
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{},
			},
			expectedSkipLiveEventRecs: []bool{false, false},
			description:               "Bot events with 'flag' or 'disable' actions should allow live event recording",
		},
		{
			name: "mixed scenario - blocked events and bot events with drop action",
			messages: []stream.Message{
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-1",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1", // Event stream source
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       false,
					},
					Payload: json.RawMessage(`{"type":"track","event":"Purchase","messageId":"msg-1","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-2",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       true,
						BotName:     "crawler-bot",
						BotAction:   "drop",
					},
					Payload: json.RawMessage(`{"type":"track","event":"PageView","messageId":"msg-2","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-3",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       true,
						BotName:     "analytics-bot",
						BotAction:   "flag",
					},
					Payload: json.RawMessage(`{"type":"track","event":"AddToCart","messageId":"msg-3","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-4",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1",
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       false,
					},
					Payload: json.RawMessage(`{"type":"track","event":"Checkout","messageId":"msg-4","userId":"user1"}`),
				},
			},
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
			expectedSkipLiveEventRecs: []bool{true, true, false, false},
			description:               "Complex scenario: blocked events and bot drop events skip recording, others don't",
		},
		{
			name: "blocked bot events with drop action and blocked events should skip live event recording",
			messages: []stream.Message{
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-1",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1", // Event stream source
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       true,
						BotName:     "blocked-bot",
						BotAction:   "drop",
					},
					Payload: json.RawMessage(`{"type":"track","event":"Purchase","messageId":"msg-1","userId":"user1"}`),
				},
				{
					Properties: stream.MessageProperties{
						RequestType: "track",
						RoutingKey:  "routing-key-2",
						WorkspaceID: "workspace1",
						SourceID:    "source-id-1", // Event stream source
						ReceivedAt:  time.Now(),
						RequestIP:   "1.1.1.1",
						IsBot:       true,
						BotName:     "blocked-bot-2",
						BotAction:   "flag",
					},
					Payload: json.RawMessage(`{"type":"track","event":"Purchase","messageId":"msg-2","userId":"user1"}`),
				},
			},
			eventBlockingSettings: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
			expectedSkipLiveEventRecs: []bool{true, true},
			description:               "Both blocked events and bot drop events should skip live event recording",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := createTestGateway(t, tt.eventBlockingSettings)

			payloadBytes, err := jsonrs.Marshal(tt.messages)
			require.NoError(t, err, "Failed to marshal test messages")

			jobs, err := gw.extractJobsFromInternalBatchPayload("batch", payloadBytes)
			require.NoError(t, err, "extractJobsFromInternalBatchPayload should not return error")

			require.Len(t, jobs, len(tt.expectedSkipLiveEventRecs), "Number of jobs should match expected")

			for i, expectedSkip := range tt.expectedSkipLiveEventRecs {
				job := jobs[i]
				require.Equal(t, expectedSkip, job.skipLiveEventRecording,
					"Job %d: skipLiveEventRecording should be %t - %s",
					i, expectedSkip, tt.description)
			}
		})
	}
}
