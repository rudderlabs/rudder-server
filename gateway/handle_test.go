package gateway

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-schemas/go/stream"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// createTestGateway creates a minimal Handle instance for testing event blocking
func createTestGateway(t *testing.T, enableEventBlocking bool, workspaceSettings map[string]backendconfig.Settings, nonEventStreamSources map[string]bool) *Handle {
	statsStore, err := memstats.New()
	require.NoError(t, err)

	if workspaceSettings == nil {
		workspaceSettings = make(map[string]backendconfig.Settings)
	}
	if nonEventStreamSources == nil {
		nonEventStreamSources = make(map[string]bool)
	}

	return &Handle{
		stats:  statsStore,
		logger: logger.NOP,
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
			enableInternalBatchValidator                                                      config.ValueLoader[bool]
			enableInternalBatchEnrichment                                                     config.ValueLoader[bool]
			enableEventBlocking                                                               bool
			webhookV2HandlerEnabled                                                           bool
		}{
			enableEventBlocking:           enableEventBlocking,
			enableInternalBatchValidator:  config.SingleValueLoader(false),
			enableInternalBatchEnrichment: config.SingleValueLoader(false),
		},
		workspaceIDSettingsMap: workspaceSettings,
		nonEventStreamSources:  nonEventStreamSources,
		configSubscriberLock:   sync.RWMutex{},
		sourceIDSourceMap: map[string]backendconfig.SourceT{
			"source-id-1": {
				WriteKey: "write-key-1",
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name: "JavaScript",
				},
				Name: "JS Source",
			},
			"source-id-2": {
				WriteKey: "write-key-2",
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name: "Android",
				},
				Name: "Android Source",
			},
			"warehouse-source-id-1": {
				WriteKey: "warehouse-write-key",
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name: "Warehouse",
				},
				Name: "Warehouse Source",
			},
		},
		requestSizeStat: statsStore.NewStat("gateway.request_size", stats.HistogramType),
	}
}

func TestIsEventBlocked(t *testing.T) {
	tests := []struct {
		name                string
		enableEventBlocking bool
		workspaceID         string
		eventType           string
		eventName           string
		workspaceSettings   map[string]backendconfig.Settings
		expected            bool
		description         string
	}{
		{
			name:                "event blocking disabled",
			enableEventBlocking: false,
			workspaceID:         "workspace1",
			eventType:           "track",
			eventName:           "Purchase",
			workspaceSettings: map[string]backendconfig.Settings{
				"workspace1": {
					EventBlocking: backendconfig.EventBlocking{
						Events: map[string][]string{
							"track": {"Purchase"},
						},
					},
				},
			},
			expected:    false,
			description: "When event blocking is disabled, no events should be blocked",
		},
		{
			name:                "empty event name",
			enableEventBlocking: true,
			workspaceID:         "workspace1",
			eventType:           "track",
			eventName:           "",
			workspaceSettings: map[string]backendconfig.Settings{
				"workspace1": {
					EventBlocking: backendconfig.EventBlocking{
						Events: map[string][]string{
							"track": {"Purchase"},
						},
					},
				},
			},
			expected:    false,
			description: "Empty event names should not be blocked",
		},
		{
			name:                "non-track event type",
			enableEventBlocking: true,
			workspaceID:         "workspace1",
			eventType:           "identify",
			eventName:           "Purchase",
			workspaceSettings: map[string]backendconfig.Settings{
				"workspace1": {
					EventBlocking: backendconfig.EventBlocking{
						Events: map[string][]string{
							"track": {"Purchase"},
						},
					},
				},
			},
			expected:    false,
			description: "Non-track events should not be blocked",
		},
		{
			name:                "blocked event",
			enableEventBlocking: true,
			workspaceID:         "workspace1",
			eventType:           "track",
			eventName:           "Purchase",
			workspaceSettings: map[string]backendconfig.Settings{
				"workspace1": {
					EventBlocking: backendconfig.EventBlocking{
						Events: map[string][]string{
							"track": {"Purchase"},
						},
					},
				},
			},
			expected:    true,
			description: "Event should be blocked when it matches the blocked events list",
		},
		{
			name:                "non-blocked event",
			enableEventBlocking: true,
			workspaceID:         "workspace1",
			eventType:           "track",
			eventName:           "PageView",
			workspaceSettings: map[string]backendconfig.Settings{
				"workspace1": {
					EventBlocking: backendconfig.EventBlocking{
						Events: map[string][]string{
							"track": {"Purchase"},
						},
					},
				},
			},
			expected:    false,
			description: "Event should not be blocked when it's not in the blocked events list",
		},
		{
			name:                "case sensitive event matching",
			enableEventBlocking: true,
			workspaceID:         "workspace1",
			eventType:           "track",
			eventName:           "purchase", // lowercase
			workspaceSettings: map[string]backendconfig.Settings{
				"workspace1": {
					EventBlocking: backendconfig.EventBlocking{
						Events: map[string][]string{
							"track": {"Purchase"}, // uppercase
						},
					},
				},
			},
			expected:    false,
			description: "Event matching should be case sensitive",
		},
		{
			name:                "events map is nil",
			enableEventBlocking: true,
			workspaceID:         "workspace1",
			eventType:           "track",
			eventName:           "Purchase",
			workspaceSettings: map[string]backendconfig.Settings{
				"workspace1": {
					EventBlocking: backendconfig.EventBlocking{
						Events: nil,
					},
				},
			},
			expected:    false,
			description: "When Events map is nil, no events should be blocked",
		},
		{
			name:                "workspace not found",
			enableEventBlocking: true,
			workspaceID:         "nonexistent",
			eventType:           "track",
			eventName:           "Purchase",
			workspaceSettings: map[string]backendconfig.Settings{
				"workspace1": {
					EventBlocking: backendconfig.EventBlocking{
						Events: map[string][]string{
							"track": {"Purchase"},
						},
					},
				},
			},
			expected:    false,
			description: "When workspace is not found, events should not be blocked",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := createTestGateway(t, tt.enableEventBlocking, tt.workspaceSettings, nil)

			result := gw.isEventBlocked(tt.workspaceID, tt.eventType, tt.eventName)
			assert.Equal(t, tt.expected, result, tt.description)
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

	workspaceSettings := map[string]backendconfig.Settings{
		"workspace1": {
			EventBlocking: backendconfig.EventBlocking{
				Events: map[string][]string{
					"track": {"Purchase"},
				},
			},
		},
	}
	nonEventStreamSources := map[string]bool{
		"warehouse-source-id-1": true,
	}
	gw := createTestGateway(t, true, workspaceSettings, nonEventStreamSources)

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
					assert.True(t, exists, "Job %d: is_event_blocked parameter should exist for blocked events", i)
					assert.True(t, isEventBlocked.(bool), "Job %d: is_event_blocked should be true", i)
				} else {
					// For non-blocked events, is_event_blocked should either not exist or be false
					if exists {
						assert.False(t, isEventBlocked.(bool), "Job %d: is_event_blocked should be false", i)
					}
				}

				// Check skipLiveEventRecording field
				assert.Equal(t, expectedJob.skipLiveEventRecording, job.skipLiveEventRecording,
					"Job %d: skipLiveEventRecording should match expected", i)

				// Verify processor behavior (events marked as blocked should be dropped)
				if expectedJob.shouldBeDropped {
					// Verify the job has the is_event_blocked parameter set to true
					// This simulates what the processor would check before dropping
					assert.True(t, eventParams["is_event_blocked"].(bool),
						"Job %d: Events that should be dropped must have is_event_blocked=true", i)
				}
			}
		})
	}
}
