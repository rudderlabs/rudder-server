package reporting

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/client"
	mocks "github.com/rudderlabs/rudder-server/mocks/enterprise/reporting/event_sampler"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestGetSampleWithEventSamplingForEDReportsDB(t *testing.T) {
	t.Run("event sampling disabled", func(t *testing.T) {
		metric := types.EDReportsDB{
			EDErrorDetails: types.EDErrorDetails{
				SampleEvent:    json.RawMessage(`{"test": "event"}`),
				SampleResponse: "test response",
			},
		}

		sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, nil, false, 60, stats.NOP.NewStat("dropped", stats.CountType))
		require.NoError(t, err)
		require.Equal(t, json.RawMessage(`{"test": "event"}`), sampleEvent)
		require.Equal(t, "test response", sampleResponse)
	})

	t.Run("event sampler is nil", func(t *testing.T) {
		metric := types.EDReportsDB{
			EDErrorDetails: types.EDErrorDetails{
				SampleEvent:    json.RawMessage(`{"test": "event"}`),
				SampleResponse: "test response",
			},
		}

		sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, nil, true, 60, stats.NOP.NewStat("dropped", stats.CountType))
		require.NoError(t, err)
		require.Equal(t, json.RawMessage(`{"test": "event"}`), sampleEvent)
		require.Equal(t, "test response", sampleResponse)
	})

	t.Run("no valid sample", func(t *testing.T) {
		metric := types.EDReportsDB{
			EDErrorDetails: types.EDErrorDetails{
				SampleEvent:    nil,
				SampleResponse: "",
			},
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockSampler := mocks.NewMockEventSampler(ctrl)

		sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, mockSampler, true, 60, stats.NOP.NewStat("dropped", stats.CountType))
		require.NoError(t, err)
		require.Nil(t, sampleEvent)
		require.Equal(t, "", sampleResponse)
	})

	t.Run("event sampling enabled with valid sample", func(t *testing.T) {
		metric := types.EDReportsDB{
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: "test-workspace",
			},
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:      "test-source",
				DestinationID: "test-destination",
			},
			PU: "test-pu",
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					StatusCode:   500,
					EventName:    "test_event",
					EventType:    "track",
					ErrorCode:    "TEST_ERROR",
					ErrorMessage: "Test error message",
				},
				SampleEvent:    json.RawMessage(`{"test": "event"}`),
				SampleResponse: "test response",
			},
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockSampler := mocks.NewMockEventSampler(ctrl)

		// Expect Get to be called and return false (not found)
		mockSampler.EXPECT().Get(gomock.Any()).Return(false, nil)
		// Expect Put to be called
		mockSampler.EXPECT().Put(gomock.Any()).Return(nil)

		sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, mockSampler, true, 60, stats.NOP.NewStat("dropped", stats.CountType))
		require.NoError(t, err)
		require.Equal(t, json.RawMessage(`{"test": "event"}`), sampleEvent)
		require.Equal(t, "test response", sampleResponse)
	})

	t.Run("event sampling enabled - sample already seen", func(t *testing.T) {
		metric := types.EDReportsDB{
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: "test-workspace",
			},
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:      "test-source",
				DestinationID: "test-destination",
			},
			PU: "test-pu",
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					StatusCode:   500,
					EventName:    "test_event",
					EventType:    "track",
					ErrorCode:    "TEST_ERROR",
					ErrorMessage: "Test error message",
				},
				SampleEvent:    json.RawMessage(`{"test": "event"}`),
				SampleResponse: "test response",
			},
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockSampler := mocks.NewMockEventSampler(ctrl)

		// Expect Get to be called and return true (already found)
		mockSampler.EXPECT().Get(gomock.Any()).Return(true, nil)
		// Put should not be called when sample is already found

		sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, mockSampler, true, 60, stats.NOP.NewStat("dropped", stats.CountType))
		require.NoError(t, err)
		require.Nil(t, sampleEvent)
		require.Equal(t, "", sampleResponse)
	})
}

func newPayloadTooLargeTestClient(t *testing.T, serverURL string, maxRetries int) *client.Client {
	t.Helper()
	conf := config.New()
	conf.Set("REPORTING_URL", serverURL)
	conf.Set("Reporting.payloadTooLargeHandling.enabled", true)
	conf.Set("Reporting.httpClient.backoff.maxRetries", maxRetries)
	return client.New(client.RouteMetrics, conf, logger.NOP, stats.NOP)
}

// testPayloadTooLargeTags returns a fresh tag map per call: memstats retains the
// caller's map by reference, so sharing one across measurements is unsafe.
func testPayloadTooLargeTags() stats.Tags { return stats.Tags{"clientName": "test"} }

// newPayloadTooLargeTestReporter returns a DefaultReporter wired with the given client
// and an in-memory stats store so tests can assert split / sample-event-replaced counts.
func newPayloadTooLargeTestReporter(t *testing.T, commonClient *client.Client) (*DefaultReporter, *memstats.Store) {
	t.Helper()
	statsStore, err := memstats.New()
	require.NoError(t, err)
	return &DefaultReporter{
		commonClient:                 commonClient,
		stats:                        statsStore,
		log:                          logger.NOP,
		oversizedSampleEventsDropped: statsStore.NewStat(StatReportingSampleEventDroppedOversized, stats.CountType),
	}, statsStore
}

// newPayloadTooLargeTestEDReporter is the ErrorDetailReporter analogue of newPayloadTooLargeTestReporter.
func newPayloadTooLargeTestEDReporter(t *testing.T, commonClient *client.Client) (*ErrorDetailReporter, *memstats.Store) {
	t.Helper()
	statsStore, err := memstats.New()
	require.NoError(t, err)
	return &ErrorDetailReporter{
		commonClient: commonClient,
		stats:        statsStore,
		log:          logger.NOP,
		statsManager: NewErrorReportingStats(statsStore),
	}, statsStore
}

func requirePayloadTooLargeStats(t *testing.T, statsStore *memstats.Store, splitStat, replacedStat string, splits, replaced float64) {
	t.Helper()
	getCount := func(name string) float64 {
		m := statsStore.Get(name, testPayloadTooLargeTags())
		if m == nil {
			return 0
		}
		return m.LastValue()
	}
	require.Equal(t, splits, getCount(splitStat), "split count")
	require.Equal(t, replaced, getCount(replacedStat), "sample event replaced count")
}
