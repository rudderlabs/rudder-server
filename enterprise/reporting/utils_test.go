package reporting

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/client"
	mocks "github.com/rudderlabs/rudder-server/mocks/enterprise/reporting/event_sampler"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// maxSampleEventSizeBytesForTest is a large-enough limit that the fixtures in
// this file never trip the oversized-sample-event guard.
const maxSampleEventSizeBytesForTest = 80 * bytesize.MB

func TestGetSampleWithEventSamplingForEDReportsDB(t *testing.T) {
	t.Run("event sampling disabled", func(t *testing.T) {
		metric := types.EDReportsDB{
			EDErrorDetails: types.EDErrorDetails{
				SampleEvent:    json.RawMessage(`{"test": "event"}`),
				SampleResponse: "test response",
			},
		}

		sampleEvent, sampleResponse, _, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, nil, false, 60, maxSampleEventSizeBytesForTest)
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

		sampleEvent, sampleResponse, _, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, nil, true, 60, maxSampleEventSizeBytesForTest)
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

		sampleEvent, sampleResponse, _, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, mockSampler, true, 60, maxSampleEventSizeBytesForTest)
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

		sampleEvent, sampleResponse, _, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, mockSampler, true, 60, maxSampleEventSizeBytesForTest)
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

		sampleEvent, sampleResponse, _, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, mockSampler, true, 60, maxSampleEventSizeBytesForTest)
		require.NoError(t, err)
		require.Nil(t, sampleEvent)
		require.Equal(t, "", sampleResponse)
	})
}

func newLargePayloadTestClient(t *testing.T, serverURL string, maxRetries int) *client.Client {
	t.Helper()
	conf := config.New()
	conf.Set("REPORTING_URL", serverURL)
	conf.Set("Reporting.httpClient.backoff.maxRetries", maxRetries)
	return client.New(client.RouteMetrics, conf, logger.NOP, stats.NOP)
}

// newLargePayloadTestReporter returns a DefaultReporter wired with the given client
// and an in-memory stats store so tests can assert split / sample-event-replaced counts.
func newLargePayloadTestReporter(t *testing.T, commonClient *client.Client) (*DefaultReporter, *memstats.Store) {
	t.Helper()
	statsStore, err := memstats.New()
	require.NoError(t, err)
	return &DefaultReporter{
		commonClient:            commonClient,
		stats:                   statsStore,
		log:                     logger.NOP,
		maxSampleEventSizeBytes: config.SingleValueLoader(maxSampleEventSizeBytesForTest),
	}, statsStore
}

// newLargePayloadTestEDReporter is the ErrorDetailReporter analogue of newLargePayloadTestReporter.
func newLargePayloadTestEDReporter(t *testing.T, commonClient *client.Client) (*ErrorDetailReporter, *memstats.Store) {
	t.Helper()
	statsStore, err := memstats.New()
	require.NoError(t, err)
	return &ErrorDetailReporter{
		commonClient:            commonClient,
		stats:                   statsStore,
		log:                     logger.NOP,
		statsManager:            NewErrorReportingStats(statsStore),
		maxSampleEventSizeBytes: config.SingleValueLoader(maxSampleEventSizeBytesForTest),
	}, statsStore
}

// requireLargePayloadStats asserts the count of the oversized-sample-event-skipped
// counter tagged stage="client" (the placeholder-replace fallback triggered by a
// 413 from the reporting service, as opposed to stage="sampler" at write time).
func requireLargePayloadStats(t *testing.T, statsStore *memstats.Store, skippedStat string, replaced float64) {
	t.Helper()
	getCount := func(name string) float64 {
		tags := stats.Tags{"stage": "client", "sourceId": "source-1", "destinationId": "destination-1"}
		m := statsStore.Get(name, tags)
		if m == nil {
			return 0
		}
		return m.LastValue()
	}
	require.Equal(t, replaced, getCount(skippedStat), "sample event replaced count")
}
