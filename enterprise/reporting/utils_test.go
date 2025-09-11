package reporting

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

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

		sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, nil, false, 60)
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

		sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, nil, true, 60)
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

		sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, mockSampler, true, 60)
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

		sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, mockSampler, true, 60)
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

		sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(metric, 123456, mockSampler, true, 60)
		require.NoError(t, err)
		require.Nil(t, sampleEvent)
		require.Equal(t, "", sampleResponse)
	})
}
