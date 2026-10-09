package router

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/jobsdb"
	mockdestinationdebugger "github.com/rudderlabs/rudder-server/mocks/services/debugger/destination"
	mockfeatures "github.com/rudderlabs/rudder-server/mocks/services/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
)

func TestSendDestinationResponseMasksLiveEventsPayload(t *testing.T) {
	t.Run("listed paths", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		features := mockfeatures.NewMockFeaturesService(ctrl)
		features.EXPECT().SecretPaths("TEST_DEST").Return([]string{"headers.Authorization"}, true)

		original := json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"}}`)
		debugger := mockdestinationdebugger.NewMockDestinationDebugger(ctrl)
		debugger.EXPECT().RecordEventDeliveryStatus("destination-id", gomock.Any()).DoAndReturn(
			func(_ string, status *destinationdebugger.DeliveryStatusT) bool {
				require.JSONEq(t, `{"endpoint":"visible","headers":{"Authorization":"******"}}`, string(status.Payload))
				return true
			},
		)

		worker, reasons := newDeliveryMaskingTestWorker(features, debugger, false)
		worker.sendDestinationResponseToConfigBackend(original, deliveryStatusMetadata(), deliveryStatusJobStatus(), []string{"source-id"})

		require.Equal(t, []string{"listed"}, *reasons)
		require.True(t, bytes.Equal(original, json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"}}`)))
	})

	t.Run("missing manifest entry", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		features := mockfeatures.NewMockFeaturesService(ctrl)
		features.EXPECT().SecretPaths("TEST_DEST").Return(nil, false)

		debugger := mockdestinationdebugger.NewMockDestinationDebugger(ctrl)
		debugger.EXPECT().RecordEventDeliveryStatus("destination-id", gomock.Any()).DoAndReturn(
			func(_ string, status *destinationdebugger.DeliveryStatusT) bool {
				require.JSONEq(t, `{"endpoint":"visible","headers":"******","body":"******"}`, string(status.Payload))
				return true
			},
		)

		worker, reasons := newDeliveryMaskingTestWorker(features, debugger, false)
		worker.sendDestinationResponseToConfigBackend(
			json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"},"body":{"token":"secret"}}`),
			deliveryStatusMetadata(),
			deliveryStatusJobStatus(),
			[]string{"source-id"},
		)

		require.Equal(t, []string{"mask_all"}, *reasons)
	})

	t.Run("older transformer without the feature", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		features := mockfeatures.NewMockFeaturesService(ctrl)
		features.EXPECT().SecretPaths("TEST_DEST").Return(nil, true)

		debugger := mockdestinationdebugger.NewMockDestinationDebugger(ctrl)
		debugger.EXPECT().RecordEventDeliveryStatus("destination-id", gomock.Any()).DoAndReturn(
			func(_ string, status *destinationdebugger.DeliveryStatusT) bool {
				require.JSONEq(t, `{"headers":{"Authorization":"secret"}}`, string(status.Payload))
				return true
			},
		)

		worker, reasons := newDeliveryMaskingTestWorker(features, debugger, false)
		worker.sendDestinationResponseToConfigBackend(
			json.RawMessage(`{"headers":{"Authorization":"secret"}}`),
			deliveryStatusMetadata(),
			deliveryStatusJobStatus(),
			nil,
		)

		require.Equal(t, []string{"listed"}, *reasons)
	})

	t.Run("rollback flag", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		features := mockfeatures.NewMockFeaturesService(ctrl)
		debugger := mockdestinationdebugger.NewMockDestinationDebugger(ctrl)
		payloads := make([]json.RawMessage, 0, 2)
		debugger.EXPECT().RecordEventDeliveryStatus("destination-id", gomock.Any()).Times(2).DoAndReturn(
			func(_ string, status *destinationdebugger.DeliveryStatusT) bool {
				payloads = append(payloads, bytes.Clone(status.Payload))
				return true
			},
		)

		worker, reasons := newDeliveryMaskingTestWorker(features, debugger, true)
		payload := json.RawMessage(`{"headers":{"Authorization":"secret"}}`)
		worker.sendDestinationResponseToConfigBackend(payload, deliveryStatusMetadata(), deliveryStatusJobStatus(), nil)

		features.EXPECT().SecretPaths("TEST_DEST").Return([]string{"headers.Authorization"}, true)
		worker.rt.reloadableConfig.disableEventDeliveryUploadMasking = config.SingleValueLoader(false)
		worker.sendDestinationResponseToConfigBackend(payload, deliveryStatusMetadata(), deliveryStatusJobStatus(), nil)

		require.JSONEq(t, `{"headers":{"Authorization":"secret"}}`, string(payloads[0]))
		require.JSONEq(t, `{"headers":{"Authorization":"******"}}`, string(payloads[1]))
		require.Equal(t, []string{"listed"}, *reasons)
	})
}

func newDeliveryMaskingTestWorker(
	features *mockfeatures.MockFeaturesService,
	debugger *mockdestinationdebugger.MockDestinationDebugger,
	disabled bool,
) (*worker, *[]string) {
	reasons := make([]string, 0, 2)
	w := &worker{rt: &Handle{
		destType:                   "TEST_DEST",
		transformerFeaturesService: features,
		debugger:                   debugger,
		reloadableConfig: &reloadableConfig{
			disableEventDeliveryUploadMasking: config.SingleValueLoader(disabled),
		},
	}}
	w.rt.deliveryPayloadMaskingListedCounter = recordingMaskingCounter{reason: "listed", reasons: &reasons}
	w.rt.deliveryPayloadMaskingAllCounter = recordingMaskingCounter{reason: "mask_all", reasons: &reasons}
	w.rt.deliveryPayloadMaskingErrorCounter = recordingMaskingCounter{reason: "mask_error", reasons: &reasons}
	return w, &reasons
}

type recordingMaskingCounter struct {
	reason  string
	reasons *[]string
}

func (c recordingMaskingCounter) Count(n int) {
	for range n {
		*c.reasons = append(*c.reasons, c.reason)
	}
}

func (c recordingMaskingCounter) Increment() {
	c.Count(1)
}

func deliveryStatusMetadata() *types.JobMetadataT {
	return &types.JobMetadataT{
		DestinationID: "destination-id",
		JobT: &jobsdb.JobT{
			Parameters: json.RawMessage(`{"event_name":"event","event_type":"track"}`),
		},
	}
}

func deliveryStatusJobStatus() *jobsdb.JobStatusT {
	return &jobsdb.JobStatusT{
		ErrorCode: "200",
		ExecTime:  time.Now(),
	}
}
