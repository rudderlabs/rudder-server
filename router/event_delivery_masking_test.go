package router

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/jobsdb"
	mockdestinationdebugger "github.com/rudderlabs/rudder-server/mocks/services/debugger/destination"
	mockfeatures "github.com/rudderlabs/rudder-server/mocks/services/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
)

const deliveryMaskingMetric = "router_delivery_payload_masking"

func TestSendDestinationResponseMasksLiveEventsPayload(t *testing.T) {
	for _, tc := range []struct {
		name       string
		paths      []string
		ok         bool
		input      string
		want       string
		wantReason string
	}{
		{
			name:       "listed paths",
			paths:      []string{"headers.Authorization"},
			ok:         true,
			input:      `{"endpoint":"visible","headers":{"Authorization":"secret"}}`,
			want:       `{"endpoint":"visible","headers":{"Authorization":"******"}}`,
			wantReason: "listed",
		},
		{
			name:       "missing manifest entry",
			ok:         false,
			input:      `{"endpoint":"visible","headers":{"Authorization":"secret"},"body":{"token":"secret"}}`,
			want:       `{"endpoint":"visible","headers":"******","body":"******"}`,
			wantReason: "mask_all",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			features := mockfeatures.NewMockFeaturesService(ctrl)
			features.EXPECT().SecretPaths("TEST_DEST").Return(tc.paths, tc.ok)

			debugger := mockdestinationdebugger.NewMockDestinationDebugger(ctrl)
			debugger.EXPECT().RecordEventDeliveryStatus("destination-id", gomock.Any()).DoAndReturn(
				func(_ string, status *destinationdebugger.DeliveryStatusT) bool {
					require.JSONEq(t, tc.want, string(status.Payload))
					return true
				},
			)

			worker, statsStore := newDeliveryMaskingTestWorker(t, features, debugger, false)
			original := json.RawMessage(tc.input)
			worker.sendDestinationResponseToConfigBackend(original, deliveryStatusMetadata(), deliveryStatusJobStatus(), nil)

			require.Equal(t, map[string]float64{tc.wantReason: 1}, maskingCounts(statsStore))
			require.Equal(t, tc.input, string(original), "caller payload must not be modified")
		})
	}

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

		worker, statsStore := newDeliveryMaskingTestWorker(t, features, debugger, true)
		payload := json.RawMessage(`{"headers":{"Authorization":"secret"}}`)
		worker.sendDestinationResponseToConfigBackend(payload, deliveryStatusMetadata(), deliveryStatusJobStatus(), nil)

		features.EXPECT().SecretPaths("TEST_DEST").Return([]string{"headers.Authorization"}, true)
		worker.rt.reloadableConfig.disableEventDeliveryUploadMasking = config.SingleValueLoader(false)
		worker.sendDestinationResponseToConfigBackend(payload, deliveryStatusMetadata(), deliveryStatusJobStatus(), nil)

		require.JSONEq(t, `{"headers":{"Authorization":"secret"}}`, string(payloads[0]))
		require.JSONEq(t, `{"headers":{"Authorization":"******"}}`, string(payloads[1]))
		require.Equal(t, map[string]float64{"listed": 1}, maskingCounts(statsStore))
	})
}

func newDeliveryMaskingTestWorker(
	t *testing.T,
	features *mockfeatures.MockFeaturesService,
	debugger *mockdestinationdebugger.MockDestinationDebugger,
	disabled bool,
) (*worker, *memstats.Store) {
	t.Helper()
	statsStore, err := memstats.New()
	require.NoError(t, err)
	return &worker{rt: &Handle{
		destType:                   "TEST_DEST",
		transformerFeaturesService: features,
		debugger:                   debugger,
		reloadableConfig: &reloadableConfig{
			disableEventDeliveryUploadMasking: config.SingleValueLoader(disabled),
		},
		deliveryPayloadMaskingStat: func(reason string) stats.Counter {
			return statsStore.NewTaggedStat(deliveryMaskingMetric, stats.CountType, stats.Tags{"destType": "TEST_DEST", "reason": reason})
		},
	}}, statsStore
}

// maskingCounts returns the non-zero masking counter values keyed by reason.
func maskingCounts(statsStore *memstats.Store) map[string]float64 {
	counts := map[string]float64{}
	for _, reason := range []string{"listed", "mask_all", "mask_error"} {
		if m := statsStore.Get(deliveryMaskingMetric, stats.Tags{"destType": "TEST_DEST", "reason": reason}); m != nil && m.LastValue() > 0 {
			counts[reason] = m.LastValue()
		}
	}
	return counts
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
