package transformationdebugger

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

const (
	WriteKeyEnabled       = "enabled-write-key"
	WriteKeyEnabledNoUT   = "enabled-write-key-no-ut"
	WriteKeyEnabledOnlyUT = "enabled-write-key-only-ut"
	WorkspaceID           = "some-workspace-id"
	SourceIDEnabled       = "enabled-source"
	SourceIDDisabled      = "disabled-source"
	DestinationIDEnabledA = "enabled-destination-a" // test destination router
	DestinationIDEnabledB = "enabled-destination-b" // test destination batch router
	DestinationIDDisabled = "disabled-destination"
)

var sampleBackendConfig = backendconfig.ConfigT{
	WorkspaceID: WorkspaceID,
	Sources: []backendconfig.SourceT{
		{
			ID:       SourceIDDisabled,
			WriteKey: WriteKeyEnabled,
			Enabled:  false,
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabled,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledA,
					Name:               "A",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]any{
						"eventDelivery": true,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
					},
				},
				{
					ID:                 DestinationIDEnabledB,
					Name:               "B",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-b-definition-id",
						Name:        "MINIO",
						DisplayName: "enabled-destination-b-definition-display-name",
						Config:      map[string]any{},
					},
					Transformations: []backendconfig.TransformationT{
						{
							VersionID: "transformation-version-id",
							Config:    map[string]any{"eventDelivery": true},
							ID:        "enabled-id",
						},
					},
				},
				// This destination should receive no events
				{
					ID:                 DestinationIDDisabled,
					Name:               "C",
					Enabled:            false,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-disabled",
						Name:        "destination-definition-name-disabled",
						DisplayName: "destination-definition-display-name-disabled",
						Config:      map[string]any{},
					},
				},
			},
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabledNoUT,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledA,
					Name:               "A",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
						Config:      map[string]any{},
					},
				},
				// This destination should receive no events
				{
					ID:                 DestinationIDDisabled,
					Name:               "C",
					Enabled:            false,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-disabled",
						Name:        "destination-definition-name-disabled",
						DisplayName: "destination-definition-display-name-disabled",
						Config:      map[string]any{},
					},
				},
			},
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabledOnlyUT,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledB,
					Name:               "B",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-b-definition-id",
						Name:        "MINIO",
						DisplayName: "enabled-destination-b-definition-display-name",
						Config:      map[string]any{},
					},
					Transformations: []backendconfig.TransformationT{
						{
							VersionID: "transformation-version-id",
							Config:    map[string]any{"eventDelivery": true},
							ID:        "enabled-id",
						},
					},
				},
			},
		},
	},
}

type eventDeliveryStatusUploaderFixture struct {
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
	h                 TransformationDebugger
	deliveryStatus    TransformStatusT
}

func newEventDeliveryStatusUploaderFixture(t *testing.T) *eventDeliveryStatusUploaderFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &eventDeliveryStatusUploaderFixture{
		mockBackendConfig: mocksBackendConfig.NewMockBackendConfig(ctrl),
	}
	f.mockBackendConfig.EXPECT().Identity().AnyTimes().Return(&testutils.BasicAuthMock{})
	f.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			// on Subscribe, emulate a backend configuration event
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{WorkspaceID: sampleBackendConfig}, Topic: string(topic)}
			close(ch)
			return ch
		}).AnyTimes()
	f.deliveryStatus = TransformStatusT{
		DestinationID:    DestinationIDEnabledB,
		SourceID:         SourceIDEnabled,
		IsError:          false,
		TransformationID: "enabled-id",
		EventBefore: &EventBeforeTransform{
			ReceivedAt: time.Now().String(),
			EventName:  "event-name",
			EventType:  "event-type",
			Payload:    types.SingularEventT{},
		},
		EventsAfter: &EventsAfterTransform{
			ReceivedAt: time.Now().String(),
			IsDropped:  false,
			Error:      "",
			StatusCode: 200,
			EventPayloads: []*EventPayloadAfterTransform{
				{
					EventName: "event-name",
					EventType: "event-type",
					Payload:   types.SingularEventT{},
				},
			},
		},
	}
	return f
}

func (f *eventDeliveryStatusUploaderFixture) withBadgerHandle(t *testing.T) {
	t.Helper()
	config.Reset()
	t.Cleanup(config.Reset)
	config.Set("RUDDER_TMPDIR", path.Join(t.TempDir(), rand.String(10)))
	f.startHandle(t)
}

func (f *eventDeliveryStatusUploaderFixture) withMemoryHandle(t *testing.T) {
	t.Helper()
	config.Reset()
	t.Cleanup(config.Reset)
	config.Set("RUDDER_TMPDIR", path.Join(t.TempDir(), rand.String(10)))
	config.Set("TransformationDebugger.cacheType", 0)
	f.startHandle(t)
}

func (f *eventDeliveryStatusUploaderFixture) startHandle(t *testing.T) {
	t.Helper()
	h, err := NewHandle(f.mockBackendConfig)
	require.NoError(t, err)
	f.h = h
	t.Cleanup(h.Stop)
}

func TestTransformationStatusUploader(t *testing.T) {
	config.Reset()
	logger.Reset()

	t.Run("Transformation Debugger Badger", func(t *testing.T) {
		t.Run("returns false if disableEventDeliveryStatusUploads is true", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withBadgerHandle(t)
			f.h.Stop()
			h, err := NewHandle(f.mockBackendConfig)
			require.NoError(t, err)
			t.Cleanup(h.Stop)
			h.(*Handle).disableTransformationUploads = config.SingleValueLoader(true)
			require.False(t, h.UploadTransformationStatus(&TransformationStatusT{}))
		})

		t.Run("records events", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withBadgerHandle(t)
			// UploadTransformationStatus mutates the destination it is given (see Limit), while the
			// handle's config subscriber reads sampleBackendConfig concurrently, so it gets a copy.
			destination := sampleBackendConfig.Sources[1].Destinations[1]
			eventuallyFunc := func() bool {
				return f.h.UploadTransformationStatus(
					&TransformationStatusT{
						Destination: &destination,
						DestID:      sampleBackendConfig.Sources[1].Destinations[1].ID,
						SourceID:    sampleBackendConfig.Sources[1].ID,
					},
				)
			}
			require.Eventually(t, eventuallyFunc, time.Second, 10*time.Millisecond)
		})

		t.Run("transforms payload properly", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withBadgerHandle(t)
			var edsUploader TransformationStatusUploader
			var payload []*TransformStatusT
			payload = append(payload, &f.deliveryStatus)
			rawJSON, err := edsUploader.Transform(payload)
			require.NoError(t, err)
			require.Equal(t, "event-name", gjson.GetBytes(rawJSON, `payload.0.eventBefore.eventName`).String())
			require.Equal(t, "event-type", gjson.GetBytes(rawJSON, `payload.0.eventBefore.eventType`).String())
			require.Equal(t, "event-name", gjson.GetBytes(rawJSON, `payload.0.eventsAfter.payload.0.eventName`).String())
			require.Equal(t, "event-type", gjson.GetBytes(rawJSON, `payload.0.eventsAfter.payload.0.eventType`).String())
		})
	})

	t.Run("Transformation Debugger Memory", func(t *testing.T) {
		t.Run("returns false if disableEventDeliveryStatusUploads is true", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withMemoryHandle(t)
			f.h.Stop()
			h, err := NewHandle(f.mockBackendConfig)
			require.NoError(t, err)
			t.Cleanup(h.Stop)
			h.(*Handle).disableTransformationUploads = config.SingleValueLoader(true)
			require.False(t, h.UploadTransformationStatus(&TransformationStatusT{}))
		})

		t.Run("records events", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withMemoryHandle(t)
			// UploadTransformationStatus mutates the destination it is given (see Limit), while the
			// handle's config subscriber reads sampleBackendConfig concurrently, so it gets a copy.
			destination := sampleBackendConfig.Sources[1].Destinations[1]
			eventuallyFunc := func() bool {
				return f.h.UploadTransformationStatus(
					&TransformationStatusT{
						Destination: &destination,
						DestID:      sampleBackendConfig.Sources[1].Destinations[1].ID,
						SourceID:    sampleBackendConfig.Sources[1].ID,
					},
				)
			}
			require.Eventually(t, eventuallyFunc, time.Second, 10*time.Millisecond)
		})

		t.Run("transforms payload properly", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withMemoryHandle(t)
			var edsUploader TransformationStatusUploader
			var payload []*TransformStatusT
			payload = append(payload, &f.deliveryStatus)
			rawJSON, err := edsUploader.Transform(payload)
			require.NoError(t, err)
			require.Equal(t, "event-name", gjson.GetBytes(rawJSON, `payload.0.eventBefore.eventName`).String())
			require.Equal(t, "event-type", gjson.GetBytes(rawJSON, `payload.0.eventBefore.eventType`).String())
			require.Equal(t, "event-name", gjson.GetBytes(rawJSON, `payload.0.eventsAfter.payload.0.eventName`).String())
			require.Equal(t, "event-type", gjson.GetBytes(rawJSON, `payload.0.eventsAfter.payload.0.eventType`).String())
		})
	})
}

func TestLimit(t *testing.T) {
	var (
		singularEvent1 = types.SingularEventT{"payload": "event-1"}
		metadata1      = types.Metadata{MessageID: "message-id-1"}
		singularEvent2 = types.SingularEventT{"payload": "event-2"}
		metadata2      = types.Metadata{MessageID: "message-id-2"}
		singularEvent3 = types.SingularEventT{"payload": "event-3"}
		metadata3      = types.Metadata{MessageID: "message-id-3"}
		singularEvent4 = types.SingularEventT{"payload": "event-4"}
		metadata4      = types.Metadata{MessageID: "message-id-4"}
		now            = time.Now()
		limit          = 1
	)
	t.Run("should limit the received transformation status", func(t *testing.T) {
		tStatus := &TransformationStatusT{
			Destination: &sampleBackendConfig.Sources[1].Destinations[1],
			DestID:      sampleBackendConfig.Sources[1].Destinations[1].ID,
			SourceID:    sampleBackendConfig.Sources[1].ID,
			UserTransformedEvents: []types.TransformerEvent{
				{
					Message:  singularEvent1,
					Metadata: metadata1,
				},
				{
					Message:  singularEvent2,
					Metadata: metadata2,
				},
			},
			EventsByMessageID: map[string]types.SingularEventWithReceivedAt{
				metadata1.MessageID: {
					SingularEvent: singularEvent1,
					ReceivedAt:    now,
				},
				metadata2.MessageID: {
					SingularEvent: singularEvent2,
					ReceivedAt:    now,
				},
				metadata3.MessageID: {
					SingularEvent: singularEvent3,
					ReceivedAt:    now,
				},
				metadata4.MessageID: {
					SingularEvent: singularEvent4,
					ReceivedAt:    now,
				},
			},
			FailedEvents: []types.TransformerResponse{
				{
					Output:   singularEvent1,
					Metadata: metadata3,
					Error:    "some_error1",
				},
				{
					Output:   singularEvent2,
					Metadata: metadata4,
					Error:    "some_error2",
				},
			},
			UniqueMessageIds: map[string]struct{}{
				metadata1.MessageID: {},
				metadata2.MessageID: {},
				metadata3.MessageID: {},
				metadata4.MessageID: {},
			},
		}
		limitedTStatus := tStatus.Limit(
			1,
			sampleBackendConfig.Sources[1].Destinations[1].Transformations[0],
		)
		require.Equal(t, limit, len(limitedTStatus.UserTransformedEvents))
		require.Equal(t, limit, len(limitedTStatus.FailedEvents))
		require.Equal(t, limit*2, len(limitedTStatus.UniqueMessageIds))
		require.Equal(t, limit*2, len(limitedTStatus.EventsByMessageID))
		require.Equal(
			t,
			[]backendconfig.TransformationT{sampleBackendConfig.Sources[1].Destinations[1].Transformations[0]},
			limitedTStatus.Destination.Transformations,
		)
		require.Equal(
			t,
			[]types.TransformerEvent{
				{
					Message:  singularEvent1,
					Metadata: metadata1,
				},
			},
			limitedTStatus.UserTransformedEvents,
		)
		require.Equal(
			t,
			[]types.TransformerResponse{
				{
					Output:   singularEvent1,
					Metadata: metadata3,
					Error:    "some_error1",
				},
			},
			limitedTStatus.FailedEvents,
		)
		require.Equal(
			t,
			map[string]struct{}{
				metadata1.MessageID: {},
				metadata3.MessageID: {},
			},
			limitedTStatus.UniqueMessageIds,
		)
		require.Equal(
			t,
			map[string]types.SingularEventWithReceivedAt{
				metadata1.MessageID: {
					SingularEvent: singularEvent1,
					ReceivedAt:    now,
				},
				metadata3.MessageID: {
					SingularEvent: singularEvent3,
					ReceivedAt:    now,
				},
			},
			limitedTStatus.EventsByMessageID,
		)
	})
}
