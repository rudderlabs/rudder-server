package destinationdebugger

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
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
						},
					},
				},
			},
		},
	},
}

var faultyData = DeliveryStatusT{
	DestinationID: DestinationIDEnabledA,
	SourceID:      SourceIDEnabled,
	Payload:       []byte(`{"t":"a"`),
	AttemptNum:    1,
	JobState:      `failed`,
	ErrorCode:     `404`,
	ErrorResponse: []byte(`{"name": "error"}`),
	SentAt:        "",
	EventName:     `some_event_name`,
	EventType:     `some_event_type`,
}

type basicAuthMock struct{}

func (*basicAuthMock) BasicAuth() (string, string) { return "test", "test" }
func (*basicAuthMock) ID() string                  { return "test" }
func (*basicAuthMock) Type() deployment.Type       { return "test" }

type eventDeliveryStatusUploaderFixture struct {
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
	deliveryStatus    DeliveryStatusT
	h                 DestinationDebugger
}

func newEventDeliveryStatusUploaderFixture(t *testing.T) *eventDeliveryStatusUploaderFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &eventDeliveryStatusUploaderFixture{
		mockBackendConfig: mocksBackendConfig.NewMockBackendConfig(ctrl),
	}
	f.mockBackendConfig.EXPECT().Identity().AnyTimes().Return(&basicAuthMock{})
	f.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			// on Subscribe, emulate a backend configuration event
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{WorkspaceID: sampleBackendConfig}, Topic: string(topic)}
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch
		}).AnyTimes()

	f.deliveryStatus = DeliveryStatusT{
		DestinationID: DestinationIDEnabledA,
		SourceID:      SourceIDEnabled,
		Payload:       []byte(`{"t":"a"}`),
		AttemptNum:    1,
		JobState:      `failed`,
		ErrorCode:     `404`,
		ErrorResponse: []byte(`{"name": "error"}`),
		SentAt:        "",
		EventName:     `some_event_name`,
		EventType:     `some_event_type`,
	}
	return f
}

func (f *eventDeliveryStatusUploaderFixture) withBadgerHandle(t *testing.T) {
	t.Helper()
	config.Reset()
	t.Cleanup(config.Reset)
	config.Set("RUDDER_TMPDIR", path.Join(t.TempDir(), rand.String(10)))
	config.Set("LiveEvent.cache.GCTime", "1s")
	f.startHandle(t)
}

func (f *eventDeliveryStatusUploaderFixture) withMemoryHandle(t *testing.T) {
	t.Helper()
	config.Reset()
	t.Cleanup(config.Reset)
	config.Set("DestinationDebugger.cacheType", 0)
	config.Set("RUDDER_TMPDIR", path.Join(t.TempDir(), rand.String(10)))
	config.Set("LiveEvent.cache.GCTime", "1s")
	f.startHandle(t)
}

func (f *eventDeliveryStatusUploaderFixture) startHandle(t *testing.T) {
	t.Helper()
	// The uploader flushes recorded events on Stop; keep them off the real config backend.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	config.Set("CONFIG_BACKEND_URL", srv.URL)
	h, err := NewHandle(f.mockBackendConfig)
	require.NoError(t, err)
	f.h = h
	t.Cleanup(h.Stop)
}

func TestEventDeliveryStatusUploader(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	t.Run("RecordEventDeliveryStatus Badger", func(t *testing.T) {
		t.Run("returns false if disableEventDeliveryStatusUploads is true", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withBadgerHandle(t)
			f.h.Stop()
			h, err := NewHandle(f.mockBackendConfig)
			require.NoError(t, err)
			t.Cleanup(h.Stop)
			h.(*Handle).disableEventDeliveryStatusUploads = config.SingleValueLoader(true)
			require.False(t, h.RecordEventDeliveryStatus(DestinationIDEnabledA, &f.deliveryStatus))
		})

		t.Run("returns false if destination_id is not in uploadEnabledDestinationIDs", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withBadgerHandle(t)
			require.False(t, f.h.RecordEventDeliveryStatus(DestinationIDEnabledB, &f.deliveryStatus))
		})

		t.Run("records events", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withBadgerHandle(t)
			eventuallyFunc := func() bool { return f.h.RecordEventDeliveryStatus(DestinationIDEnabledA, &f.deliveryStatus) }
			require.Eventually(t, eventuallyFunc, time.Second, 10*time.Millisecond)
		})

		t.Run("transforms payload properly", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withBadgerHandle(t)
			var edsUploader EventDeliveryStatusUploader
			var payload []*DeliveryStatusT
			payload = append(payload, &f.deliveryStatus)
			rawJSON, err := edsUploader.Transform(payload)
			require.NoError(t, err)
			require.Equal(t, "some_event_name", gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventName`).String())
			require.Equal(t, "some_event_type", gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventType`).String())
		})

		t.Run("sends empty json if transformation fails", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withBadgerHandle(t)
			edsUploader := NewEventDeliveryStatusUploader(logger.NOP)
			var payload []*DeliveryStatusT
			payload = append(payload, &faultyData)
			rawJSON, err := edsUploader.Transform(payload)
			if err != nil {
				require.Error(t, err)
			} else { // jsoniter doesn't return an error, just sets null for invalid json.RawMessage
				require.Equal(t, "null", gjson.GetBytes(rawJSON, DestinationIDEnabledA+".0.payload").Raw)
			}
		})
	})

	t.Run("RecordEventDeliveryStatus Memory", func(t *testing.T) {
		t.Run("returns false if disableEventDeliveryStatusUploads is true", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withMemoryHandle(t)
			f.h.Stop()
			h, err := NewHandle(f.mockBackendConfig)
			require.NoError(t, err)
			t.Cleanup(h.Stop)
			h.(*Handle).disableEventDeliveryStatusUploads = config.SingleValueLoader(true)
			require.False(t, h.RecordEventDeliveryStatus(DestinationIDEnabledA, &f.deliveryStatus))
		})

		t.Run("returns false if destination_id is not in uploadEnabledDestinationIDs", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withMemoryHandle(t)
			require.False(t, f.h.RecordEventDeliveryStatus(DestinationIDEnabledB, &f.deliveryStatus))
		})

		t.Run("records events", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withMemoryHandle(t)
			eventuallyFunc := func() bool { return f.h.RecordEventDeliveryStatus(DestinationIDEnabledA, &f.deliveryStatus) }
			require.Eventually(t, eventuallyFunc, time.Second, 10*time.Millisecond)
		})

		t.Run("transforms payload properly", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withMemoryHandle(t)
			var edsUploader EventDeliveryStatusUploader
			var payload []*DeliveryStatusT
			payload = append(payload, &f.deliveryStatus)
			rawJSON, err := edsUploader.Transform(payload)
			require.NoError(t, err)
			require.Equal(t, "some_event_name", gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventName`).String())
			require.Equal(t, "some_event_type", gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventType`).String())
		})

		t.Run("sends empty json if transformation fails", func(t *testing.T) {
			f := newEventDeliveryStatusUploaderFixture(t)
			f.withMemoryHandle(t)
			edsUploader := NewEventDeliveryStatusUploader(logger.NOP)
			var payload []*DeliveryStatusT
			payload = append(payload, &faultyData)
			rawJSON, err := edsUploader.Transform(payload)
			if err != nil {
				require.Nil(t, rawJSON)
			} else {
				// jsoniter doesn't return an error, just sets null for invalid json.RawMessage
				require.Equal(t, "null", gjson.GetBytes(rawJSON, DestinationIDEnabledA+".0.payload").Raw)
			}
		})
	})
}
