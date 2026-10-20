package sourcedebugger

import (
	"context"
	"encoding/json"
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
	SourceIDEnabled       = "enabled-source"
	SourceIDDisabled      = "disabled-source"
	DestinationIDEnabledA = "enabled-destination-a" // test destination router
	DestinationIDEnabledB = "enabled-destination-b" // test destination batch router
	DestinationIDDisabled = "disabled-destination"
)

type basicAuthMock struct{}

func (*basicAuthMock) BasicAuth() (string, string) { return "test", "test" }
func (*basicAuthMock) ID() string                  { return "test" }
func (*basicAuthMock) Type() deployment.Type       { return "test" }

type eventUploaderFixture struct {
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
	recordingEvent    string
	h                 SourceDebugger
}

func newEventUploaderFixture(t *testing.T) *eventUploaderFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &eventUploaderFixture{
		mockBackendConfig: mocksBackendConfig.NewMockBackendConfig(ctrl),
		recordingEvent:    `{"t":"a"}`,
	}
	f.mockBackendConfig.EXPECT().Identity().AnyTimes().Return(&basicAuthMock{})
	f.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).AnyTimes().
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{workspaceID: sampleBackendConfig}, Topic: string(topic)}
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch
		})
	return f
}

func (f *eventUploaderFixture) withBadgerHandle(t *testing.T) {
	t.Helper()
	config.Reset()
	t.Cleanup(config.Reset)
	config.Set("RUDDER_TMPDIR", path.Join(t.TempDir(), rand.String(10)))
	config.Set("LiveEvent.cache.GCTime", "1s")
	f.startHandle(t)
}

func (f *eventUploaderFixture) withMemoryHandle(t *testing.T) {
	t.Helper()
	config.Reset()
	t.Cleanup(config.Reset)
	config.Set("SourceDebugger.cacheType", 0)
	config.Set("RUDDER_TMPDIR", path.Join(t.TempDir(), rand.String(10)))
	config.Set("LiveEvent.cache.GCTime", "1s")
	f.startHandle(t)
}

func (f *eventUploaderFixture) startHandle(t *testing.T) {
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

func TestEventUploader(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	t.Run("Source Debugger Test Badger", func(t *testing.T) {
		t.Run("returns false if disableEventUploads is true", func(t *testing.T) {
			f := newEventUploaderFixture(t)
			f.withBadgerHandle(t)
			f.h.Stop()
			h, err := NewHandle(f.mockBackendConfig)
			require.NoError(t, err)
			t.Cleanup(h.Stop)
			h.(*Handle).disableEventUploads = config.SingleValueLoader(true)
			require.False(t, h.RecordEvent(sampleWriteKey, []byte(f.recordingEvent)))
		})

		t.Run("returns false if writeKey is not part of uploadEnabledWriteKeys", func(t *testing.T) {
			f := newEventUploaderFixture(t)
			f.withBadgerHandle(t)
			require.False(t, f.h.RecordEvent(sampleWriteKey, []byte(f.recordingEvent)))
		})

		t.Run("transforms payload properly", func(t *testing.T) {
			f := newEventUploaderFixture(t)
			f.withBadgerHandle(t)
			recordingEvent0 := `{"receivedAt":"2021-08-03T17:26:","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":{"name": "Demo Track"},"integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`
			f.recordingEvent = `{"receivedAt":"2021-08-03T17:26:00.279+05:30","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}` // gitleaks:allow
			eventUploader := NewEventUploader(logger.NOP)
			var payload []*GatewayEventBatchT
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(recordingEvent0)})
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(f.recordingEvent)})
			rawJson, err := eventUploader.Transform(payload)
			require.NoError(t, err)
			require.Equal(t, "{\"name\":\"Demo Track\"}", gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.0.eventName`).String())
			require.Equal(t, "Demo Track", gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.1.eventName`).String())
			require.Equal(t, "track", gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.1.eventType`).String())
		})

		t.Run("ignores improperly built payload", func(t *testing.T) {
			f := newEventUploaderFixture(t)
			f.withBadgerHandle(t)
			recordingEvent0 := `{"receivedAt":"2021-08-03T17:26:","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":{"name": "Demo Track"},"integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}`
			eventUploader := NewEventUploader(logger.NOP)
			var payload []*GatewayEventBatchT
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(recordingEvent0)})
			rawJson, err := eventUploader.Transform(payload)
			require.NoError(t, err)
			require.Equal(t, `{"version":"v2"}`, string(rawJson))
		})

		t.Run("records events", func(t *testing.T) {
			f := newEventUploaderFixture(t)
			f.withBadgerHandle(t)
			f.recordingEvent = `{"receivedAt":"2021-08-03T17:26:00.279+05:30","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}` // gitleaks:allow
			eventuallyFunc := func() bool { return f.h.RecordEvent(WriteKeyEnabled, []byte(f.recordingEvent)) }
			require.Eventually(t, eventuallyFunc, time.Second, 10*time.Millisecond)
		})
	})

	t.Run("Source Debugger Test Memory", func(t *testing.T) {
		t.Run("returns false if disableEventUploads is true", func(t *testing.T) {
			f := newEventUploaderFixture(t)
			f.withMemoryHandle(t)
			f.h.Stop()
			h, err := NewHandle(f.mockBackendConfig)
			require.NoError(t, err)
			t.Cleanup(h.Stop)
			h.(*Handle).disableEventUploads = config.SingleValueLoader(true)
			require.False(t, h.RecordEvent(sampleWriteKey, []byte(f.recordingEvent)))
		})

		t.Run("returns false if writeKey is not part of uploadEnabledWriteKeys", func(t *testing.T) {
			f := newEventUploaderFixture(t)
			f.withMemoryHandle(t)
			require.False(t, f.h.RecordEvent(sampleWriteKey, []byte(f.recordingEvent)))
		})

		t.Run("transforms payload properly", func(t *testing.T) {
			f := newEventUploaderFixture(t)
			f.withMemoryHandle(t)
			recordingEvent0 := `{"receivedAt":"2021-08-03T17:26:","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":{"name": "Demo Track"},"integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`
			f.recordingEvent = `{"receivedAt":"2021-08-03T17:26:00.279+05:30","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}` // gitleaks:allow
			eventUploader := NewEventUploader(logger.NOP)
			var payload []*GatewayEventBatchT
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(recordingEvent0)})
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(f.recordingEvent)})
			rawJson, err := eventUploader.Transform(payload)
			require.NoError(t, err)
			require.Equal(t, "{\"name\":\"Demo Track\"}", gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.0.eventName`).String())
			require.Equal(t, "Demo Track", gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.1.eventName`).String())
			require.Equal(t, "track", gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.1.eventType`).String())
		})

		t.Run("ignores improperly built payload", func(t *testing.T) {
			f := newEventUploaderFixture(t)
			f.withMemoryHandle(t)
			recordingEvent0 := `{"receivedAt":"2021-08-03T17:26:","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":{"name": "Demo Track"},"integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}`
			eventUploader := NewEventUploader(logger.NOP)
			var payload []*GatewayEventBatchT
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(recordingEvent0)})
			rawJson, err := eventUploader.Transform(payload)
			require.NoError(t, err)
			require.Equal(t, `{"version":"v2"}`, string(rawJson))
		})

		t.Run("records events", func(t *testing.T) {
			f := newEventUploaderFixture(t)
			f.withMemoryHandle(t)
			f.recordingEvent = `{"receivedAt":"2021-08-03T17:26:00.279+05:30","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}` // gitleaks:allow
			eventuallyFunc := func() bool { return f.h.RecordEvent(WriteKeyEnabled, []byte(f.recordingEvent)) }
			require.Eventually(t, eventuallyFunc, time.Second, 10*time.Millisecond)
		})
	})
}

var workspaceID = "workspace"

var sampleBackendConfig = backendconfig.ConfigT{
	WorkspaceID: workspaceID,
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
			Config:   json.RawMessage(`{"eventUpload": true}`),
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

var sampleWriteKey = "random_write_key"
