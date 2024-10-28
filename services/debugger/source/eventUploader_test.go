package sourcedebugger

import (
	"context"
	"path"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
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

type eventUploaderContext struct {
	mockCtrl          *gomock.Controller
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
}

// Initialize mocks and common expectations
func (c *eventUploaderContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockBackendConfig.EXPECT().Identity().AnyTimes().Return(&testutils.BasicAuthMock{})
}

func (c *eventUploaderContext) Finish() {
	c.mockCtrl.Finish()
}

func initEventUploader() {
	config.Reset()
	logger.Reset()
	misc.Init()
}

var _ = Describe("eventUploader", func() {
	initEventUploader()

	var (
		c              *eventUploaderContext
		recordingEvent string
		h              SourceDebugger
	)

	BeforeEach(func() {
		c = &eventUploaderContext{}
		c.Setup()
		recordingEvent = `{"t":"a"}`
		c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).AnyTimes().
			DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
				ch := make(chan pubsub.DataEvent, 1)
				ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{workspaceID: sampleBackendConfig}, Topic: string(topic)}
				go func() {
					<-ctx.Done()
					close(ch)
				}()
				return ch
			})
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Source Debugger Test Badger", func() {
		BeforeEach(func() {
			var err error
			config.Reset()
			config.Set("RUDDER_TMPDIR", path.Join(GinkgoT().TempDir(), rand.String(10)))
			config.Set("LiveEvent.cache.GCTime", "1s")
			h, err = NewHandle(c.mockBackendConfig)
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			h.Stop()
		})

		It("returns false if disableEventUploads is true", func() {
			h.Stop()
			h, err := NewHandle(c.mockBackendConfig)
			Expect(err).To(BeNil())
			h.(*Handle).disableEventUploads = config.SingleValueLoader(true)
			Expect(h.RecordEvent(sampleWriteKey, []byte(recordingEvent))).To(BeFalse())
		})

		It("returns false if writeKey is not part of uploadEnabledWriteKeys", func() {
			Expect(h.RecordEvent(sampleWriteKey, []byte(recordingEvent))).To(BeFalse())
		})

		It("transforms payload properly", func() {
			recordingEvent0 := `{"receivedAt":"2021-08-03T17:26:","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":{"name": "Demo Track"},"integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`
			recordingEvent = `{"receivedAt":"2021-08-03T17:26:00.279+05:30","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`
			eventUploader := NewEventUploader(logger.NOP)
			var payload []*GatewayEventBatchT
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(recordingEvent0)})
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(recordingEvent)})
			rawJson, err := eventUploader.Transform(payload)
			Expect(err).To(BeNil())
			Expect(gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.0.eventName`).String()).To(Equal("{\"name\":\"Demo Track\"}"))
			Expect(gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.1.eventName`).String()).To(Equal("Demo Track"))
			Expect(gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.1.eventType`).String()).To(Equal("track"))
		})

		It("ignores improperly built payload", func() {
			recordingEvent0 := `{"receivedAt":"2021-08-03T17:26:","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":{"name": "Demo Track"},"integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}`
			eventUploader := NewEventUploader(logger.NOP)
			var payload []*GatewayEventBatchT
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(recordingEvent0)})
			rawJson, err := eventUploader.Transform(payload)
			Expect(err).To(BeNil())
			Expect(string(rawJson)).To(Equal(`{"version":"v2"}`))
		})

		It("records events", func() {
			recordingEvent = `{"receivedAt":"2021-08-03T17:26:00.279+05:30","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`
			eventuallyFunc := func() bool { return h.RecordEvent(WriteKeyEnabled, []byte(recordingEvent)) }
			Eventually(eventuallyFunc).Should(BeTrue())
		})
	})

	Context("Source Debugger Test Memory", func() {
		BeforeEach(func() {
			var err error
			config.Reset()
			config.Set("SourceDebugger.cacheType", 0)
			config.Set("RUDDER_TMPDIR", path.Join(GinkgoT().TempDir(), rand.String(10)))
			config.Set("LiveEvent.cache.GCTime", "1s")
			h, err = NewHandle(c.mockBackendConfig)
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			h.Stop()
		})

		It("returns false if disableEventUploads is true", func() {
			h.Stop()
			h, err := NewHandle(c.mockBackendConfig)
			Expect(err).To(BeNil())
			h.(*Handle).disableEventUploads = config.SingleValueLoader(true)
			Expect(h.RecordEvent(sampleWriteKey, []byte(recordingEvent))).To(BeFalse())
		})

		It("returns false if writeKey is not part of uploadEnabledWriteKeys", func() {
			Expect(h.RecordEvent(sampleWriteKey, []byte(recordingEvent))).To(BeFalse())
		})

		It("transforms payload properly", func() {
			recordingEvent0 := `{"receivedAt":"2021-08-03T17:26:","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":{"name": "Demo Track"},"integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`
			recordingEvent = `{"receivedAt":"2021-08-03T17:26:00.279+05:30","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`
			eventUploader := NewEventUploader(logger.NOP)
			var payload []*GatewayEventBatchT
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(recordingEvent0)})
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(recordingEvent)})
			rawJson, err := eventUploader.Transform(payload)
			Expect(err).To(BeNil())
			Expect(gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.0.eventName`).String()).To(Equal("{\"name\":\"Demo Track\"}"))
			Expect(gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.1.eventName`).String()).To(Equal("Demo Track"))
			Expect(gjson.GetBytes(rawJson, `1vWezJfHKkbUHexNepDsGcSVWae.1.eventType`).String()).To(Equal("track"))
		})

		It("ignores improperly built payload", func() {
			recordingEvent0 := `{"receivedAt":"2021-08-03T17:26:","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":{"name": "Demo Track"},"integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}`
			eventUploader := NewEventUploader(logger.NOP)
			var payload []*GatewayEventBatchT
			payload = append(payload, &GatewayEventBatchT{WriteKey: WriteKeyEnabled, EventBatch: []byte(recordingEvent0)})
			rawJson, err := eventUploader.Transform(payload)
			Expect(err).To(BeNil())
			Expect(string(rawJson)).To(Equal(`{"version":"v2"}`))
		})

		It("records events", func() {
			recordingEvent = `{"receivedAt":"2021-08-03T17:26:00.279+05:30","writeKey":"1vWezJfHKkbUHexNepDsGcSVWae","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"7a355fdd-0325-4778-9905-b43f586acdd4","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`
			eventuallyFunc := func() bool { return h.RecordEvent(WriteKeyEnabled, []byte(recordingEvent)) }
			Eventually(eventuallyFunc).Should(BeTrue())
		})
	})
})

type sourceConfigMap map[string]interface{}

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
			Config:   sourceConfigMap{"eventUpload": true},
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
						Config:      map[string]interface{}{},
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
						Config:      map[string]interface{}{},
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
						Config:      map[string]interface{}{},
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
						Config:      map[string]interface{}{},
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
						Config:      map[string]interface{}{},
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
						Config:      map[string]interface{}{},
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
