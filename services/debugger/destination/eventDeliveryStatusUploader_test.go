package destinationdebugger

import (
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
	"github.com/tidwall/gjson"
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
					Config: map[string]interface{}{
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

type eventDeliveryStatusUploaderContext struct {
	asyncHelper       testutils.AsyncTestHelper
	mockCtrl          *gomock.Controller
	configInitialised bool
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
}

func (c *eventDeliveryStatusUploaderContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.configInitialised = false
	Setup(c.mockBackendConfig)
}

func (c *eventDeliveryStatusUploaderContext) Finish() {
	c.mockCtrl.Finish()
}

func initEventDeliveryStatusUploader() {
	config.Load()
	logger.Init()
	Init()
}

var _ = Describe("eventDeliveryStatusUploader", func() {
	initEventDeliveryStatusUploader()

	var (
		c              *eventDeliveryStatusUploaderContext
		deliveryStatus DeliveryStatusT
	)

	BeforeEach(func() {
		c = &eventDeliveryStatusUploaderContext{}
		c.Setup()
		deliveryStatus = DeliveryStatusT{
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
		disableEventDeliveryStatusUploads = false
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("RecordEventDeliveryStatus", func() {
		It("returns false if disableEventDeliveryStatusUploads is true", func() {
			mockCall := c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
				Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
					// on Subscribe, emulate a backend configuration event
					go func() {
						channel <- pubsub.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
						c.configInitialised = true
						close(channel)
					}()
				})
			tFunc := c.asyncHelper.ExpectAndNotifyCallback()
			mockCall.Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
				tFunc()
			}).Return().Times(1)

			c.asyncHelper.WaitWithTimeout(5 * time.Second)
			disableEventDeliveryStatusUploads = true
			Expect(RecordEventDeliveryStatus(DestinationIDEnabledA, &deliveryStatus)).To(BeFalse())
		})

		It("returns false if destination_id is not in uploadEnabledDestinationIDs", func() {
			mockCall := c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
				Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
					// on Subscribe, emulate a backend configuration event
					go func() {
						channel <- pubsub.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
						c.configInitialised = true
						close(channel)
					}()
				})
			tFunc := c.asyncHelper.ExpectAndNotifyCallback()
			mockCall.Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
				tFunc()
			}).Return().Times(1)

			c.asyncHelper.WaitWithTimeout(5 * time.Second)
			Expect(RecordEventDeliveryStatus(DestinationIDEnabledB, &deliveryStatus)).To(BeFalse())
		})

		It("records events", func() {
			mockCall := c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
				Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
					// on Subscribe, emulate a backend configuration event
					go func() {
						channel <- pubsub.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
						c.configInitialised = true
						close(channel)
					}()
				})
			tFunc := c.asyncHelper.ExpectAndNotifyCallback()
			mockCall.Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
				tFunc()
			}).Return().Times(1)

			c.asyncHelper.WaitWithTimeout(5 * time.Second)
			eventuallyFunc := func() bool { return RecordEventDeliveryStatus(DestinationIDEnabledA, &deliveryStatus) }
			Eventually(eventuallyFunc).Should(BeTrue())
		})

		It("transforms payload properly", func() {
			mockCall := c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
				Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
					// on Subscribe, emulate a backend configuration event
					go func() {
						channel <- pubsub.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
						c.configInitialised = true
						close(channel)
					}()
				})
			tFunc := c.asyncHelper.ExpectAndNotifyCallback()
			mockCall.Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
				tFunc()
			}).Return().Times(1)

			c.asyncHelper.WaitWithTimeout(5 * time.Second)
			edsUploader := EventDeliveryStatusUploader{}
			rawJSON, err := edsUploader.Transform([]interface{}{&deliveryStatus})
			Expect(err).To(BeNil())
			Expect(gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventName`).String()).To(Equal("some_event_name"))
			Expect(gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventType`).String()).To(Equal("some_event_type"))
		})

		It("sends empty json if transformation fails", func() {
			mockCall := c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
				Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
					// on Subscribe, emulate a backend configuration event
					go func() {
						channel <- pubsub.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
						c.configInitialised = true
						close(channel)
					}()
				})
			tFunc := c.asyncHelper.ExpectAndNotifyCallback()
			mockCall.Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
				tFunc()
			}).Return().Times(1)

			c.asyncHelper.WaitWithTimeout(5 * time.Second)
			edsUploader := EventDeliveryStatusUploader{}
			rawJSON, err := edsUploader.Transform([]interface{}{&faultyData})
			Expect(err.Error()).To(ContainSubstring("error calling MarshalJSON"))
			Expect(rawJSON).To(BeNil())
		})
	})
})
