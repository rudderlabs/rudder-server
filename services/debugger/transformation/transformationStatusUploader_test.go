package transformationdebugger

import (
	"context"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testUtils "github.com/rudderlabs/rudder-server/utils/tests"
	"github.com/rudderlabs/rudder-server/utils/types"
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
							Config:    map[string]interface{}{"eventDelivery": true},
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
							Config:    map[string]interface{}{"eventDelivery": true},
							ID:        "enabled-id",
						},
					},
				},
			},
		},
	},
}

type eventDeliveryStatusUploaderContext struct {
	asyncHelper testUtils.AsyncTestHelper
	mockCtrl    *gomock.Controller
}

func (c *eventDeliveryStatusUploaderContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	tFunc := c.asyncHelper.ExpectAndNotifyCallback()
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			// on Subscribe, emulate a backend configuration event
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{WorkspaceID: sampleBackendConfig}, Topic: string(topic)}
			close(ch)
			tFunc()
			return ch
		}).Times(1)
	Setup(mockBackendConfig)
	c.asyncHelper.WaitWithTimeout(1 * time.Second)
}

func initEventDeliveryStatusUploader() {
	config.Reset()
	logger.Reset()
	Init()
}

var _ = Describe("eventDeliveryStatusUploader", func() {
	initEventDeliveryStatusUploader()

	var (
		c              *eventDeliveryStatusUploaderContext
		deliveryStatus TransformStatusT
	)

	BeforeEach(func() {
		c = &eventDeliveryStatusUploaderContext{}
		c.Setup()
		deliveryStatus = TransformStatusT{
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
		disableTransformationUploads = false
	})

	AfterEach(func() {
		c.mockCtrl.Finish()
	})

	Context("RecordEventDeliveryStatus", func() {
		It("returns false if disableEventDeliveryStatusUploads is true", func() {
			disableTransformationUploads = true
			Expect(UploadTransformationStatus(&TransformationStatusT{})).To(BeFalse())
		})

		It("records events", func() {
			eventuallyFunc := func() bool {
				return UploadTransformationStatus(
					&TransformationStatusT{
						Destination: &sampleBackendConfig.Sources[1].Destinations[1],
						DestID:      sampleBackendConfig.Sources[1].Destinations[1].ID,
						SourceID:    sampleBackendConfig.Sources[1].ID,
					},
				)
			}
			Eventually(eventuallyFunc).Should(BeTrue())
		})

		It("transforms payload properly", func() {
			var edsUploader TransformationStatusUploader
			var payload []*TransformStatusT
			payload = append(payload, &deliveryStatus)
			rawJSON, err := edsUploader.Transform(payload)
			Expect(err).To(BeNil())
			Expect(gjson.GetBytes(rawJSON, `payload.0.eventBefore.eventName`).String()).To(Equal("event-name"))
			Expect(gjson.GetBytes(rawJSON, `payload.0.eventBefore.eventType`).String()).To(Equal("event-type"))
			Expect(gjson.GetBytes(rawJSON, `payload.0.eventsAfter.payload.0.eventName`).String()).To(Equal("event-name"))
			Expect(gjson.GetBytes(rawJSON, `payload.0.eventsAfter.payload.0.eventType`).String()).To(Equal("event-type"))
		})
	})
})
