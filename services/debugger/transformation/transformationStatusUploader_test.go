package transformationdebugger

import (
	"context"
	"path"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
	"github.com/rudderlabs/rudder-server/utils/types"
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
	mockCtrl          *gomock.Controller
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
}

func (c *eventDeliveryStatusUploaderContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockBackendConfig.EXPECT().Identity().AnyTimes().Return(&testutils.BasicAuthMock{})
	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			// on Subscribe, emulate a backend configuration event
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{WorkspaceID: sampleBackendConfig}, Topic: string(topic)}
			close(ch)
			return ch
		}).AnyTimes()
}

func initEventDeliveryStatusUploader() {
	config.Reset()
	logger.Reset()
}

var _ = Describe("eventDeliveryStatusUploader", func() {
	initEventDeliveryStatusUploader()

	var (
		c              *eventDeliveryStatusUploaderContext
		h              TransformationDebugger
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
	})

	AfterEach(func() {
		c.mockCtrl.Finish()
	})

	Context("Transformation Debugger Badger", func() {
		BeforeEach(func() {
			var err error
			config.Reset()
			config.Set("RUDDER_TMPDIR", path.Join(GinkgoT().TempDir(), rand.String(10)))
			h, err = NewHandle(c.mockBackendConfig)
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			h.Stop()
		})

		It("returns false if disableEventDeliveryStatusUploads is true", func() {
			h.Stop()
			h, err := NewHandle(c.mockBackendConfig)
			Expect(err).To(BeNil())
			h.(*Handle).disableTransformationUploads = config.SingleValueLoader(true)
			Expect(h.UploadTransformationStatus(&TransformationStatusT{})).To(BeFalse())
		})

		It("records events", func() {
			eventuallyFunc := func() bool {
				return h.UploadTransformationStatus(
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

	Context("Transformation Debugger Memory", func() {
		BeforeEach(func() {
			var err error
			config.Reset()
			config.Set("RUDDER_TMPDIR", path.Join(GinkgoT().TempDir(), rand.String(10)))
			config.Set("TransformationDebugger.cacheType", 0)
			h, err = NewHandle(c.mockBackendConfig)
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			h.Stop()
		})

		It("returns false if disableEventDeliveryStatusUploads is true", func() {
			h.Stop()
			h, err := NewHandle(c.mockBackendConfig)
			Expect(err).To(BeNil())
			h.(*Handle).disableTransformationUploads = config.SingleValueLoader(true)
			Expect(h.UploadTransformationStatus(&TransformationStatusT{})).To(BeFalse())
		})

		It("records events", func() {
			eventuallyFunc := func() bool {
				return h.UploadTransformationStatus(
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

func TestLimit(t *testing.T) {
	var (
		singularEvent1 = types.SingularEventT{"payload": "event-1"}
		metadata1      = transformer.Metadata{MessageID: "message-id-1"}
		singularEvent2 = types.SingularEventT{"payload": "event-2"}
		metadata2      = transformer.Metadata{MessageID: "message-id-2"}
		singularEvent3 = types.SingularEventT{"payload": "event-3"}
		metadata3      = transformer.Metadata{MessageID: "message-id-3"}
		singularEvent4 = types.SingularEventT{"payload": "event-4"}
		metadata4      = transformer.Metadata{MessageID: "message-id-4"}
		now            = time.Now()
		limit          = 1
	)
	t.Run("should limit the received transformation status", func(t *testing.T) {
		tStatus := &TransformationStatusT{
			Destination: &sampleBackendConfig.Sources[1].Destinations[1],
			DestID:      sampleBackendConfig.Sources[1].Destinations[1].ID,
			SourceID:    sampleBackendConfig.Sources[1].ID,
			UserTransformedEvents: []transformer.TransformerEvent{
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
			FailedEvents: []transformer.TransformerResponse{
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
			[]transformer.TransformerEvent{
				{
					Message:  singularEvent1,
					Metadata: metadata1,
				},
			},
			limitedTStatus.UserTransformedEvents,
		)
		require.Equal(
			t,
			[]transformer.TransformerResponse{
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
