package destinationdebugger_test

import (
	"context"
	"path"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
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

var faultyData = destinationdebugger.DeliveryStatusT{
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
	mockCtrl          *gomock.Controller
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
}

func (c *eventDeliveryStatusUploaderContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
}

func initEventDeliveryStatusUploader() {
	config.Reset()
	logger.Reset()
	misc.Init()
}

var _ = Describe("eventDeliveryStatusUploader", func() {
	initEventDeliveryStatusUploader()

	var (
		c              *eventDeliveryStatusUploaderContext
		deliveryStatus destinationdebugger.DeliveryStatusT
		h              destinationdebugger.DestinationDebugger
	)

	BeforeEach(func() {
		c = &eventDeliveryStatusUploaderContext{}
		c.Setup()

		c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
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

		deliveryStatus = destinationdebugger.DeliveryStatusT{
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
	})

	AfterEach(func() {
		c.mockCtrl.Finish()
	})

	Context("RecordEventDeliveryStatus Badger", func() {
		BeforeEach(func() {
			var err error
			config.Reset()
			config.Set("RUDDER_TMPDIR", path.Join(GinkgoT().TempDir(), rand.String(10)))
			config.Set("LiveEvent.cache.GCTime", "1s")
			h, err = destinationdebugger.NewHandle(c.mockBackendConfig, destinationdebugger.WithDisableEventUploads(false))
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			h.Stop()
		})

		It("returns false if disableEventDeliveryStatusUploads is true", func() {
			h.Stop()
			h, err := destinationdebugger.NewHandle(c.mockBackendConfig, destinationdebugger.WithDisableEventUploads(true))
			Expect(err).To(BeNil())
			Expect(h.RecordEventDeliveryStatus(DestinationIDEnabledA, &deliveryStatus)).To(BeFalse())
		})

		It("returns false if destination_id is not in uploadEnabledDestinationIDs", func() {
			Expect(h.RecordEventDeliveryStatus(DestinationIDEnabledB, &deliveryStatus)).To(BeFalse())
		})

		It("records events", func() {
			eventuallyFunc := func() bool { return h.RecordEventDeliveryStatus(DestinationIDEnabledA, &deliveryStatus) }
			Eventually(eventuallyFunc).Should(BeTrue())
		})

		It("transforms payload properly", func() {
			var edsUploader destinationdebugger.EventDeliveryStatusUploader
			var payload []*destinationdebugger.DeliveryStatusT
			payload = append(payload, &deliveryStatus)
			rawJSON, err := edsUploader.Transform(payload)
			Expect(err).To(BeNil())
			Expect(gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventName`).String()).To(Equal("some_event_name"))
			Expect(gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventType`).String()).To(Equal("some_event_type"))
		})

		It("sends empty json if transformation fails", func() {
			edsUploader := destinationdebugger.NewEventDeliveryStatusUploader(logger.NOP)
			var payload []*destinationdebugger.DeliveryStatusT
			payload = append(payload, &faultyData)
			rawJSON, err := edsUploader.Transform(payload)
			Expect(err.Error()).To(ContainSubstring("error calling MarshalJSON"))
			Expect(rawJSON).To(BeNil())
		})
	})

	Context("RecordEventDeliveryStatus Memory", func() {
		BeforeEach(func() {
			var err error
			config.Reset()
			config.Set("DestinationDebugger.cacheType", 0)
			config.Set("RUDDER_TMPDIR", path.Join(GinkgoT().TempDir(), rand.String(10)))
			config.Set("LiveEvent.cache.GCTime", "1s")
			h, err = destinationdebugger.NewHandle(c.mockBackendConfig, destinationdebugger.WithDisableEventUploads(false))
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			h.Stop()
		})

		It("returns false if disableEventDeliveryStatusUploads is true", func() {
			h.Stop()
			h, err := destinationdebugger.NewHandle(c.mockBackendConfig, destinationdebugger.WithDisableEventUploads(true))
			Expect(err).To(BeNil())
			Expect(h.RecordEventDeliveryStatus(DestinationIDEnabledA, &deliveryStatus)).To(BeFalse())
		})

		It("returns false if destination_id is not in uploadEnabledDestinationIDs", func() {
			Expect(h.RecordEventDeliveryStatus(DestinationIDEnabledB, &deliveryStatus)).To(BeFalse())
		})

		It("records events", func() {
			eventuallyFunc := func() bool { return h.RecordEventDeliveryStatus(DestinationIDEnabledA, &deliveryStatus) }
			Eventually(eventuallyFunc).Should(BeTrue())
		})

		It("transforms payload properly", func() {
			var edsUploader destinationdebugger.EventDeliveryStatusUploader
			var payload []*destinationdebugger.DeliveryStatusT
			payload = append(payload, &deliveryStatus)
			rawJSON, err := edsUploader.Transform(payload)
			Expect(err).To(BeNil())
			Expect(gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventName`).String()).To(Equal("some_event_name"))
			Expect(gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventType`).String()).To(Equal("some_event_type"))
		})

		It("sends empty json if transformation fails", func() {
			edsUploader := destinationdebugger.NewEventDeliveryStatusUploader(logger.NOP)
			var payload []*destinationdebugger.DeliveryStatusT
			payload = append(payload, &faultyData)
			rawJSON, err := edsUploader.Transform(payload)
			Expect(err.Error()).To(ContainSubstring("error calling MarshalJSON"))
			Expect(rawJSON).To(BeNil())
		})
	})
})
