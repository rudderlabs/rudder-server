package destinationdebugger

import (
	"context"
	"encoding/json"
	"path"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
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

type staticSecretPaths struct {
	state transformer.SecretPathsState
	paths []string
	seen  []string
}

func (s *staticSecretPaths) SecretPaths(destType string) (transformer.SecretPathsState, []string) {
	s.seen = append(s.seen, destType)
	return s.state, s.paths
}

type captureUploader struct {
	mu     sync.Mutex
	events []*DeliveryStatusT
}

func (*captureUploader) Start() {}
func (*captureUploader) Stop()  {}
func (u *captureUploader) RecordEvent(event *DeliveryStatusT) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.events = append(u.events, event)
}

func (u *captureUploader) last() *DeliveryStatusT {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.events[len(u.events)-1]
}

func enableEventDelivery(handle *Handle, destinationID, destType string) {
	handle.updateConfig(map[string]backendconfig.ConfigT{WorkspaceID: {
		Sources: []backendconfig.SourceT{{Destinations: []backendconfig.DestinationT{{
			ID: destinationID, Enabled: true, Config: map[string]any{"eventDelivery": true},
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: destType},
		}}}},
	}})
}

type eventDeliveryStatusUploaderContext struct {
	mockCtrl          *gomock.Controller
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
}

func (c *eventDeliveryStatusUploaderContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockBackendConfig.EXPECT().Identity().AnyTimes().Return(&testutils.BasicAuthMock{})
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
		deliveryStatus DeliveryStatusT
		h              DestinationDebugger
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
	})

	AfterEach(func() {
		c.mockCtrl.Finish()
	})

	Context("delivery payload masking boundary", func() {
		var (
			handle   *Handle
			provider *staticSecretPaths
			uploader *captureUploader
			counts   map[string]int
		)

		BeforeEach(func() {
			config.Set("DestinationDebugger.cacheType", 0)
			config.Set("DestinationDebugger.disableEventDeliveryUploadMasking", false)
			provider = &staticSecretPaths{
				state: transformer.SecretPathsMaskListed,
				paths: []string{"headers.Authorization"},
			}
			created, err := NewHandle(c.mockBackendConfig, provider)
			Expect(err).ToNot(HaveOccurred())
			handle = created.(*Handle)
			handle.uploader.Stop()
			uploader = &captureUploader{}
			handle.uploader = uploader
			counts = map[string]int{}
			handle.maskingCounter = func(destType, reason string) {
				counts[destType+"/"+reason]++
			}
			Eventually(handle.initialized).Should(BeClosed())
		})

		AfterEach(func() {
			handle.Stop()
		})

		It("masks the upload copy using destination definition name and preserves the caller payload", func() {
			original := json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"}}`)
			status := &DeliveryStatusT{DestinationID: DestinationIDEnabledA, Payload: append(json.RawMessage(nil), original...)}

			Expect(handle.RecordEventDeliveryStatus(DestinationIDEnabledA, status)).To(BeTrue())
			Expect(provider.seen).To(Equal([]string{"enabled-destination-a-definition-name"}))
			Expect(gjson.GetBytes(uploader.last().Payload, "headers.Authorization").String()).To(Equal(maskedValue))
			Expect(status.Payload).To(Equal(original))
		})

		It("stores only a masked copy for upload-disabled destinations", func() {
			status := &DeliveryStatusT{DestinationID: DestinationIDDisabled, Payload: json.RawMessage(`{"headers":{"Authorization":"secret"}}`)}
			Expect(handle.RecordEventDeliveryStatus(DestinationIDDisabled, status)).To(BeFalse())

			cached, err := handle.eventsDeliveryCache.Read(DestinationIDDisabled)
			Expect(err).ToNot(HaveOccurred())
			Expect(cached).To(HaveLen(1))
			Expect(gjson.GetBytes(cached[0].Payload, "headers.Authorization").String()).To(Equal(maskedValue))
			Expect(gjson.GetBytes(status.Payload, "headers.Authorization").String()).To(Equal("secret"))
		})

		It("idempotently remasks cached payloads on replay", func() {
			status := &DeliveryStatusT{DestinationID: DestinationIDEnabledB, Payload: json.RawMessage(`{"headers":{"Authorization":"secret"}}`)}
			Expect(handle.RecordEventDeliveryStatus(DestinationIDEnabledB, status)).To(BeFalse())
			Expect(provider.seen).To(Equal([]string{"MINIO"}))

			provider.seen = nil
			enableEventDelivery(handle, DestinationIDEnabledB, "MINIO")

			Expect(gjson.GetBytes(uploader.last().Payload, "headers.Authorization").String()).To(Equal(maskedValue))
			Expect(provider.seen).To(Equal([]string{"MINIO"}))
		})

		It("masks cached plaintext from a disabled-masking period before replay", func() {
			handle.disableEventDeliveryUploadMasking = config.SingleValueLoader(true)
			status := &DeliveryStatusT{DestinationID: DestinationIDEnabledB, Payload: json.RawMessage(`{"headers":{"Authorization":"secret"}}`)}
			Expect(handle.RecordEventDeliveryStatus(DestinationIDEnabledB, status)).To(BeFalse())

			cached, err := handle.eventsDeliveryCache.Read(DestinationIDEnabledB)
			Expect(err).ToNot(HaveOccurred())
			Expect(cached).To(HaveLen(1))
			Expect(gjson.GetBytes(cached[0].Payload, "headers.Authorization").String()).To(Equal("secret"))
			Expect(handle.eventsDeliveryCache.Update(DestinationIDEnabledB, cached[0])).To(Succeed())

			provider.seen = nil
			handle.disableEventDeliveryUploadMasking = config.SingleValueLoader(false)
			enableEventDelivery(handle, DestinationIDEnabledB, "MINIO")

			Expect(gjson.GetBytes(uploader.last().Payload, "headers.Authorization").String()).To(Equal(maskedValue))
			Expect(provider.seen).To(Equal([]string{"MINIO"}))
		})

		It("fails an unknown destination closed using a bounded metric tag", func() {
			status := &DeliveryStatusT{Payload: json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"},"body":{"token":"secret"}}`)}
			Expect(handle.RecordEventDeliveryStatus("customer-specific-id", status)).To(BeFalse())

			cached, err := handle.eventsDeliveryCache.Read("customer-specific-id")
			Expect(err).ToNot(HaveOccurred())
			Expect(gjson.GetBytes(cached[0].Payload, "endpoint").String()).To(Equal("visible"))
			Expect(gjson.GetBytes(cached[0].Payload, "headers").String()).To(Equal(maskedValue))
			Expect(counts["unknown/missing_destination"]).To(Equal(1))
			Expect(counts).ToNot(HaveKey("customer-specific-id/missing_destination"))
		})

		It("uses refreshed destination type mappings", func() {
			status := &DeliveryStatusT{Payload: json.RawMessage(`{"headers":{"Authorization":"secret"}}`)}
			Expect(handle.RecordEventDeliveryStatus(DestinationIDEnabledA, status)).To(BeTrue())
			enableEventDelivery(handle, DestinationIDEnabledA, "REFRESHED_TYPE")
			Expect(handle.RecordEventDeliveryStatus(DestinationIDEnabledA, status)).To(BeTrue())
			Expect(provider.seen).To(Equal([]string{"enabled-destination-a-definition-name", "REFRESHED_TYPE"}))
		})

		It("bypasses and re-enables masking without restart", func() {
			status := &DeliveryStatusT{Payload: json.RawMessage(`{"headers":{"Authorization":"secret"}}`)}
			handle.disableEventDeliveryUploadMasking = config.SingleValueLoader(true)
			Expect(handle.RecordEventDeliveryStatus(DestinationIDEnabledA, status)).To(BeTrue())
			Expect(gjson.GetBytes(uploader.last().Payload, "headers.Authorization").String()).To(Equal("secret"))
			Expect(counts).To(BeEmpty())

			handle.disableEventDeliveryUploadMasking = config.SingleValueLoader(false)
			Expect(handle.RecordEventDeliveryStatus(DestinationIDEnabledA, status)).To(BeTrue())
			Expect(gjson.GetBytes(uploader.last().Payload, "headers.Authorization").String()).To(Equal(maskedValue))
		})
	})

	Context("RecordEventDeliveryStatus Badger", func() {
		BeforeEach(func() {
			var err error
			config.Reset()
			config.Set("RUDDER_TMPDIR", path.Join(GinkgoT().TempDir(), rand.String(10)))
			config.Set("LiveEvent.cache.GCTime", "1s")
			h, err = NewHandle(c.mockBackendConfig, transformer.NewNoOpService())
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			h.Stop()
		})

		It("returns false if disableEventDeliveryStatusUploads is true", func() {
			h.Stop()
			h, err := NewHandle(c.mockBackendConfig, transformer.NewNoOpService())
			Expect(err).To(BeNil())
			h.(*Handle).disableEventDeliveryStatusUploads = config.SingleValueLoader(true)
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
			var edsUploader EventDeliveryStatusUploader
			var payload []*DeliveryStatusT
			payload = append(payload, &deliveryStatus)
			rawJSON, err := edsUploader.Transform(payload)
			Expect(err).To(BeNil())
			Expect(gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventName`).String()).To(Equal("some_event_name"))
			Expect(gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventType`).String()).To(Equal("some_event_type"))
		})

		It("sends empty json if transformation fails", func() {
			edsUploader := NewEventDeliveryStatusUploader(logger.NOP)
			var payload []*DeliveryStatusT
			payload = append(payload, &faultyData)
			rawJSON, err := edsUploader.Transform(payload)
			if err != nil {
				Expect(err.Error()).To(Not(BeNil()))
			} else { // jsoniter doesn't return an error, just sets null for invalid json.RawMessage
				Expect(gjson.GetBytes(rawJSON, DestinationIDEnabledA+".0.payload").Raw).To(Equal("null"))
			}
		})
	})

	Context("RecordEventDeliveryStatus Memory", func() {
		BeforeEach(func() {
			var err error
			config.Reset()
			config.Set("DestinationDebugger.cacheType", 0)
			config.Set("RUDDER_TMPDIR", path.Join(GinkgoT().TempDir(), rand.String(10)))
			config.Set("LiveEvent.cache.GCTime", "1s")
			h, err = NewHandle(c.mockBackendConfig, transformer.NewNoOpService())
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			h.Stop()
		})

		It("returns false if disableEventDeliveryStatusUploads is true", func() {
			h.Stop()
			h, err := NewHandle(c.mockBackendConfig, transformer.NewNoOpService())
			Expect(err).To(BeNil())
			h.(*Handle).disableEventDeliveryStatusUploads = config.SingleValueLoader(true)
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
			var edsUploader EventDeliveryStatusUploader
			var payload []*DeliveryStatusT
			payload = append(payload, &deliveryStatus)
			rawJSON, err := edsUploader.Transform(payload)
			Expect(err).To(BeNil())
			Expect(gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventName`).String()).To(Equal("some_event_name"))
			Expect(gjson.GetBytes(rawJSON, `enabled-destination-a.0.eventType`).String()).To(Equal("some_event_type"))
		})

		It("sends empty json if transformation fails", func() {
			edsUploader := NewEventDeliveryStatusUploader(logger.NOP)
			var payload []*DeliveryStatusT
			payload = append(payload, &faultyData)
			rawJSON, err := edsUploader.Transform(payload)
			if err != nil {
				Expect(rawJSON).To(BeNil())
			} else {
				// jsoniter doesn't return an error, just sets null for invalid json.RawMessage
				Expect(gjson.GetBytes(rawJSON, DestinationIDEnabledA+".0.payload").Raw).To(Equal("null"))
			}
		})
	})
})
