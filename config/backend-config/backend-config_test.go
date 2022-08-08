package backendconfig

import (
	"context"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	mocklogger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var sampleBackendConfig2 = ConfigT{
	Sources: []SourceT{
		{
			ID:       "3",
			WriteKey: "d3",
			Enabled:  false,
		}, {
			ID:       "4",
			WriteKey: "d4",
			Enabled:  false,
		},
	},
}

var (
	originalLogger     = pkgLogger
	mockLogger         *mocklogger.MockLoggerI
	originalMockPubSub = pubsub.PublishSubscriber{}
	ctrl               *gomock.Controller
)

func initBackendConfig() {
	config.Load()
	admin.Init()
	diagnostics.Init()
	Init()
}

var _ = Describe("BackendConfig", func() {
	initBackendConfig()
	var backendConfig *commonBackendConfig

	BeforeEach(func() {
		backendConfig = &commonBackendConfig{
			eb: &originalMockPubSub,
			workspaceConfig: &SingleWorkspaceConfig{
				Token: "test_token",
			},
		}
		ctrl = gomock.NewController(GinkgoT())
		mockLogger = mocklogger.NewMockLoggerI(ctrl)
		pkgLogger = mockLogger
	})
	AfterEach(func() {
		ctrl.Finish()
		pkgLogger = originalLogger
	})

	Context("filterProcessorEnabledDestinations method", func() {
		It("Expect to return the correct value", func() {
			mockLogger.EXPECT().Debug("processor Enabled", " IsProcessorEnabled: ", true).Times(1)
			mockLogger.EXPECT().Debug("processor Disabled", " IsProcessorEnabled: ", false).Times(1)
			result := filterProcessorEnabledDestinations(sampleBackendConfig)
			Expect(result).To(Equal(sampleFilteredSources))
		})
	})

	Context("Subscribe method", func() {
		It("Expect make the correct actions for processConfig topic", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			backendConfig.curSourceJSON = sampleBackendConfig
			mockLogger.EXPECT().Debug("processor Enabled", " IsProcessorEnabled: ", true).Times(1)
			mockLogger.EXPECT().Debug("processor Disabled", " IsProcessorEnabled: ", false).Times(1)
			filteredSourcesJSON := filterProcessorEnabledDestinations(backendConfig.curSourceJSON)
			backendConfig.eb.Publish(string(TopicProcessConfig), filteredSourcesJSON)

			ch := backendConfig.Subscribe(ctx, TopicProcessConfig)
			backendConfig.eb.Publish(string(TopicProcessConfig), filteredSourcesJSON)
			Expect((<-ch).Data).To(Equal(filteredSourcesJSON))
		})
		It("Expect make the correct actions for backendConfig topic", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			backendConfig.curSourceJSON = sampleBackendConfig

			ch := backendConfig.Subscribe(ctx, TopicBackendConfig)
			backendConfig.eb.Publish(string(TopicBackendConfig), backendConfig.curSourceJSON)
			Expect((<-ch).Data).To(Equal(backendConfig.curSourceJSON))
		})
	})

	Context("WaitForConfig method", func() {
		It("Should not wait if initialized is true", func() {
			backendConfig.initialized = true
			mockLogger.EXPECT().Info("Waiting for backend config").Times(0)
			backendConfig.WaitForConfig(context.TODO())
		})
		It("Should wait until initialized", func() {
			backendConfig.initialized = false
			pollInterval = 2000
			count := 0
			mockLogger.EXPECT().Info("Waiting for backend config").Do(func(v string) {
				count++
				if count == 5 {
					backendConfig.initialized = true
				}
			}).Times(5)
			backendConfig.WaitForConfig(context.TODO())
		})
	})
})
