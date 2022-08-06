package backendconfig

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	mocklogger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	mocksysutils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

// This configuration is assumed by all gateway tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = ConfigT{
	Sources: []SourceT{
		{
			ID:       "1",
			WriteKey: "d",
			Enabled:  false,
		}, {
			ID:       "2",
			WriteKey: "d2",
			Enabled:  false,
			Destinations: []DestinationT{
				{
					ID:                 "d1",
					Name:               "processor Disabled",
					IsProcessorEnabled: false,
				}, {
					ID:                 "d2",
					Name:               "processor Enabled",
					IsProcessorEnabled: true,
				},
			},
		},
	},
}

var sampleFilteredSources = ConfigT{
	Sources: []SourceT{
		{
			ID:           "1",
			WriteKey:     "d",
			Enabled:      false,
			Destinations: []DestinationT{},
		}, {
			ID:       "2",
			WriteKey: "d2",
			Enabled:  false,
			Destinations: []DestinationT{
				{
					ID:                 "d2",
					Name:               "processor Enabled",
					IsProcessorEnabled: true,
				},
			},
		},
	},
}

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

	Context("configUpdate method", func() {
		var (
			ctx                    = context.Background()
			statConfigBackendError stats.RudderStats
			mockIoUtil             *mocksysutils.MockIoUtilI
			originalIoUtil         = IoUtil
			originalConfigFromFile = configFromFile
		)
		BeforeEach(func() {
			pollInterval = 500
			stats.Setup()
			statConfigBackendError = stats.DefaultStats.NewStat("config_backend.errors", stats.CountType)
			mockIoUtil = mocksysutils.NewMockIoUtilI(ctrl)
			IoUtil = mockIoUtil
			configFromFile = true
			mockLogger.EXPECT().Info("Reading workspace config from JSON file").Times(1)
		})
		AfterEach(func() {
			configFromFile = originalConfigFromFile
			IoUtil = originalIoUtil
		})
		It("Expect to make the correct actions if Get method fails", func() {
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(nil, errors.New("TestRequestError")).Times(1)
			mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any()).Times(1)
			mockLogger.EXPECT().Warnf("Error fetching config from backend: %v", gomock.Any()).Times(1)
			mockLogger.EXPECT().Info(gomock.Any()).Times(0)

			backendConfig.initialized = false
			backendConfig.configUpdate(ctx, statConfigBackendError, "")
			Expect(backendConfig.initialized).To(BeFalse())
		})
		It("Expect to make the correct actions if Get method ok but not new config", func() {
			config, _ := json.Marshal(sampleBackendConfig)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(config, nil).Times(1)
			mockLogger.EXPECT().Info(gomock.Any()).Times(0)
			backendConfig.curSourceJSON = sampleBackendConfig
			backendConfig.configUpdate(ctx, statConfigBackendError, "")
		})
		It("Expect to make the correct actions if Get method ok and new config", func() {
			config, _ := json.Marshal(sampleBackendConfig)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(config, nil).Times(1)
			pubSub := pubsub.PublishSubscriber{}
			bc := &commonBackendConfig{
				eb: &pubSub,
				workspaceConfig: &SingleWorkspaceConfig{
					Token: "test_token",
				},
			}
			bc.curSourceJSON = sampleBackendConfig2
			mockLogger.EXPECT().Infof("Workspace Config changed: %s", "").Times(1)
			mockLogger.EXPECT().Debug("processor Enabled", " IsProcessorEnabled: ", true).Times(1)
			mockLogger.EXPECT().Debug("processor Disabled", " IsProcessorEnabled: ", false).Times(1)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			chProcess := pubSub.Subscribe(ctx, string(TopicProcessConfig))
			chBackend := pubSub.Subscribe(ctx, string(TopicBackendConfig))

			bc.configUpdate(ctx, statConfigBackendError, "")
			Expect(bc.initialized).To(BeTrue())

			Expect((<-chProcess).Data).To(Equal(sampleFilteredSources))
			Expect((<-chBackend).Data).To(Equal(sampleBackendConfig))
		})
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
