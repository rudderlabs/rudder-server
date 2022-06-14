package backendconfig

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	mock_sysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// This configuration is assumed by all gateway tests and, is returned on Subscribe of mocked backend config
var SampleBackendConfig = ConfigT{
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
var SampleFilteredSources = ConfigT{
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
var SampleBackendConfig2 = ConfigT{
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
	originalHttp       = Http
	originalLogger     = pkgLogger
	mockLogger         *mock_logger.MockLoggerI
	originalMockPubSub = pubsub.PublishSubscriber{}
	ctrl               *gomock.Controller
	testRequestData    map[string]interface{} = map[string]interface{}{
		"instanceName":         "1",
		"replayConfigDataList": "test",
	}
)

func initBackendConfig() {
	config.Load()
	admin.Init()
	diagnostics.Init()
	Init()
}

var _ = Describe("newForDeployment", func() {

	It("supports single workspace config", func() {
		os.Setenv("WORKSPACE_TOKEN", "password")
		config, err := newForDeployment(deployment.DedicatedType, nil)

		Expect(err).To(BeNil())
		_, ok := config.(*SingleWorkspaceConfig)
		Expect(ok).To(BeTrue())
	})

	It("supports hosted workspace config", func() {
		os.Setenv("HOSTED_SERVICE_SECRET", "password")
		config, err := newForDeployment(deployment.HostedType, nil)

		Expect(err).To(BeNil())
		_, ok := config.(*HostedWorkspacesConfig)
		Expect(ok).To(BeTrue())
	})

	It("supports hosted workspace config", func() {
		os.Setenv("HOSTED_MULTITENANT_SERVICE_SECRET", "password")
		config, err := newForDeployment(deployment.MultiTenantType, nil)

		Expect(err).To(BeNil())
		_, ok := config.(*MultiTenantWorkspacesConfig)
		Expect(ok).To(BeTrue())
	})

	It("return err for unsupported type", func() {
		config, err := newForDeployment("UNSUPPORTED_TYPE", nil)

		Expect(err).To(MatchError("Deployment type \"UNSUPPORTED_TYPE\" not supported"))
		Expect(config).To(BeNil())
	})
})

var _ = Describe("BackendConfig", func() {
	initBackendConfig()

	BeforeEach(func() {
		backendConfig = &SingleWorkspaceConfig{CommonBackendConfig: CommonBackendConfig{eb: &originalMockPubSub}}
		ctrl = gomock.NewController(GinkgoT())
		mockLogger = mock_logger.NewMockLoggerI(ctrl)
		pkgLogger = mockLogger
	})
	AfterEach(func() {
		ctrl.Finish()
		Http = originalHttp
		pkgLogger = originalLogger
	})

	Context("configUpdate method", func() {
		var (
			statConfigBackendError stats.RudderStats
			mockIoUtil             *mock_sysUtils.MockIoUtilI
			originalIoUtil         = IoUtil
			originalConfigFromFile = configFromFile
		)
		BeforeEach(func() {
			pollInterval = 500
			stats.Setup()
			statConfigBackendError = stats.DefaultStats.NewStat("config_backend.errors", stats.CountType)
			mockIoUtil = mock_sysUtils.NewMockIoUtilI(ctrl)
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
			mockLogger.EXPECT().Info(gomock.Any()).Times(0)
			configUpdate(&originalMockPubSub, statConfigBackendError, "test_token")
		})
		It("Expect to make the correct actions if Get method ok but not new config", func() {
			config, _ := json.Marshal(SampleBackendConfig)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(config, nil).Times(1)
			curSourceJSON = SampleBackendConfig
			mockLogger.EXPECT().Info(gomock.Any()).Times(0)
			configUpdate(&originalMockPubSub, statConfigBackendError, "test_token")
		})
		It("Expect to make the correct actions if Get method ok and new config", func() {
			config, _ := json.Marshal(SampleBackendConfig)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(config, nil).Times(1)
			initialized = false
			pubSub := pubsub.PublishSubscriber{}
			curSourceJSON = SampleBackendConfig2
			Expect(initialized).To(BeFalse())
			mockLogger.EXPECT().Info(gomock.Any()).Times(1)
			mockLogger.EXPECT().Debug("processor Enabled", " IsProcessorEnabled: ", true).Times(1)
			mockLogger.EXPECT().Debug("processor Disabled", " IsProcessorEnabled: ", false).Times(1)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			chProcess := pubSub.Subscribe(ctx, string(TopicProcessConfig))
			chBackend := pubSub.Subscribe(ctx, string(TopicBackendConfig))

			configUpdate(&pubSub, statConfigBackendError, "test_token")
			Expect(initialized).To(BeTrue())

			Expect((<-chProcess).Data).To(Equal(SampleFilteredSources))
			Expect((<-chBackend).Data).To(Equal(SampleBackendConfig))
		})
	})

	Context("filterProcessorEnabledDestinations method", func() {
		It("Expect to return the correct value", func() {
			mockLogger.EXPECT().Debug("processor Enabled", " IsProcessorEnabled: ", true).Times(1)
			mockLogger.EXPECT().Debug("processor Disabled", " IsProcessorEnabled: ", false).Times(1)
			result := filterProcessorEnabledDestinations(SampleBackendConfig)
			Expect(result).To(Equal((SampleFilteredSources)))
		})
	})

	Context("Subscribe method", func() {
		It("Expect make the correct actions for processConfig topic", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			curSourceJSON = SampleBackendConfig
			mockLogger.EXPECT().Debug("processor Enabled", " IsProcessorEnabled: ", true).Times(1)
			mockLogger.EXPECT().Debug("processor Disabled", " IsProcessorEnabled: ", false).Times(1)
			filteredSourcesJSON := filterProcessorEnabledDestinations(curSourceJSON)
			backendConfig.(*SingleWorkspaceConfig).eb.Publish(string(TopicProcessConfig), filteredSourcesJSON)

			ch := backendConfig.Subscribe(ctx, TopicProcessConfig)
			backendConfig.(*SingleWorkspaceConfig).eb.Publish(string(TopicProcessConfig), filteredSourcesJSON)
			Expect((<-ch).Data).To(Equal(filteredSourcesJSON))
		})
		It("Expect make the correct actions for backendConfig topic", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			curSourceJSON = SampleBackendConfig

			ch := backendConfig.Subscribe(ctx, TopicBackendConfig)
			backendConfig.(*SingleWorkspaceConfig).eb.Publish(string(TopicBackendConfig), curSourceJSON)
			Expect((<-ch).Data).To(Equal(curSourceJSON))
		})
	})

	Context("WaitForConfig method", func() {
		It("Should not wait if initialized is true", func() {
			initialized = true
			mockLogger.EXPECT().Info("Waiting for initializing backend config").Times(0)
			backendConfig.WaitForConfig(context.TODO())
		})
		It("Should wait until initialized", func() {
			initialized = false
			pollInterval = 2000
			count := 0
			mockLogger.EXPECT().Info("Waiting for initializing backend config").Do(func(v string) {
				count++
				if count == 5 {
					initialized = true
				}
			}).Times(5)
			backendConfig.WaitForConfig(context.TODO())
		})
	})
})
