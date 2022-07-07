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
	mocklogger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	mocksysutils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
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

var _ = Describe("newForDeployment", func() {
	It("supports single workspace config", func() {
		Expect(os.Setenv("WORKSPACE_TOKEN", "password")).To(BeNil())
		config, err := newForDeployment(deployment.DedicatedType, nil)

		Expect(err).To(BeNil())
		_, ok := config.(*SingleWorkspaceConfig)
		Expect(ok).To(BeTrue())
	})

	It("supports hosted workspace config", func() {
		Expect(os.Setenv("HOSTED_SERVICE_SECRET", "password")).To(BeNil())
		config, err := newForDeployment(deployment.HostedType, nil)

		Expect(err).To(BeNil())
		_, ok := config.(*HostedWorkspacesConfig)
		Expect(ok).To(BeTrue())
	})

	It("supports hosted workspace config", func() {
		Expect(os.Setenv("HOSTED_MULTITENANT_SERVICE_SECRET", "password")).To(BeNil())
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
		mockLogger = mocklogger.NewMockLoggerI(ctrl)
		pkgLogger = mockLogger
	})
	AfterEach(func() {
		ctrl.Finish()
		Http = originalHttp
		pkgLogger = originalLogger
	})

	Context("configUpdate method", func() {
		var (
			ctx                    = context.Background()
			statConfigBackendError stats.RudderStats
			mockIoUtil             *mocksysutils.MockIoUtilI
			originalIoUtil         = IoUtil
			originalConfigFromFile = configFromFile
			bc                     = &CommonBackendConfig{
				eb: &originalMockPubSub,
			}
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

			done := make(chan struct{})
			bc.waitForConfigErrs = make(chan error, 1)
			go func() {
				Expect(<-bc.waitForConfigErrs).To(MatchError("TestRequestError"))
				close(done)
			}()
			bc.configUpdate(ctx, statConfigBackendError, "test_token")
			<-done
		})
		It("Expect to make the correct actions if Get method ok but not new config", func() {
			config, _ := json.Marshal(SampleBackendConfig)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(config, nil).Times(1)
			curSourceJSON = SampleBackendConfig
			mockLogger.EXPECT().Info(gomock.Any()).Times(0)
			bc.configUpdate(ctx, statConfigBackendError, "test_token")
		})
		It("Expect to make the correct actions if Get method ok and new config", func() {
			config, _ := json.Marshal(SampleBackendConfig)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(config, nil).Times(1)
			pubSub := pubsub.PublishSubscriber{}
			bc := &CommonBackendConfig{eb: &pubSub}
			curSourceJSON = SampleBackendConfig2
			mockLogger.EXPECT().Info(gomock.Any()).Times(1)
			mockLogger.EXPECT().Debug("processor Enabled", " IsProcessorEnabled: ", true).Times(1)
			mockLogger.EXPECT().Debug("processor Disabled", " IsProcessorEnabled: ", false).Times(1)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			chProcess := pubSub.Subscribe(ctx, string(TopicProcessConfig))
			chBackend := pubSub.Subscribe(ctx, string(TopicBackendConfig))

			bc.configUpdate(ctx, statConfigBackendError, "test_token")
			Expect(bc.initialized).To(BeTrue())

			Expect((<-chProcess).Data).To(Equal(SampleFilteredSources))
			Expect((<-chBackend).Data).To(Equal(SampleBackendConfig))
		})
	})

	Context("filterProcessorEnabledDestinations method", func() {
		It("Expect to return the correct value", func() {
			mockLogger.EXPECT().Debug("processor Enabled", " IsProcessorEnabled: ", true).Times(1)
			mockLogger.EXPECT().Debug("processor Disabled", " IsProcessorEnabled: ", false).Times(1)
			result := filterProcessorEnabledDestinations(SampleBackendConfig)
			Expect(result).To(Equal(SampleFilteredSources))
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
		bc := &CommonBackendConfig{}

		It("Should not wait if initialized is true", func() {
			bc.initialized = true
			mockLogger.EXPECT().Info("Waiting for initializing backend config").Times(0)
			_ = bc.WaitForConfig(context.TODO())
		})
		It("Should wait until initialized", func() {
			bc.initialized = false
			pollInterval = 2000
			count := 0
			mockLogger.EXPECT().Info("Waiting for initializing backend config").Do(func(v string) {
				count++
				if count == 5 {
					bc.initialized = true
				}
			}).Times(5)
			_ = bc.WaitForConfig(context.TODO())
		})
	})
})
