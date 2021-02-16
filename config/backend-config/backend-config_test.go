package backendconfig

import (
	"encoding/json"
	"errors"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	mock_utils "github.com/rudderlabs/rudder-server/mocks/utils"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	mock_sysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	stats "github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
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
	originalMockPubSub = Eb
	ctrl               *gomock.Controller
	testRequestData    map[string]interface{} = map[string]interface{}{
		"instanceName":         "1",
		"replayConfigDataList": "test",
	}
)

var _ = Describe("BackendConfig", func() {
	BeforeEach(func() {
		backendConfig = new(WorkspaceConfig)
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
			configUpdate(statConfigBackendError)
		})
		It("Expect to make the correct actions if Get method ok but not new config", func() {
			config, _ := json.Marshal(SampleBackendConfig)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(config, nil).Times(1)
			curSourceJSON = SampleBackendConfig
			mockLogger.EXPECT().Info(gomock.Any()).Times(0)
			configUpdate(statConfigBackendError)
		})
		It("Expect to make the correct actions if Get method ok and new config", func() {
			config, _ := json.Marshal(SampleBackendConfig)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(config, nil).Times(1)
			initialized = false
			mockPubSub := mock_utils.NewMockPublishSubscriber(ctrl)
			Eb = mockPubSub
			curSourceJSON = SampleBackendConfig2
			Expect(initialized).To(BeFalse())
			mockLogger.EXPECT().Info(gomock.Any()).Times(1)
			mockLogger.EXPECT().Debug("processor Enabled", " IsProcessorEnabled: ", true).Times(1)
			mockLogger.EXPECT().Debug("processor Disabled", " IsProcessorEnabled: ", false).Times(1)
			mockPubSub.EXPECT().Publish(string(TopicProcessConfig), gomock.Eq(SampleFilteredSources)).Times(1)
			mockPubSub.EXPECT().Publish(string(TopicBackendConfig), SampleBackendConfig).Times(1)
			configUpdate(statConfigBackendError)
			Expect(initialized).To(BeTrue())
			Eb = originalMockPubSub
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
		var mockPubSub *mock_utils.MockPublishSubscriber
		BeforeEach(func() {
			mockPubSub = mock_utils.NewMockPublishSubscriber(ctrl)
			Eb = mockPubSub
		})
		AfterEach(func() {
			Eb = originalMockPubSub
		})
		It("Expect make the correct actions for processConfig topic", func() {
			ch := make(chan utils.DataEvent)
			curSourceJSON = SampleBackendConfig
			mockLogger.EXPECT().Debug("processor Enabled", " IsProcessorEnabled: ", true).Times(1)
			mockLogger.EXPECT().Debug("processor Disabled", " IsProcessorEnabled: ", false).Times(1)
			mockPubSub.EXPECT().Subscribe(string(TopicProcessConfig), gomock.AssignableToTypeOf(ch)).Times(1)
			mockPubSub.EXPECT().PublishToChannel(gomock.AssignableToTypeOf(ch), string(TopicProcessConfig), gomock.Eq(SampleFilteredSources)).Times(1)
			backendConfig.Subscribe(ch, TopicProcessConfig)

		})
		It("Expect make the correct actions for backendConfig topic", func() {
			ch := make(chan utils.DataEvent)
			curSourceJSON = SampleBackendConfig
			mockPubSub.EXPECT().Subscribe(string(TopicBackendConfig), gomock.AssignableToTypeOf(ch)).Times(1)
			mockPubSub.EXPECT().PublishToChannel(gomock.AssignableToTypeOf(ch), string(TopicBackendConfig), SampleBackendConfig).Times(1)
			backendConfig.Subscribe(ch, TopicBackendConfig)
		})
	})

	Context("WaitForConfig method", func() {
		It("Should not wait if initialized is true", func() {
			initialized = true
			waitForRegulations = false
			mockLogger.EXPECT().Info("Waiting for initializing backend config").Times(0)
			backendConfig.WaitForConfig()
		})
		It("Should wait until initialized", func() {
			initialized = false
			waitForRegulations = true
			pollInterval = 2000
			count := 0
			mockLogger.EXPECT().Info("Waiting for initializing backend config").Do(func(v string) {
				count++
				if count == 5 {
					initialized = true
					waitForRegulations = false
				}
			}).Times(5)
			backendConfig.WaitForConfig()
		})
	})
})
