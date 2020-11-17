package backendconfig

import (
	"bytes"
	"encoding/json"
	"errors"

	"net/http"
	"net/http/httptest"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	mock_stats "github.com/rudderlabs/rudder-server/mocks/stats"
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
	originalLogger     = log
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
		log = mockLogger
	})
	AfterEach(func() {
		ctrl.Finish()
		Http = originalHttp
		log = originalLogger
	})

	Context("MakePostRequest method", func() {
		var mockHttp *mock_sysUtils.MockHttpI
		BeforeEach(func() {
			mockHttp = mock_sysUtils.NewMockHttpI(ctrl)
			Http = mockHttp
		})
		It("Expect to execute request with the correct body and headers and return successful response", func() {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				username, pass, ok := req.BasicAuth()
				Expect(username).To(Equal("testToken"))
				Expect(pass).To(Equal(""))
				Expect(ok).To(BeTrue())
				Expect(req.Header.Get("Content-Type")).To(Equal("application/json"))
				rw.WriteHeader(http.StatusAccepted)
				rw.Write([]byte("test body"))
			}))

			defer server.Close()

			testRequestDataSON, _ := json.Marshal(testRequestData)
			testRequest, _ := http.NewRequest("POST", server.URL, bytes.NewBuffer(testRequestDataSON))
			mockHttp.EXPECT().NewRequest("POST", server.URL+"/test", bytes.NewBuffer(testRequestDataSON)).Return(testRequest, nil).Times(1)

			mockLogger.EXPECT().Debugf(gomock.Any(), gomock.Any()).Times(1)
			workspaceToken = "testToken"
			body, ok := MakePostRequest(server.URL, "/test", testRequestData)
			Expect(string(body)).To(Equal("test body"))
			Expect(ok).To(BeTrue())
		})
		It("Expect to make the correct actions if fail to send the request", func() {
			testRequest, _ := http.NewRequest("GET", "", nil)
			mockHttp.EXPECT().NewRequest("POST", "", gomock.Any()).Return(testRequest, nil)
			mockLogger.EXPECT().Errorf("ConfigBackend: Failed to execute request: %s, Error: %s", "", gomock.Any()).Times(1)
			body, ok := MakePostRequest("", "", testRequestData)
			Expect(body).To(Equal([]byte{}))
			Expect(ok).To(BeFalse())
		})
		It("Expect to make the correct actions if fail to create the request", func() {
			mockHttp.EXPECT().NewRequest("POST", "http://rudderstack.com/test", gomock.Any()).Return(nil, errors.New("TestError"))
			mockLogger.EXPECT().Errorf("ConfigBackend: Failed to make request: %s, Error: %s", "http://rudderstack.com/test", "TestError").Times(1)
			body, ok := MakePostRequest("http://rudderstack.com", "/test", testRequestData)
			Expect(body).To(Equal([]byte{}))
			Expect(ok).To(BeFalse())
		})
		It("Expect to make the correct actions if request return non 200 or 202 status code", func() {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusNotFound)
				rw.Write([]byte("Not found"))
			}))
			defer server.Close()

			testRequestDataSON, _ := json.Marshal(testRequestData)
			testRequest, _ := http.NewRequest("POST", server.URL, bytes.NewBuffer(testRequestDataSON))
			mockHttp.EXPECT().NewRequest("POST", server.URL+"/test", bytes.NewBuffer(testRequestDataSON)).Return(testRequest, nil).Times(1)

			mockLogger.EXPECT().Errorf("ConfigBackend: Got error response %d", http.StatusNotFound).Times(1)
			mockLogger.EXPECT().Debugf("ConfigBackend: Successful %s", "Not found").Times(1)
			body, ok := MakePostRequest(server.URL, "/test", testRequestData)
			Expect(string(body)).To(Equal("Not found"))
			Expect(ok).To(BeTrue())
		})
	})
	Context("configUpdate method", func() {
		var (
			mockStats              *mock_stats.MockStats
			mockRubberStats        *mock_stats.MockRudderStats
			statConfigBackendError stats.RudderStats
			mockIoUtil             *mock_sysUtils.MockIoUtilI
			originalIoUtil         = IoUtil
			originalConfigFromFile = configFromFile
		)
		BeforeEach(func() {
			pollInterval = 500
			mockStats = mock_stats.NewMockStats(ctrl)
			mockRubberStats = mock_stats.NewMockRudderStats(ctrl)
			var statsmock stats.Stats = mockStats
			mockStats.EXPECT().NewStat(gomock.Any(), gomock.Any()).Return(mockRubberStats).Times(1)
			statConfigBackendError = statsmock.NewStat("config_backend.errors", stats.CountType)
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
			mockRubberStats.EXPECT().Increment().Times(1)
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
