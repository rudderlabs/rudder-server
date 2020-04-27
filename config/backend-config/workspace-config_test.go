package backendconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/logger"
	mock_sysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
)

var originalBackendConfig = backendConfig
var _ = Describe("workspace-config", func() {
	BeforeEach(func() {
		backendConfig = &WorkspaceConfig{}
		ctrl = gomock.NewController(GinkgoT())
		mockLogger = mock_logger.NewMockLoggerI(ctrl)
		log = mockLogger
	})
	AfterEach(func() {
		ctrl.Finish()
		Http = originalHttp
		backendConfig = originalBackendConfig
	})

	Context("getFromAPI method", func() {
		It("Expect to execute request with the correct body and headers and return successfull response", func() {
			configFromFile = false
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				Expect(req.URL.String()).To(Equal(fmt.Sprintf("%s/workspaceConfig?fetchAll=true", "")))
				username, pass, ok := req.BasicAuth()
				Expect(username).To(Equal("testToken"))
				Expect(pass).To(Equal(""))
				Expect(ok).To(BeTrue())
				Expect(req.Header.Get("Content-Type")).To(Equal("application/json"))
				rw.WriteHeader(http.StatusAccepted)
				js, _ := json.Marshal(SampleBackendConfig)
				rw.Header().Set("Content-Type", "application/json")
				rw.Write(js)
			}))
			defer server.Close()
			configBackendURL = server.URL
			workspaceToken = "testToken"
			config, ok := backendConfig.Get()
			Expect(ok).To(BeTrue())
			Expect(config).To(Equal(SampleBackendConfig))
		})
		It("Expect to make the correct actions if fail to create the request", func() {
			configFromFile = false
			ctrl := gomock.NewController(GinkgoT())
			mockHttp := mock_sysUtils.NewMockHttpI(ctrl)
			configBackendURL = "http://rudderstack.com"
			Http = mockHttp
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL), nil).Return(nil, errors.New("TestError"))
			mockLogger.EXPECT().Error("Error when creating request", gomock.Eq(errors.New("TestError"))).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(SourcesT{}))
			Expect(ok).To(BeFalse())
		})

		It("Expect to make the correct actions if fail to send the request", func() {
			configFromFile = false
			configBackendURL = ""
			ctrl := gomock.NewController(GinkgoT())
			mockHttp := mock_sysUtils.NewMockHttpI(ctrl)
			Http = mockHttp
			testRequest, _ := http.NewRequest("GET", "", nil)
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL), nil).Return(testRequest, nil)
			mockLogger.EXPECT().Error("Error when sending request to the server", gomock.Any()).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(SourcesT{}))
			Expect(ok).To(BeFalse())
		})
	})

	Context("getFromFile method", func() {
		var mockIoUtil *mock_sysUtils.MockIoUtilI
		BeforeEach(func() {
			mockIoUtil = mock_sysUtils.NewMockIoUtilI(ctrl)
			IoUtils = mockIoUtil
			log = mockLogger
		})
		It("Expect to make the correct actions in case of error when reading the config file", func() {
			configFromFile = true
			mockLogger.EXPECT().Info("Reading workspace config from JSON file").Times(1)
			mockLogger.EXPECT().Errorf("Unable to read backend config from file: %s", configJSONPath).Times(1)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(nil, errors.New("TestError")).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(SourcesT{}))
			Expect(ok).To(BeFalse())
		})

		It("Expect to make the correct actions in case of successfull reading but failed parsing", func() {
			configFromFile = true
			data := []byte(`""`)
			mockLogger.EXPECT().Info("Reading workspace config from JSON file").Times(1)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(data, nil).Times(1)
			mockLogger.EXPECT().Errorf("Unable to parse backend config from file: %s", configJSONPath).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(SourcesT{}))
			Expect(ok).To(BeFalse())
		})
		It("Expect to make the correct actions in case of successfull reading of the config file and return the correct value", func() {
			configFromFile = true
			data := []byte(`{
			"sources": [
				{
					"id": "1",
					"writeKey": "d",
					"enabled": false
				},
				{
					"id": "2",
					"writeKey": "d2",
					"enabled": false,
					"destinations": [
						{
							"id": "d1",
							"name": "processor Disabled",
							"isProcessorEnabled": false
						},
						{
							"id": "d2",
							"name": "processor Enabled",
							"isProcessorEnabled": true
						}
					]
				}
			]
		}`)
			mockLogger.EXPECT().Info("Reading workspace config from JSON file").Times(1)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(data, nil).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(SampleBackendConfig))
			Expect(ok).To(BeTrue())
		})
	})
})
