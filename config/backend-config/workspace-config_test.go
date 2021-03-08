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
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	mock_sysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
)

var originalBackendConfig = backendConfig
var _ = Describe("workspace-config", func() {
	BeforeEach(func() {
		backendConfig = &WorkspaceConfig{}
		ctrl = gomock.NewController(GinkgoT())
		mockLogger = mock_logger.NewMockLoggerI(ctrl)
		pkgLogger = mockLogger
	})
	AfterEach(func() {
		ctrl.Finish()
		Http = originalHttp
		pkgLogger = originalLogger
	})

	Context("getFromAPI method", func() {
		var mockHttp *mock_sysUtils.MockHttpI
		BeforeEach(func() {
			mockHttp = mock_sysUtils.NewMockHttpI(ctrl)
			Http = mockHttp
		})
		It("Expect to execute request with the correct body and headers and return successfull response", func() {
			configFromFile = false
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
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

			testRequest, _ := http.NewRequest("GET", server.URL, nil)
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL), nil).Return(testRequest, nil).Times(1)

			workspaceToken = "testToken"
			config, ok := backendConfig.Get()
			Expect(ok).To(BeTrue())
			Expect(config).To(Equal(SampleBackendConfig))
		})
		It("Expect to make the correct actions if fail to create the request", func() {
			configFromFile = false
			configBackendURL = "http://rudderstack.com"
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL), nil).Return(nil, errors.New("TestError")).AnyTimes()
			mockLogger.EXPECT().Errorf("[[ Workspace-config ]] Failed to fetch config from API with error: %v, retrying after %v", gomock.Eq(errors.New("TestError")), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Error("Error sending request to the server", gomock.Eq(errors.New("TestError"))).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(ConfigT{}))
			Expect(ok).To(BeFalse())
		})

		It("Expect to make the correct actions if fail to send the request", func() {
			configFromFile = false
			configBackendURL = ""
			Http = mockHttp
			testRequest, _ := http.NewRequest("GET", "", nil)
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/workspaceConfig?fetchAll=true", configBackendURL), nil).Return(testRequest, nil).AnyTimes()
			mockLogger.EXPECT().Errorf("[[ Workspace-config ]] Failed to fetch config from API with error: %v, retrying after %v", gomock.Any(), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Error("Error sending request to the server", gomock.Any()).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(ConfigT{}))
			Expect(ok).To(BeFalse())
		})
	})

	Context("getFromFile method", func() {
		var mockIoUtil *mock_sysUtils.MockIoUtilI
		var originalIoUtil = IoUtil
		BeforeEach(func() {
			mockIoUtil = mock_sysUtils.NewMockIoUtilI(ctrl)
			IoUtil = mockIoUtil
		})
		AfterEach(func() {
			IoUtil = originalIoUtil
		})
		It("Expect to make the correct actions in case of error when reading the config file", func() {
			configFromFile = true
			mockLogger.EXPECT().Info("Reading workspace config from JSON file").Times(1)
			fileErr := errors.New("TestError")
			mockLogger.EXPECT().Errorf("Unable to read backend config from file: %s with error : %s", configJSONPath, fileErr.Error()).Times(1)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(nil, fileErr).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(ConfigT{}))
			Expect(ok).To(BeFalse())
		})

		It("Expect to make the correct actions in case of successfull reading but failed parsing", func() {
			configFromFile = true
			data := []byte(`""`)
			mockLogger.EXPECT().Info("Reading workspace config from JSON file").Times(1)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(data, nil).Times(1)
			mockLogger.EXPECT().Errorf("Unable to parse backend config from file: %s", configJSONPath).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(ConfigT{}))
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
