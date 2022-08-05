package backendconfig

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	mocklogger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	mocksysutils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
)

var _ = Describe("workspace-config", func() {
	var backendConfig *commonBackendConfig

	BeforeEach(func() {
		backendConfig = &commonBackendConfig{
			workspaceConfig: &SingleWorkspaceConfig{
				Token: "testToken",
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

	Context("getFromAPI method", func() {
		ctx := context.Background()
		It("Expect to execute request with the correct body and headers and return successful response", func() {
			configFromFile = false
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				username, pass, ok := req.BasicAuth()
				Expect(username).To(Equal("testToken"))
				Expect(pass).To(Equal(""))
				Expect(ok).To(BeTrue())
				Expect(req.Header.Get("Content-Type")).To(Equal("application/json"))
				rw.WriteHeader(http.StatusAccepted)
				js, _ := json.Marshal(sampleBackendConfig)
				rw.Header().Set("Content-Type", "application/json")
				_, _ = rw.Write(js)
			}))
			defer server.Close()

			backendConfig.workspaceConfig.(*SingleWorkspaceConfig).configBackendURL = server.URL
			config, err := backendConfig.Get(ctx, "")
			Expect(err).To(BeNil())
			Expect(config).To(Equal(sampleBackendConfig))
		})
		It("Expect to make the correct actions if fail to create the request", func() {
			backendConfig.workspaceConfig.(*SingleWorkspaceConfig).configBackendURL = "://example.com"
			mockLogger.EXPECT().Info(gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Debugf("Fetching config from %s", gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Warnf("Failed to fetch config from API with error: %v, retrying after %v", gomock.Any(), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Errorf("Failed to fetch config from API with error: %v, retrying after %v", gomock.Any(), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Errorf("Error sending request to the server: %v", gomock.Any()).Times(1)
			config, err := backendConfig.Get(ctx, "testToken")
			Expect(config).To(Equal(ConfigT{}))
			Expect(err).NotTo(BeNil())
		})
	})

	Context("getFromFile method", func() {
		ctx := context.Background()
		var mockIoUtil *mocksysutils.MockIoUtilI
		originalIoUtil := IoUtil
		BeforeEach(func() {
			mockIoUtil = mocksysutils.NewMockIoUtilI(ctrl)
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
			config, err := backendConfig.Get(ctx, "")
			Expect(config).To(Equal(ConfigT{}))
			Expect(err).NotTo(BeNil())
		})

		It("Expect to make the correct actions in case of successful reading but failed parsing", func() {
			configFromFile = true
			data := []byte(`""`)
			mockLogger.EXPECT().Info("Reading workspace config from JSON file").Times(1)
			mockIoUtil.EXPECT().ReadFile(configJSONPath).Return(data, nil).Times(1)
			mockLogger.EXPECT().Errorf("Unable to parse backend config from file: %s", configJSONPath).Times(1)
			config, err := backendConfig.Get(ctx, "")
			Expect(config).To(Equal(ConfigT{}))
			Expect(err).NotTo(BeNil())
		})
		It("Expect to make the correct actions in case of successful reading of the config file and return the correct value", func() {
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
			config, err := backendConfig.Get(ctx, "")
			Expect(config).To(Equal(sampleBackendConfig))
			Expect(err).To(BeNil())
		})
	})
})
