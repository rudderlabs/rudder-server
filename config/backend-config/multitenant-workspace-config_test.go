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

var SampleWorkspaceSourcesMultitenant = map[string]ConfigT{
	workspaceId: SampleBackendConfig,
}
var _ = Describe("workspace-config", func() {
	BeforeEach(func() {
		backendConfig = &MultiTenantWorkspaceConfig{
			writeKeyToWorkspaceIDMap: map[string]string{"testKey": "testWorkSpaceId"},
		}
		ctrl = gomock.NewController(GinkgoT())
		mockLogger = mock_logger.NewMockLoggerI(ctrl)
		pkgLogger = mockLogger
	})
	AfterEach(func() {
		ctrl.Finish()
		backendConfig = originalBackendConfig
		Http = originalHttp
		pkgLogger = originalLogger
	})

	Context("GetWorkspaceIDForWriteKey method: Multitenant", func() {
		It("Expect to get the correct workspace id: Multitenant", func() {
			workspaceID := backendConfig.GetWorkspaceIDForWriteKey("testKey")
			Expect(workspaceID).To(Equal("testWorkSpaceId"))
		})
		It("Expect to get empty string if no workspace id found: Multitenant", func() {
			workspaceID := backendConfig.GetWorkspaceIDForWriteKey("keyNotExists")
			Expect(workspaceID).To(Equal(""))
		})
	})

	Context("Get method : Multitenant", func() {
		var mockHttp *mock_sysUtils.MockHttpI
		BeforeEach(func() {
			mockHttp = mock_sysUtils.NewMockHttpI(ctrl)
			Http = mockHttp
		})
		It("Expect to execute request with the correct body and headers and return successful response: Multitenant - 1", func() {
			multitenantWorkspaceSecret = "multitenantWorkspaceSecret"
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				username, pass, ok := req.BasicAuth()
				Expect(username).To(Equal("multitenantWorkspaceSecret"))
				Expect(pass).To(Equal(""))
				Expect(ok).To(BeTrue())
				Expect(req.Header.Get("Content-Type")).To(Equal("application/json"))
				rw.WriteHeader(http.StatusAccepted)
				js, _ := json.Marshal(SampleWorkspaceSources)
				rw.Header().Set("Content-Type", "application/json")
				rw.Write(js)
			}))
			defer server.Close()

			testRequest, _ := http.NewRequest("GET", server.URL, nil)
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/multitenantWorkspaceConfig?ids=[testToken]&fetchAll=true", configBackendURL), nil).Return(testRequest, nil).Times(1)

			workspaceToken = "testToken"
			config, ok := backendConfig.Get("testToken")
			Expect(backendConfig.GetWorkspaceIDForWriteKey("d2")).To(Equal("testWordSpaceId"))
			Expect(backendConfig.GetWorkspaceIDForWriteKey("d")).To(Equal("testWordSpaceId"))
			Expect(ok).To(BeTrue())
			multiConfig := SampleBackendConfig
			Expect(config).To(Equal(multiConfig))
		})

		It("Expect to execute request with the correct body and headers and return successful response: Multitenant - 2", func() {
			multitenantWorkspaceSecret = "multitenantWorkspaceSecret"
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusNoContent)
				rw.Header().Set("Content-Type", "application/json")
				rw.Write([]byte(`""`))
			}))
			defer server.Close()

			testRequest, _ := http.NewRequest("GET", server.URL, nil)
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/multitenantWorkspaceConfig?ids=[testToken]&fetchAll=true", configBackendURL), nil).Return(testRequest, nil).Times(1)

			workspaceToken = "testToken"
			mockLogger.EXPECT().Error("Error while parsing request", gomock.Any(), http.StatusNoContent).Times(1)
			config, ok := backendConfig.Get("testToken")
			Expect(config).To(Equal(ConfigT{}))
			Expect(ok).To(BeFalse())
		})
		It("Expect to make the correct actions if fail to create the request: Multitenant", func() {
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/multitenantWorkspaceConfig?ids=[testToken]&fetchAll=true", configBackendURL), nil).Return(nil, errors.New("TestError")).AnyTimes()
			mockLogger.EXPECT().Errorf("Failed to fetch config from API with error: %v, retrying after %v", gomock.Eq(errors.New("TestError")), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Error("Error sending request to the server", gomock.Eq(errors.New("TestError"))).Times(1)
			config, ok := backendConfig.Get("testToken")
			Expect(config).To(Equal(ConfigT{}))
			Expect(ok).To(BeFalse())
		})
		It("Expect to make the correct actions if fail to send the request: Multitenant", func() {
			testRequest, _ := http.NewRequest("GET", "", nil)
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/multitenantWorkspaceConfig?ids=[testToken]&fetchAll=true", configBackendURL), nil).Return(testRequest, nil).AnyTimes()
			mockLogger.EXPECT().Errorf("Failed to fetch config from API with error: %v, retrying after %v", gomock.Any(), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Error("Error sending request to the server", gomock.Any()).Times(1)
			config, ok := backendConfig.Get("testToken")
			Expect(config).To(Equal(ConfigT{}))
			Expect(ok).To(BeFalse())
		})
	})
})
