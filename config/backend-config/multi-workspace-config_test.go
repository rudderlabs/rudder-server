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

var SampleWorkspaceSources = map[string][]SourceT{
	"testWordSpaceId": SampleBackendConfig.Sources,
}
var _ = Describe("workspace-config", func() {
	BeforeEach(func() {
		backendConfig = &MultiWorkspaceConfig{
			writeKeyToWorkspaceIDMap: map[string]string{"testKey": "testWordSpaceId"},
		}
		ctrl = gomock.NewController(GinkgoT())
		mockLogger = mock_logger.NewMockLoggerI(ctrl)
		log = mockLogger
	})
	AfterEach(func() {
		ctrl.Finish()
		backendConfig = originalBackendConfig
		Http = originalHttp
		backendConfig = &MultiWorkspaceConfig{
			writeKeyToWorkspaceIDMap: map[string]string{},
		}
	})

	Context("GetWorkspaceIDForWriteKey method", func() {
		It("Expect to get the correct workspace id", func() {
			workspaceID := backendConfig.GetWorkspaceIDForWriteKey("testKey")
			Expect(workspaceID).To(Equal("testWordSpaceId"))
		})
		It("Expect to get empty string if no workspace id found", func() {
			workspaceID := backendConfig.GetWorkspaceIDForWriteKey("keyNotExists")
			Expect(workspaceID).To(Equal(""))
		})
	})

	Context("Get method", func() {
		It("Expect to execute request with the correct body and headers and return successfull response", func() {
			multiWorkspaceSecret = "multiworkspaceSecret"
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				Expect(req.URL.String()).To(Equal(fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", "")))
				username, pass, ok := req.BasicAuth()
				Expect(username).To(Equal("multiworkspaceSecret"))
				Expect(pass).To(Equal(""))
				Expect(ok).To(BeTrue())
				Expect(req.Header.Get("Content-Type")).To(Equal("application/json"))
				rw.WriteHeader(http.StatusAccepted)
				js, _ := json.Marshal(SampleWorkspaceSources)
				rw.Header().Set("Content-Type", "application/json")
				rw.Write(js)
			}))
			// {testWordSpaceId:[{}]}defer server.Close()
			configBackendURL = server.URL
			workspaceToken = "testToken"
			config, ok := backendConfig.Get()
			Expect(backendConfig.GetWorkspaceIDForWriteKey("d2")).To(Equal("testWordSpaceId"))
			Expect(backendConfig.GetWorkspaceIDForWriteKey("d")).To(Equal("testWordSpaceId"))
			Expect(ok).To(BeTrue())
			Expect(config).To(Equal(SampleBackendConfig))
		})

		It("Expect to execute request with the correct body and headers and return successfull response", func() {
			multiWorkspaceSecret = "multiworkspaceSecret"
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusNoContent)
				rw.Header().Set("Content-Type", "application/json")
				rw.Write([]byte(`""`))
			}))
			// {testWordSpaceId:[{}]}defer server.Close()
			configBackendURL = server.URL
			workspaceToken = "testToken"
			mockLogger.EXPECT().Error("Error while parsing request", gomock.Any(), "", http.StatusNoContent).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(SourcesT{}))
			Expect(ok).To(BeFalse())
		})
		It("Expect to make the correct actions if fail to create the request", func() {
			configBackendURL = "http://rudderstack.com"
			ctrl := gomock.NewController(GinkgoT())
			mockHttp := mock_sysUtils.NewMockHttpI(ctrl)
			Http = mockHttp
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL), nil).Return(nil, errors.New("TestError"))
			mockLogger.EXPECT().Error("Error when creating request to the server", gomock.Eq(errors.New("TestError"))).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(SourcesT{}))
			Expect(ok).To(BeFalse())
		})
		It("Expect to make the correct actions if fail to send the request", func() {
			configBackendURL = ""
			ctrl := gomock.NewController(GinkgoT())
			mockHttp := mock_sysUtils.NewMockHttpI(ctrl)
			Http = mockHttp
			testRequest, _ := http.NewRequest("GET", "", nil)
			mockHttp.EXPECT().NewRequest("GET", fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL), nil).Return(testRequest, nil)
			mockLogger.EXPECT().Error("Error when sending request to the server", gomock.Any()).Times(1)
			config, ok := backendConfig.Get()
			Expect(config).To(Equal(SourcesT{}))
			Expect(ok).To(BeFalse())
		})
	})
})
