package backendconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	mock_sysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
)

var (
	workspaceId            = "testWordSpaceId"
	SampleWorkspaceSources = map[string]ConfigT{
		workspaceId: SampleBackendConfig,
	}
)

var _ = Describe("workspace-config", func() {
	BeforeEach(func() {
		backendConfig = &HostedWorkspacesConfig{
			writeKeyToWorkspaceIDMap: map[string]string{"testKey": "testWordSpaceId"},
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
		ctx := context.Background()
		var mockHttp *mock_sysUtils.MockHttpI
		BeforeEach(func() {
			mockHttp = mock_sysUtils.NewMockHttpI(ctrl)
			Http = mockHttp
		})
		It("Expect to execute request with the correct body and headers and return successful response", func() {
			backendConfig.(*HostedWorkspacesConfig).Token = "multiworkspaceSecret"
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				username, pass, ok := req.BasicAuth()
				Expect(username).To(Equal("multiworkspaceSecret"))
				Expect(pass).To(Equal(""))
				Expect(ok).To(BeTrue())
				Expect(req.Header.Get("Content-Type")).To(Equal("application/json"))
				rw.WriteHeader(http.StatusAccepted)
				js, _ := json.Marshal(SampleWorkspaceSources)
				rw.Header().Set("Content-Type", "application/json")
				_, _ = rw.Write(js)
			}))
			defer server.Close()

			testRequest, _ := http.NewRequest("GET", server.URL, nil)
			mockHttp.EXPECT().NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL), nil).Return(testRequest, nil).Times(1)

			config, err := backendConfig.Get(ctx, "testToken")
			Expect(backendConfig.GetWorkspaceIDForWriteKey("d2")).To(Equal("testWordSpaceId"))
			Expect(backendConfig.GetWorkspaceIDForWriteKey("d")).To(Equal("testWordSpaceId"))
			Expect(err).To(BeNil())
			mutliConfig := SampleBackendConfig
			mutliConfig.ConnectionFlags.Services = map[string]bool{"warehouse": true}
			Expect(config).To(Equal(mutliConfig))
		})

		It("Expect to execute request with the correct body and headers and return successful response", func() {
			backendConfig.(*HostedWorkspacesConfig).Token = "multiworkspaceSecret"
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusNoContent)
				rw.Header().Set("Content-Type", "application/json")
				_, _ = rw.Write([]byte(`""`))
			}))
			defer server.Close()

			testRequest, _ := http.NewRequest("GET", server.URL, nil)
			mockHttp.EXPECT().NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL), nil).Return(testRequest, nil).Times(1)

			mockLogger.EXPECT().Error("Error while parsing request", gomock.Any(), http.StatusNoContent).Times(1)
			config, err := backendConfig.Get(ctx, "testToken")
			Expect(config).To(Equal(ConfigT{}))
			Expect(err).NotTo(BeNil())
		})
		It("Expect to make the correct actions if fail to create the request", func() {
			mockHttp.EXPECT().NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL), nil).Return(nil, errors.New("TestError")).AnyTimes()
			mockLogger.EXPECT().Errorf("[[ Multi-workspace-config ]] Failed to fetch multi workspace config from API with error: %v, retrying after %v", gomock.Eq(errors.New("TestError")), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Error("Error sending request to the server", gomock.Eq(errors.New("TestError"))).Times(1)
			config, err := backendConfig.Get(ctx, "testToken")
			Expect(config).To(Equal(ConfigT{}))
			Expect(err).NotTo(BeNil())
		})
		It("Expect to make the correct actions if fail to send the request", func() {
			testRequest, _ := http.NewRequest("GET", "", nil)
			mockHttp.EXPECT().NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/hostedWorkspaceConfig?fetchAll=true", configBackendURL), nil).Return(testRequest, nil).AnyTimes()
			mockLogger.EXPECT().Errorf("[[ Multi-workspace-config ]] Failed to fetch multi workspace config from API with error: %v, retrying after %v", gomock.Any(), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Error("Error sending request to the server", gomock.Any()).Times(1)
			config, err := backendConfig.Get(ctx, "testToken")
			Expect(config).To(Equal(ConfigT{}))
			Expect(err).NotTo(BeNil())
		})
	})
})
