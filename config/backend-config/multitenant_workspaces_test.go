package backendconfig

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	mocklogger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
)

var _ = Describe("workspace-config", func() {
	var (
		backendConfig          *commonBackendConfig
		workspaceId            = "testWordSpaceId"
		SampleWorkspaceSources = map[string]ConfigT{
			workspaceId: sampleBackendConfig,
		}
	)

	BeforeEach(func() {
		backendConfig = &commonBackendConfig{
			workspaceConfig: &MultiTenantWorkspacesConfig{
				writeKeyToWorkspaceIDMap: map[string]string{"testKey": "testWorkSpaceId"},
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
		ctx := context.Background()
		It("Expect to execute request with the correct body and headers and return successful response: Multitenant - 1", func() {
			secretToken := "multitenantWorkspaceSecret"
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				username, pass, ok := req.BasicAuth()
				Expect(username).To(Equal(secretToken))
				Expect(pass).To(Equal(""))
				Expect(ok).To(BeTrue())
				Expect(req.Header.Get("Content-Type")).To(Equal("application/json"))
				rw.WriteHeader(http.StatusAccepted)
				js, _ := json.Marshal(SampleWorkspaceSources)
				rw.Header().Set("Content-Type", "application/json")
				_, _ = rw.Write(js)
			}))
			defer server.Close()

			backendConfig.workspaceConfig.(*MultiTenantWorkspacesConfig).Token = secretToken
			backendConfig.workspaceConfig.(*MultiTenantWorkspacesConfig).configBackendURL = server.URL
			mockLogger.EXPECT().Debugf("Fetching config from %s", gomock.Any())

			config, err := backendConfig.Get(ctx, "testToken")
			Expect(backendConfig.GetWorkspaceIDForWriteKey("d2")).To(Equal("testWordSpaceId"))
			Expect(backendConfig.GetWorkspaceIDForWriteKey("d")).To(Equal("testWordSpaceId"))
			Expect(err).To(BeNil())
			multiConfig := sampleBackendConfig
			Expect(config).To(Equal(multiConfig))
		})

		It("Expect to execute request with the correct body and headers and return successful response: Multitenant - 2", func() {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusNoContent)
				rw.Header().Set("Content-Type", "application/json")
				_, _ = rw.Write([]byte(`""`))
			}))
			defer server.Close()

			backendConfig.workspaceConfig.(*MultiTenantWorkspacesConfig).Token = "multitenantWorkspaceSecret"
			backendConfig.workspaceConfig.(*MultiTenantWorkspacesConfig).configBackendURL = server.URL

			mockLogger.EXPECT().Debugf(
				"Fetching config from %s",
				gomock.Any(),
			).Times(1)
			mockLogger.EXPECT().Errorf("Error while parsing request [%d]: %v", http.StatusNoContent, gomock.Any()).Times(1)
			config, err := backendConfig.Get(ctx, "testToken")
			Expect(config).To(Equal(ConfigT{}))
			Expect(err).NotTo(BeNil())
		})
		It("Expect to make the correct actions if fail to create the request: Multitenant", func() {
			backendConfig.workspaceConfig.(*MultiTenantWorkspacesConfig).configBackendURL = "://example.com"
			mockLogger.EXPECT().Info(gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Debugf("Fetching config from %s", gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Warnf("Failed to fetch config from API with error: %v, retrying after %v", gomock.Any(), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Errorf("Failed to fetch config from API with error: %v, retrying after %v", gomock.Any(), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Errorf("Error sending request to the server: %v", gomock.Any()).Times(1)
			config, err := backendConfig.Get(ctx, "testToken")
			Expect(config).To(Equal(ConfigT{}))
			Expect(err).NotTo(BeNil())
		})
		It("Expect to make the correct actions if fail to send the request: Multitenant", func() {
			backendConfig.workspaceConfig.(*MultiTenantWorkspacesConfig).configBackendURL = "invalid"
			mockLogger.EXPECT().Info(gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Debugf("Fetching config from %s", gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Errorf("Failed to fetch config from API with error: %v, retrying after %v", gomock.Any(), gomock.Any()).AnyTimes()
			mockLogger.EXPECT().Errorf("Error sending request to the server: %v", gomock.Any()).Times(1)
			config, err := backendConfig.Get(ctx, "testToken")
			Expect(config).To(Equal(ConfigT{}))
			Expect(err).NotTo(BeNil())
		})
	})
})
