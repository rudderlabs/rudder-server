package backendconfig

import (
	"context"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	mocklogger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var sampleBackendConfig2 = ConfigT{
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

var _ = Describe("BackendConfig", func() {
	initBackendConfig()
	var backendConfig *commonBackendConfig

	BeforeEach(func() {
		backendConfig = &commonBackendConfig{
			eb: &originalMockPubSub,
			workspaceConfig: &SingleWorkspaceConfig{
				Token: "test_token",
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

	Context("WaitForConfig method", func() {
		It("Should not wait if initialized is true", func() {
			backendConfig.initialized = true
			mockLogger.EXPECT().Info("Waiting for backend config").Times(0)
			backendConfig.WaitForConfig(context.TODO())
		})
		It("Should wait until initialized", func() {
			backendConfig.initialized = false
			pollInterval = 2000
			count := 0
			mockLogger.EXPECT().Info("Waiting for backend config").Do(func(v string) {
				count++
				if count == 5 {
					backendConfig.initialized = true
				}
			}).Times(5)
			backendConfig.WaitForConfig(context.TODO())
		})
	})
})
