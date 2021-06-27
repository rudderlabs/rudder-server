package router

import (
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	mocksRouter "github.com/rudderlabs/rudder-server/mocks/router"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type networkContext struct {
	mockCtrl       *gomock.Controller
	mockHTTPClient *mocksRouter.MockHTTPClient
}

// Initiaze mocks and common expectations
func (c *networkContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockHTTPClient = mocksRouter.NewMockHTTPClient(c.mockCtrl)
}

func (c *networkContext) Finish() {
	c.mockCtrl.Finish()
}

var _ = Describe("Network", func() {
	var c *networkContext

	BeforeEach(func() {
		c = &networkContext{}
		c.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Send requests", func() {

		It("should initialize and recover after crash", func() {
			network := &NetHandleT{}
			network.logger = logger.NewLogger().Child("network")
			network.httpClient = c.mockHTTPClient
			//TODO remove testing Setup and test SendPost
			network.Setup("GA", 30*time.Second)
		})
	})
})
