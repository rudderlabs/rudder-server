package destination_connection_tester

import (
	"time"

	"github.com/rudderlabs/rudder-server/config"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"
)

var _ = Describe("DestinationConnectionTester", func() {
	Context("makePostRequest test", func() {
		var server *ghttp.Server
		var endpoint string
		var url string
		var payload = DestinationConnectionTesterResponse{
			DestinationId: "123",
			InstanceId:    "hosted",
			Error:         "",
			TestedAt:      time.Now(),
		}
		BeforeEach(func() {
			server = ghttp.NewServer()
			endpoint = destinationConnectionTesterEndpoint
			url = server.URL()
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("POST", "/"+endpoint),
					ghttp.VerifyBasicAuth(config.GetWorkspaceToken(), ""),
					ghttp.VerifyContentType("application/json;charset=UTF-8"),
				),
			)
		})
		It("make a post request to the url and return err should be nil", func() {
			url := url + "/" + endpoint
			err := makePostRequest(url, payload)
			Ω(server.ReceivedRequests()).Should(HaveLen(1))
			Ω(err).ShouldNot(HaveOccurred())
		})

		AfterEach(func() {
			//shut down the server between tests
			server.Close()
		})
	})

})
