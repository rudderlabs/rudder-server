package router

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	mocksSysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type networkContext struct {
	mockCtrl       *gomock.Controller
	mockHTTPClient *mocksSysUtils.MockHTTPClientI
}

// Initiaze mocks and common expectations
func (c *networkContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockHTTPClient = mocksSysUtils.NewMockHTTPClientI(c.mockCtrl)
}

func (c *networkContext) Finish() {
	c.mockCtrl.Finish()
}

var _ = Describe("Network", func() {
	var c *networkContext

	BeforeEach(func() {
		initRouter()

		c = &networkContext{}
		c.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Send requests", func() {

		It("should successfully send the request to google analytics", func() {
			network := &NetHandleT{}
			network.logger = logger.NewLogger().Child("network")
			network.httpClient = c.mockHTTPClient

			var structData integrations.PostParametersT
			structData.Type = "REST"
			structData.URL = "https://www.google-analytics.com/collect"
			structData.UserID = "anon_id"
			structData.Headers = map[string]interface{}{}
			structData.QueryParams = map[string]interface{}{"aiid": "com.rudderlabs.android.sdk",
				"an":  "RudderAndroidClient",
				"av":  "1.0",
				"cid": "anon_id",
				"ds":  "android-sdk",
				"ea":  "Demo Track",
				"ec":  "Demo Category",
				"el":  "Demo Label",
				"ni":  0,
				"qt":  "5.9190508594e+10",
				"t":   "event",
				"tid": "UA-185645846-1",
				"uip": "[::1]",
				"ul":  "en-US",
				"v":   1}
			structData.Body = map[string]interface{}{"FORM": map[string]interface{}{},
				"JSON": map[string]interface{}{},
				"XML":  map[string]interface{}{}}
			structData.Files = map[string]interface{}{}

			//Response JSON
			jsonResponse := `[{
				"full_name": "mock-repo"
   			}]`
			//New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			c.mockHTTPClient.EXPECT().Do(gomock.Any()).Times(1).Do(func(req *http.Request) {
				//asserting http request
				req.Method = "POST"
				req.URL.Host = "www.google-analytics.com"
				req.URL.RawQuery = "aiid=com.rudderlabs.android.sdk&an=RudderAndroidClient&av=1.0&cid=anon_id&ds=android-sdk&ea=Demo+Track&ec=Demo+Category&el=Demo+Label&ni=0&qt=5.9190508594e%2B10&t=event&tid=UA-185645846-1&uip=%5B%3A%3A1%5D&ul=en-US&v=1"
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil)

			network.SendPost(context.Background(), structData)
		})

		It("should respect ctx cancelation", func() {

			network := &NetHandleT{}
			network.logger = logger.NewLogger().Child("network")
			network.httpClient = &http.Client{}

			structData := integrations.PostParametersT{
				Type: "REST",
				URL:  "https://www.google-analytics.com/collect",
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			resp := network.SendPost(ctx, structData)
			gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusGatewayTimeout))
			gomega.Expect(string(resp.ResponseBody)).To(gomega.Equal("504 Unable to make \"\" request for URL : \"https://www.google-analytics.com/collect\""))
		})
	})
})
