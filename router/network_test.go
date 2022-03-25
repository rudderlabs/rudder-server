package router

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	mocksSysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type networkContext struct {
	mockCtrl       *gomock.Controller
	mockHTTPClient *mocksSysUtils.MockHTTPClientI
}

// Initialize mocks and common expectations
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
			Expect(resp.StatusCode).To(Equal(http.StatusGatewayTimeout))
			Expect(string(resp.ResponseBody)).To(Equal("504 Unable to make \"\" request for URL : \"https://www.google-analytics.com/collect\""))
		})

	})

	Context("Verify response bodies are propagated/filtered based on the response's content-type", func() {
		const mockResponseBody = `[{"full_name": "mock-repo"}]`
		var network *NetHandleT
		var requestParams integrations.PostParametersT
		var mockResponse http.Response
		var mockResponseContentType func(contentType string) = func(contentType string) {
			mockResponse.Header.Del("Content-Type")
			mockResponse.Header.Add("Content-Type", contentType)
			mockResponse.Body = io.NopCloser(bytes.NewReader([]byte(mockResponseBody)))
		}

		BeforeEach(func() {
			network = &NetHandleT{}
			network.logger = logger.NewLogger().Child("network")
			network.httpClient = c.mockHTTPClient

			// use the same request for all tests
			requestParams = integrations.PostParametersT{}
			requestParams.Type = "REST"
			requestParams.URL = "https://www.google-analytics.com/collect"
			requestParams.UserID = "anon_id"
			requestParams.Headers = map[string]interface{}{}
			requestParams.QueryParams = map[string]interface{}{
				"aiid": "com.rudderlabs.android.sdk",
			}
			requestParams.Body = map[string]interface{}{"FORM": map[string]interface{}{},
				"JSON": map[string]interface{}{},
				"XML":  map[string]interface{}{}}
			requestParams.Files = map[string]interface{}{}

			mockResponse = http.Response{
				StatusCode: 200,
				Header:     make(http.Header, 0),
			}
			c.mockHTTPClient.EXPECT().Do(gomock.Any()).AnyTimes().Return(&mockResponse, nil)
		})

		DescribeTable("depending on the content type",
			func(contentType string, altered bool) {

				mockResponseContentType(contentType)
				resp := network.SendPost(context.Background(), requestParams)
				if altered {
					Expect(resp.ResponseBody).To(Equal([]byte("redacted due to unsupported content-type")))
				} else {
					Expect(resp.ResponseBody).To(Equal([]byte(mockResponseBody)))
				}
			},
			Entry("'text/html' should result in non-altered body", "text/html", false),
			Entry("'text/xml' should result in non-altered body", "text/xml", false),
			Entry("'text/plain;charset=UTF-8' should result in non-altered body", "text/plain;charset=UTF-8", false),
			Entry("'application/json' should result in non-altered body", "application/json", false),
			Entry("'application/problem+json; charset=utf-8' should result in non-altered body", "application/problem+json; charset=utf-8", false),
			Entry("'application/vnd.collection+json' should result in non-altered body", "application/vnd.collection+json", false),
			Entry("'application/xml' should result in non-altered body", "application/xml", false),
			Entry("'application/atom+xml; charset=utf-8' should result in non-altered body", "application/atom+xml; charset=utf-8", false),
			Entry("'application/soap+xml' should result in non-altered body", "application/soap+xml", false),
			Entry("'application/soap+xml' should result in altered body", "application/jwt", true),
			Entry("'image/jpeg' should result in altered body", "image/jpeg", true),
			Entry("'video/mpeg' should result in altered body", "video/mpeg", true),
			Entry("'invalidcontenttype' should result in altered body", "invalidcontenttype", true),
		)
	})
})
