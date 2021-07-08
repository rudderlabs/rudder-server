package debugger

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	mocksDebugger "github.com/rudderlabs/rudder-server/mocks/services/debugger"
	mocksSysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

type uploaderContext struct {
	mockCtrl        *gomock.Controller
	mockTransformer *mocksDebugger.MockTransformer
	mockHTTP        *mocksSysUtils.MockHttpI
}

// Initiaze mocks and common expectations
func (c *uploaderContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockTransformer = mocksDebugger.NewMockTransformer(c.mockCtrl)
	c.mockHTTP = mocksSysUtils.NewMockHttpI(c.mockCtrl)
}

func (c *uploaderContext) Finish() {
	c.mockCtrl.Finish()
}

var _ = Describe("Uploader", func() {
	var c *uploaderContext

	BeforeEach(func() {
		c = &uploaderContext{}
		c.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Send live requests", func() {
		var (
			uploader       *Uploader
			recordingEvent string
			mockHTTPClient *mocksSysUtils.MockHTTPClientI
		)

		BeforeEach(func() {
			mockHTTPClient = mocksSysUtils.NewMockHTTPClientI(c.mockCtrl)

			uploader = New("http://test", c.mockTransformer)
			uploader.Client = mockHTTPClient

			recordingEvent = `{"t":"a"}`
			uploader.Setup()
		})

		It("should successfully send the live events request", func() {
			uploader.RecordEvent(recordingEvent)
			c.mockTransformer.EXPECT().Transform(gomock.Any()).Times(1).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					for _, e := range eventBuffer {
						Expect(e).To(Equal(recordingEvent))
					}

					rawJSON, err := json.Marshal(data)
					Expect(err).To(BeNil())
					return rawJSON, nil
				})

			//Response JSON
			jsonResponse := `OK`
			//New reader with that JSON
			r := ioutil.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			mockHTTPClient.EXPECT().Do(gomock.Any()).Times(1).Do(func(req *http.Request) {
				//asserting http request
				req.Method = "POST"
				req.URL.Host = "test"
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil)

			time.Sleep(5 * time.Second)
		})

		It("should not send the live events request if transform data fails", func() {
			uploader.RecordEvent(recordingEvent)
			c.mockTransformer.EXPECT().Transform(gomock.Any()).Times(1).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					for _, e := range eventBuffer {
						Expect(e).To(Equal(recordingEvent))
					}

					rawJSON, err := json.Marshal(data)
					Expect(err).To(BeNil())
					return rawJSON, errors.New("transform error")
				})

			time.Sleep(5 * time.Second)
		})

		It("should not send the live events request if http newrequest fails", func() {
			Http = c.mockHTTP
			uploader.RecordEvent(recordingEvent)
			c.mockTransformer.EXPECT().Transform(gomock.Any()).Times(1).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					for _, e := range eventBuffer {
						Expect(e).To(Equal(recordingEvent))
					}

					rawJSON, err := json.Marshal(data)
					Expect(err).To(BeNil())
					return rawJSON, nil
				})
			c.mockHTTP.EXPECT().NewRequest("POST", "http://test", gomock.Any()).Times(1).
				Return(nil, errors.New("http new request error"))

			time.Sleep(5 * time.Second)
		})

		It("should not send the live events request if client do fails. Retry 3 times.", func() {
			Http = sysUtils.NewHttp()
			uploader.RecordEvent(recordingEvent)
			c.mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					for _, e := range eventBuffer {
						Expect(e).To(Equal(recordingEvent))
					}

					rawJSON, err := json.Marshal(data)
					Expect(err).To(BeNil())
					return rawJSON, nil
				})

			//Response JSON
			jsonResponse := `OK`
			//New reader with that JSON
			r := ioutil.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			retrySleep = time.Second

			mockHTTPClient.EXPECT().Do(gomock.Any()).Times(maxRetry).Do(func(req *http.Request) {
				//asserting http request
				req.Method = "POST"
				req.URL.Host = "test"
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, errors.New("client do failed"))

			time.Sleep(5 * time.Second)
		})

		/*It("should drop some events if number of events to record is more than queue size", func() {
			Http = sysUtils.NewHttp()
			maxESQueueSize = 1

			c.mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					Expect(len(eventBuffer)).To(Equal(1))
					for _, e := range eventBuffer {
						Expect(e).To(Equal(recordingEvent))
					}

					rawJSON, err := json.Marshal(data)
					Expect(err).To(BeNil())
					return rawJSON, nil
				})

			//Response JSON
			jsonResponse := `OK`
			//New reader with that JSON
			r := ioutil.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			mockHTTPClient.EXPECT().Do(gomock.Any()).Times(1).Do(func(req *http.Request) {
				//asserting http request
				req.Method = "POST"
				req.URL.Host = "test"
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil)

			uploader.RecordEvent(recordingEvent)
			uploader.RecordEvent(recordingEvent)

			time.Sleep(5 * time.Second)
		})*/
	})
})
