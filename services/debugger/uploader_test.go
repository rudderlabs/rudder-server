package debugger

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	mocksDebugger "github.com/rudderlabs/rudder-server/mocks/services/debugger"
	mocksSysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

type uploaderContext struct {
	mockCtrl *gomock.Controller
}

// Initiaze mocks and common expectations
func (c *uploaderContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
}

func (c *uploaderContext) Finish() {
	c.mockCtrl.Finish()
}

func initUploader() {
	config.Load()
	logger.Init()
}

var _ = Describe("Uploader", func() {
	initUploader()

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
			recordingEvent  string
			recordingEvent1 string
		)

		BeforeEach(func() {
			recordingEvent = `{"t":"a"}`
			recordingEvent1 = `{"t1":"a1"}`
		})

		It("should successfully send the live events request", func() {
			mockHTTPClient := mocksSysUtils.NewMockHTTPClientI(c.mockCtrl)
			mockTransformer := mocksDebugger.NewMockTransformer(c.mockCtrl)
			uploader := New("http://test", mockTransformer)
			uploader.Start()
			uploader.(*Uploader).Client = mockHTTPClient

			uploader.RecordEvent(recordingEvent)
			mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					for _, e := range eventBuffer {
						Expect(e).To(Equal(recordingEvent))
					}

					rawJSON, err := json.Marshal(data)
					Expect(err).To(BeNil())
					return rawJSON, nil
				}).AnyTimes()

			//Response JSON
			jsonResponse := `OK`
			//New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				//asserting http request
				req.Method = "POST"
				req.URL.Host = "test"
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil).AnyTimes()

			time.Sleep(5 * time.Second)
		})

		It("should log error message from config backend if post request returns non 200", func() {
			mockHTTPClient := mocksSysUtils.NewMockHTTPClientI(c.mockCtrl)
			mockTransformer := mocksDebugger.NewMockTransformer(c.mockCtrl)
			uploader := New("http://test", mockTransformer)
			uploader.Start()
			uploader.(*Uploader).Client = mockHTTPClient

			uploader.RecordEvent(recordingEvent)
			mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					for _, e := range eventBuffer {
						Expect(e).To(Equal(recordingEvent))
					}

					rawJSON, err := json.Marshal(data)
					Expect(err).To(BeNil())
					return rawJSON, nil
				}).AnyTimes()

			//Response JSON
			jsonResponse := `OK`
			//New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				//asserting http request
				req.Method = "POST"
				req.URL.Host = "test"
			}).Return(&http.Response{
				StatusCode: 400,
				Body:       r,
			}, nil).AnyTimes()

			time.Sleep(5 * time.Second)
		})

		It("should not send the live events request if transform data fails", func() {
			mockHTTPClient := mocksSysUtils.NewMockHTTPClientI(c.mockCtrl)
			mockTransformer := mocksDebugger.NewMockTransformer(c.mockCtrl)
			uploader := New("http://test", mockTransformer)
			uploader.Start()
			uploader.(*Uploader).Client = mockHTTPClient
			uploader.RecordEvent(recordingEvent)
			mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					for _, e := range eventBuffer {
						Expect(e).To(Equal(recordingEvent))
					}

					rawJSON, err := json.Marshal(data)
					Expect(err).To(BeNil())
					return rawJSON, errors.New("transform error")
				}).AnyTimes()

			time.Sleep(5 * time.Second)
		})

		It("should not send the live events request if http newrequest fails", func() {
			mockTransformer := mocksDebugger.NewMockTransformer(c.mockCtrl)
			mockHTTP := mocksSysUtils.NewMockHttpI(c.mockCtrl)
			uploader := New("http://test", mockTransformer)
			uploader.Start()
			Http = mockHTTP
			uploader.RecordEvent(recordingEvent)
			mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					for _, e := range eventBuffer {
						Expect(e).To(Equal(recordingEvent))
					}

					rawJSON, err := json.Marshal(data)
					Expect(err).To(BeNil())
					return rawJSON, nil
				}).AnyTimes()
			mockHTTP.EXPECT().NewRequest("POST", "http://test", gomock.Any()).
				Return(nil, errors.New("http new request error")).AnyTimes()

			time.Sleep(5 * time.Second)
		})

		It("should not send the live events request if client do fails. Retry 3 times.", func() {
			mockTransformer := mocksDebugger.NewMockTransformer(c.mockCtrl)
			uploader := New("http://test", mockTransformer)
			uploader.Start()
			mockHTTPClient := mocksSysUtils.NewMockHTTPClientI(c.mockCtrl)
			uploader.(*Uploader).Client = mockHTTPClient
			Http = sysUtils.NewHttp()
			uploader.RecordEvent(recordingEvent)
			mockTransformer.EXPECT().Transform(gomock.Any()).
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
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			uploader.(*Uploader).retrySleep = time.Second

			mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				//asserting http request
				req.Method = "POST"
				req.URL.Host = "test"
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, errors.New("client do failed")).AnyTimes()

			time.Sleep(5 * time.Second)
		})

		It("should drop some events if number of events to record is more than queue size", func() {
			mockTransformer := mocksDebugger.NewMockTransformer(c.mockCtrl)
			uploader := New("http://test", mockTransformer)
			uploader.Start()
			mockHTTPClient := mocksSysUtils.NewMockHTTPClientI(c.mockCtrl)
			uploader.(*Uploader).Client = mockHTTPClient
			Http = sysUtils.NewHttp()
			uploader.(*Uploader).maxESQueueSize = 1

			mockTransformer.EXPECT().Transform(gomock.Any()).
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
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				//asserting http request
				req.Method = "POST"
				req.URL.Host = "test"
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil).AnyTimes()

			uploader.RecordEvent(recordingEvent)
			uploader.RecordEvent(recordingEvent)

			time.Sleep(5 * time.Second)
		})

		It("should send events in batches", func() {
			mockTransformer := mocksDebugger.NewMockTransformer(c.mockCtrl)
			uploader := New("http://test", mockTransformer)
			uploader.Start()
			mockHTTPClient := mocksSysUtils.NewMockHTTPClientI(c.mockCtrl)
			uploader.(*Uploader).Client = mockHTTPClient
			Http = sysUtils.NewHttp()
			uploader.(*Uploader).maxBatchSize = 1

			i := 0
			mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					Expect(len(eventBuffer)).To(Equal(1))
					for _, e := range eventBuffer {
						var re string
						if i == 0 {
							re = recordingEvent
						} else {
							re = recordingEvent1
						}
						Expect(e).To(Equal(re))
						i++
					}

					rawJSON, err := json.Marshal(data)
					Expect(err).To(BeNil())
					return rawJSON, nil
				}).Times(2)

			//Response JSON
			jsonResponse := `OK`
			//New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				//asserting http request
				req.Method = "POST"
				req.URL.Host = "test"
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil).AnyTimes()

			uploader.RecordEvent(recordingEvent)
			uploader.RecordEvent(recordingEvent1)

			time.Sleep(5 * time.Second)
		})
	})
})
