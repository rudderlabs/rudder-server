package debugger

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/mock/gomock"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	mocksDebugger "github.com/rudderlabs/rudder-server/mocks/services/debugger"
	mocksSysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

type uploaderContext struct {
	mockCtrl *gomock.Controller
}

// Initialize mocks and common expectations
func (c *uploaderContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
}

func (c *uploaderContext) Finish() {
	c.mockCtrl.Finish()
}

func initUploader() {
	config.Reset()
	logger.Reset()
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
			recordingEvent  []byte
			recordingEvent1 []byte
			mockHTTPClient  *mocksSysUtils.MockHTTPClientI
			mockTransformer *mocksDebugger.MockTransformerAny
			mockHTTP        *mocksSysUtils.MockHttpI
			uploader        Uploader[any]
		)

		BeforeEach(func() {
			recordingEvent = []byte(`{"t":"a"}`)
			recordingEvent1 = []byte(`{"t1":"a1"}`)
			mockHTTPClient = mocksSysUtils.NewMockHTTPClientI(c.mockCtrl)
			mockHTTP = mocksSysUtils.NewMockHttpI(c.mockCtrl)
			mockTransformer = mocksDebugger.NewMockTransformerAny(c.mockCtrl)
			uploader = New[any]("http://test", &testutils.BasicAuthMock{}, mockTransformer)
			uploader.Start()
		})

		AfterEach(func() {
			uploader.Stop()
		})

		It("should successfully send the live events request", func() {
			uploader.(*uploaderImpl[any]).Client = mockHTTPClient
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

			// Response JSON
			jsonResponse := `OK`
			// New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				// asserting http request
				assertRequest(req)
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil).AnyTimes()
		})

		It("should log error message from config backend if post request returns non 200", func() {
			uploader.(*uploaderImpl[any]).Client = mockHTTPClient
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

			// Response JSON
			jsonResponse := `OK`
			// New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				// asserting http request
				assertRequest(req)
			}).Return(&http.Response{
				StatusCode: 400,
				Body:       r,
			}, nil).AnyTimes()
		})

		It("should not send the live events request if transform data fails", func() {
			uploader.(*uploaderImpl[any]).Client = mockHTTPClient
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
		})

		It("should not send the live events request if http new request fails", func() {
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
		})

		It("should not send the live events request if client do fails. Retry 3 times.", func() {
			uploader.(*uploaderImpl[any]).Client = mockHTTPClient
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

			// Response JSON
			jsonResponse := `OK`
			// New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			uploader.(*uploaderImpl[any]).batchTimeout = config.SingleValueLoader(time.Millisecond)
			uploader.(*uploaderImpl[any]).retrySleep = config.SingleValueLoader(time.Millisecond)

			var wg sync.WaitGroup
			wg.Add(3)

			mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				// asserting http request
				assertRequest(req)
				wg.Done()
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, errors.New("client do failed")).Times(3)

			uploader.RecordEvent(recordingEvent)
			wg.Wait()
		})

		It("should drop some events if number of events to record is more than queue size", func() {
			uploader.(*uploaderImpl[any]).Client = mockHTTPClient
			uploader.(*uploaderImpl[any]).maxESQueueSize = config.SingleValueLoader(1)

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

			// Response JSON
			jsonResponse := `OK`
			// New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				// asserting http request
				assertRequest(req)
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil).AnyTimes()

			uploader.RecordEvent(recordingEvent)
			uploader.RecordEvent(recordingEvent)
		})

		It("should send events in batches", func() {
			uploader.(*uploaderImpl[any]).Client = mockHTTPClient
			uploader.(*uploaderImpl[any]).maxBatchSize = config.SingleValueLoader(1)
			uploader.(*uploaderImpl[any]).batchTimeout = config.SingleValueLoader(time.Millisecond)

			var wg sync.WaitGroup
			wg.Add(2)

			i := 0
			mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data interface{}) ([]byte, error) {
					eventBuffer := data.([]interface{})
					Expect(len(eventBuffer)).To(Equal(1))
					for _, e := range eventBuffer {
						var re []byte
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

			// Response JSON
			jsonResponse := `OK`
			// New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				// asserting http request
				assertRequest(req)
				wg.Done()
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil).AnyTimes()

			uploader.RecordEvent(recordingEvent)
			uploader.RecordEvent(recordingEvent1)

			wg.Wait()
		})
	})
})

func assertRequest(req *http.Request) {
	username, password, ok := req.BasicAuth()
	Expect(ok).To(BeTrue())
	Expect(username).To(Equal("test"))
	Expect(password).To(Equal("test"))
	Expect(req.Method).To(Equal("POST"))
	Expect(req.URL.Host).To(Equal("test"))
}
