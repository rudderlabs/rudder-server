package debugger

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	mocksDebugger "github.com/rudderlabs/rudder-server/mocks/services/debugger"
	mocksSysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

type uploaderFixture struct {
	recordingEvent  []byte
	recordingEvent1 []byte
	mockHTTPClient  *mocksSysUtils.MockHTTPClientI
	mockTransformer *mocksDebugger.MockTransformerAny
	mockHTTP        *mocksSysUtils.MockHttpI
	uploader        Uploader[any]
}

func newUploaderFixture(t *testing.T) *uploaderFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &uploaderFixture{
		recordingEvent:  []byte(`{"t":"a"}`),
		recordingEvent1: []byte(`{"t1":"a1"}`),
		mockHTTPClient:  mocksSysUtils.NewMockHTTPClientI(ctrl),
		mockHTTP:        mocksSysUtils.NewMockHttpI(ctrl),
		mockTransformer: mocksDebugger.NewMockTransformerAny(ctrl),
	}
	f.uploader = New[any]("http://test", &testutils.BasicAuthMock{}, f.mockTransformer)
	return f
}

// impl gives access to the uploader's fields, which may only be overridden before start is called.
func (f *uploaderFixture) impl() *uploaderImpl[any] {
	return f.uploader.(*uploaderImpl[any])
}

func (f *uploaderFixture) start(t *testing.T) {
	t.Helper()
	f.uploader.Start()
	t.Cleanup(f.uploader.Stop)
}

func TestUploader(t *testing.T) {
	config.Reset()
	logger.Reset()

	t.Run("Send live requests", func(t *testing.T) {
		t.Run("should successfully send the live events request", func(t *testing.T) {
			f := newUploaderFixture(t)
			f.impl().Client = f.mockHTTPClient
			f.start(t)
			f.uploader.RecordEvent(f.recordingEvent)
			f.mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data any) ([]byte, error) {
					eventBuffer := data.([]any)
					for _, e := range eventBuffer {
						assert.Equal(t, f.recordingEvent, e)
					}

					rawJSON, err := jsonrs.Marshal(data)
					assert.NoError(t, err)
					return rawJSON, nil
				}).AnyTimes()

			// Response JSON
			jsonResponse := `OK`
			// New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			f.mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				// asserting http request
				assertRequest(t, req)
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil).AnyTimes()
		})

		t.Run("should log error message from config backend if post request returns non 200", func(t *testing.T) {
			f := newUploaderFixture(t)
			f.impl().Client = f.mockHTTPClient
			f.start(t)
			f.uploader.RecordEvent(f.recordingEvent)
			f.mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data any) ([]byte, error) {
					eventBuffer := data.([]any)
					for _, e := range eventBuffer {
						assert.Equal(t, f.recordingEvent, e)
					}

					rawJSON, err := jsonrs.Marshal(data)
					assert.NoError(t, err)
					return rawJSON, nil
				}).AnyTimes()

			// Response JSON
			jsonResponse := `OK`
			// New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			f.mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				// asserting http request
				assertRequest(t, req)
			}).Return(&http.Response{
				StatusCode: 400,
				Body:       r,
			}, nil).AnyTimes()
		})

		t.Run("should not send the live events request if transform data fails", func(t *testing.T) {
			f := newUploaderFixture(t)
			f.impl().Client = f.mockHTTPClient
			f.start(t)
			f.uploader.RecordEvent(f.recordingEvent)

			f.mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data any) ([]byte, error) {
					eventBuffer := data.([]any)
					for _, e := range eventBuffer {
						assert.Equal(t, f.recordingEvent, e)
					}

					rawJSON, err := jsonrs.Marshal(data)
					assert.NoError(t, err)
					return rawJSON, errors.New("transform error")
				}).AnyTimes()
		})

		t.Run("should not send the live events request if http new request fails", func(t *testing.T) {
			f := newUploaderFixture(t)
			f.start(t)
			f.uploader.RecordEvent(f.recordingEvent)

			f.mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data any) ([]byte, error) {
					eventBuffer := data.([]any)
					for _, e := range eventBuffer {
						assert.Equal(t, f.recordingEvent, e)
					}

					rawJSON, err := jsonrs.Marshal(data)
					assert.NoError(t, err)
					return rawJSON, nil
				}).AnyTimes()
			f.mockHTTP.EXPECT().NewRequest("POST", "http://test", gomock.Any()).
				Return(nil, errors.New("http new request error")).AnyTimes()
		})

		t.Run("should not send the live events request if client do fails. Retry 3 times.", func(t *testing.T) {
			f := newUploaderFixture(t)
			f.impl().Client = f.mockHTTPClient
			f.mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data any) ([]byte, error) {
					eventBuffer := data.([]any)
					for _, e := range eventBuffer {
						assert.Equal(t, f.recordingEvent, e)
					}

					rawJSON, err := jsonrs.Marshal(data)
					assert.NoError(t, err)
					return rawJSON, nil
				})

			// Response JSON
			jsonResponse := `OK`
			// New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			f.impl().batchTimeout = config.SingleValueLoader(time.Millisecond)
			f.impl().retrySleep = config.SingleValueLoader(time.Millisecond)
			f.start(t)

			var wg sync.WaitGroup
			wg.Add(3)

			f.mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				// asserting http request
				assertRequest(t, req)
				wg.Done()
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, errors.New("client do failed")).Times(3)

			f.uploader.RecordEvent(f.recordingEvent)
			wg.Wait()
		})

		t.Run("should drop some events if number of events to record is more than queue size", func(t *testing.T) {
			f := newUploaderFixture(t)
			f.impl().Client = f.mockHTTPClient
			f.impl().maxESQueueSize = config.SingleValueLoader(1)
			f.start(t)

			f.mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data any) ([]byte, error) {
					eventBuffer := data.([]any)
					assert.Len(t, eventBuffer, 1)
					for _, e := range eventBuffer {
						assert.Equal(t, f.recordingEvent, e)
					}

					rawJSON, err := jsonrs.Marshal(data)
					assert.NoError(t, err)
					return rawJSON, nil
				})

			// Response JSON
			jsonResponse := `OK`
			// New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			f.mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				// asserting http request
				assertRequest(t, req)
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil).AnyTimes()

			f.uploader.RecordEvent(f.recordingEvent)
			f.uploader.RecordEvent(f.recordingEvent)
		})

		t.Run("should send events in batches", func(t *testing.T) {
			f := newUploaderFixture(t)
			f.impl().Client = f.mockHTTPClient
			f.impl().maxBatchSize = config.SingleValueLoader(1)
			f.impl().batchTimeout = config.SingleValueLoader(time.Millisecond)
			f.start(t)

			var wg sync.WaitGroup
			wg.Add(2)

			i := 0
			f.mockTransformer.EXPECT().Transform(gomock.Any()).
				DoAndReturn(func(data any) ([]byte, error) {
					eventBuffer := data.([]any)
					assert.Len(t, eventBuffer, 1)
					for _, e := range eventBuffer {
						var re []byte
						if i == 0 {
							re = f.recordingEvent
						} else {
							re = f.recordingEvent1
						}
						assert.Equal(t, re, e)
						i++
					}

					rawJSON, err := jsonrs.Marshal(data)
					assert.NoError(t, err)
					return rawJSON, nil
				}).Times(2)

			// Response JSON
			jsonResponse := `OK`
			// New reader with that JSON
			r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

			f.mockHTTPClient.EXPECT().Do(gomock.Any()).Do(func(req *http.Request) {
				// asserting http request
				assertRequest(t, req)
				wg.Done()
			}).Return(&http.Response{
				StatusCode: 200,
				Body:       r,
			}, nil).AnyTimes()

			f.uploader.RecordEvent(f.recordingEvent)
			f.uploader.RecordEvent(f.recordingEvent1)

			wg.Wait()
		})
	})
}

// assertRequest runs on the uploader's goroutine, where require must not be used.
func assertRequest(t *testing.T, req *http.Request) {
	t.Helper()
	username, password, ok := req.BasicAuth()
	assert.True(t, ok)
	assert.Equal(t, "test", username)
	assert.Equal(t, "test", password)
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "test", req.URL.Host)
}
