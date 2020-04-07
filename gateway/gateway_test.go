package gateway

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/mocks"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

var _ = Describe("Gateway", func() {
	const (
		WriteKeyEnabled  = "enabled-write-key"
		WriteKeyDisabled = "disabled-write-key"
		WriteKeyInvalid  = "invalid-write-key"
		WriteKeyEmpty    = ""
		SourceIDEnabled  = "enabled-source"
		SourceIDDisabled = "disabled-source"
	)

	var (
		asyncHelper testutils.AsyncTestHelper

		mockCtrl          *gomock.Controller
		mockJobsDB        *mocks.MockJobsDB
		mockBackendConfig *mocks.MockBackendConfig
	)

	allHandlers := func(gateway *HandleT) []http.HandlerFunc {
		return []http.HandlerFunc{
			gateway.webAliasHandler,
			gateway.webBatchHandler,
			gateway.webGroupHandler,
			gateway.webIdentifyHandler,
			gateway.webPageHandler,
			gateway.webScreenHandler,
			gateway.webTrackHandler,
		}
	}

	BeforeEach(func() {
		logger.Setup()
		stats.Setup()

		mockCtrl = gomock.NewController(GinkgoT())
		mockJobsDB = mocks.NewMockJobsDB(mockCtrl)
		mockBackendConfig = mocks.NewMockBackendConfig(mockCtrl)

		// During Setup, gateway subscribes to backend config and waits until it is received.
		mockBackendConfig.EXPECT().WaitForConfig().Return().Times(1)
		mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
			Do(func(channel chan utils.DataEvent, topic string) {
				// on Subscribe, emulate a backend configuration event
				data := backendconfig.SourcesT{
					Sources: []backendconfig.SourceT{
						backendconfig.SourceT{
							ID:       SourceIDDisabled,
							WriteKey: WriteKeyDisabled,
							Enabled:  false,
						},
						backendconfig.SourceT{
							ID:       SourceIDEnabled,
							WriteKey: WriteKeyEnabled,
							Enabled:  true,
						},
					},
				}
				event := utils.DataEvent{Data: data, Topic: topic}
				go func() { channel <- event }()
			}).
			Do(asyncHelper.ExpectAndNotifyCallback()).
			Return().Times(1)
	})

	AfterEach(func() {
		asyncHelper.WaitWithTimeout(time.Second)
		mockCtrl.Finish()
	})

	Context("Initialization", func() {
		gateway := &HandleT{}
		var clearDB = false

		It("should wait for backend config", func() {
			gateway.Setup(mockBackendConfig, mockJobsDB, nil, &clearDB)
		})
	})

	Context("Valid requests", func() {
		var (
			gateway      = &HandleT{}
			clearDB bool = false
		)

		BeforeEach(func() {
			gateway.Setup(mockBackendConfig, mockJobsDB, nil, &clearDB)
		})

		// common tests for all web handlers
		assertHandler := func(handler http.HandlerFunc) {
			It("should accept valid requests, and store to jobsdb", func() {
				validBody := `{"data":"valid-json"}`

				mockJobsDB.
					EXPECT().Store(gomock.Any()).
					DoAndReturn(func(jobs []*jobsdb.JobT) map[uuid.UUID]string {
						// a valid jobsdb Store should return a map of jobs' uuids with empty error strings.
						var result = make(map[uuid.UUID]string)
						for _, job := range jobs {
							Expect(misc.IsValidUUID(job.UUID.String())).To(Equal(true))
							Expect(job.Parameters).To(Equal(json.RawMessage(fmt.Sprintf(`{"source_id": "%v"}`, SourceIDEnabled))))
							Expect(job.CustomVal).To(Equal("GW"))
							Expect(job.EventPayload).To(Equal(json.RawMessage(validBody)))
							result[job.UUID] = ""
						}
						asyncHelper.ExpectAndNotifyCallback()()
						return result
					}).
					Times(1)

				expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(validBody)), 200, "OK")
			})
		}

		for _, handler := range allHandlers(gateway) {
			assertHandler(handler)
		}
	})

	Context("Invalid requests", func() {
		var (
			gateway      = &HandleT{}
			clearDB bool = false
		)

		BeforeEach(func() {
			// all of these request errors will cause JobsDB.Store to be called with an empty job list
			var emptyJobsList []*jobsdb.JobT

			mockJobsDB.
				EXPECT().Store(emptyJobsList).
				Do(asyncHelper.ExpectAndNotifyCallback()).
				Return(make(map[uuid.UUID]string)).
				Times(1)

			gateway.Setup(mockBackendConfig, mockJobsDB, nil, &clearDB)
		})

		// common tests for all web handlers
		assertHandler := func(handler http.HandlerFunc) {
			It("should reject requests without Authorization header", func() {
				expectHandlerResponse(handler, unauthorizedRequest(nil), 400, NoWriteKeyInBasicAuth+"\n")
			})

			It("should reject requests without username in Authorization header", func() {
				expectHandlerResponse(handler, authorizedRequest(WriteKeyEmpty, nil), 400, NoWriteKeyInBasicAuth+"\n")
			})

			It("should reject requests without request body", func() {
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, nil), 400, RequestBodyNil+"\n")
			})

			It("should reject requests without valid json in request body", func() {
				invalidBody := "not-a-valid-json"
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(invalidBody)), 400, InvalidJson+"\n")
			})

			It("should reject requests with request bodies larger than configured limit", func() {
				data := make([]byte, gateway.MaxReqSize())
				for i := range data {
					data[i] = 'a'
				}

				body := fmt.Sprintf(`{"data":"%s"}`, string(data))
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(body)), 400, RequestBodyTooLarge+"\n")
			})

			It("should reject requests with invalid write keys", func() {
				validBody := `{"data":"valid-json"}`
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(validBody)), 400, InvalidWriteKey+"\n")
			})

			It("should reject requests with disabled write keys", func() {
				validBody := `{"data":"valid-json"}`
				expectHandlerResponse(handler, authorizedRequest(WriteKeyDisabled, bytes.NewBufferString(validBody)), 400, InvalidWriteKey+"\n")
			})
		}

		for _, handler := range allHandlers(gateway) {
			assertHandler(handler)
		}
	})
})

func unauthorizedRequest(body io.Reader) *http.Request {
	req, err := http.NewRequest("GET", "", body)
	if err != nil {
		panic(err)
	}

	return req
}

func authorizedRequest(username string, body io.Reader) *http.Request {
	req := unauthorizedRequest(body)

	basicAuth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:password-should-be-ignored", username)))

	req.Header.Set("Authorization", fmt.Sprintf("Basic %s", basicAuth))
	return req
}

func expectHandlerResponse(handler http.HandlerFunc, req *http.Request, responseStatus int, responseBody string) {
	testutils.RunTestWithTimeout(func() {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		bodyBytes, _ := ioutil.ReadAll(rr.Body)
		body := string(bodyBytes)
		Expect(rr.Result().StatusCode).To(Equal(responseStatus))
		Expect(body).To(Equal(responseBody))
	}, time.Second)
}
