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
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/mocks"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

const (
	WriteKeyEnabled   = "enabled-write-key"
	WriteKeyDisabled  = "disabled-write-key"
	WriteKeyInvalid   = "invalid-write-key"
	WriteKeyEmpty     = ""
	SourceIDEnabled   = "enabled-source"
	SourceIDDisabled  = "disabled-source"
	TestRemoteAddress = "test.com"
)

var _ = Describe("Gateway", func() {

	var (
		asyncHelper testutils.AsyncTestHelper

		mockCtrl          *gomock.Controller
		mockJobsDB        *mocks.MockJobsDB
		mockBackendConfig *mocks.MockBackendConfig
	)

	allHandlers := func(gateway *HandleT) map[string]http.HandlerFunc {
		return map[string]http.HandlerFunc{
			"alias":    gateway.webAliasHandler,
			"batch":    gateway.webBatchHandler,
			"group":    gateway.webGroupHandler,
			"identify": gateway.webIdentifyHandler,
			"page":     gateway.webPageHandler,
			"screen":   gateway.webScreenHandler,
			"track":    gateway.webTrackHandler,
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
		assertSingleMessageHandler := func(handlerType string, handler http.HandlerFunc) {
			It("should accept valid requests, and store to jobsdb", func() {
				validBody := `{"data":{"string":"valid-json","nested":{"child":1}}}`
				validBodyJSON := json.RawMessage(validBody)

				mockJobsDB.
					EXPECT().Store(gomock.Any()).
					DoAndReturn(func(jobs []*jobsdb.JobT) map[uuid.UUID]string {
						// a valid jobsdb Store should return a map of jobs' uuids with empty error strings.
						var result = make(map[uuid.UUID]string)
						for _, job := range jobs {
							Expect(misc.IsValidUUID(job.UUID.String())).To(Equal(true))
							Expect(job.Parameters).To(Equal(json.RawMessage(fmt.Sprintf(`{"source_id": "%v"}`, SourceIDEnabled))))
							Expect(job.CustomVal).To(Equal(CustomVal))

							responseData := []byte(job.EventPayload)
							receivedAt := gjson.GetBytes(responseData, "receivedAt")
							writeKey := gjson.GetBytes(responseData, "writeKey")
							requestIP := gjson.GetBytes(responseData, "requestIP")
							batch := gjson.GetBytes(responseData, "batch")
							payload := gjson.GetBytes(responseData, "batch.0")
							messageID := payload.Get("messageId")
							anonymousID := payload.Get("anonymousId")
							messageType := payload.Get("type")

							strippedPayload, _ := sjson.Delete(payload.String(), "messageId")
							strippedPayload, _ = sjson.Delete(strippedPayload, "anonymousId")
							strippedPayload, _ = sjson.Delete(strippedPayload, "type")

							// Assertions regarding response metadata
							Expect(time.Parse(misc.RFC3339Milli, receivedAt.String())).To(BeTemporally("~", time.Now()))
							Expect(writeKey.String()).To(Equal(WriteKeyEnabled))
							Expect(requestIP.String()).To(Equal(TestRemoteAddress))

							// Assertions regarding batch
							Expect(batch.Array()).To(HaveLen(1))

							// Assertions regarding batch message
							Expect(messageID.Exists()).To(BeTrue())
							Expect(messageID.String()).To(testutils.BeValidUUID())
							Expect(anonymousID.Exists()).To(BeTrue())
							Expect(anonymousID.String()).To(testutils.BeValidUUID())
							Expect(messageType.Exists()).To(BeTrue())
							Expect(messageType.String()).To(Equal(handlerType))
							Expect(strippedPayload).To(MatchJSON(validBodyJSON))

							result[job.UUID] = ""
						}
						asyncHelper.ExpectAndNotifyCallback()()
						return result
					}).
					Times(1)

				expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(validBody)), 200, "OK")
			})
		}

		for handlerType, handler := range allHandlers(gateway) {
			if handlerType != "batch" {
				assertSingleMessageHandler(handlerType, handler)
			}
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
	req.RemoteAddr = TestRemoteAddress
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
