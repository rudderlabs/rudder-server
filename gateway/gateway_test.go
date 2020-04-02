package gateway

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/mocks"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var _ = Describe("Gateway", func() {
	var (
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
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("Initialization", func() {
		gateway := &HandleT{webRequestQ: make(chan *webRequestT)}
		var clearDB = false

		It("should wait for backend config", func() {
			mockBackendConfig.EXPECT().WaitForConfig().Return().Times(1)
			gateway.Setup(mockBackendConfig, mockJobsDB, nil, &clearDB)
		})
	})

	Context("Normal operation", func() {
		var (
			gateway        = &HandleT{webRequestQ: make(chan *webRequestT)}
			clearDB   bool = false
			waitGroup sync.WaitGroup
		)

		BeforeEach(func() {
			waitGroup = sync.WaitGroup{}
			waitGroup.Add(1)
			mockBackendConfig.EXPECT().WaitForConfig().Return().Times(1)
			gateway.Setup(mockBackendConfig, mockJobsDB, nil, &clearDB)
		})

		AfterEach(func() {
			waitWithTimeout(&waitGroup, time.Second)
		})

		assertHandler := func(handler http.HandlerFunc) {
			It("should reject requests without write key", func() {
				var emptyJobsList []*jobsdb.JobT

				mockJobsDB.
					EXPECT().Store(emptyJobsList).
					Do(notifyWaitGroup(&waitGroup)).
					Return(make(map[uuid.UUID]string)).
					Times(1)

				expectHandlerResponse(handler, unauthorizedRequest(nil), 400, NoWriteKeyInBasicAuth)
			})

			It("should reject requests without request body", func() {
				var emptyJobsList []*jobsdb.JobT

				mockJobsDB.
					EXPECT().Store(emptyJobsList).
					Do(notifyWaitGroup(&waitGroup)).
					Return(make(map[uuid.UUID]string)).
					Times(1)

				expectHandlerResponse(handler, authorizedRequest(nil), 400, RequestBodyNil)
			})

			It("should reject requests without valid json in request body", func() {
				var invalidBody = "not-a-valid-json"
				var emptyJobsList []*jobsdb.JobT

				mockJobsDB.
					EXPECT().Store(emptyJobsList).
					Do(notifyWaitGroup(&waitGroup)).
					Return(make(map[uuid.UUID]string)).
					Times(1)

				expectHandlerResponse(handler, authorizedRequest(bytes.NewBufferString(invalidBody)), 400, InvalidJson)
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

func authorizedRequest(body io.Reader) *http.Request {
	req := unauthorizedRequest(body)

	username := "some-valid-token"
	password := "password-should-be-ignored"
	basicAuth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))

	req.Header.Set("Authorization", fmt.Sprintf("Basic %s", basicAuth))
	return req
}

func expectHandlerResponse(handler http.HandlerFunc, req *http.Request, responseStatus int, responseBody string) {
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)
	bodyBytes, _ := ioutil.ReadAll(rr.Body)
	body := string(bodyBytes)
	Expect(rr.Result().StatusCode).To(Equal(responseStatus))
	Expect(body).To(Equal(responseBody + "\n"))
}

func notifyWaitGroup(wg *sync.WaitGroup) func(interface{}) {
	return func(interface{}) {
		wg.Done()
	}
}

func waitWithTimeout(wg *sync.WaitGroup, d time.Duration) {
	c := make(chan struct{})

	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
	case <-time.After(d):
		ginkgo.Fail("wait group timed out")
	}
}
