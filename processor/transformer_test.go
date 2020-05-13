package processor

import (
	"net/http"
	"net/http/httptest"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/logger"
	mock_stats "github.com/rudderlabs/rudder-server/mocks/stats"
	mock_misc "github.com/rudderlabs/rudder-server/mocks/utils/misc"
	mock_sysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/services/stats"
)

type TestData struct {
	test string
}

var (
	transformer               *transformerHandleT
	originalLogger            = log
	originaIoUtil             = Ioutil
	originaIo                 = Io
	mockLogger                *mock_logger.MockLoggerI
	ctrl                      *gomock.Controller
	mockStats                 *mock_stats.MockStats
	mockRubberStats           *mock_stats.MockRudderStats
	mockIoUtil                *mock_sysUtils.MockIoUtilI
	mockIo                    *mock_sysUtils.MockIoI
	transformRequestTimerStat stats.RudderStats
)
var _ = Describe("Transformer", func() {
	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		mockLogger = mock_logger.NewMockLoggerI(ctrl)
		log = mockLogger
		mockStats = mock_stats.NewMockStats(ctrl)
		mockRubberStats = mock_stats.NewMockRudderStats(ctrl)
		var statsmock stats.Stats = mockStats
		mockStats.EXPECT().NewStat(gomock.Any(), gomock.Any()).Return(mockRubberStats).Times(1)
		transformRequestTimerStat = statsmock.NewStat("config_backend.errors", stats.CountType)
	})
	Context("transformWorker", func() {
		BeforeEach(func() {
			transformer = &transformerHandleT{}
			transformer.requestQ = make(chan *transformMessageT, 2)
			transformer.responseQ = make(chan *transformMessageT, 2)
			mockIoUtil = mock_sysUtils.NewMockIoUtilI(ctrl)
			mockIo = mock_sysUtils.NewMockIoI(ctrl)
			Ioutil = mockIoUtil
			Io = mockIo
		})
		AfterEach(func() {
			ctrl.Finish()
			log = originalLogger
			Ioutil = originaIoUtil
			Io = originaIo
		})
		It("Expect to panic if wrong job data", func() {
			defer func() {
				if r := recover(); r != nil {
					Expect(r).To(HaveOccurred())
				} else if r == nil {
					Expect(r).To(HaveOccurred())
				}
			}()
			// we create channel as data to enforce json.marshal to throw an error
			transformer.requestQ <- &transformMessageT{index: 0, data: make(chan int), url: ""}
			transformer.transformWorker(transformRequestTimerStat)
		})

		It("Expect to panic if all retries fail", func() {
			defer func() {
				if r := recover(); r != nil {
					Expect(r).To(HaveOccurred())
				} else if r == nil {
					Expect(r).To(HaveOccurred())
				}
			}()
			maxRetry = 1
			mockRubberStats.EXPECT().Start().Times(3)
			mockRubberStats.EXPECT().End().Times(3)
			mockLogger.EXPECT().Errorf("JS HTTP connection error: URL: %v Error: %+v", "", gomock.Any()).Times(3)
			transformer.requestQ <- &transformMessageT{index: 0, data: &TestData{test: "test"}, url: ""}
			transformer.transformWorker(transformRequestTimerStat)
		})
		It("Expect to make the correct actions if the first request fail but succeed in retry", func() {
			maxRetry = 1
			defer func() {
				if r := recover(); r != nil {
					Expect(r).To(HaveOccurred())
				} else if r == nil {
					Expect(r).To(HaveOccurred())
				}
			}()

			countRequest := 0
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				// simulate client.post failure using redirect.
				// After 10 redirects, request will fail and worker should retry
				if countRequest < 10 {
					http.Redirect(rw, req, "", http.StatusMovedPermanently)
					countRequest++
				} else {
					rw.WriteHeader(http.StatusOK)
					rw.Write([]byte(""))
				}
			}))
			mockIoUtil.EXPECT().ReadAll(gomock.Any()).Times(1)
			mockRubberStats.EXPECT().Start().Times(2)
			mockRubberStats.EXPECT().End().Times(2)
			mockLogger.EXPECT().Errorf("JS HTTP connection error: URL: %v Error: %+v", gomock.Any(), gomock.Any()).Times(1)
			mockLogger.EXPECT().Errorf("Failed request succeeded after %v retries, URL: %v", 1, gomock.Any()).Times(1)
			transformer.requestQ <- &transformMessageT{index: 0, data: &TestData{test: "test"}, url: server.URL}
			transformer.transformWorker(transformRequestTimerStat)
		})

		It("Expect to panic if fail to read response body", func() {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusOK)
				rw.Write([]byte("{}"))
			}))
			defer func() {
				if r := recover(); r != nil {
					Expect(r).To(HaveOccurred())
				} else if r == nil {
					Expect(r).To(HaveOccurred())
				}
			}()
			mockIoUtil.EXPECT().ReadAll(gomock.Any()).Times(1)
			mockRubberStats.EXPECT().Start().Times(1)
			mockRubberStats.EXPECT().End().Times(1)
			transformer.requestQ <- &transformMessageT{index: 0, data: &TestData{test: "test"}, url: server.URL}
			transformer.transformWorker(transformRequestTimerStat)

		})
		It("Expect to make the correct actions if response status is different from 200,400,404,413", func() {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusAccepted)
				rw.Write([]byte("{}"))
			}))
			mockRubberStats.EXPECT().Start().Times(1)
			mockRubberStats.EXPECT().End().Times(1)
			mockLogger.EXPECT().Errorf("Transformer returned status code: %v", 202).Times(1)
			mockIo.EXPECT().Copy(gomock.Any(), gomock.Any()).Times(1)
			transformer.requestQ <- &transformMessageT{index: 0, data: &TestData{test: "test"}, url: server.URL}
			go func() {
				transformer.transformWorker(transformRequestTimerStat)
			}()
			resp := <-transformer.responseQ
			Expect(resp.index).To(Equal(0))
		})
		It("Expect to make the correct if a request succeed", func() {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusOK)
				rw.Write([]byte("{}"))
			}))
			mockRubberStats.EXPECT().Start().Times(1)
			mockRubberStats.EXPECT().End().Times(1)
			mockIoUtil.EXPECT().ReadAll(gomock.Any()).Return([]byte("{}"), nil).Times(1)
			transformer.requestQ <- &transformMessageT{index: 0, data: &TestData{test: "test"}, url: server.URL}
			go func() {
				transformer.transformWorker(transformRequestTimerStat)
			}()
			resp := <-transformer.responseQ
			Expect(resp.index).To(Equal(0))
		})
	})
	Context("Transform", func() {
		var (
			transformer            *transformerHandleT
			mockSentStat           *mock_stats.MockRudderStats
			mockReceivedStat       *mock_stats.MockRudderStats
			mockFailedStat         *mock_stats.MockRudderStats
			mockPerfStats          *mock_misc.MockPerfStatsI
			mockTransformTimerStat *mock_stats.MockRudderStats
			eventsDataList         []interface{}
			eventData              map[string]interface{}
		)
		BeforeEach(func() {
			mockSentStat = mock_stats.NewMockRudderStats(ctrl)
			mockReceivedStat = mock_stats.NewMockRudderStats(ctrl)
			mockFailedStat = mock_stats.NewMockRudderStats(ctrl)
			mockTransformTimerStat = mock_stats.NewMockRudderStats(ctrl)
			mockPerfStats = mock_misc.NewMockPerfStatsI(ctrl)
			transformer = &transformerHandleT{
				requestQ:           make(chan *transformMessageT, maxChanSize),
				responseQ:          make(chan *transformMessageT, maxChanSize),
				sentStat:           mockSentStat,
				receivedStat:       mockReceivedStat,
				failedStat:         mockFailedStat,
				transformTimerStat: mockTransformTimerStat,
				perfStats:          mockPerfStats,
			}
			mockTransformTimerStat.EXPECT().Start().Times(1)

			eventsDataList = make([]interface{}, 0)
			eventData = make(map[string]interface{})
			eventData["destination"] = backendconfig.DestinationT{
				ID:                 "d2",
				Name:               "processor Enabled",
				IsProcessorEnabled: true,
			}
			eventData["message"] = make(map[string]interface{})
			eventData["message"].(map[string]interface{})["anonymousId"] = "a1"
			eventsDataList = append(eventsDataList, eventData)
			mockPerfStats.EXPECT().Start().Times(1)
		})

		It("Expect to panic if no anonymous id found", func() {
			eventData2 := make(map[string]interface{})
			eventData2["destination"] = backendconfig.DestinationT{
				ID:                 "d2",
				Name:               "processor Enabled",
				IsProcessorEnabled: true,
			}
			eventData2["message"] = make(map[string]interface{})
			eventsDataList = append(eventsDataList, eventData2)
			defer func() {
				if r := recover(); r != nil {
					Expect(r).To(HaveOccurred())
					Expect(r.(error).Error()).To(Equal("GetAnonymousID from current user failed"))
				} else if r == nil {
					Expect(r).To(HaveOccurred())
					Expect(r.(error).Error()).To(Equal("GetAnonymousID from current user failed"))
				}
			}()

			mockSentStat.EXPECT().Count(2).Times(1)
			transformer.requestQ <- &transformMessageT{index: 0, data: &TestData{test: "test"}, url: ""}
			transformer.Transform(eventsDataList, "", 1, true)
		})

		It("Expect to make the correct actions if current and prev user ids are not equal", func() {
			eventData2 := make(map[string]interface{})
			eventData2["destination"] = backendconfig.DestinationT{
				ID:                 "d2",
				Name:               "processor Enabled",
				IsProcessorEnabled: true,
			}
			eventData2["message"] = make(map[string]interface{})
			eventData2["message"].(map[string]interface{})["anonymousId"] = "a2"
			eventsDataList = append(eventsDataList, eventData2)
			mockLogger.EXPECT().Debug("Breaking batch at", 1, "a1", "a2").AnyTimes()
			// mockLogger.EXPECT().Debug("Breaking batch at", 0, "a1", "a2").Times(1)

			mockSentStat.EXPECT().Count(0).AnyTimes()
			mockSentStat.EXPECT().Count(1).AnyTimes()
			go func() {
				respData := make([]interface{}, 0)
				respData = append(respData, "testData")
				defer GinkgoRecover()
				req := <-transformer.requestQ
				req2 := <-transformer.requestQ
				transformer.responseQ <- &transformMessageT{index: 1, data: respData, url: ""}
				transformer.responseQ <- &transformMessageT{index: 2, data: respData, url: ""}
				Expect(req.index).To(Equal(0))
				Expect(req2.index).To(Equal(1))
			}()
			mockReceivedStat.EXPECT().Count(2).Times(1)
			mockPerfStats.EXPECT().End(2).Times(1)
			mockPerfStats.EXPECT().Print().Times(1)
			mockTransformTimerStat.EXPECT().End().Times(1)
			transformer.requestQ <- &transformMessageT{index: 0, data: &TestData{test: "test"}, url: ""}
			transformer.Transform(eventsDataList, "", 1, true)
		})

		It("Expect to panic if response data not an array", func() {
			defer func() {
				if r := recover(); r != nil {
					Expect(r).To(HaveOccurred())
					Expect(r.(error).Error()).To(Equal("typecast of resp.data to []interface{} failed"))
				} else if r == nil {
					Expect(r).To(HaveOccurred())
					Expect(r.(error).Error()).To(Equal("typecast of resp.data to []interface{} failed"))
				}
			}()

			mockSentStat.EXPECT().Count(1).Times(1)
			mockReceivedStat.EXPECT().Count(1).Times(1)
			mockTransformTimerStat.EXPECT().End().Times(1)
			mockPerfStats.EXPECT().End(1).Times(1)
			mockPerfStats.EXPECT().Print().Times(1)
			respData := make([]interface{}, 0)
			respData = append(respData, "testData")
			go func() {
				defer GinkgoRecover()
				req := <-transformer.requestQ
				transformer.responseQ <- &transformMessageT{index: 1, data: "", url: ""}
				Expect(req.index).To(Equal(1))
			}()
			transformer.Transform(eventsDataList, "", 1, false)
		})

		Context("Failed Increment stats", func() {
			BeforeEach(func() {

				mockSentStat.EXPECT().Count(1).Times(1)
				mockReceivedStat.EXPECT().Count(0).Times(1)
				mockTransformTimerStat.EXPECT().End().Times(1)
				mockPerfStats.EXPECT().End(1).Times(1)
				mockPerfStats.EXPECT().Print().Times(1)
				mockFailedStat.EXPECT().Increment().Times(1)
			})
			It("Expect to Increment failed stats if not outpout in response data", func() {

				respData := make([]interface{}, 0)
				respDataElement := make(map[string]interface{})
				respDataElement["test"] = ""
				respData = append(respData, respDataElement)
				go func() {
					defer GinkgoRecover()
					req := <-transformer.requestQ
					transformer.responseQ <- &transformMessageT{index: 1, data: respData, url: ""}
					Expect(req.index).To(Equal(1))
				}()
				transformer.Transform(eventsDataList, "", 1, false)
			})

			It("Expect to Increment failed stats if not outpout in response data", func() {
				respData := make([]interface{}, 0)
				respDataElement := make(map[string]interface{})
				respDataElement["test"] = ""
				respData = append(respData, respDataElement)
				go func() {
					defer GinkgoRecover()
					req := <-transformer.requestQ
					transformer.responseQ <- &transformMessageT{index: 1, data: respData, url: ""}
					Expect(req.index).To(Equal(1))
				}()
				response := transformer.Transform(eventsDataList, "", 1, false)
				Expect(response.Success).To(BeTrue())
				Expect(response.Events).To(Equal(make([]interface{}, 0)))
			})

			It("Expect to Increment failed stats if status code is 400", func() {
				respData := make([]interface{}, 0)
				respDataElement := make(map[string]interface{})
				output := make(map[string]interface{})
				output["statusCode"] = 400
				respDataElement["output"] = output
				respData = append(respData, respDataElement)
				go func() {
					defer GinkgoRecover()
					req := <-transformer.requestQ
					transformer.responseQ <- &transformMessageT{index: 1, data: respData, url: ""}
					Expect(req.index).To(Equal(1))
				}()

				response := transformer.Transform(eventsDataList, "", 1, false)
				Expect(response.Success).To(BeTrue())
				Expect(response.Events).To(Equal(make([]interface{}, 0)))
			})
		})

		It("Expect to make the correct actions and return the correct result", func() {

			mockSentStat.EXPECT().Count(1).Times(1)
			mockReceivedStat.EXPECT().Count(1).Times(1)
			mockTransformTimerStat.EXPECT().End().Times(1)
			mockPerfStats.EXPECT().End(1).Times(1)
			mockPerfStats.EXPECT().Print().Times(1)
			respData := make([]interface{}, 0)
			respData = append(respData, "testData")
			go func() {
				defer GinkgoRecover()
				req := <-transformer.requestQ
				transformer.responseQ <- &transformMessageT{index: 1, data: respData, url: ""}
				Expect(req.index).To(Equal(1))
			}()
			response := transformer.Transform(eventsDataList, "", 1, false)
			Expect(response.Success).To(BeTrue())
			Expect(response.Events).To(Equal(respData))
		})
	})
})
