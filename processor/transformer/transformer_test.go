package transformer

import (
	"net/http"
	"net/http/httptest"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mock_stats "github.com/rudderlabs/rudder-server/mocks/stats"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	mock_misc "github.com/rudderlabs/rudder-server/mocks/utils/misc"
	mock_sysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var TestData = []TransformerEventT{
	{
		SessionID: "test",
	},
}
var (
	transformer               *HandleT
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
			transformer = &HandleT{}
			transformer.requestQ = make(chan *transformMessageT, 2)
			transformer.responseQ = make(chan *transformedMessageT, 2)
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
			transformer.requestQ <- &transformMessageT{index: 0, data: TestData, url: ""}
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
			transformer.requestQ <- &transformMessageT{index: 0, data: TestData, url: server.URL}
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
			transformer.requestQ <- &transformMessageT{index: 0, data: TestData, url: server.URL}
			transformer.transformWorker(transformRequestTimerStat)

		})
		It("Expect to make the correct actions if response status is different from 200,400,404,413", func() {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusAccepted)
				rw.Write([]byte("[{}]"))
			}))
			mockRubberStats.EXPECT().Start().Times(1)
			mockRubberStats.EXPECT().End().Times(1)
			mockLogger.EXPECT().Errorf("Transformer returned status code: %v", 202).Times(1)
			mockIo.EXPECT().Copy(gomock.Any(), gomock.Any()).Times(1)
			transformer.requestQ <- &transformMessageT{index: 0, data: TestData, url: server.URL}
			go func() {
				transformer.transformWorker(transformRequestTimerStat)
			}()
			resp := <-transformer.responseQ
			Expect(resp.index).To(Equal(0))
		})
		It("Expect to make the correct if a request succeed", func() {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusOK)
				rw.Write([]byte("[{}]"))
			}))
			mockRubberStats.EXPECT().Start().Times(1)
			mockRubberStats.EXPECT().End().Times(1)
			mockIoUtil.EXPECT().ReadAll(gomock.Any()).Return([]byte("[{}]"), nil).Times(1)
			transformer.requestQ <- &transformMessageT{index: 0, data: TestData, url: server.URL}
			go func() {
				transformer.transformWorker(transformRequestTimerStat)
			}()
			resp := <-transformer.responseQ
			Expect(resp.index).To(Equal(0))
		})
	})
	Context("Transform", func() {
		var (
			transformer            *HandleT
			mockSentStat           *mock_stats.MockRudderStats
			mockReceivedStat       *mock_stats.MockRudderStats
			mockFailedStat         *mock_stats.MockRudderStats
			mockPerfStats          *mock_misc.MockPerfStatsI
			mockTransformTimerStat *mock_stats.MockRudderStats
			eventsDataList         []TransformerEventT
			eventData              TransformerEventT
		)
		BeforeEach(func() {
			mockSentStat = mock_stats.NewMockRudderStats(ctrl)
			mockReceivedStat = mock_stats.NewMockRudderStats(ctrl)
			mockFailedStat = mock_stats.NewMockRudderStats(ctrl)
			mockTransformTimerStat = mock_stats.NewMockRudderStats(ctrl)
			mockPerfStats = mock_misc.NewMockPerfStatsI(ctrl)
			transformer = &HandleT{
				requestQ:           make(chan *transformMessageT, maxChanSize),
				responseQ:          make(chan *transformedMessageT, maxChanSize),
				sentStat:           mockSentStat,
				receivedStat:       mockReceivedStat,
				failedStat:         mockFailedStat,
				transformTimerStat: mockTransformTimerStat,
				perfStats:          mockPerfStats,
			}
			mockTransformTimerStat.EXPECT().Start().Times(1)

			eventsDataList = make([]TransformerEventT, 0)

			eventMessage := make(types.SingularEventT)
			eventMessage["anonymousId"] = "a1"

			eventData = TransformerEventT{
				Destination: backendconfig.DestinationT{
					ID:                 "d2",
					Name:               "processor Enabled",
					IsProcessorEnabled: true,
				},
				Message: eventMessage,
			}
			eventsDataList = append(eventsDataList, eventData)

			mockPerfStats.EXPECT().Start().Times(1)
		})

		It("Expect to panic if no anonymous id found", func() {
			eventData2 := TransformerEventT{
				Destination: backendconfig.DestinationT{
					ID:                 "d2",
					Name:               "processor Enabled",
					IsProcessorEnabled: true,
				},
				Message: make(types.SingularEventT),
			}
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
			transformer.requestQ <- &transformMessageT{index: 0, data: TestData, url: ""}
			transformer.Transform(eventsDataList, "", 1, true)
		})

		It("Expect to make the correct actions if current and prev user ids are not equal", func() {

			eventMessage := make(types.SingularEventT)
			eventMessage["anonymousId"] = "a2"
			eventData2 := TransformerEventT{
				Destination: backendconfig.DestinationT{
					ID:                 "d2",
					Name:               "processor Enabled",
					IsProcessorEnabled: true,
				},
				Message: eventMessage,
			}
			eventsDataList = append(eventsDataList, eventData2)
			mockLogger.EXPECT().Debug("Breaking batch at", 1, "a1", "a2").AnyTimes()
			// mockLogger.EXPECT().Debug("Breaking batch at", 0, "a1", "a2").Times(1)

			mockSentStat.EXPECT().Count(0).AnyTimes()
			mockSentStat.EXPECT().Count(1).AnyTimes()
			go func() {
				respData := []TransformerResponseT{{}}
				defer GinkgoRecover()
				req := <-transformer.requestQ
				req2 := <-transformer.requestQ
				transformer.responseQ <- &transformedMessageT{index: 1, data: respData}
				transformer.responseQ <- &transformedMessageT{index: 2, data: respData}
				Expect(req.index).To(Equal(0))
				Expect(req2.index).To(Equal(1))
			}()
			mockReceivedStat.EXPECT().Count(2).Times(1)
			mockPerfStats.EXPECT().End(2).Times(1)
			mockPerfStats.EXPECT().Print().Times(1)
			mockTransformTimerStat.EXPECT().End().Times(1)
			transformer.requestQ <- &transformMessageT{index: 0, data: TestData, url: ""}
			transformer.Transform(eventsDataList, "test", 1, true)
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

				respData := []TransformerResponseT{}
				go func() {
					defer GinkgoRecover()
					req := <-transformer.requestQ
					transformer.responseQ <- &transformedMessageT{index: 1, data: respData}
					Expect(req.index).To(Equal(1))
				}()
				transformer.Transform(eventsDataList, "", 1, false)
			})

			It("Expect to Increment failed stats if not outpout in response data", func() {
				respData := []TransformerResponseT{}
				go func() {
					defer GinkgoRecover()
					req := <-transformer.requestQ
					transformer.responseQ <- &transformedMessageT{index: 1, data: respData}
					Expect(req.index).To(Equal(1))
				}()
				var expectedEventResponse []TransformerResponseT
				response := transformer.Transform(eventsDataList, "", 1, false)
				Expect(response.Success).To(BeTrue())
				Expect(response.Events).To(Equal(expectedEventResponse))
			})

			It("Expect to Increment failed stats if status code is 400", func() {
				output := make(map[string]interface{})
				output["statusCode"] = 400
				respData := []TransformerResponseT{
					{
						Output: output,
					},
				}
				var expectedEventResponse []TransformerResponseT
				go func() {
					defer GinkgoRecover()
					req := <-transformer.requestQ
					transformer.responseQ <- &transformedMessageT{index: 1, data: respData}
					Expect(req.index).To(Equal(1))
				}()

				response := transformer.Transform(eventsDataList, "", 1, false)
				Expect(response.Success).To(BeTrue())
				Expect(response.Events).To(Equal(expectedEventResponse))
			})
		})

		It("Expect to make the correct actions and return the correct result", func() {

			mockSentStat.EXPECT().Count(1).Times(1)
			mockReceivedStat.EXPECT().Count(1).Times(1)
			mockTransformTimerStat.EXPECT().End().Times(1)
			mockPerfStats.EXPECT().End(1).Times(1)
			mockPerfStats.EXPECT().Print().Times(1)
			respData := []TransformerResponseT{{}}
			go func() {
				defer GinkgoRecover()
				req := <-transformer.requestQ
				transformer.responseQ <- &transformedMessageT{index: 1, data: respData}
				Expect(req.index).To(Equal(1))
			}()
			response := transformer.Transform(eventsDataList, "", 1, false)
			Expect(response.Success).To(BeTrue())
			Expect(response.Events).To(Equal(respData))
		})
	})
})
