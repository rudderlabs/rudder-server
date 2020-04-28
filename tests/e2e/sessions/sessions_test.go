package sessions_test

import (
	"database/sql"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/tests/helpers"
)

var dbHandle *sql.DB
var gatewayDBPrefix string
var routerDBPrefix string
var dbPollFreqInS int = 1
var gatewayDBCheckBufferInS int = 2

var _ = BeforeSuite(func() {
	var err error
	psqlInfo := jobsdb.GetConnectionString()
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	gatewayDBPrefix = config.GetString("Gateway.CustomVal", "GW")
	routerDBPrefix = config.GetString("Router.CustomVal", "RT")
})

var _ = Describe("E2E", func() {
	Context("With user sessions processing", func() {
		It("verify event is processed only after sessionInactivityThresholdInS", func() {
			initGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			initialRouterJobsCount := helpers.GetJobsCount(dbHandle, routerDBPrefix)

			helpers.SendEventRequest(helpers.EventOptsT{})

			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initGatewayJobsCount + 1))
			Consistently(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, config.GetInt("Processor.sessionInactivityThresholdInS", 10)-1, dbPollFreqInS).Should(Equal(initialRouterJobsCount))
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, config.GetInt("Processor.sessionInactivityThresholdInS", 10)+1, dbPollFreqInS).Should(Equal(initialRouterJobsCount + 1))
		})

		It("verify events are sessioned as per sessionInactivityThresholdInS", func() {
			initialRouterJobsCount := helpers.GetJobsCount(dbHandle, routerDBPrefix)

			totalEvents := 5 // should be less than transformBatchSize
			for i := 1; i <= totalEvents; i++ {
				helpers.SendEventRequest(helpers.EventOptsT{})
			}
			time.AfterFunc(time.Duration(config.GetInt("Processor.sessionInactivityThresholdInS", 10))*time.Second, func() {
				helpers.SendEventRequest(helpers.EventOptsT{})
			})

			Consistently(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, config.GetInt("Processor.sessionInactivityThresholdInS", 10)-1, dbPollFreqInS).Should(Equal(initialRouterJobsCount))

			// verify that first totalEvents events are batched into one session
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, config.GetInt("Processor.sessionInactivityThresholdInS", 10)+2, dbPollFreqInS).Should(Equal(initialRouterJobsCount + totalEvents))

			time.AfterFunc(time.Duration(config.GetInt("Processor.sessionInactivityThresholdInS", 10))*time.Second, func() {
				Consistently(func() int {
					return helpers.GetJobsCount(dbHandle, routerDBPrefix)
				}, config.GetInt("Processor.sessionInactivityThresholdInS", 10)-1, dbPollFreqInS).Should(Equal(initialRouterJobsCount + totalEvents))

				// and next request after sessionThreshold has passes is in next session
				Eventually(func() int {
					return helpers.GetJobsCount(dbHandle, routerDBPrefix)
				}, config.GetInt("Processor.sessionInactivityThresholdInS", 10)+2, dbPollFreqInS).Should(Equal(initialRouterJobsCount + totalEvents + 1))
			})

		})

		XIt("verify events are sessioned as per event time rather than server req time", func() {

		})
	})
})
