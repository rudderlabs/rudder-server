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
		It("verify event is processed only after sessionThresholdInS", func() {
			initGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			initRouterobsCount := helpers.GetJobsCount(dbHandle, routerDBPrefix)

			helpers.SendEventRequest(helpers.EventOpts{})

			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			}, 5, 1).Should(Equal(initGatewayJobsCount + 1))
			Consistently(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, config.GetInt("Processor.sessionThresholdInS", 10)-1, 1).Should(Equal(initRouterobsCount))
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, config.GetInt("Processor.sessionThresholdInS", 10)+1, 1).Should(Equal(initRouterobsCount + 1))
		})

		It("verify events are sessioned as per sessionThresholdInS", func() {
			initRouterobsCount := helpers.GetJobsCount(dbHandle, routerDBPrefix)

			for i := 1; i <= 100; i++ {
				helpers.SendEventRequest(helpers.EventOpts{})
			}
			time.AfterFunc(time.Duration(config.GetInt("Processor.sessionThresholdInS", 10))*time.Second, func() {
				helpers.SendEventRequest(helpers.EventOpts{})
			})

			// verify that first 100 events are batched into one session
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, config.GetInt("Processor.sessionThresholdInS", 10)+2, 1).Should(Equal(initRouterobsCount + 100))

			// and next request after sessionThreshold has passes is in next session
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, config.GetInt("Processor.sessionThresholdInS", 10)*2+1, 1).Should(Equal(initRouterobsCount + 101))
		})

		XIt("verify events are sessioned as per event time rather than server req time", func() {

		})
	})
})
