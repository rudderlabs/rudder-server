package e2e_test

import (
	"database/sql"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/tests/helpers"
	"github.com/tidwall/gjson"
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

	Context("Without user sessions processing", func() {

		It("verify event is stored in both gateway and router db", func() {
			initGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			initRouterobsCount := helpers.GetJobsCount(dbHandle, routerDBPrefix)

			helpers.SendEventRequest(helpers.EventOpts{})

			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			}, 5, 1).Should(Equal(initGatewayJobsCount + 1))
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, 5, 1).Should(Equal(initRouterobsCount + 1))
		})

		It("should create router job for both GA and AM for single event request", func() {
			initRouterobsCount := helpers.GetJobsCount(dbHandle, routerDBPrefix)
			helpers.SendEventRequest(helpers.EventOpts{
				Integrations: map[string]bool{"All": true},
			})
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, 5, 1).Should(Equal(initRouterobsCount + 2))
		})

		It("should not create job with invalid write key", func() {
			initGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			helpers.SendEventRequest(helpers.EventOpts{
				WriteKey: "invalid_write_key",
			})
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			}, 2, 1).Should(Equal(initGatewayJobsCount))
		})

		It("should maintain order of events", func() {
			for i := 1; i <= 100; i++ {
				helpers.SendEventRequest(helpers.EventOpts{
					GaVal: i,
				})
			}
			time.Sleep(2 * time.Second)
			jobs := helpers.GetJobs(dbHandle, routerDBPrefix, 100)
			for index, _ := range jobs {
				if index == 0 {
					continue
				}
				result1, _ := strconv.Atoi(gjson.Get(string(jobs[index].EventPayload), "payload.ev").String())
				result2, _ := strconv.Atoi(gjson.Get(string(jobs[index-1].EventPayload), "payload.ev").String())
				if result1 > result2 {
					Expect(1).To(Equal(2))
				}
			}
		})

	})

})
