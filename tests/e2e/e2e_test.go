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
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
)

var dbHandle *sql.DB
var gatewayDBPrefix string
var routerDBPrefix string
var dbPollFreqInS int = 1
var gatewayDBCheckBufferInS int = 15
var jobSuccessStatus string = "succeeded"

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
			initialRouterJobsCount := helpers.GetJobsCount(dbHandle, routerDBPrefix)
			//initialRouterJobStatusCount := helpers.GetJobStatusCount(dbHandle, jobSuccessStatus, routerDBPrefix)

			//Source with WriteKey: 1Yc6YbOGg6U2E8rlj97ZdOawPyr has one S3 and one GA as destinations.
			helpers.SendEventRequest(helpers.EventOptsT{
				WriteKey: "1Yc6YbOGg6U2E8rlj97ZdOawPyr",
			})

			// wait for some seconds for events to be processed by gateway
			time.Sleep(6 * time.Second)
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initGatewayJobsCount + 1))
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initialRouterJobsCount + 1))
			/*
				Commenting checking succeeded job state check to remove dependency on destination.
				// also check jobstatus records are created with 'succeeded' status
				Eventually(func() int {
					return helpers.GetJobStatusCount(dbHandle, jobSuccessStatus, routerDBPrefix)
				}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initialRouterJobStatusCount + 1))*/
		})

		//Source with WriteKey: 1YcF00dWZXGjWpSIkfFnbGuI6OI has one GA and one AMPLITUDE as destinations.
		It("should create router job for both GA and AM for single event request", func() {
			initialRouterJobsCount := helpers.GetJobsCount(dbHandle, routerDBPrefix)
			//initialRouterJobStatusCount := helpers.GetJobStatusCount(dbHandle, jobSuccessStatus, routerDBPrefix)
			helpers.SendEventRequest(helpers.EventOptsT{
				WriteKey: "1YcF00dWZXGjWpSIkfFnbGuI6OI",
			})
			// wait for some seconds for events to be processed by gateway
			time.Sleep(6 * time.Second)
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initialRouterJobsCount + 2))
			/*
				Commenting checking succeeded job state check to remove dependency on destination.
				// also check jobstatus records are created with 'succeeded' status
				Eventually(func() int {
					return helpers.GetJobStatusCount(dbHandle, jobSuccessStatus, routerDBPrefix)
				}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initialRouterJobStatusCount + 2))*/
			Eventually(func() []string {
				jobs := helpers.GetJobs(dbHandle, routerDBPrefix, 2)
				customVals := []string{}
				for _, job := range jobs {
					customVals = append(customVals, job.CustomVal)
				}
				return customVals
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(SatisfyAny(Or(BeEquivalentTo([]string{"GA", "AM"}), BeEquivalentTo([]string{"AM", "GA"}))))
		})

		It("should not create job with invalid write key", func() {
			initGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			helpers.SendEventRequest(helpers.EventOptsT{
				WriteKey: "invalid_write_key",
			})
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			}, 2, dbPollFreqInS).Should(Equal(initGatewayJobsCount))
		})

		It("should maintain order of events", func() {
			initialRouterJobsCount := helpers.GetJobsCount(dbHandle, routerDBPrefix)
			numOfTestEvents := 100
			for i := 1; i <= numOfTestEvents; i++ {
				helpers.SendEventRequest(helpers.EventOptsT{
					GaVal: i,
				})
			}
			// wait for some seconds for events to be processed by gateway
			time.Sleep(6 * time.Second)
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, routerDBPrefix)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initialRouterJobsCount + numOfTestEvents))
			jobs := helpers.GetJobs(dbHandle, routerDBPrefix, numOfTestEvents)
			for index, _ := range jobs {
				if index == 0 {
					continue
				}
				result1, _ := strconv.Atoi(gjson.Get(string(jobs[index].EventPayload), "params.ev").String())
				result2, _ := strconv.Atoi(gjson.Get(string(jobs[index-1].EventPayload), "params.ev").String())
				Expect(result1).Should(BeNumerically("<", result2))
			}
		})

		It("should dedup duplicate events", func() {
			sampleID := uuid.NewV4().String()

			initGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			helpers.SendEventRequest(helpers.EventOptsT{
				MessageID: sampleID,
			})
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initGatewayJobsCount + 1))

			// send 2 events and verify event with prev messageID is dropped
			currentGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			helpers.SendEventRequest(helpers.EventOptsT{
				MessageID: sampleID,
			})
			helpers.SendEventRequest(helpers.EventOptsT{
				MessageID: uuid.NewV4().String(),
			})
			Consistently(func() int {
				return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(currentGatewayJobsCount + 1))
		})

		It("should dedup duplicate events only till specified TTL", func() {
			sampleID := uuid.NewV4().String()

			initGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			helpers.SendEventRequest(helpers.EventOptsT{
				MessageID: sampleID,
			})
			helpers.SendEventRequest(helpers.EventOptsT{
				MessageID: sampleID,
			})
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initGatewayJobsCount + 1))

			// send 2 events and verify event with prev messageID is dropped
			currentGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			// wait for 5 seconds for messageID to exceed its TTL
			time.Sleep(5 * time.Second)
			helpers.SendEventRequest(helpers.EventOptsT{
				MessageID: sampleID,
			})
			Eventually(func() int {
				return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(currentGatewayJobsCount + 1))

		})

	})

})
