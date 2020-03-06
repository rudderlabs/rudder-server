package e2e_test

import (
	"database/sql"
	"encoding/json"
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

		It("verify event stored in gateway 1. has right sourceId and writeKey, 2. enhanced with messageId, anonymousId, requestIP and receivedAt fields", func() {

			eventTypeMap := []string{"BATCH", "IDENTIFY", "GROUP", "TRACK", "SCREEN", "PAGE", "ALIAS"}
			for _, eventType := range eventTypeMap {
				time.Sleep(time.Second)
				initGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)

				//Source with WriteKey: 1Yc6YbOGg6U2E8rlj97ZdOawPyr has one S3 and one GA as destinations.
				//Source id for the above writekey is 1Yc6YceKLOcUYk8je9B0GQ65mmL
				switch eventType {
				case "BATCH":
					helpers.SendRequestToBatch("1Yc6YbOGg6U2E8rlj97ZdOawPyr", helpers.BatchWithoutMessageIdAndWithoutAnonymousId)
				case "IDENTIFY":
					helpers.SendRequestToIdentify("1Yc6YbOGg6U2E8rlj97ZdOawPyr", helpers.IdentifyWithoutMessageIdAndWithoutAnonymousId)
				case "GROUP":
					helpers.SendRequestToGroup("1Yc6YbOGg6U2E8rlj97ZdOawPyr", helpers.GroupWithoutMessageIdAndWithoutAnonymousId)
				case "TRACK":
					helpers.SendRequestToTrack("1Yc6YbOGg6U2E8rlj97ZdOawPyr", helpers.TrackWithoutMessageIdAndWithoutAnonymousId)
				case "SCREEN":
					helpers.SendRequestToScreen("1Yc6YbOGg6U2E8rlj97ZdOawPyr", helpers.ScreenWithoutMessageIdAndWithoutAnonymousId)
				case "PAGE":
					helpers.SendRequestToPage("1Yc6YbOGg6U2E8rlj97ZdOawPyr", helpers.PageWithoutMessageIdAndWithoutAnonymousId)
				case "ALIAS":
					helpers.SendRequestToAlias("1Yc6YbOGg6U2E8rlj97ZdOawPyr", helpers.AliasWithoutMessageIdAndWithoutAnonymousId)

				}

				Eventually(func() int {
					return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
				}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initGatewayJobsCount + 1))
				Eventually(func() map[string]bool {
					jobs := helpers.GetJobs(dbHandle, gatewayDBPrefix, 1)
					var sourceId, messageId, anonymousId, writeKey, requestIP, receivedAt string
					for _, job := range jobs {
						sourceId = gjson.GetBytes(job.Parameters, "source_id").String()
						messageId = gjson.GetBytes(job.EventPayload, "batch.0.messageId").String()
						anonymousId = gjson.GetBytes(job.EventPayload, "batch.0.anonymousId").String()
						writeKey = gjson.GetBytes(job.EventPayload, "writeKey").String()
						requestIP = gjson.GetBytes(job.EventPayload, "requestIP").String()
						receivedAt = gjson.GetBytes(job.EventPayload, "receivedAt").String()
					}
					return map[string]bool{sourceId: true,
						"messageId":   len(messageId) > 0,
						"anonymousId": len(anonymousId) > 0,
						writeKey:      true,
						"requestIP":   len(requestIP) > 0,
						"receivedAt":  len(receivedAt) > 0}
				}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(map[string]bool{"1Yc6YceKLOcUYk8je9B0GQ65mmL": true,
					"messageId":                   true,
					"anonymousId":                 true,
					"1Yc6YbOGg6U2E8rlj97ZdOawPyr": true,
					"requestIP":                   true,
					"receivedAt":                  true}))
			}
		})

		It("verify health, version endpoint", func() {

			endPoints := []string{"HEALTH", "VERSION"}
			for _, endPoint := range endPoints {
				Eventually(func() bool {
					var resp []byte
					switch endPoint {
					case "HEALTH":
						resp = helpers.SendHealthRequest()
					case "VERSION":
						resp = helpers.SendVersionRequest()
					}

					// a map container to decode the JSON structure into
					c := make(map[string]interface{})

					// unmarschal JSON
					e := json.Unmarshal(resp, &c)
					if e != nil {
						panic(e)
					}

					// a string slice to hold the keys
					k := make([]string, len(c))

					i := 0
					// copy c's keys into k
					for s, _ := range c {
						k[i] = s
						i++
					}

					switch endPoint {
					case "HEALTH":
						return helpers.SameStringSlice([]string{"server", "db", "acceptingEvents", "routingEvents", "mode", "goroutines"}, k)
					case "VERSION":
						return helpers.SameStringSlice([]string{"BuildDate", "BuiltBy", "Commit", "GitUrl", "Major", "Minor", "Patch", "Version"}, k)
					}

					return false
				}, 2, dbPollFreqInS).Should(Equal(true))
			}
		})

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
