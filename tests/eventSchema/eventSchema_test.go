package eventSchema_test

import (
	"database/sql"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/tests/helpers"
)

var dbHandle *sql.DB
var dbPollFreqInS int = 1
var gatewayDBCheckBufferInS int = 10

var _ = BeforeSuite(func() {
	var err error
	psqlInfo := helpers.GetConnectionString()
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
})

var _ = Describe("EventSchema", func() {

	Context("Without user sessions processing", func() {

		It("verify event schema is uploaded only when eventUpload toggle is turned on", func() {
			sourceID := "1Yc6YceKLOcUYk8je9B0GQ65mmL"
			initEventSchemaCount := helpers.FetchEventSchemaCount(sourceID)

			helpers.ToggleEventUpload(sourceID, true)
			// helpers.ToggleEventUpload("1Z1o0HK5ya0ngCRMKMEbBphMmS4", true)
			time.Sleep(6 * time.Second) // Waiting for workspace config to be updated
			helpers.SendEventRequest(helpers.EventOptsT{
				WriteKey: "1Yc6YbOGg6U2E8rlj97ZdOawPyr",
			})

			Eventually(func() int {
				return helpers.FetchEventSchemaCount(sourceID)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initEventSchemaCount + 1))
			helpers.ToggleEventUpload(sourceID, false)
			time.Sleep(6 * time.Second) // Waiting for workspace config to be updated
			helpers.SendEventRequest(helpers.EventOptsT{
				WriteKey: "1Yc6YbOGg6U2E8rlj97ZdOawPyr",
			})
			Consistently(func() int {
				return helpers.FetchEventSchemaCount(sourceID)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initEventSchemaCount + 1)) // Should not increase further after eventUpload toggled to false

		})
	})
})
